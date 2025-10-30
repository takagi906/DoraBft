use std::collections::{BTreeMap, HashMap, VecDeque};
// Copyright (c) 2021, Facebook, Inc. and its affiliates
// Copyright (c) 2022, Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0
use crate::metrics::WorkerMetrics;
#[cfg(feature = "trace_transaction")]
use byteorder::{BigEndian, ReadBytesExt};
use config::{Committee, SharedWorkerCache, WorkerId};
use crypto::{NetworkPublicKey, PublicKey};
use network::{P2pNetwork, ReliableNetwork};
#[cfg(feature = "benchmark")]
use std::convert::TryInto;
use std::sync::Arc;
use tokio::sync::{Mutex, Notify};
use tokio::{
    sync::watch,
    task::JoinHandle,
    time::{sleep, Duration, Instant},
};
use tracing::error;
use types::{
    error::DagError,
    metered_channel::{Receiver, Sender},
    Batch, LoadBatch, LoadTransaction, ReconfigureNotification, Transaction, WorkerMessage,
};
use uuid::{uuid, Uuid};

fn gen_uuid_string() -> String {
    Uuid::new_v4().to_string()
}

fn gen_uuid_u128() -> u128 {
    Uuid::new_v4().as_u128()
}

#[cfg(test)]
#[path = "tests/batch_maker_tests.rs"]
pub mod batch_maker_tests;

/// Assemble clients transactions into batches.
pub struct BatchMaker {
    /// The public key of this authority.
    name: PublicKey,
    /// The id of this worker.
    id: WorkerId,
    /// The committee information.
    committee: Committee,
    /// The preferred batch size (in bytes).
    batch_size: usize,
    /// The maximum delay after which to seal the batch.
    max_batch_delay: Duration,
    /// Receive reconfiguration updates.
    rx_reconfigure: watch::Receiver<ReconfigureNotification>,
    /// Channel to receive transactions from the network.
    // rx_transaction: Receiver<Transaction>,
    /// Output channel to deliver sealed batches to the `QuorumWaiter`.
    tx_message: Sender<Batch>,
    /// Holds the current batch.
    current_batch: Batch,
    /// Holds the size of the current batch (in bytes).
    current_batch_size: usize,
    /// Metrics handler
    node_metrics: Arc<WorkerMetrics>,

    tx_pool: Arc<TransactionPool>,

    network: P2pNetwork,

    worker_cache: SharedWorkerCache,

    is_send: bool,
}

// 简单的异步交易池（支持 push / pop_front / len）
pub struct TransactionPool {
    inner: Mutex<BTreeMap<String, Transaction>>,
    notify: Notify,
    prefix: String,
}

impl TransactionPool {
    pub fn new<N: Into<String>>(name: N) -> Self {
        Self {
            inner: Mutex::new(BTreeMap::new()),
            notify: Notify::new(),
            prefix: name.into(),
        }
    }

    pub async fn push_our_batch(&self, tx: Transaction, id: u128) {
        let key = format!("{}:{}", self.prefix, id);
        let mut guard = self.inner.lock().await;
        guard.insert(key, tx);
        drop(guard);
        self.notify.notify_one();
    }

    pub async fn push_others_batch(&self, tx: Transaction, key: String) {
        let mut guard = self.inner.lock().await;
        guard.insert(key, tx);
        drop(guard);
        self.notify.notify_one();
    }

    /// 异步弹出队首元素（若为空则等待通知）
    pub async fn pop_front(&self) -> Option<(String, Transaction)> {
        loop {
            // 尝试取出
            {
                let mut guard = self.inner.lock().await;
                if let Some((key, tx)) = guard.pop_first() {
                    return Some((key, tx));
                }
            }
            // 等待直到有新事务被 push
            self.notify.notified().await;
        }
    }

    // 新增：按 key 弹出指定交易（存在则返回）
    pub async fn pop_by_id(&self, key: String) -> Option<Transaction> {
        let mut guard = self.inner.lock().await;
        guard.remove(&key)
    }

    pub async fn len(&self) -> usize {
        let guard = self.inner.lock().await;
        guard.len()
    }

    pub async fn pop_count(&self, count: usize) -> Vec<(String, Transaction)> {
        // 先把 total 个交易取出来（保留所有权）
        let mut res = Vec::with_capacity(count);
        {
            let mut guard = self.inner.lock().await;
            for _ in 0..count {
                if let Some((key, tx)) = guard.pop_first() {
                    res.push((key, tx));
                } else {
                    break;
                }
            }
        }
        res
    }
}

impl BatchMaker {
    #[must_use]
    pub fn spawn(
        name: PublicKey,
        id: WorkerId,
        committee: Committee,
        batch_size: usize,
        max_batch_delay: Duration,
        rx_reconfigure: watch::Receiver<ReconfigureNotification>,
        rx_transaction: Receiver<Transaction>,
        rx_unload_batch: Receiver<LoadBatch>,
        tx_message: Sender<Batch>,
        node_metrics: Arc<WorkerMetrics>,
        network: P2pNetwork,
        worker_cache: SharedWorkerCache,
    ) -> JoinHandle<()> {
        tokio::spawn(async move {
            let pool = Arc::new(TransactionPool::new(gen_uuid_string()));
            let mut tx_id = 1;
            {
                let fwd_pool = pool.clone();
                tokio::spawn(async move {
                    let mut rx = rx_transaction;
                    while let Some(tx) = rx.recv().await {
                        fwd_pool.push_our_batch(tx, tx_id).await;
                        tx_id += 1;
                        let size = fwd_pool.len().await;
                        tracing::info!("收到交易，当前池中交易数 = {}", size);
                    }
                });
            }
            {
                let fwd_pool = pool.clone();
                tokio::spawn(async move {
                    let mut rx = rx_unload_batch;
                    while let Some(tx) = rx.recv().await {
                        for load_tx in tx.0.into_iter() {
                            tracing::info!("收到其他节点的交易，交易的key = {:?}", load_tx.clone());
                            fwd_pool.push_others_batch(load_tx.tx, load_tx.key).await;
                        }
                    }
                });
            }
            Self {
                name,
                id,
                committee,
                batch_size,
                max_batch_delay,
                rx_reconfigure,
                tx_pool: pool,
                tx_message,
                current_batch: Batch(Vec::with_capacity(batch_size * 2)),
                current_batch_size: 0,
                node_metrics,
                network,
                worker_cache,
                is_send: false,
            }
            .run()
            .await;
        })
    }

    /// Main loop receiving incoming transactions and creating batches.
    async fn run(&mut self) {
        let timer = sleep(self.max_batch_delay);
        tokio::pin!(timer);

        let workers: Vec<_> = self
            .worker_cache
            .load()
            .others_workers(&self.name.clone(), &self.id)
            .into_iter()
            .map(|(name, info)| (name, info.name))
            .collect();
        let (primary_names, worker_names): (Vec<PublicKey>, Vec<NetworkPublicKey>) =
            workers.into_iter().unzip();
        let mut ratios: HashMap<PublicKey, f32> = HashMap::new();
        for pk in primary_names {
            ratios.insert(pk, 0.25f32);
        }
        loop {
            tokio::select! {
                // Assemble client transactions into batches of preset size.
               opt = self.tx_pool.pop_front() => {
                    if let Some((id, transaction)) = opt {
                        self.current_batch_size += transaction.len();
                        self.current_batch.0.push(transaction);
                        if self.tx_pool.len().await>100 && !self.is_send{
                            self.is_send = true;
                            self.send_parts_by_ratio(ratios.clone()).await;
                        }
                        if self.current_batch_size >= self.batch_size {
                            error!("Batch size reached: {}", self.current_batch_size);
                            self.seal(false).await;
                            timer.as_mut().reset(Instant::now() + self.max_batch_delay);
                        }
                    }
                },

                // If the timer triggers, seal the batch even if it contains few transactions.
                () = &mut timer => {
                    if !self.current_batch.0.is_empty() {
                        self.seal(true).await;
                    }
                    timer.as_mut().reset(Instant::now() + self.max_batch_delay);
                }

                // Trigger reconfigure.
                result = self.rx_reconfigure.changed() => {
                    result.expect("Committee channel dropped");
                    let message = self.rx_reconfigure.borrow().clone();
                    match message {
                        ReconfigureNotification::NewEpoch(new_committee) => {
                            self.committee = new_committee;
                        },
                        ReconfigureNotification::UpdateCommittee(new_committee) => {
                            self.committee = new_committee;

                        },
                        ReconfigureNotification::Shutdown => return
                    }
                    tracing::debug!("Committee updated to {}", self.committee);
                }
            }

            // Give the change to schedule other tasks.
            tokio::task::yield_now().await;
        }
    }

    /// Seal and broadcast the current batch.
    async fn seal(&mut self, timeout: bool) {
        let size = self.current_batch_size;

        // Serialize the batch.
        self.current_batch_size = 0;
        let batch: Batch = Batch(self.current_batch.0.drain(..).collect());

        #[cfg(feature = "benchmark")]
        {
            use fastcrypto::Hash;
            let digest = batch.digest();

            // Look for sample txs (they all start with 0) and gather their txs id (the next 8 bytes).
            let tx_ids: Vec<_> = batch
                .0
                .iter()
                .filter(|tx| tx[0] == 0u8 && tx.len() > 8)
                .filter_map(|tx| tx[1..9].try_into().ok())
                .collect();

            for id in tx_ids {
                // NOTE: This log entry is used to compute performance.
                tracing::info!(
                    "Batch {:?} contains sample tx {}",
                    digest,
                    u64::from_be_bytes(id)
                );
            }

            #[cfg(feature = "trace_transaction")]
            {
                // The first 8 bytes of each transaction message is reserved for an identifier
                // that's useful for debugging and tracking the lifetime of messages between
                // Narwhal and clients.
                let tracking_ids: Vec<_> = batch
                    .0
                    .iter()
                    .map(|tx| {
                        let len = tx.len();
                        if len >= 8 {
                            (&tx[0..8]).read_u64::<BigEndian>().unwrap_or_default()
                        } else {
                            0
                        }
                    })
                    .collect();
                tracing::debug!(
                    "Tracking IDs of transactions in the Batch {:?}: {:?}",
                    digest,
                    tracking_ids
                );
            }

            // NOTE: This log entry is used to compute performance.
            tracing::info!("Batch {:?} contains {} B", digest, size);
        }

        let reason = if timeout { "timeout" } else { "size_reached" };

        self.node_metrics
            .created_batch_size
            .with_label_values(&[self.committee.epoch.to_string().as_str(), reason])
            .observe(size as f64);

        // Send the batch through the deliver channel for further processing.
        if self.tx_message.send(batch).await.is_err() {
            tracing::debug!("{}", DagError::ShuttingDown);
        }
    }

    pub async fn send_parts_by_ratio(&mut self, ratios: HashMap<PublicKey, f32>) {
        let mut entries: Vec<(PublicKey, f32)> = ratios.into_iter().map(|(k, v)| (k, v)).collect();
        let size = self.tx_pool.len().await;
        let worker_map: HashMap<PublicKey, NetworkPublicKey> = self
            .worker_cache
            .load()
            .others_workers(&self.name, &self.id)
            .into_iter()
            .map(|(pk, info)| (pk, info.name))
            .collect();
        for (peer, ratio) in entries.iter_mut() {
            if peer == &self.name {
                continue;
            }
            let count = (size as f32 * (*ratio)) as usize;
            let part = self.tx_pool.pop_count(count).await;
            let load_txs: Vec<LoadTransaction> = part
                .into_iter()
                .map(|(key, tx)| LoadTransaction { key, tx })
                .collect();
            if let Some(worker_name) = worker_map.get(peer).cloned() {
                tracing::info!(
                    "卸载 {} 个交易到 peer={:?}",
                    load_txs.clone().len(),
                    worker_name,
                );
                let message = WorkerMessage::UnloadBatch(LoadBatch(load_txs));

                let handler = self.network.send(worker_name, &message).await;
                if let Err(e) = handler.await {
                    tracing::error!("send UnloadBatch failed: {:?}", e);
                }
            } else {
                tracing::warn!("no NetworkPublicKey for peer {:?}", peer);
            }
        }
    }
}
