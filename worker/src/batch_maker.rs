use std::collections::VecDeque;
// Copyright (c) 2021, Facebook, Inc. and its affiliates
// Copyright (c) 2022, Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0
use crate::metrics::WorkerMetrics;
#[cfg(feature = "trace_transaction")]
use byteorder::{BigEndian, ReadBytesExt};
use config::Committee;
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
    Batch, ReconfigureNotification, Transaction,
};

#[cfg(test)]
#[path = "tests/batch_maker_tests.rs"]
pub mod batch_maker_tests;

/// Assemble clients transactions into batches.
pub struct BatchMaker {
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
}

// 简单的异步交易池（支持 push / pop_front / len）
pub struct TransactionPool {
    inner: Mutex<VecDeque<Transaction>>,
    notify: Notify,
}

impl TransactionPool {
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(VecDeque::new()),
            notify: Notify::new(),
        }
    }

    pub async fn push(&self, tx: Transaction) {
        let mut guard = self.inner.lock().await;
        guard.push_back(tx);
        drop(guard);
        self.notify.notify_one();
    }

    /// 异步弹出队首元素（若为空则等待通知）
    pub async fn pop_front(&self) -> Option<Transaction> {
        loop {
            // 尝试取出
            {
                let mut guard = self.inner.lock().await;
                if let Some(tx) = guard.pop_front() {
                    return Some(tx);
                }
            }
            // 等待直到有新事务被 push
            self.notify.notified().await;
        }
    }

    pub async fn len(&self) -> usize {
        let guard = self.inner.lock().await;
        guard.len()
    }
}

impl BatchMaker {
    #[must_use]
    pub fn spawn(
        committee: Committee,
        batch_size: usize,
        max_batch_delay: Duration,
        rx_reconfigure: watch::Receiver<ReconfigureNotification>,
        rx_transaction: Receiver<Transaction>,
        tx_message: Sender<Batch>,
        node_metrics: Arc<WorkerMetrics>,
    ) -> JoinHandle<()> {
        tokio::spawn(async move {
            let pool = Arc::new(TransactionPool::new());
            {
                let fwd_pool = pool.clone();
                tokio::spawn(async move {
                    let mut rx = rx_transaction;
                    while let Some(tx) = rx.recv().await {
                        fwd_pool.push(tx).await;
                        let size = fwd_pool.len().await;
                        tracing::info!("收到交易，当前池中交易数 = {}", size);
                    }
                });
            }
            Self {
                committee,
                batch_size,
                max_batch_delay,
                rx_reconfigure,
                tx_pool: pool,
                tx_message,
                current_batch: Batch(Vec::with_capacity(batch_size * 2)),
                current_batch_size: 0,
                node_metrics,
            }
            .run()
            .await;
        })
    }

    /// Main loop receiving incoming transactions and creating batches.
    async fn run(&mut self) {
        let timer = sleep(self.max_batch_delay);
        tokio::pin!(timer);

        loop {
            tokio::select! {
                // Assemble client transactions into batches of preset size.
                transaction = self.tx_pool.pop_front() => {
                    if let Some(transaction) = transaction {
                        self.current_batch_size += transaction.len();
                        self.current_batch.0.push(transaction);
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
}
