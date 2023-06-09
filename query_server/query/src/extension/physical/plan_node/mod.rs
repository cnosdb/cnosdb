use std::sync::Arc;
use std::task::Poll;

use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::DataFusionError;
use datafusion::physical_plan::metrics::{BaselineMetrics, ExecutionPlanMetricsSet, Time};
use memory_pool::{MemoryConsumer, MemoryPool, MemoryReservation};
use parking_lot::RwLock;

pub mod aggregate_filter_scan;
pub mod expand;
pub mod gapfill;
pub mod state_restore;
pub mod state_save;
pub mod table_writer;
pub mod table_writer_merge;
pub mod tag_scan;
pub mod traced_proxy;
pub mod tskv_exec;
pub mod watermark;

/// Stores metrics about the table writer execution.
#[derive(Debug)]
pub struct TableScanMetrics {
    baseline_metrics: BaselineMetrics,
    reservation: Option<RwLock<MemoryReservation>>,
}

impl TableScanMetrics {
    /// Create new metrics
    pub fn new(
        metrics: &ExecutionPlanMetricsSet,
        partition: usize,
        pool: Option<&Arc<dyn MemoryPool>>,
    ) -> Self {
        let baseline_metrics = BaselineMetrics::new(metrics, partition);
        let reservation = match pool {
            None => None,
            Some(pool) => {
                let reservation = RwLock::new(
                    MemoryConsumer::new(format!("TableScanMetrics[{partition}]")).register(pool),
                );
                Some(reservation)
            }
        };

        Self {
            baseline_metrics,
            reservation,
        }
    }

    /// return the metric for cpu time spend in this operator
    pub fn elapsed_compute(&self) -> &Time {
        self.baseline_metrics.elapsed_compute()
    }

    /// Process a poll result of a stream producing output for an
    /// operator, recording the output rows and stream done time and
    /// returning the same poll result
    pub fn record_poll(
        &self,
        poll: Poll<Option<std::result::Result<RecordBatch, DataFusionError>>>,
    ) -> Poll<Option<std::result::Result<RecordBatch, DataFusionError>>> {
        self.baseline_metrics.record_poll(poll)
    }

    pub fn record_memory(&self, rb: &RecordBatch) -> Result<(), DataFusionError> {
        if let Some(res) = &self.reservation {
            res.write().try_grow(rb.get_array_memory_size())?
        }
        Ok(())
    }

    /// Records the fact that this operator's execution is complete
    /// (recording the `end_time` metric).
    pub fn done(&self) {
        self.baseline_metrics.done()
    }
}
