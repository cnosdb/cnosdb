use futures::executor::block_on;

use std::task::Poll;

use datafusion::{
    arrow::{datatypes::SchemaRef, error::ArrowError, record_batch::RecordBatch},
    physical_plan::{
        metrics::{self, BaselineMetrics, ExecutionPlanMetricsSet, MetricBuilder},
        RecordBatchStream,
    },
};
use futures::Stream;
use models::codec::Encoding;
use models::schema::TskvTableSchemaRef;
use models::{
    predicate::domain::PredicateRef,
    schema::{ColumnType, TableColumn, TskvTableSchema, TIME_FIELD},
};

use tskv::engine::EngineRef;

use crate::iterator::{QueryOption, RowIterator};
use tskv::Error;

#[allow(dead_code)]
pub struct TableScanStream {
    proj_schema: SchemaRef,
    batch_size: usize,
    store_engine: EngineRef,

    iterator: RowIterator,

    metrics: TableScanMetrics,
}

impl TableScanStream {
    pub fn new(
        table_schema: TskvTableSchemaRef,
        proj_schema: SchemaRef,
        filter: PredicateRef,
        batch_size: usize,
        store_engine: EngineRef,
        metrics: TableScanMetrics,
    ) -> Result<Self, Error> {
        let mut proj_fileds = Vec::with_capacity(proj_schema.fields().len());
        for item in proj_schema.fields().iter() {
            let field_name = item.name();
            if field_name == TIME_FIELD {
                let encoding = match table_schema.column(TIME_FIELD) {
                    None => Encoding::Default,
                    Some(v) => v.encoding,
                };
                proj_fileds.push(TableColumn::new(
                    0,
                    TIME_FIELD.to_string(),
                    ColumnType::Time,
                    encoding,
                ));
                continue;
            }

            if let Some(v) = table_schema.column(field_name) {
                proj_fileds.push(v.clone());
            } else {
                return Err(Error::NotFoundField {
                    reason: field_name.clone(),
                });
            }
        }

        let proj_table_schema = TskvTableSchema::new(
            table_schema.db.clone(),
            table_schema.name.clone(),
            proj_fileds,
        );

        let filter = filter
            .filter()
            .translate_column(|c| proj_table_schema.column(&c.name).cloned());

        // 提取过滤条件
        let time_filter = filter.translate_column(|e| match e.column_type {
            ColumnType::Time => Some(e.name.clone()),
            _ => None,
        });
        let tags_filter = filter.translate_column(|e| match e.column_type {
            ColumnType::Tag => Some(e.name.clone()),
            _ => None,
        });
        let fields_filter = filter.translate_column(|e| match e.column_type {
            ColumnType::Field(_) => Some(e.name.clone()),
            _ => None,
        });
        let option = QueryOption {
            table_schema: proj_table_schema,
            datafusion_schema: proj_schema.clone(),
            time_filter,
            tags_filter,
            fields_filter,
        };

        let iterator = match RowIterator::new(
            metrics.tskv_metrics(),
            store_engine.clone(),
            option,
            batch_size,
        ) {
            Ok(it) => it,
            Err(err) => return Err(err),
        };

        Ok(Self {
            proj_schema,
            batch_size,
            store_engine,
            iterator,
            metrics,
        })
    }
}

impl Stream for TableScanStream {
    type Item = Result<RecordBatch, ArrowError>;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();

        let timer = this.metrics.elapsed_compute().timer();
        let res = block_on(this.iterator.next());
        let result = match res {
            Some(data) => match data {
                Ok(batch) => Poll::Ready(Some(Ok(batch))),
                Err(err) => Poll::Ready(Some(Err(ArrowError::CastError(err.to_string())))),
            },
            None => {
                this.metrics.done();
                Poll::Ready(None)
            }
        };

        timer.done();
        this.metrics.record_poll(result)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        // todo   (self.data.len(), Some(self.data.len()))
        (0, Some(0))
    }
}

impl RecordBatchStream for TableScanStream {
    fn schema(&self) -> SchemaRef {
        self.proj_schema.clone()
    }
}

/// Stores metrics about the table writer execution.
#[derive(Debug)]
pub struct TableScanMetrics {
    baseline_metrics: BaselineMetrics,

    partition: usize,
    metrics: ExecutionPlanMetricsSet,
}

impl TableScanMetrics {
    /// Create new metrics
    pub fn new(metrics: &ExecutionPlanMetricsSet, partition: usize) -> Self {
        let baseline_metrics = BaselineMetrics::new(metrics, partition);
        Self {
            baseline_metrics,
            partition,
            metrics: metrics.clone(),
        }
    }

    pub fn tskv_metrics(&self) -> TskvSourceMetrics {
        TskvSourceMetrics::new(&self.metrics.clone(), self.partition)
    }

    /// return the metric for cpu time spend in this operator
    pub fn elapsed_compute(&self) -> &metrics::Time {
        self.baseline_metrics.elapsed_compute()
    }

    /// Process a poll result of a stream producing output for an
    /// operator, recording the output rows and stream done time and
    /// returning the same poll result
    pub fn record_poll(
        &self,
        poll: Poll<Option<std::result::Result<RecordBatch, ArrowError>>>,
    ) -> Poll<Option<std::result::Result<RecordBatch, ArrowError>>> {
        self.baseline_metrics.record_poll(poll)
    }

    /// Records the fact that this operator's execution is complete
    /// (recording the `end_time` metric).
    pub fn done(&self) {
        self.baseline_metrics.done()
    }
}

/// Stores metrics about the table writer execution.
#[derive(Debug)]
pub struct TskvSourceMetrics {
    elapsed_point_to_record_batch: metrics::Time,
    elapsed_field_scan: metrics::Time,
    elapsed_series_scan: metrics::Time,
}

impl TskvSourceMetrics {
    /// Create new metrics
    pub fn new(metrics: &ExecutionPlanMetricsSet, partition: usize) -> Self {
        let elapsed_point_to_record_batch =
            MetricBuilder::new(metrics).subset_time("elapsed_point_to_record_batch", partition);

        let elapsed_field_scan =
            MetricBuilder::new(metrics).subset_time("elapsed_field_scan", partition);

        let elapsed_series_scan =
            MetricBuilder::new(metrics).subset_time("elapsed_series_scan", partition);

        Self {
            elapsed_point_to_record_batch,
            elapsed_field_scan,
            elapsed_series_scan,
        }
    }

    pub fn elapsed_point_to_record_batch(&self) -> &metrics::Time {
        &self.elapsed_point_to_record_batch
    }

    pub fn elapsed_field_scan(&self) -> &metrics::Time {
        &self.elapsed_field_scan
    }

    pub fn elapsed_series_scan(&self) -> &metrics::Time {
        &self.elapsed_series_scan
    }
}
