use std::sync::Arc;

use async_trait::async_trait;
use chrono::Local;
use coordinator::service::CoordinatorRef;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::Result as DFResult;
use datafusion::error::DataFusionError;
use datafusion::execution::context::{SessionState, TaskContext};
use datafusion::logical_expr::logical_plan::AggWithGrouping;
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use datafusion::physical_plan::SendableRecordBatchStream;
use datafusion::prelude::{col, lit_timestamp_nano, Column, Expr};
use models::predicate::domain::{Predicate, PredicateRef};
use models::schema::TskvTableSchemaRef;
use spi::query::datasource::stream::{PartitionStream, PartitionStreamFactory, StreamProvider};
use trace::debug;
use tskv::iterator::TableScanMetrics;

use crate::extension::expr::expr_fn::{ge, lt};
use crate::extension::physical::plan_node::tskv_exec::TableScanStream;

pub struct TskvStreamProvider {
    client: CoordinatorRef,

    event_time_column: Column,
    table_schema: TskvTableSchemaRef,
    schema: SchemaRef,
}

impl TskvStreamProvider {
    pub fn new(
        client: CoordinatorRef,
        event_time_column: Column,
        table_schema: TskvTableSchemaRef,
    ) -> Self {
        Self {
            client,
            event_time_column,
            schema: table_schema.to_arrow_schema(),
            table_schema,
        }
    }
}

#[async_trait]
impl StreamProvider for TskvStreamProvider {
    type Offset = i64;

    /// Event time column of stream table
    fn event_time_column(&self) -> &Column {
        &self.event_time_column
    }

    /// Returns the latest (highest) available offsets
    async fn latest_available_offset(&self) -> DFResult<Option<Self::Offset>> {
        // TODO
        let offset = Local::now().naive_utc().timestamp_nanos();
        Ok(Some(offset))
    }

    async fn create_reader_factory(
        &self,
        _state: &SessionState,
        _projection: Option<&Vec<usize>>,
        filters: &[Expr],
        agg_with_grouping: Option<&AggWithGrouping>,
        range: (Option<Self::Offset>, Self::Offset),
    ) -> DFResult<Arc<dyn PartitionStreamFactory>> {
        let (start, end) = range;

        let col = col(self.event_time_column.clone());

        // time < end
        let lt_expr = lt(col.clone(), lit_timestamp_nano(end));

        let offset_range = start
            // time >= start
            .map(|start| ge(col, lit_timestamp_nano(start)))
            // time >= start and time < end
            .map(|e| e.and(lt_expr.clone()))
            // only time < end
            .unwrap_or(lt_expr);

        let mut final_expr = Vec::with_capacity(filters.len() + 1);
        final_expr.extend(filters.to_vec());
        final_expr.extend([offset_range]);

        let filter = Arc::new(
            Predicate::default().push_down_filter(&final_expr, self.table_schema.as_ref()),
        );

        if let Some(_agg_with_grouping) = agg_with_grouping {
            debug!("Create aggregate filter tskv scan.");
            return Err(DataFusionError::NotImplemented(
                "TskvStreamProvider::create_reader_factory with agg_with_grouping".to_string(),
            ));
        }

        Ok(Arc::new(TskvPartitionStreamFactory {
            client: self.client.clone(),
            metrics: ExecutionPlanMetricsSet::default(),
            table_schema: self.table_schema.clone(),
            filter,
        }))
    }

    /// Informs the source that stream has completed processing all data for offsets less than or
    /// equal to `end` and will only request offsets greater than `end` in the future.
    async fn commit(&self, end: Self::Offset) -> DFResult<()> {
        // TODO
        debug!("Stream source commit offset: {end}");
        Ok(())
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

struct TskvPartitionStreamFactory {
    client: CoordinatorRef,
    metrics: ExecutionPlanMetricsSet,

    table_schema: TskvTableSchemaRef,
    filter: PredicateRef,
}

#[async_trait]
impl PartitionStreamFactory for TskvPartitionStreamFactory {
    fn partition_count(&self) -> usize {
        // TODO
        1
    }

    async fn create_reader(
        &self,
        _state: &SessionState,
        _partition: usize,
    ) -> DFResult<Arc<dyn PartitionStream>> {
        Ok(Arc::new(TskvPartitionStream {
            client: self.client.clone(),
            metrics: self.metrics.clone(),
            table_schema: self.table_schema.clone(),
            arrow_schema: self.table_schema.to_arrow_schema(),
            filter: self.filter.clone(),
        }))
    }
}

struct TskvPartitionStream {
    client: CoordinatorRef,
    metrics: ExecutionPlanMetricsSet,

    table_schema: TskvTableSchemaRef,
    arrow_schema: SchemaRef,
    filter: PredicateRef,
}

impl PartitionStream for TskvPartitionStream {
    fn schema(&self) -> &SchemaRef {
        &self.arrow_schema
    }

    fn execute(&self, context: Arc<TaskContext>) -> DFResult<SendableRecordBatchStream> {
        let batch_size = context.session_config().batch_size();

        let metrics = TableScanMetrics::new(&self.metrics, 0, Some(context.memory_pool()));

        let table_stream = TableScanStream::new(
            self.table_schema.clone(),
            self.arrow_schema.clone(),
            self.client.clone(),
            self.filter.clone(),
            batch_size,
            metrics,
        )
        .map_err(|err| DataFusionError::External(Box::new(err)))?;

        Ok(Box::pin(table_stream))
    }
}
