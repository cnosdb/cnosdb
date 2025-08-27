pub mod checker;

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::error::Result;
use datafusion::execution::context::SessionState;
use datafusion::logical_expr::logical_plan::TableScanAggregate;
use datafusion::logical_expr::{TableProviderAggregationPushDown, TableProviderFilterPushDown};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::Expr;
use meta::model::MetaClientRef;
use models::schema::stream_table_schema::{StreamTable, Watermark};

use self::checker::SchemaChecker;
use crate::QueryError;

pub type StreamProviderManagerRef = Arc<StreamProviderManager>;

/// Maintain and manage all registered streaming data sources
#[derive(Default)]
pub struct StreamProviderManager {
    factories: HashMap<String, StreamProviderFactoryRef>,
}

impl StreamProviderManager {
    pub fn register_stream_provider_factory(
        &mut self,
        stream_type: impl Into<String>,
        factory: StreamProviderFactoryRef,
    ) -> Result<(), QueryError> {
        let stream_type = stream_type.into();

        if self.factories.contains_key(&stream_type) {
            return Err(QueryError::StreamSourceFactoryAlreadyExists { stream_type });
        }

        let _ = self.factories.insert(stream_type, factory);

        Ok(())
    }

    pub fn create_provider(
        &self,
        meta: MetaClientRef,
        table: &StreamTable,
    ) -> Result<StreamProviderRef, QueryError> {
        let stream_type = table.stream_type();
        self.factories
            .get(stream_type)
            .ok_or_else(|| QueryError::UnsupportedStreamType {
                stream_type: stream_type.to_string(),
            })?
            .create(meta, table)
    }
}

pub type StreamProviderFactoryRef = Arc<dyn StreamProviderFactory + Send + Sync>;

/// Each type of [`StreamTable`] corresponds to a unique [`StreamProviderFactory`]\
/// When supporting new streaming data sources, this interface needs to be implemented and registered with [`StreamProviderManager`].
pub trait StreamProviderFactory: SchemaChecker<StreamTable> {
    /// Create the corresponding [`StreamProviderRef`] according to the type of the given [`StreamTable`].\
    /// [`MetaClientRef`] is for possible stream tables associated with internal tables
    fn create(
        &self,
        meta: MetaClientRef,
        table: &StreamTable,
    ) -> Result<StreamProviderRef, QueryError>;
}

pub type Offset = i64;
pub type RangeOffset = (Option<Offset>, Offset);
pub type StreamProviderRef<T = Offset> = Arc<dyn StreamProvider<Offset = T> + Send + Sync>;

/// The table that implements this trait can be used as the source of the stream processing
#[async_trait]
pub trait StreamProvider {
    type Offset;

    fn id(&self) -> String;

    /// Event time column of stream table
    fn watermark(&self) -> &Watermark;

    /// Returns the latest (highest) available offsets
    async fn latest_available_offset(&self) -> Result<Option<Self::Offset>>;

    async fn scan(
        &self,
        state: &SessionState,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        _aggregate: Option<&TableScanAggregate>,
        range: Option<&(Option<Self::Offset>, Self::Offset)>,
    ) -> Result<Arc<dyn ExecutionPlan>>;

    /// Informs the source that stream has completed processing all data for offsets less than or
    /// equal to `end` and will only request offsets greater than `end` in the future.
    async fn commit(&self, end: Self::Offset) -> Result<()>;

    fn schema(&self) -> SchemaRef;

    /// Tests whether the table provider can make use of a filter expression
    /// to optimize data retrieval.
    fn supports_filter_pushdown(&self, _filter: &Expr) -> Result<TableProviderFilterPushDown> {
        Ok(TableProviderFilterPushDown::Unsupported)
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        filters
            .iter()
            .map(|f| self.supports_filter_pushdown(f))
            .collect()
    }

    /// true if the aggregation can be pushed down to datasource, false otherwise.
    fn supports_aggregate_pushdown(
        &self,
        _group_expr: &[Expr],
        _aggr_expr: &[Expr],
    ) -> Result<TableProviderAggregationPushDown> {
        Ok(TableProviderAggregationPushDown::Unsupported)
    }
}
