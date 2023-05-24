use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::error::ArrowError;
use datafusion::common::Result as DFResult;
use datafusion::datasource::TableProvider;
use datafusion::error::DataFusionError;
use datafusion::execution::context::SessionState;
use datafusion::logical_expr::logical_plan::AggWithGrouping;
use datafusion::physical_plan::empty::EmptyExec;
use datafusion::physical_plan::{project_schema, ExecutionPlan};
use datafusion::prelude::{col, lit_timestamp_nano, Expr};
use models::schema::Watermark;
use spi::query::datasource::stream::StreamProvider;
use trace::debug;

use crate::data_source::batch::tskv::ClusterTable;
use crate::extension::expr::expr_fn::{ge, lt};

pub struct TskvStreamProvider {
    watermark: Watermark,
    table: Arc<ClusterTable>,
    used_schema: SchemaRef,
}

impl TskvStreamProvider {
    pub fn new(watermark: Watermark, table: Arc<ClusterTable>, used_schema: SchemaRef) -> Self {
        Self {
            watermark,
            table,
            used_schema,
        }
    }

    fn construct_filters(&self, range: &(Option<i64>, i64), filters: &[Expr]) -> Vec<Expr> {
        let (start, end) = range;

        let col = col(&self.watermark.column);

        // time < end
        let lt_expr = lt(col.clone(), lit_timestamp_nano(*end));

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
        final_expr
    }
}

#[async_trait]
impl StreamProvider for TskvStreamProvider {
    type Offset = i64;

    fn id(&self) -> String {
        let schema = self.table.table_schema();
        format!("{}.{}.{}", schema.tenant, schema.db, schema.name)
    }

    /// Event time column of stream table
    fn watermark(&self) -> &Watermark {
        &self.watermark
    }

    /// Returns the latest (highest) available offsets
    async fn latest_available_offset(&self) -> DFResult<Option<Self::Offset>> {
        // TODO 从tskv获取最新的offset
        // 当前使用最大时间作为offset会导致空转浪费资源
        // let offset = Local::now().naive_utc().timestamp_nanos();
        // Ok(Some(offset))
        Ok(Some(Self::Offset::MAX))
    }

    async fn scan(
        &self,
        state: &SessionState,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        agg_with_grouping: Option<&AggWithGrouping>,
        range: Option<&(Option<Self::Offset>, Self::Offset)>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        if let Some(_agg_with_grouping) = agg_with_grouping {
            debug!("Create aggregate filter tskv scan.");
            return Err(DataFusionError::NotImplemented(
                "TskvStreamProvider::create_reader_factory with agg_with_grouping".to_string(),
            ));
        }

        let projected_schema = project_schema(&self.used_schema, projection)?;
        let source_schema = self.table.schema();
        let new_projection = projected_schema
            .fields()
            .iter()
            .map(|f| source_schema.index_of(f.name()))
            .collect::<Result<Vec<_>, ArrowError>>()?;

        let filters = if let Some(range) = range {
            self.construct_filters(range, filters)
        } else {
            // offset range为None,直接返回空Scan
            return Ok(Arc::new(EmptyExec::new(false, projected_schema)));
        };

        self.table
            .scan(
                state,
                Some(&new_projection),
                &filters,
                agg_with_grouping,
                None,
            )
            .await
    }

    /// Informs the source that stream has completed processing all data for offsets less than or
    /// equal to `end` and will only request offsets greater than `end` in the future.
    async fn commit(&self, end: Self::Offset) -> DFResult<()> {
        // TODO
        debug!("Stream source commit offset: {end}");
        Ok(())
    }

    fn schema(&self) -> SchemaRef {
        self.used_schema.clone()
    }
}
