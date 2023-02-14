use std::any::Any;
use std::sync::Arc;

use async_trait::async_trait;
use coordinator::service::CoordinatorRef;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::DFSchemaRef;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::context::SessionState;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use datafusion::physical_plan::{project_schema, ExecutionPlan};
use meta::error::MetaError;
use models::predicate::domain::{Predicate, PredicateRef};
use models::schema::{TskvTableSchema, TskvTableSchemaRef};

use crate::data_source::sink::tskv::TskvRecordBatchSinkProvider;
use crate::data_source::WriteExecExt;
use crate::extension::physical::plan_node::table_writer::TableWriterExec;
use crate::extension::physical::plan_node::tag_scan::TagScanExec;
use crate::tskv_exec::TskvExec;

#[derive(Clone)]
pub struct ClusterTable {
    coord: CoordinatorRef,
    schema: TskvTableSchemaRef,
}

impl ClusterTable {
    async fn create_physical_plan(
        &self,
        projection: Option<&Vec<usize>>,
        predicate: PredicateRef,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let proj_schema = self.project_schema(projection)?;

        Ok(Arc::new(TskvExec::new(
            self.schema.clone(),
            proj_schema,
            predicate,
            self.coord.clone(),
        )))
    }

    pub fn new(coord: CoordinatorRef, schema: TskvTableSchemaRef) -> Self {
        ClusterTable { coord, schema }
    }

    pub async fn tag_scan(
        &self,
        _ctx: &SessionState,
        projected_schema: &DFSchemaRef,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let filter = Arc::new(
            Predicate::default()
                .set_limit(limit)
                .push_down_filter(filters, &self.schema),
        );

        Ok(Arc::new(TagScanExec::new(
            self.schema.clone(),
            Arc::new(projected_schema.as_ref().into()),
            filter,
            self.coord.clone(),
        )))
    }

    pub fn table_schema(&self) -> TskvTableSchemaRef {
        self.schema.clone()
    }

    // Check and return the projected schema
    fn project_schema(&self, projection: Option<&Vec<usize>>) -> Result<SchemaRef> {
        valid_project(&self.schema, projection)
            .map_err(|err| DataFusionError::External(Box::new(err)))?;
        project_schema(&self.schema.to_arrow_schema(), projection)
    }
}

#[async_trait]
impl TableProvider for ClusterTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.to_arrow_schema()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &SessionState,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        // limit can be used to reduce the amount scanned
        // from the datasource as a performance optimization.
        // If set, it contains the amount of rows needed by the `LogicalPlan`,
        // The datasource should return *at least* this number of rows if available.
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let filter = Arc::new(
            Predicate::default()
                .set_limit(limit)
                .push_down_filter(filters, &self.schema),
        );

        return self.create_physical_plan(projection, filter).await;
    }
    fn supports_filter_pushdown(&self, _: &Expr) -> Result<TableProviderFilterPushDown> {
        Ok(TableProviderFilterPushDown::Inexact)
    }
}

#[async_trait]
impl WriteExecExt for ClusterTable {
    async fn write(
        &self,
        _state: &SessionState,
        input: Arc<dyn ExecutionPlan>,
    ) -> Result<Arc<TableWriterExec>> {
        let record_batch_sink_privider = Arc::new(TskvRecordBatchSinkProvider::new(
            self.coord.clone(),
            self.schema.clone(),
        ));

        Ok(Arc::new(TableWriterExec::new(
            input,
            self.schema.name.clone(),
            record_batch_sink_privider,
        )))
    }
}

/// Check the validity of the projection
///
/// 1. If the projection contains the time column, it must contain the field column, otherwise an error will be reported
pub fn valid_project(
    schema: &TskvTableSchema,
    projection: Option<&Vec<usize>>,
) -> std::result::Result<(), MetaError> {
    let mut field_count = 0;
    let mut contains_time_column = false;

    if let Some(e) = projection {
        e.iter().cloned().for_each(|idx| {
            if let Some(c) = schema.column_by_index(idx) {
                if c.column_type.is_time() {
                    contains_time_column = true;
                }
                if c.column_type.is_field() {
                    field_count += 1;
                }
            };
        });
    }

    if contains_time_column && field_count == 0 {
        return Err(MetaError::CommonError {
            msg: "If the projection contains the time column, it must contain the field column"
                .to_string(),
        });
    }

    Ok(())
}
