use std::{any::Any, sync::Arc};

use async_trait::async_trait;
use datafusion::{
    arrow::datatypes::SchemaRef,
    datasource::{TableProvider, TableType},
    error::Result,
    execution::context::SessionState,
    logical_expr::{Expr, TableProviderFilterPushDown},
    physical_plan::{project_schema, ExecutionPlan},
};

use crate::{
    helper::expr_applicable_for_cols, predicate::Predicate, schema::TableSchema,
    tskv_exec::TskvExec,
};

pub struct ClusterTable {
    schema: TableSchema,
}

impl ClusterTable {
    pub(crate) async fn create_physical_plan(&self,
                                             projections: &Option<Vec<usize>>,
                                             predicate: Arc<Predicate>,
                                             schema: SchemaRef)
                                             -> Result<Arc<dyn ExecutionPlan>> {
        let proj_schema = project_schema(&schema, projections.as_ref()).unwrap();
        Ok(Arc::new(TskvExec::new(proj_schema, predicate.clone())))
    }
}

#[async_trait]
impl TableProvider for ClusterTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.to_arrow_schema().clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(&self,
                  _ctx: &SessionState,
                  projection: &Option<Vec<usize>>,
                  filters: &[Expr],
                  limit: Option<usize>)
                  -> Result<Arc<dyn ExecutionPlan>> {
        let filter = Arc::new(Predicate::default().set_limit(limit).pushdown_exprs(filters));
        return self.create_physical_plan(projection, filter.clone(), self.schema()).await;
    }
    fn supports_filter_pushdown(&self, filter: &Expr) -> Result<TableProviderFilterPushDown> {
        let cols = vec!["test".to_string()];
        if expr_applicable_for_cols(&cols, filter) {
            Ok(TableProviderFilterPushDown::Exact)
        } else {
            Ok(TableProviderFilterPushDown::Inexact)
        }
    }
}
