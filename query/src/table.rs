use std::{any::Any, ops::Index, sync::Arc};

use async_trait::async_trait;
use datafusion::{
    arrow::{datatypes::SchemaRef, record_batch::RecordBatch},
    datasource::{TableProvider, TableType},
    error::Result,
    execution::context::{SessionState, TaskContext},
    logical_expr::{Expr, TableProviderFilterPushDown},
    physical_expr::PhysicalSortExpr,
    physical_plan::{
        memory::MemoryStream, project_schema, ExecutionPlan, Partitioning,
        SendableRecordBatchStream, Statistics,
    },
    prelude::col,
};

use crate::{
    helper::expr_applicable_for_cols,
    predicate::{Predicate, PredicateRef},
    stream,
    stream::TableScanStream,
};

#[derive(Debug, Clone)]
struct TsmExec {
    // connection
    // db: CustomDataSource,
    proj_schema: SchemaRef,
    filter: PredicateRef,
}

impl TsmExec {
    fn new(proj_schema: SchemaRef, filter: PredicateRef) -> Self {
        Self { proj_schema, filter }
    }
    pub fn filter(&self) -> PredicateRef {
        self.filter.clone()
    }
}

impl ExecutionPlan for TsmExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.proj_schema.clone()
    }

    fn output_partitioning(&self) -> datafusion::physical_plan::Partitioning {
        // todo: get partition
        Partitioning::UnknownPartitioning(2)
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        None
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(self: Arc<Self>,
                         _: Vec<Arc<dyn ExecutionPlan>>)
                         -> Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(&self,
               _partition: usize,
               _context: Arc<TaskContext>)
               -> Result<SendableRecordBatchStream> {
        // TODO 实现一个stream 去远端读文件，文件批量返回
        Ok(Box::pin(TableScanStream::try_new(self.schema(), self.filter())))
    }

    fn statistics(&self) -> Statistics {
        todo!()
    }
}

pub struct ClusterTable {}

impl ClusterTable {
    pub(crate) async fn create_physical_plan(&self,
                                             projections: &Option<Vec<usize>>,
                                             predicate: Arc<Predicate>,
                                             schema: SchemaRef)
                                             -> Result<Arc<dyn ExecutionPlan>> {
        let proj_schema = project_schema(&schema, projections.as_ref()).unwrap();
        Ok(Arc::new(TsmExec::new(proj_schema, predicate.clone())))
    }
}

#[async_trait]
impl TableProvider for ClusterTable {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        todo!()
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
        // TODO: get table cols
        let cols = vec!["test".to_string()];
        if expr_applicable_for_cols(&cols, filter) {
            Ok(TableProviderFilterPushDown::Exact)
        } else {
            Ok(TableProviderFilterPushDown::Inexact)
        }
    }
}

// impl ExecutionPlan for CsvExec {
// FileStream ==> TSM stream
