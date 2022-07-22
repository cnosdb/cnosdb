use std::{any::Any, sync::Arc};

use datafusion::{
    arrow::datatypes::SchemaRef,
    error::Result,
    execution::context::TaskContext,
    physical_expr::PhysicalSortExpr,
    physical_plan::{ExecutionPlan, Partitioning, SendableRecordBatchStream, Statistics},
};

use crate::{predicate::PredicateRef, stream::TableScanStream};

#[derive(Debug, Clone)]
pub struct TskvExec {
    // connection
    // db: CustomDataSource,
    proj_schema: SchemaRef,
    filter: PredicateRef,
}

impl TskvExec {
    pub(crate) fn new(proj_schema: SchemaRef, filter: PredicateRef) -> Self {
        Self { proj_schema, filter }
    }
    pub fn filter(&self) -> PredicateRef {
        self.filter.clone()
    }
}

impl ExecutionPlan for TskvExec {
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
