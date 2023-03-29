use std::sync::Arc;

use datafusion::arrow::compute::filter_record_batch;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::cast::as_boolean_array;
use datafusion::common::Result as DFResult;
use datafusion::logical_expr::UserDefinedLogicalNode;
use datafusion::physical_plan::{ExecutionPlan, PhysicalExpr};

pub fn downcast_plan_node<T: 'static>(node: &dyn UserDefinedLogicalNode) -> Option<&T> {
    node.as_any().downcast_ref::<T>()
}

pub fn downcast_execution_plan<T: 'static>(plan: &dyn ExecutionPlan) -> Option<&T> {
    plan.as_any().downcast_ref::<T>()
}

pub fn batch_filter(
    batch: &RecordBatch,
    predicate: &Arc<dyn PhysicalExpr>,
) -> DFResult<RecordBatch> {
    predicate
        .evaluate(batch)
        .map(|v| v.into_array(batch.num_rows()))
        .and_then(|array| {
            Ok(as_boolean_array(&array)?)
                // apply filter array to record batch
                .and_then(|filter_array| Ok(filter_record_batch(batch, filter_array)?))
        })
}
