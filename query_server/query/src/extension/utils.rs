use std::ops::{Bound, Range};
use std::sync::Arc;

use datafusion::arrow::compute::filter_record_batch;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::cast::as_boolean_array;
use datafusion::common::Result as DFResult;
use datafusion::logical_expr::{TableSource, UserDefinedLogicalNode};
use datafusion::physical_plan::{ExecutionPlan, PhysicalExpr};

pub fn downcast_plan_node<T: 'static>(node: &dyn UserDefinedLogicalNode) -> Option<&T> {
    node.as_any().downcast_ref::<T>()
}

pub fn downcast_execution_plan<T: 'static>(plan: &dyn ExecutionPlan) -> Option<&T> {
    plan.as_any().downcast_ref::<T>()
}

pub fn downcast_table_source<T: 'static>(plan: &dyn TableSource) -> Option<&T> {
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

pub fn try_map_range<T, U, F>(tr: &Range<T>, mut f: F) -> DFResult<Range<U>>
where
    F: FnMut(&T) -> DFResult<U>,
{
    Ok(Range {
        start: f(&tr.start)?,
        end: f(&tr.end)?,
    })
}

pub fn try_map_bound<T, U, F>(bt: Bound<T>, mut f: F) -> DFResult<Bound<U>>
where
    F: FnMut(T) -> DFResult<U>,
{
    Ok(match bt {
        Bound::Excluded(t) => Bound::Excluded(f(t)?),
        Bound::Included(t) => Bound::Included(f(t)?),
        Bound::Unbounded => Bound::Unbounded,
    })
}

pub fn bound_extract<T>(b: &Bound<T>) -> Option<&T> {
    match b {
        Bound::Included(t) | Bound::Excluded(t) => Some(t),
        Bound::Unbounded => None,
    }
}
