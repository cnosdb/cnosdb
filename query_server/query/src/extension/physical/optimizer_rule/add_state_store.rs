use core::fmt::Debug;
use std::sync::Arc;

use datafusion::common::Result as DFResult;
use datafusion::config::ConfigOptions;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::aggregates::{AggregateExec, AggregateMode};
use datafusion::physical_plan::rewrite::TreeNodeRewritable;
use datafusion::physical_plan::{with_new_children_if_necessary, ExecutionPlan};

use crate::extension::physical::plan_node::state_restore::StateRestoreExec;
use crate::extension::physical::plan_node::state_save::StateSaveExec;
use crate::extension::utils::downcast_execution_plan;
use crate::stream::state_store::StateStoreFactory;

#[derive(Default)]
pub struct AddStateStore<T> {
    watermark_ns: i64,
    state_store_factory: Arc<T>,
}

impl<T> AddStateStore<T> {
    #[allow(missing_docs)]
    pub fn new(watermark_ns: i64, state_store_factory: Arc<T>) -> Self {
        Self {
            watermark_ns,
            state_store_factory,
        }
    }
}

impl<T> PhysicalOptimizerRule for AddStateStore<T>
where
    T: StateStoreFactory + Send + Sync + Debug + 'static,
    T::SS: Send + Sync + Debug,
{
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        plan.transform_up(&|plan| {
            if let Some(aggregate_exec) = downcast_execution_plan::<AggregateExec>(plan.as_ref()) {
                match aggregate_exec.mode() {
                    AggregateMode::Final | AggregateMode::FinalPartitioned => {
                        // Original plan
                        // ```
                        // ......
                        //   AggExec
                        //     ......
                        // ```
                        //
                        // Converted plan
                        // ```
                        // ......
                        // AggExec(Final)
                        //   StateSaveExec
                        //     AggExec(PartialMerge)
                        //       StateRestoreExec
                        //         ......
                        // ```
                        let state_restore_exec = Arc::new(StateRestoreExec::try_new(
                            aggregate_exec.input().clone(),
                            self.state_store_factory.clone(),
                        )?);
                        let partial_merge_agg = Arc::new(AggregateExec::try_new(
                            AggregateMode::PartialMerge,
                            aggregate_exec.group_expr().clone(),
                            aggregate_exec.aggr_expr().to_vec(),
                            state_restore_exec.clone(),
                            state_restore_exec.schema(),
                        )?);
                        let state_save_exec = Arc::new(StateSaveExec::try_new(
                            self.watermark_ns,
                            self.state_store_factory.clone(),
                            partial_merge_agg,
                        )?);
                        let new_agg_exec =
                            with_new_children_if_necessary(plan.clone(), vec![state_save_exec])?;

                        return Ok(Some(new_agg_exec));
                    }
                    _ => return Ok(None),
                }
            }

            Ok(None)
        })
    }

    fn name(&self) -> &str {
        "add_state_store"
    }

    fn schema_check(&self) -> bool {
        true
    }
}
