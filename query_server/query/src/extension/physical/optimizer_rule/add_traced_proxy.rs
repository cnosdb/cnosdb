use std::sync::Arc;

use datafusion::common::tree_node::{TreeNode, TreeNodeRewriter};
use datafusion::common::Result as DFResult;
use datafusion::config::ConfigOptions;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::ExecutionPlan;
use trace::SpanContext;

use crate::extension::physical::plan_node::traced_proxy::TracedProxyExec;

#[derive(Default)]
pub struct AddTracedProxy {
    root_span_ctx: Option<SpanContext>,
}

impl AddTracedProxy {
    pub fn new(root_span_ctx: Option<SpanContext>) -> Self {
        Self { root_span_ctx }
    }
}

impl PhysicalOptimizerRule for AddTracedProxy {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        if self.root_span_ctx.is_some() {
            return plan.rewrite_down(&mut AddTracedProxyRewriter);
        }

        Ok(plan)
    }

    fn name(&self) -> &str {
        "add_traced_proxy"
    }

    fn schema_check(&self) -> bool {
        false
    }
}

struct AddTracedProxyRewriter;

impl TreeNodeRewriter for AddTracedProxyRewriter {
    type N = Arc<dyn ExecutionPlan>;

    fn mutate(&mut self, plan: Self::N) -> DFResult<Self::N> {
        Ok(Arc::new(TracedProxyExec::new(plan)))
    }
}
