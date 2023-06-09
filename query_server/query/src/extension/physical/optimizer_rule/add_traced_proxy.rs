use std::sync::Arc;

use datafusion::common::tree_node::{TreeNode, TreeNodeRewriter};
use datafusion::common::Result as DFResult;
use datafusion::config::ConfigOptions;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::ExecutionPlan;
use trace::{SpanContext, SpanExt, SpanRecorder};

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
            let span_recorder = SpanRecorder::new(self.root_span_ctx.child_span("trace stream"));
            let mut rewriter = AddTracedProxyRewriter::new(span_recorder);
            return plan.rewrite_down(&mut rewriter);
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

struct AddTracedProxyRewriter {
    parent_span_recorder: SpanRecorder,
}

impl AddTracedProxyRewriter {
    pub fn new(parent_span_recorder: SpanRecorder) -> Self {
        Self {
            parent_span_recorder,
        }
    }
}

impl TreeNodeRewriter for AddTracedProxyRewriter {
    type N = Arc<dyn ExecutionPlan>;

    fn mutate(&mut self, plan: Self::N) -> DFResult<Self::N> {
        let parent_span_ctx = self.parent_span_recorder.span_ctx();
        let new_plan = Arc::new(TracedProxyExec::new(plan, parent_span_ctx.cloned()));

        let mut span_recorder = self
            .parent_span_recorder
            .child(format!("& {}", new_plan.name()));

        span_recorder.set_metadata("desc", new_plan.desc());

        self.parent_span_recorder = span_recorder;

        Ok(new_plan)
    }
}
