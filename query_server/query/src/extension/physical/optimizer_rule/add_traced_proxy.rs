use std::sync::Arc;

use datafusion::common::tree_node::{
    Transformed, TransformedResult, TreeNode, TreeNodeRecursion, TreeNodeRewriter,
};
use datafusion::common::Result as DFResult;
use datafusion::config::ConfigOptions;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::ExecutionPlan;
use trace::SpanContext;

use crate::extension::physical::plan_node::traced_proxy::TracedProxyExec;

#[derive(Default, Debug)]
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
            return AddTracedProxyRewriter.rewrite_down(plan).data();
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
    type Node = Arc<dyn ExecutionPlan>;

    fn f_up(&mut self, plan: Self::Node) -> DFResult<Transformed<Self::Node>> {
        Ok(Transformed::yes(Arc::new(TracedProxyExec::new(plan))))
    }
}

impl AddTracedProxyRewriter {
    fn rewrite_down(
        &mut self,
        plan: Arc<dyn ExecutionPlan>,
    ) -> DFResult<Transformed<Arc<dyn ExecutionPlan>>> {
        let down = self.f_down(plan)?;
        if down.transformed {
            return self.f_up(plan);
        }
        let need_mutate = match down.tnr {
            TreeNodeRecursion::Stop => return Ok(down),
            TreeNodeRecursion::Continue => true,
            TreeNodeRecursion::Jump => false,
        };

        if need_mutate {
            let up = self.f_up(down.data)?;
            up.data.map_children(|node| self.rewrite_down(node))
        } else {
            Ok(down)
        }
    }
}
