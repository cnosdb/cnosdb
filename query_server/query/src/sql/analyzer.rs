use std::sync::Arc;

use datafusion::logical_expr::LogicalPlan;
use datafusion::optimizer::analyzer::Analyzer as DFAnalyzer;
use spi::query::analyzer::Analyzer;
use spi::query::session::SessionCtx;
use spi::Result;

use crate::extension::analyse::initial_plan_checker::InitialPlanChecker;
use crate::extension::analyse::transform_bottom_func_to_topk_node::TransformBottomFuncToTopkNodeRule;
use crate::extension::analyse::transform_gapfill::TransformGapFill;
use crate::extension::analyse::transform_time_window::TransformTimeWindowRule;
use crate::extension::analyse::transform_topk_func_to_topk_node::TransformTopkFuncToTopkNodeRule;

pub struct DefaultAnalyzer {
    inner: DFAnalyzer,
}

impl DefaultAnalyzer {
    pub fn new() -> Self {
        let mut analyzer = DFAnalyzer::default();

        let rules = &mut analyzer.rules;
        rules.push(Arc::new(InitialPlanChecker {}));
        rules.push(Arc::new(TransformBottomFuncToTopkNodeRule {}));
        rules.push(Arc::new(TransformTopkFuncToTopkNodeRule {}));
        rules.push(Arc::new(TransformGapFill::new()));
        rules.push(Arc::new(TransformTimeWindowRule {}));

        Self { inner: analyzer }
    }
}

impl Default for DefaultAnalyzer {
    fn default() -> Self {
        Self::new()
    }
}

impl Analyzer for DefaultAnalyzer {
    fn analyze(&self, plan: &LogicalPlan, session: &SessionCtx) -> Result<LogicalPlan> {
        let plan = self
            .inner
            .execute_and_check(plan, session.inner().state().config_options())?;
        Ok(plan)
    }
}
