use std::sync::Arc;

use datafusion::logical_expr::LogicalPlan;
use datafusion::optimizer::analyzer::{Analyzer as DFAnalyzer, AnalyzerRule};
use spi::query::analyzer::Analyzer;
use spi::query::session::SessionCtx;
use spi::Result;

use crate::extension::analyse::transform_bottom_func_to_topk_node::TransformBottomFuncToTopkNodeRule;
use crate::extension::analyse::transform_time_window::TransformTimeWindowRule;
use crate::extension::analyse::transform_topk_func_to_topk_node::TransformTopkFuncToTopkNodeRule;

pub struct DefaultAnalyzer {
    inner: DFAnalyzer,
}

impl DefaultAnalyzer {
    pub fn new() -> Self {
        let ext_rules: Vec<Arc<dyn AnalyzerRule + Send + Sync>> = vec![
            Arc::new(TransformBottomFuncToTopkNodeRule {}),
            Arc::new(TransformTopkFuncToTopkNodeRule {}),
            Arc::new(TransformTimeWindowRule {}),
        ];
        Self {
            inner: DFAnalyzer::with_rules(ext_rules),
        }
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
