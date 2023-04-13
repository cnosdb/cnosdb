use datafusion::logical_expr::LogicalPlan;
use datafusion::optimizer::analyzer::Analyzer as DFAnalyzer;
use spi::query::analyzer::Analyzer;
use spi::query::session::SessionCtx;
use spi::Result;

pub struct DefaultAnalyzer {
    inner: DFAnalyzer,
}

impl DefaultAnalyzer {
    pub fn new() -> Self {
        let inner = DFAnalyzer::default();
        Self { inner }
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
