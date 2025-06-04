use std::sync::Arc;

use datafusion::logical_expr::LogicalPlan;
use datafusion::optimizer::analyzer::Analyzer as DFAnalyzer;
use spi::query::analyzer::Analyzer;
use spi::query::session::SessionCtx;
use spi::QueryResult;

use crate::extension::analyse::add_time_for_tsgenfunc::AddTimeForTimeSeriesGenFunc;
use crate::extension::analyse::initial_plan_checker::InitialPlanChecker;
use crate::extension::analyse::transform_bottom_func_to_topk_node::TransformBottomFuncToTopkNodeRule;
use crate::extension::analyse::transform_count_gen_time_col::TransformCountGenTimeColRule;
use crate::extension::analyse::transform_exact_count_to_count::TransformExactCountToCountRule;
use crate::extension::analyse::transform_time_window::TransformTimeWindowRule;
use crate::extension::analyse::transform_topk_func_to_topk_node::TransformTopkFuncToTopkNodeRule;
use crate::extension::analyse::transform_ts_gen_func::TransformTimeSeriesGenFunc;
use crate::extension::analyse::transform_update::TransformUpdateRule;

pub struct DefaultAnalyzer {
    inner: DFAnalyzer,
}

impl DefaultAnalyzer {
    pub fn new() -> Self {
        let mut analyzer = DFAnalyzer::default();

        let rules = &mut analyzer.rules;
        rules.insert(0, Arc::new(TransformUpdateRule::new()));
        rules.push(Arc::new(InitialPlanChecker {}));
        rules.push(Arc::new(TransformBottomFuncToTopkNodeRule {}));
        rules.push(Arc::new(TransformTopkFuncToTopkNodeRule {}));
        rules.push(Arc::new(TransformTimeWindowRule {}));
        rules.push(Arc::new(TransformTimeSeriesGenFunc));
        rules.push(Arc::new(AddTimeForTimeSeriesGenFunc {}));
        rules.push(Arc::new(TransformExactCountToCountRule {}));
        rules.push(Arc::new(TransformCountGenTimeColRule {}));

        Self { inner: analyzer }
    }
}

impl Default for DefaultAnalyzer {
    fn default() -> Self {
        Self::new()
    }
}

impl Analyzer for DefaultAnalyzer {
    fn analyze(&self, plan: LogicalPlan, session: &SessionCtx) -> QueryResult<LogicalPlan> {
        let plan =
            self.inner
                .execute_and_check(plan, session.inner().config_options(), |_, _| {})?;
        Ok(plan)
    }
}
