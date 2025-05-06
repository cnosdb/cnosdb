use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion, TreeNodeVisitor};
use datafusion::common::Result as DFResult;
use datafusion::config::ConfigOptions;
use datafusion::datasource::source_as_provider;
use datafusion::error::DataFusionError;
use datafusion::logical_expr::{LogicalPlan, TableScan};
use datafusion::optimizer::analyzer::AnalyzerRule;
use spi::AnalyzerSnafu;

#[derive(Default)]
pub struct InitialPlanChecker {}

impl AnalyzerRule for InitialPlanChecker {
    fn analyze(&self, plan: LogicalPlan, _config: &ConfigOptions) -> DFResult<LogicalPlan> {
        let mut visitor = InitialPlanCheckerVisitor::default();
        let _ = plan.visit(&mut visitor)?;
        Ok(plan)
    }

    fn name(&self) -> &str {
        "initial_plan_checker"
    }
}

#[derive(Default)]
struct InitialPlanCheckerVisitor {}

impl<'a> TreeNodeVisitor<'a> for InitialPlanCheckerVisitor {
    type Node = LogicalPlan;

    fn f_down(&mut self, plan: &Self::Node) -> DFResult<TreeNodeRecursion> {
        if let LogicalPlan::TableScan(TableScan { source, .. }) = plan {
            match source_as_provider(source) {
                Ok(table) if table.get_logical_plan().is_some() => {
                    return Err(DataFusionError::External(Box::new(
                        AnalyzerSnafu {
                            err: format!("Still have unresolved table source {}", source.name()),
                        }
                        .build(),
                    )));
                }
                Err(_) => {
                    return Err(DataFusionError::External(Box::new(
                        AnalyzerSnafu {
                            err: format!("Unresolved table source {}", source.name()),
                        }
                        .build(),
                    )));
                }
                _ => {}
            }
        }

        Ok(TreeNodeRecursion::Continue)
    }
}
