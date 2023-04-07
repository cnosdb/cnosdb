use datafusion::common::Result as DFResult;
use datafusion::error::DataFusionError;
use datafusion::logical_expr::{LogicalPlan, PlanVisitor};

use super::AnalyzerRule;

#[derive(Default)]
pub struct UnsupportedOperationChecker {}

impl AnalyzerRule for UnsupportedOperationChecker {
    fn analyze(&self, plan: &LogicalPlan) -> DFResult<Option<LogicalPlan>> {
        let mut visitor = UnsupportedOperationVisitor::default();
        let _ = plan.accept(&mut visitor)?;
        Ok(None)
    }

    fn name(&self) -> &str {
        "unsupported_operation_checker"
    }
}

#[derive(Default)]
struct UnsupportedOperationVisitor {
    agg_count: usize,
}

impl PlanVisitor for UnsupportedOperationVisitor {
    type Error = DataFusionError;

    fn pre_visit(&mut self, plan: &LogicalPlan) -> Result<bool, Self::Error> {
        match plan {
            LogicalPlan::Aggregate(_) => {
                self.agg_count += 1;
                if self.agg_count > 1 {
                    return Err(DataFusionError::Plan(
                        "Unsupported operation in streaming query: multiple aggregate".to_string(),
                    ));
                }
            }
            LogicalPlan::Join(_) => {
                return Err(DataFusionError::Plan(
                    "Unsupported operation in streaming query: join".to_string(),
                ));
            }
            LogicalPlan::CrossJoin(_) => {
                return Err(DataFusionError::Plan(
                    "Unsupported operation in streaming query: cross join".to_string(),
                ));
            }
            LogicalPlan::Limit(_) => {
                return Err(DataFusionError::Plan(
                    "Unsupported operation in streaming query: limit".to_string(),
                ));
            }
            LogicalPlan::Sort(_) => {
                return Err(DataFusionError::Plan(
                    "Unsupported operation in streaming query: sort".to_string(),
                ));
            }
            _ => {}
        }

        Ok(true)
    }
}
