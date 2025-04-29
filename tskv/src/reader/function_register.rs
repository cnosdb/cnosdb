use std::collections::HashSet;
use std::sync::Arc;

use datafusion::error::{DataFusionError, Result};
use datafusion::execution::FunctionRegistry;
use datafusion::logical_expr::planner::ExprPlanner;
use datafusion::logical_expr::{AggregateUDF, ScalarUDF, WindowUDF};

pub struct NoRegistry;

impl FunctionRegistry for NoRegistry {
    fn udfs(&self) -> HashSet<String> {
        HashSet::new()
    }

    fn udf(&self, name: &str) -> Result<Arc<ScalarUDF>> {
        Err(DataFusionError::Plan(
            format!("No function registry provided to deserialize, so can not deserialize User Defined Function '{name}'"))
        )
    }

    fn udaf(&self, name: &str) -> Result<Arc<AggregateUDF>> {
        Err(DataFusionError::Plan(
            format!("No function registry provided to deserialize, so can not deserialize User Defined Aggregate Function '{name}'"))
        )
    }

    fn udwf(&self, name: &str) -> Result<Arc<WindowUDF>> {
        Err(DataFusionError::Plan(
            format!("No function registry provided to deserialize, so can not deserialize User Defined Window Function '{name}'"))
        )
    }

    fn expr_planners(&self) -> Vec<Arc<dyn ExprPlanner>> {
        vec![]
    }
}
