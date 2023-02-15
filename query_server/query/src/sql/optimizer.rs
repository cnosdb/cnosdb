use std::sync::Arc;

use async_trait::async_trait;
use datafusion::logical_expr::LogicalPlan;
use datafusion::physical_plan::{displayable, ExecutionPlan};
use spi::query::optimizer::Optimizer;
use spi::query::physical_planner::PhysicalPlanner;
use spi::query::session::IsiphoSessionCtx;
use spi::Result;
use trace::debug;

use super::logical::optimizer::{DefaultLogicalOptimizer, LogicalOptimizer};
use super::physical::optimizer::PhysicalOptimizer;
use super::physical::planner::DefaultPhysicalPlanner;

pub struct CascadeOptimizer {
    logical_optimizer: Arc<dyn LogicalOptimizer + Send + Sync>,
    physical_planner: Arc<dyn PhysicalPlanner + Send + Sync>,
    physical_optimizer: Arc<dyn PhysicalOptimizer + Send + Sync>,
}

#[async_trait]
impl Optimizer for CascadeOptimizer {
    async fn optimize(
        &self,
        plan: &LogicalPlan,
        session: &IsiphoSessionCtx,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        debug!("Original logical plan:\n{}\n", plan.display_indent_schema(),);

        let optimized_logical_plan = self.logical_optimizer.optimize(plan, session)?;

        debug!(
            "Final logical plan:\n{}\n",
            optimized_logical_plan.display_indent_schema(),
        );

        let physical_plan = self
            .physical_planner
            .create_physical_plan(&optimized_logical_plan, session)
            .await?;

        let optimized_physical_plan = self.physical_optimizer.optimize(physical_plan, session)?;

        debug!(
            "Final physical plan:\nOutput partition count: {}\n{}\n",
            optimized_physical_plan
                .output_partitioning()
                .partition_count(),
            displayable(optimized_physical_plan.as_ref()).indent()
        );

        Ok(optimized_physical_plan)
    }
}

#[derive(Default)]
pub struct CascadeOptimizerBuilder {
    logical_optimizer: Option<Arc<dyn LogicalOptimizer + Send + Sync>>,
    physical_planner: Option<Arc<dyn PhysicalPlanner + Send + Sync>>,
    physical_optimizer: Option<Arc<dyn PhysicalOptimizer + Send + Sync>>,
}

impl CascadeOptimizerBuilder {
    pub fn with_logical_optimizer(
        mut self,
        logical_optimizer: Arc<dyn LogicalOptimizer + Send + Sync>,
    ) -> Self {
        self.logical_optimizer = Some(logical_optimizer);
        self
    }

    pub fn with_physical_planner(
        mut self,
        physical_planner: Arc<dyn PhysicalPlanner + Send + Sync>,
    ) -> Self {
        self.physical_planner = Some(physical_planner);
        self
    }

    pub fn with_physical_optimizer(
        mut self,
        physical_optimizer: Arc<dyn PhysicalOptimizer + Send + Sync>,
    ) -> Self {
        self.physical_optimizer = Some(physical_optimizer);
        self
    }

    pub fn build(self) -> CascadeOptimizer {
        let default_logical_optimizer = Arc::new(DefaultLogicalOptimizer::default());
        let default_physical_planner = Arc::new(DefaultPhysicalPlanner::default());

        let logical_optimizer = self.logical_optimizer.unwrap_or(default_logical_optimizer);
        let physical_planner = self
            .physical_planner
            .unwrap_or_else(|| default_physical_planner.clone());
        let physical_optimizer = self.physical_optimizer.unwrap_or(default_physical_planner);

        CascadeOptimizer {
            logical_optimizer,
            physical_planner,
            physical_optimizer,
        }
    }
}
