use std::sync::Arc;

use async_trait::async_trait;
use datafusion::{
    logical_plan::LogicalPlan,
    physical_optimizer::PhysicalOptimizerRule,
    physical_plan::{
        planner::{DefaultPhysicalPlanner as DFDefaultPhysicalPlanner, ExtensionPlanner},
        ExecutionPlan, PhysicalPlanner as DFPhysicalPlanner,
    },
};
use snafu::ResultExt;
use spi::query::{physical_planner::PhysicalPlanner, Result};
use spi::query::{session::IsiphoSessionCtx, PhysicalPlanerSnafu};

use crate::extension::physical::transform_rule::topk::TopKPlanner;

use super::optimizer::PhysicalOptimizer;

pub struct DefaultPhysicalPlanner {
    ext_physical_transform_rules: Vec<Arc<dyn ExtensionPlanner + Send + Sync>>,
    /// Responsible for optimizing a physical execution plan
    ext_physical_optimizer_rules: Vec<Arc<dyn PhysicalOptimizerRule + Send + Sync>>,
}

impl DefaultPhysicalPlanner {
    #[allow(dead_code)]
    fn with_physical_transform_rules(
        mut self,
        rules: Vec<Arc<dyn ExtensionPlanner + Send + Sync>>,
    ) -> Self {
        self.ext_physical_transform_rules = rules;
        self
    }
}

impl DefaultPhysicalPlanner {
    #[allow(dead_code)]
    fn with_optimizer_rules(
        mut self,
        rules: Vec<Arc<dyn PhysicalOptimizerRule + Send + Sync>>,
    ) -> Self {
        self.ext_physical_optimizer_rules = rules;
        self
    }
}

impl Default for DefaultPhysicalPlanner {
    fn default() -> Self {
        let ext_physical_transform_rules: Vec<Arc<dyn ExtensionPlanner + Send + Sync>> =
            vec![Arc::new(TopKPlanner {})];

        let ext_physical_optimizer_rules: Vec<Arc<dyn PhysicalOptimizerRule + Send + Sync>> =
            vec![];

        Self {
            ext_physical_transform_rules,
            ext_physical_optimizer_rules,
        }
    }
}

#[async_trait]
impl PhysicalPlanner for DefaultPhysicalPlanner {
    async fn create_physical_plan(
        &self,
        logical_plan: &LogicalPlan,
        session: &IsiphoSessionCtx,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let mut new_state = session.inner().state();
        // 通过扩展的物理计划转换规则构造df 的 Physical Planner
        let planner = DFDefaultPhysicalPlanner::with_extension_planners(
            self.ext_physical_transform_rules.clone(),
        );
        // 将扩展的物理计划优化规则注入df 的 session state
        new_state.physical_optimizers = self.ext_physical_optimizer_rules.clone();
        // 执行df的物理计划规划及优化
        planner
            .create_physical_plan(logical_plan, &new_state)
            .await
            .context(PhysicalPlanerSnafu)
    }

    fn inject_physical_transform_rule(&mut self, rule: Arc<dyn ExtensionPlanner + Send + Sync>) {
        self.ext_physical_transform_rules.push(rule)
    }
}

impl PhysicalOptimizer for DefaultPhysicalPlanner {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _session: &IsiphoSessionCtx,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // df plan阶段已经优化过，直接返回
        Ok(plan)
    }

    fn inject_optimizer_rule(
        &mut self,
        optimizer_rule: Arc<dyn PhysicalOptimizerRule + Send + Sync>,
    ) {
        self.ext_physical_optimizer_rules.push(optimizer_rule);
    }
}
