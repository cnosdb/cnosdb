use std::sync::Arc;

use async_trait::async_trait;
use datafusion::logical_expr::LogicalPlan;
use datafusion::physical_optimizer::aggregate_statistics::AggregateStatistics;
use datafusion::physical_optimizer::coalesce_batches::CoalesceBatches;
use datafusion::physical_optimizer::dist_enforcement::EnforceDistribution;
use datafusion::physical_optimizer::global_sort_selection::GlobalSortSelection;
use datafusion::physical_optimizer::join_selection::JoinSelection;
use datafusion::physical_optimizer::pipeline_checker::PipelineChecker;
use datafusion::physical_optimizer::pipeline_fixer::PipelineFixer;
use datafusion::physical_optimizer::repartition::Repartition;
use datafusion::physical_optimizer::sort_enforcement::EnforceSorting;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::planner::{
    DefaultPhysicalPlanner as DFDefaultPhysicalPlanner, ExtensionPlanner,
};
use datafusion::physical_plan::{ExecutionPlan, PhysicalPlanner as DFPhysicalPlanner};
use spi::query::physical_planner::PhysicalPlanner;
use spi::query::session::SessionCtx;
use spi::Result;

use super::optimizer::PhysicalOptimizer;
use crate::extension::physical::transform_rule::expand::ExpandPlanner;
use crate::extension::physical::transform_rule::table_writer::TableWriterPlanner;
use crate::extension::physical::transform_rule::tag_scan::TagScanPlanner;

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
        let ext_physical_transform_rules: Vec<Arc<dyn ExtensionPlanner + Send + Sync>> = vec![
            Arc::new(TableWriterPlanner {}),
            Arc::new(TagScanPlanner {}),
            Arc::new(ExpandPlanner::new()),
        ];

        // We need to take care of the rule ordering. They may influence each other.
        let ext_physical_optimizer_rules: Vec<Arc<dyn PhysicalOptimizerRule + Sync + Send>> = vec![
            Arc::new(AggregateStatistics::new()),
            // In order to increase the parallelism, the Repartition rule will change the
            // output partitioning of some operators in the plan tree, which will influence
            // other rules. Therefore, it should run as soon as possible. It is optional because:
            // - It's not used for the distributed engine, Ballista.
            // - It's conflicted with some parts of the EnforceDistribution, since it will
            //   introduce additional repartitioning while EnforceDistribution aims to
            //   reduce unnecessary repartitioning.
            Arc::new(Repartition::new()),
            // - Currently it will depend on the partition number to decide whether to change the
            // single node sort to parallel local sort and merge. Therefore, GlobalSortSelection
            // should run after the Repartition.
            // - Since it will change the output ordering of some operators, it should run
            // before JoinSelection and EnforceSorting, which may depend on that.
            Arc::new(GlobalSortSelection::new()),
            // Statistics-based join selection will change the Auto mode to a real join implementation,
            // like collect left, or hash join, or future sort merge join, which will influence the
            // EnforceDistribution and EnforceSorting rules as they decide whether to add additional
            // repartitioning and local sorting steps to meet distribution and ordering requirements.
            // Therefore, it should run before EnforceDistribution and EnforceSorting.
            Arc::new(JoinSelection::new()),
            // If the query is processing infinite inputs, the PipelineFixer rule applies the
            // necessary transformations to make the query runnable (if it is not already runnable).
            // If the query can not be made runnable, the rule emits an error with a diagnostic message.
            // Since the transformations it applies may alter output partitioning properties of operators
            // (e.g. by swapping hash join sides), this rule runs before EnforceDistribution.
            Arc::new(PipelineFixer::new()),
            // The EnforceDistribution rule is for adding essential repartition to satisfy the required
            // distribution. Please make sure that the whole plan tree is determined before this rule.
            Arc::new(EnforceDistribution::new()),
            // The EnforceSorting rule is for adding essential local sorting to satisfy the required
            // ordering. Please make sure that the whole plan tree is determined before this rule.
            // Note that one should always run this rule after running the EnforceDistribution rule
            // as the latter may break local sorting requirements.
            Arc::new(EnforceSorting::new()),
            // The CoalesceBatches rule will not influence the distribution and ordering of the
            // whole plan tree. Therefore, to avoid influencing other rules, it should run last.
            Arc::new(CoalesceBatches::new()),
            // The PipelineChecker rule will reject non-runnable query plans that use
            // pipeline-breaking operators on infinite input(s). The rule generates a
            // diagnostic error message when this happens. It makes no changes to the
            // given query plan; i.e. it only acts as a final gatekeeping rule.
            Arc::new(PipelineChecker::new()),
        ];

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
        session: &SessionCtx,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // 将扩展的物理计划优化规则注入df 的 session state
        let new_state = session
            .inner()
            .state()
            .with_physical_optimizer_rules(self.ext_physical_optimizer_rules.clone());

        // 通过扩展的物理计划转换规则构造df 的 Physical Planner
        let planner = DFDefaultPhysicalPlanner::with_extension_planners(
            self.ext_physical_transform_rules.clone(),
        );

        // 执行df的物理计划规划及优化
        planner
            .create_physical_plan(logical_plan, &new_state)
            .await
            .map_err(|e| e.into())
    }

    fn inject_physical_transform_rule(&mut self, rule: Arc<dyn ExtensionPlanner + Send + Sync>) {
        self.ext_physical_transform_rules.push(rule)
    }
}

impl PhysicalOptimizer for DefaultPhysicalPlanner {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _session: &SessionCtx,
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
