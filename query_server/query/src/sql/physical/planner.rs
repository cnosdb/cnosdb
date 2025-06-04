use std::sync::Arc;

use async_trait::async_trait;
use datafusion::execution::SessionStateBuilder;
use datafusion::logical_expr::LogicalPlan;
use datafusion::physical_optimizer::aggregate_statistics::AggregateStatistics;
use datafusion::physical_optimizer::coalesce_batches::CoalesceBatches;
use datafusion::physical_optimizer::enforce_distribution::EnforceDistribution;
use datafusion::physical_optimizer::enforce_sorting::EnforceSorting;
use datafusion::physical_optimizer::join_selection::JoinSelection;
use datafusion::physical_optimizer::sanity_checker::SanityCheckPlan;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_planner::{
    DefaultPhysicalPlanner as DFDefaultPhysicalPlanner, ExtensionPlanner,
    PhysicalPlanner as DFPhysicalPlanner,
};
use spi::query::physical_planner::PhysicalPlanner;
use spi::query::session::SessionCtx;
use spi::QueryResult;

use super::optimizer::PhysicalOptimizer;
use crate::extension::physical::optimizer_rule::add_assert::AddAssertExec;
use crate::extension::physical::optimizer_rule::add_sort::AddSortExec;
use crate::extension::physical::transform_rule::expand::ExpandPlanner;
use crate::extension::physical::transform_rule::table_writer::TableWriterPlanner;
use crate::extension::physical::transform_rule::tag_scan::TagScanPlanner;
use crate::extension::physical::transform_rule::ts_gen_func::TimeSeriesGenFuncPlanner;
use crate::extension::physical::transform_rule::update_tag::UpdateTagValuePlanner;

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
            Arc::new(UpdateTagValuePlanner {}),
            Arc::new(TagScanPlanner {}),
            Arc::new(ExpandPlanner::new()),
            Arc::new(TimeSeriesGenFuncPlanner),
        ];

        // We need to take care of the rule ordering. They may influence each other.
        let ext_physical_optimizer_rules: Vec<Arc<dyn PhysicalOptimizerRule + Sync + Send>> = vec![
            Arc::new(AggregateStatistics::new()),
            // Statistics-based join selection will change the Auto mode to a real join implementation,
            // like collect left, or hash join, or future sort merge join, which will influence the
            // EnforceDistribution and EnforceSorting rules as they decide whether to add additional
            // repartitioning and local sorting steps to meet distribution and ordering requirements.
            // Therefore, it should run before EnforceDistribution and EnforceSorting.
            Arc::new(JoinSelection::new()),
            // The EnforceDistribution rule is for adding essential repartitioning to satisfy distribution
            // requirements. Please make sure that the whole plan tree is determined before this rule.
            // This rule increases parallelism if doing so is beneficial to the physical plan; i.e. at
            // least one of the operators in the plan benefits from increased parallelism.
            Arc::new(EnforceDistribution::new()),
            // The EnforceSorting rule is for adding essential local sorting to satisfy the required
            // ordering. Please make sure that the whole plan tree is determined before this rule.
            // Note that one should always run this rule after running the EnforceDistribution rule
            // as the latter may break local sorting requirements.
            Arc::new(EnforceSorting::new()),
            // The CoalesceBatches rule will not influence the distribution and ordering of the
            // whole plan tree. Therefore, to avoid influencing other rules, it should run last.
            Arc::new(CoalesceBatches::new()),
            // The SanityCheckPlan rule checks whether the order and
            // distribution requirements of each node in the plan
            // is satisfied. It will also reject non-runnable query
            // plans that use pipeline-breaking operators on infinite
            // input(s). The rule generates a diagnostic error
            // message for invalid plans. It makes no changes to the
            // given query plan; i.e. it only acts as a final
            // gatekeeping rule.
            Arc::new(SanityCheckPlan::new()),
            // CnosDB
            Arc::new(AddAssertExec::new()),
            Arc::new(AddSortExec::new()),
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
    ) -> QueryResult<Arc<dyn ExecutionPlan>> {
        // 将扩展的物理计划优化规则注入df 的 session state
        let new_state = SessionStateBuilder::new_from_existing(session.inner().clone())
            .with_physical_optimizer_rules(self.ext_physical_optimizer_rules.clone())
            .build();

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
    ) -> QueryResult<Arc<dyn ExecutionPlan>> {
        let partition_count = plan.properties().output_partitioning().partition_count();

        let merged_plan = if partition_count > 1 {
            Arc::new(CoalescePartitionsExec::new(plan))
        } else {
            plan
        };

        debug_assert_eq!(
            1,
            merged_plan
                .properties()
                .output_partitioning()
                .partition_count()
        );

        Ok(merged_plan)
    }

    fn inject_optimizer_rule(
        &mut self,
        optimizer_rule: Arc<dyn PhysicalOptimizerRule + Send + Sync>,
    ) {
        self.ext_physical_optimizer_rules.push(optimizer_rule);
    }
}
