use std::sync::Arc;

use async_trait::async_trait;
use datafusion::config::ConfigOptions;
use datafusion::logical_expr::LogicalPlan;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::{displayable, ExecutionPlan};
use spi::query::optimizer::Optimizer;
use spi::query::physical_planner::PhysicalPlanner;
use spi::query::session::SessionCtx;
use spi::Result;
use trace::debug;

use super::logical::optimizer::{DefaultLogicalOptimizer, LogicalOptimizer};
use super::physical::optimizer::PhysicalOptimizer;
use super::physical::planner::DefaultPhysicalPlanner;
use crate::extension::physical::optimizer_rule::add_traced_proxy::AddTracedProxy;

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
        session: &SessionCtx,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        debug!("Original logical plan:\n{}\n", plan.display_indent_schema(),);

        let optimized_logical_plan = self.logical_optimizer.optimize(plan, session)?;

        debug!(
            "Final logical plan:\n{}\n",
            optimized_logical_plan.display_indent_schema(),
        );

        let physical_plan = {
            let mut span_recorder =
                session.get_child_span_recorder("logical plan to physical plan");

            self.physical_planner
                .create_physical_plan(&optimized_logical_plan, session)
                .await
                .map(|p| {
                    span_recorder.ok("complete physical plan creation");
                    span_recorder.set_metadata(
                        "original physical plan",
                        displayable(p.as_ref()).indent().to_string(),
                    );
                    p
                })
                .map_err(|err| {
                    span_recorder.error(err.to_string());
                    err
                })?
        };

        debug!(
            "Original physical plan:\n{}\n",
            displayable(physical_plan.as_ref()).indent()
        );

        let optimized_physical_plan = {
            let mut span_recorder = session.get_child_span_recorder("optimize physical plan");

            self.physical_optimizer
                .optimize(physical_plan, session)
                .map(|p| {
                    span_recorder.ok("complete physical plan optimization");
                    span_recorder.set_metadata(
                        "final physical plan",
                        displayable(p.as_ref()).indent().to_string(),
                    );
                    p
                })
                .map_err(|err| {
                    span_recorder.error(err.to_string());
                    err
                })?
        };

        let traced_plan = {
            let span_recorder = session.get_child_span_recorder("add traced proxy");
            AddTracedProxy::new(span_recorder.span_ctx().cloned())
                .optimize(optimized_physical_plan, &ConfigOptions::default())?
        };

        debug!(
            "Final physical plan:\n{}\n",
            displayable(traced_plan.as_ref()).indent()
        );

        Ok(traced_plan)
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

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use coordinator::service_mock::{MockCoordinator, WITH_NONEMPTY_DATABASE_FOR_TEST};
    use datafusion::datasource::provider_as_source;
    use datafusion::error::Result;
    use datafusion::execution::context::default_session_builder;
    use datafusion::logical_expr::{LogicalPlan, LogicalPlanBuilder, UNNAMED_TABLE};
    use datafusion::optimizer::optimizer::Optimizer;
    use datafusion::optimizer::{OptimizerContext, OptimizerRule};
    use datafusion::physical_plan::planner::DefaultPhysicalPlanner;
    use datafusion::physical_plan::{displayable, PhysicalPlanner};
    use datafusion::prelude::{col, count, max, min, sum, Expr, SessionConfig};
    use meta::model::meta_client_mock::MockMetaClient;
    use models::schema::{ColumnType, TableColumn, TskvTableSchema};
    use models::ValueType;

    use crate::data_source::batch::tskv::ClusterTable;
    use crate::data_source::split;

    fn observe(_plan: &LogicalPlan, _rule: &dyn OptimizerRule) {}

    fn optimize_plan(plan: &LogicalPlan) -> Result<LogicalPlan> {
        let opt = Optimizer::new();
        let config = OptimizerContext::new().with_skip_failing_rules(false);

        opt.optimize(plan, &config, &observe)
    }

    fn test_table_scan(with_nonempty_database: bool) -> Result<LogicalPlan> {
        let dn_name = if with_nonempty_database {
            WITH_NONEMPTY_DATABASE_FOR_TEST
        } else {
            "default"
        };
        let mut schema = TskvTableSchema::default();
        schema.add_column(TableColumn::new_with_default(
            "flag".to_string(),
            ColumnType::Tag,
        ));
        schema.add_column(TableColumn::new_with_default(
            "value".to_string(),
            ColumnType::Field(ValueType::Integer),
        ));
        schema.db = dn_name.to_string();

        let provider = Arc::new(ClusterTable::new(
            Arc::new(MockCoordinator::default()),
            split::default_split_manager_ref_only_for_test(),
            Arc::new(MockMetaClient::default()),
            Arc::new(schema),
        ));

        LogicalPlanBuilder::scan(UNNAMED_TABLE, provider_as_source(provider), None)?.build()
    }

    async fn test_plan(
        plan: LogicalPlan,
        opt_logical_plan_str: &str,
        final_physical_plan_str: &str,
    ) -> Result<()> {
        let opt_plan = optimize_plan(&plan)?;
        let result_str = format!("{opt_plan:?}");

        assert_eq!(opt_logical_plan_str, result_str);

        let planner = DefaultPhysicalPlanner::default();
        let optimized_physical_plan = planner
            .create_physical_plan(
                &opt_plan,
                &default_session_builder(SessionConfig::default().with_target_partitions(8)),
            )
            .await?;
        let result_str = format!("{}", displayable(optimized_physical_plan.as_ref()).indent());

        assert_eq!(final_physical_plan_str, result_str);

        Ok(())
    }

    #[tokio::test]
    async fn test_count_with_group() -> Result<()> {
        {
            let plan = LogicalPlanBuilder::from(test_table_scan(false)?)
                .aggregate(vec![col("flag")], vec![count(col("value"))])?
                .build()?;

            test_plan(
                plan,
                "\
                Aggregate: groupBy=[[?table?.flag]], aggr=[[COUNT(?table?.value)]]\
                \n  TableScan: ?table? projection=[flag, value]",
                "\
                AggregateExec: mode=FinalPartitioned, gby=[flag@0 as flag], aggr=[COUNT(?table?.value)]\
                \n  CoalesceBatchesExec: target_batch_size=8192\
                \n    RepartitionExec: partitioning=Hash([Column { name: \"flag\", index: 0 }], 8), input_partitions=8\
                \n      RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1\
                \n        AggregateExec: mode=Partial, gby=[flag@0 as flag], aggr=[COUNT(?table?.value)]\
                \n          EmptyExec: produce_one_row=false\
                \n",
            ).await?;
        }
        let plan = LogicalPlanBuilder::from(test_table_scan(true)?)
            .aggregate(vec![col("flag")], vec![count(col("value"))])?
            .build()?;

        test_plan(
            plan,
            "\
            Aggregate: groupBy=[[?table?.flag]], aggr=[[COUNT(?table?.value)]]\
            \n  TableScan: ?table? projection=[flag, value]",
            "\
            AggregateExec: mode=FinalPartitioned, gby=[flag@0 as flag], aggr=[COUNT(?table?.value)]\
            \n  CoalesceBatchesExec: target_batch_size=8192\
            \n    RepartitionExec: partitioning=Hash([Column { name: \"flag\", index: 0 }], 8), input_partitions=8\
            \n      AggregateExec: mode=Partial, gby=[flag@0 as flag], aggr=[COUNT(?table?.value)]\
            \n        TskvExec: limit=None, predicate=ColumnDomains { column_to_domain: Some({}) }, split_num=8, projection=[flag,value]\
            \n",
        ).await
    }

    #[tokio::test]
    async fn test_count_without_group() -> Result<()> {
        {
            let plan = LogicalPlanBuilder::from(test_table_scan(false)?)
                .aggregate(Vec::<Expr>::new(), vec![count(col("value"))])?
                .build()?;

            test_plan(
                plan,
                "\
                Projection: SUM(COUNT(?table?.value)) AS COUNT(?table?.value)\
                \n  Aggregate: groupBy=[[]], aggr=[[SUM(COUNT(?table?.value))]]\
                \n    TableScan: ?table?, grouping=[], agg=[COUNT(?table?.value)]",
                "\
                ProjectionExec: expr=[SUM(COUNT(?table?.value))@0 as COUNT(?table?.value)]\
                \n  AggregateExec: mode=Final, gby=[], aggr=[SUM(COUNT(?table?.value))]\
                \n    AggregateExec: mode=Partial, gby=[], aggr=[SUM(COUNT(?table?.value))]\
                \n      EmptyExec: produce_one_row=false\
                \n",
            )
            .await?;
        }

        let plan = LogicalPlanBuilder::from(test_table_scan(true)?)
            .aggregate(Vec::<Expr>::new(), vec![count(col("value"))])?
            .build()?;

        test_plan(
            plan,
            "\
            Projection: SUM(COUNT(?table?.value)) AS COUNT(?table?.value)\
            \n  Aggregate: groupBy=[[]], aggr=[[SUM(COUNT(?table?.value))]]\
            \n    TableScan: ?table?, grouping=[], agg=[COUNT(?table?.value)]",
            "\
            ProjectionExec: expr=[SUM(COUNT(?table?.value))@0 as COUNT(?table?.value)]\
            \n  AggregateExec: mode=Final, gby=[], aggr=[SUM(COUNT(?table?.value))]\
            \n    CoalescePartitionsExec\
            \n      AggregateExec: mode=Partial, gby=[], aggr=[SUM(COUNT(?table?.value))]\
            \n        AggregateFilterTskvExec: agg=[[Count(\"value\")]], filter=[Predicate { pushed_down_domains: ColumnDomains { column_to_domain: Some({}) }, limit: None }]\
            \n",
        ).await
    }

    #[tokio::test]
    async fn test_max_with_group() -> Result<()> {
        {
            let plan = LogicalPlanBuilder::from(test_table_scan(false)?)
                .aggregate(vec![col("flag")], vec![max(col("value"))])?
                .build()?;

            test_plan(
                plan,
                "\
                Aggregate: groupBy=[[?table?.flag]], aggr=[[MAX(?table?.value)]]\
                \n  TableScan: ?table? projection=[flag, value]",
                "\
                AggregateExec: mode=FinalPartitioned, gby=[flag@0 as flag], aggr=[MAX(?table?.value)]\
                \n  CoalesceBatchesExec: target_batch_size=8192\
                \n    RepartitionExec: partitioning=Hash([Column { name: \"flag\", index: 0 }], 8), input_partitions=8\
                \n      RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1\
                \n        AggregateExec: mode=Partial, gby=[flag@0 as flag], aggr=[MAX(?table?.value)]\
                \n          EmptyExec: produce_one_row=false\
                \n",
            ).await?;
        }

        let plan = LogicalPlanBuilder::from(test_table_scan(true)?)
            .aggregate(vec![col("value")], vec![max(col("value"))])?
            .build()?;

        test_plan(
            plan,
            "\
            Aggregate: groupBy=[[?table?.value]], aggr=[[MAX(?table?.value)]]\
            \n  TableScan: ?table? projection=[value]",
            "\
            AggregateExec: mode=FinalPartitioned, gby=[value@0 as value], aggr=[MAX(?table?.value)]\
            \n  CoalesceBatchesExec: target_batch_size=8192\
            \n    RepartitionExec: partitioning=Hash([Column { name: \"value\", index: 0 }], 8), input_partitions=8\
            \n      AggregateExec: mode=Partial, gby=[value@0 as value], aggr=[MAX(?table?.value)]\
            \n        TskvExec: limit=None, predicate=ColumnDomains { column_to_domain: Some({}) }, split_num=8, projection=[value]\
            \n",
        ).await
    }

    #[tokio::test]
    async fn test_max_without_group() -> Result<()> {
        {
            let plan = LogicalPlanBuilder::from(test_table_scan(false)?)
                .aggregate(Vec::<Expr>::new(), vec![max(col("value"))])?
                .build()?;

            test_plan(
                plan,
                "\
                Aggregate: groupBy=[[]], aggr=[[MAX(?table?.value)]]\
                \n  TableScan: ?table? projection=[value]",
                "\
                AggregateExec: mode=Final, gby=[], aggr=[MAX(?table?.value)]\
                \n  AggregateExec: mode=Partial, gby=[], aggr=[MAX(?table?.value)]\
                \n    EmptyExec: produce_one_row=false\
                \n",
            )
            .await?;
        }

        let plan = LogicalPlanBuilder::from(test_table_scan(true)?)
            .aggregate(Vec::<Expr>::new(), vec![max(col("value"))])?
            .build()?;

        test_plan(
            plan,
            "\
            Aggregate: groupBy=[[]], aggr=[[MAX(?table?.value)]]\
            \n  TableScan: ?table? projection=[value]",
            "\
            AggregateExec: mode=Final, gby=[], aggr=[MAX(?table?.value)]\
            \n  CoalescePartitionsExec\
            \n    AggregateExec: mode=Partial, gby=[], aggr=[MAX(?table?.value)]\
            \n      TskvExec: limit=None, predicate=ColumnDomains { column_to_domain: Some({}) }, split_num=8, projection=[value]\
            \n",
        ).await
    }

    #[tokio::test]
    async fn test_min_with_group() -> Result<()> {
        {
            let plan = LogicalPlanBuilder::from(test_table_scan(false)?)
                .aggregate(vec![col("flag")], vec![min(col("value"))])?
                .build()?;

            test_plan(
                plan,
                "\
                Aggregate: groupBy=[[?table?.flag]], aggr=[[MIN(?table?.value)]]\
                \n  TableScan: ?table? projection=[flag, value]",
                "\
                AggregateExec: mode=FinalPartitioned, gby=[flag@0 as flag], aggr=[MIN(?table?.value)]\
                \n  CoalesceBatchesExec: target_batch_size=8192\
                \n    RepartitionExec: partitioning=Hash([Column { name: \"flag\", index: 0 }], 8), input_partitions=8\
                \n      RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1\
                \n        AggregateExec: mode=Partial, gby=[flag@0 as flag], aggr=[MIN(?table?.value)]\
                \n          EmptyExec: produce_one_row=false\
                \n",
            ).await?;
        }

        let plan = LogicalPlanBuilder::from(test_table_scan(true)?)
            .aggregate(vec![col("value")], vec![min(col("value"))])?
            .build()?;

        test_plan(
            plan,
            "\
            Aggregate: groupBy=[[?table?.value]], aggr=[[MIN(?table?.value)]]\
            \n  TableScan: ?table? projection=[value]",
            "\
            AggregateExec: mode=FinalPartitioned, gby=[value@0 as value], aggr=[MIN(?table?.value)]\
            \n  CoalesceBatchesExec: target_batch_size=8192\
            \n    RepartitionExec: partitioning=Hash([Column { name: \"value\", index: 0 }], 8), input_partitions=8\
            \n      AggregateExec: mode=Partial, gby=[value@0 as value], aggr=[MIN(?table?.value)]\
            \n        TskvExec: limit=None, predicate=ColumnDomains { column_to_domain: Some({}) }, split_num=8, projection=[value]\
            \n",
        ).await
    }

    #[tokio::test]
    async fn test_min_without_group() -> Result<()> {
        {
            let plan = LogicalPlanBuilder::from(test_table_scan(false)?)
                .aggregate(Vec::<Expr>::new(), vec![min(col("value"))])?
                .build()?;

            test_plan(
                plan,
                "\
                Aggregate: groupBy=[[]], aggr=[[MIN(?table?.value)]]\
                \n  TableScan: ?table? projection=[value]",
                "\
                AggregateExec: mode=Final, gby=[], aggr=[MIN(?table?.value)]\
                \n  AggregateExec: mode=Partial, gby=[], aggr=[MIN(?table?.value)]\
                \n    EmptyExec: produce_one_row=false\
                \n",
            )
            .await?;
        }

        let plan = LogicalPlanBuilder::from(test_table_scan(true)?)
            .aggregate(Vec::<Expr>::new(), vec![min(col("value"))])?
            .build()?;

        test_plan(
            plan,
            "\
            Aggregate: groupBy=[[]], aggr=[[MIN(?table?.value)]]\
            \n  TableScan: ?table? projection=[value]",
            "\
            AggregateExec: mode=Final, gby=[], aggr=[MIN(?table?.value)]\
            \n  CoalescePartitionsExec\
            \n    AggregateExec: mode=Partial, gby=[], aggr=[MIN(?table?.value)]\
            \n      TskvExec: limit=None, predicate=ColumnDomains { column_to_domain: Some({}) }, split_num=8, projection=[value]\
            \n",
        ).await
    }

    #[tokio::test]
    async fn test_sum_with_group() -> Result<()> {
        {
            let plan = LogicalPlanBuilder::from(test_table_scan(false)?)
                .aggregate(vec![col("value")], vec![sum(col("value"))])?
                .build()?;

            test_plan(
                plan,
                "\
                Aggregate: groupBy=[[?table?.value]], aggr=[[SUM(?table?.value)]]\
                \n  TableScan: ?table? projection=[value]",
                "\
                AggregateExec: mode=FinalPartitioned, gby=[value@0 as value], aggr=[SUM(?table?.value)]\
                \n  CoalesceBatchesExec: target_batch_size=8192\
                \n    RepartitionExec: partitioning=Hash([Column { name: \"value\", index: 0 }], 8), input_partitions=8\
                \n      RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1\
                \n        AggregateExec: mode=Partial, gby=[value@0 as value], aggr=[SUM(?table?.value)]\
                \n          EmptyExec: produce_one_row=false\
                \n",
            ).await?;
        }

        let plan = LogicalPlanBuilder::from(test_table_scan(true)?)
            .aggregate(vec![col("value")], vec![sum(col("value"))])?
            .build()?;

        test_plan(
            plan,
            "\
            Aggregate: groupBy=[[?table?.value]], aggr=[[SUM(?table?.value)]]\
            \n  TableScan: ?table? projection=[value]",
            "\
            AggregateExec: mode=FinalPartitioned, gby=[value@0 as value], aggr=[SUM(?table?.value)]\
            \n  CoalesceBatchesExec: target_batch_size=8192\
            \n    RepartitionExec: partitioning=Hash([Column { name: \"value\", index: 0 }], 8), input_partitions=8\
            \n      AggregateExec: mode=Partial, gby=[value@0 as value], aggr=[SUM(?table?.value)]\
            \n        TskvExec: limit=None, predicate=ColumnDomains { column_to_domain: Some({}) }, split_num=8, projection=[value]\
            \n",
        ).await
    }

    #[tokio::test]
    async fn test_sum_without_group() -> Result<()> {
        {
            let plan = LogicalPlanBuilder::from(test_table_scan(false)?)
                .aggregate(Vec::<Expr>::new(), vec![sum(col("value"))])?
                .build()?;

            test_plan(
                plan,
                "\
                Aggregate: groupBy=[[]], aggr=[[SUM(?table?.value)]]\
                \n  TableScan: ?table? projection=[value]",
                "\
                AggregateExec: mode=Final, gby=[], aggr=[SUM(?table?.value)]\
                \n  AggregateExec: mode=Partial, gby=[], aggr=[SUM(?table?.value)]\
                \n    EmptyExec: produce_one_row=false\
                \n",
            )
            .await?;
        }

        let plan = LogicalPlanBuilder::from(test_table_scan(true)?)
            .aggregate(Vec::<Expr>::new(), vec![sum(col("value"))])?
            .build()?;

        test_plan(
            plan,
            "\
            Aggregate: groupBy=[[]], aggr=[[SUM(?table?.value)]]\
            \n  TableScan: ?table? projection=[value]",
            "\
            AggregateExec: mode=Final, gby=[], aggr=[SUM(?table?.value)]\
            \n  CoalescePartitionsExec\
            \n    AggregateExec: mode=Partial, gby=[], aggr=[SUM(?table?.value)]\
            \n      TskvExec: limit=None, predicate=ColumnDomains { column_to_domain: Some({}) }, split_num=8, projection=[value]\
            \n",
        ).await
    }
}
