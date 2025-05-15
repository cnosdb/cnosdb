use std::sync::Arc;

use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::config::ConfigOptions;
use datafusion::error::Result;
use datafusion::functions_aggregate::count::Count;
use datafusion::logical_expr::expr::{AggregateFunction, AggregateFunctionParams};
use datafusion::logical_expr::{Aggregate, AggregateUDF, LogicalPlan};
use datafusion::optimizer::analyzer::AnalyzerRule;
use datafusion::prelude::Expr;

/// convert exact_count to count, but unsupported pushdown
pub struct TransformExactCountToCountRule {}

impl AnalyzerRule for TransformExactCountToCountRule {
    fn analyze(&self, plan: LogicalPlan, _config: &ConfigOptions) -> Result<LogicalPlan> {
        plan.transform_up(&analyze_internal)
    }

    fn name(&self) -> &str {
        "transform_exact_count_to_count"
    }
}

fn analyze_internal(plan: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
    if let LogicalPlan::Projection(mut projection) = plan {
        if let LogicalPlan::Aggregate(aggregate) = projection.input.as_ref() {
            if transform_expressions(&mut projection.expr) {
                // Create a new `Aggregate` with transformed UDFs
                let new_aggregate = transform_aggregation(aggregate.clone())?;
                projection.input = Arc::new(LogicalPlan::Aggregate(new_aggregate));
            }
            return Ok(Transformed::Yes(LogicalPlan::Projection(projection)));
        }
        return Ok(Transformed::No(LogicalPlan::Projection(projection)));
    }

    Ok(Transformed::No(plan))
}

/// Transform logical plan's expressions, return true if any transformation is done:
/// - `exact_count(<expr>)` to  `COUNT(<expr>)`.
fn transform_expressions(expressions: &mut [Expr]) -> bool {
    let mut transformed = false;
    for expr in expressions {
        if let Expr::Column(c) = expr {
            if let Some(suffix) = c.name.strip_prefix("exact_count") {
                c.name = format!("COUNT{suffix}");
                transformed = true;
            }
        }
    }
    transformed
}

/// Transform UDF definitions of the plan's `expressions`:
/// - `AggregateUDF::exact_count(<expr>)` to `AggregateFunction::COUNT(<expr>)`
fn transform_aggregation(aggregate: Aggregate) -> Result<Aggregate> {
    let mut new_aggr_exprs = Vec::with_capacity(aggregate.aggr_expr.len());
    for aggr_expr in aggregate.aggr_expr {
        match aggr_expr {
            Expr::AggregateFunction(udaf) if udaf.func.name == "exact_count" => {
                let new_function = AggregateFunction {
                    func: Arc::new(AggregateUDF::new_from_impl(Count::new())),
                    params: AggregateFunctionParams {
                        args: udaf.params.args,
                        distinct: false,
                        filter: udaf.params.filter,
                        order_by: udaf.params.order_by,
                        null_treatment: udaf.params.null_treatment,
                        can_be_pushed_down: false,
                    },
                };
                new_aggr_exprs.push(Expr::AggregateFunction(new_function));
            }
            _ => new_aggr_exprs.push(aggr_expr),
        }
    }

    Aggregate::try_new(aggregate.input, aggregate.group_expr, new_aggr_exprs)
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use datafusion::datasource::MemTable;
    use datafusion::prelude::*;
    use models::arrow::{DataType, Field, Schema};

    use crate::extension::analyse::transform_exact_count_to_count::{
        analyze_internal, transform_expressions,
    };
    use crate::extension::expr::func_manager::DFSessionContextFuncAdapter;
    use crate::extension::expr::load_all_functions;

    #[test]
    fn test_transform_expressions() {
        {
            let mut exprs = vec![col("a"), col("b")];

            let result = transform_expressions(&mut exprs);
            assert!(!result);
            assert_eq!(exprs, vec![col("a"), col("b"),]);
        }
        {
            let mut exprs = vec![
                col("a"),
                col("exact_count(b)"),
                col("exact_count(c)"),
                col("d"),
            ];

            let result = transform_expressions(&mut exprs);
            assert!(result);
            assert_eq!(
                exprs,
                vec![col("a"), col("COUNT(b)"), col("COUNT(c)"), col("d"),]
            );
        }
    }

    fn ctx() -> SessionContext {
        let mut ctx = SessionContext::new();

        let mem_table = MemTable::try_new(
            Arc::new(Schema::new(vec![
                Field::new("a", DataType::Int64, true),
                Field::new("b", DataType::Boolean, true),
                Field::new("c", DataType::Utf8, true),
                Field::new("d", DataType::Float64, true),
            ])),
            vec![],
        )
        .unwrap();
        ctx.register_table("t", Arc::new(mem_table)).unwrap();

        let mut func_manager = DFSessionContextFuncAdapter::new(&mut ctx);
        load_all_functions(&mut func_manager).expect("load_all_functions");

        ctx
    }

    #[tokio::test]
    async fn test_analyze_internal() {
        {
            let df = ctx()
                .sql("SELECT exACT_CoUNt(a), b FROM t GROUP BY b")
                .await
                .unwrap();
            let (plan_2, was_transformed) = analyze_internal(df.logical_plan().clone())
                .unwrap()
                .into_pair();
            assert!(was_transformed);
            assert_eq!(
                format!("{plan_2:?}"),
                "Projection: COUNT(t.a), t.b\
                    \n  Aggregate: groupBy=[[t.b]], aggr=[[COUNT(t.a)]]\
                    \n    TableScan: t"
            );
        }
        {
            let df = ctx()
                .sql("SELECT count(a), b, exact_count(c), max(d) FROM t GROUP BY b")
                .await
                .unwrap();
            let (plan_2, was_transformed) = analyze_internal(df.logical_plan().clone())
                .unwrap()
                .into_pair();
            assert!(was_transformed);
            assert_eq!(
                format!("{plan_2:?}"),
                "Projection: COUNT(t.a), t.b, COUNT(t.c), MAX(t.d)\
                    \n  Aggregate: groupBy=[[t.b]], aggr=[[COUNT(t.a), COUNT(t.c), MAX(t.d)]]\
                    \n    TableScan: t"
            );
        }
    }
}
