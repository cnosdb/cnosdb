use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::logical_expr::expr_rewriter::ExprRewritable;
use datafusion::logical_expr::{
    Aggregate, Extension, Filter, LogicalPlan, Projection, Repartition, Sort, Window,
};
use datafusion::prelude::{Expr, Partitioning};

use crate::extension::expr::expr_rewriter::ExprReplacer;

pub mod expand;
pub mod stream_scan;
pub mod table_writer;
pub mod table_writer_merge;
pub mod tag_scan;
pub mod topk;
pub mod watermark;

pub trait LogicalPlanExt: Sized {
    type Error;
    fn transform_expressions_down<F>(&self, f: &F) -> Result<Self, Self::Error>
    where
        F: Fn(&Expr) -> Option<Expr>;
}

impl LogicalPlanExt for LogicalPlan {
    type Error = DataFusionError;

    fn transform_expressions_down<F>(&self, f: &F) -> Result<Self, Self::Error>
    where
        F: Fn(&Expr) -> Option<Expr>,
    {
        let mut replacer = ExprReplacer::new(f);

        match self {
            LogicalPlan::Projection(Projection {
                expr,
                input,
                schema,
                ..
            }) => {
                let new_exprs = expr
                    .iter()
                    .cloned()
                    .map(|e| e.rewrite(&mut replacer))
                    .collect::<DFResult<Vec<_>>>()?;

                Ok(LogicalPlan::Projection(Projection::try_new_with_schema(
                    new_exprs,
                    input.clone(),
                    schema.clone(),
                )?))
            }
            LogicalPlan::Filter(Filter {
                predicate, input, ..
            }) => {
                let new_predicate = predicate.clone().rewrite(&mut replacer)?;

                Ok(LogicalPlan::Filter(Filter::try_new(
                    new_predicate,
                    input.clone(),
                )?))
            }
            LogicalPlan::Repartition(Repartition {
                partitioning_scheme,
                input,
            }) => {
                let new_partitioning_scheme = match partitioning_scheme {
                    Partitioning::Hash(expr, partitions) => {
                        let new_exprs = expr
                            .iter()
                            .cloned()
                            .map(|e| e.rewrite(&mut replacer))
                            .collect::<DFResult<Vec<_>>>()?;

                        Partitioning::Hash(new_exprs, *partitions)
                    }
                    Partitioning::DistributeBy(expr) => {
                        let new_exprs = expr
                            .iter()
                            .cloned()
                            .map(|e| e.rewrite(&mut replacer))
                            .collect::<DFResult<Vec<_>>>()?;

                        Partitioning::DistributeBy(new_exprs)
                    }
                    Partitioning::RoundRobinBatch(_) => partitioning_scheme.clone(),
                };

                Ok(LogicalPlan::Repartition(Repartition {
                    partitioning_scheme: new_partitioning_scheme,
                    input: input.clone(),
                }))
            }
            LogicalPlan::Window(Window {
                input,
                window_expr,
                schema,
            }) => {
                let new_exprs = window_expr
                    .iter()
                    .cloned()
                    .map(|e| e.rewrite(&mut replacer))
                    .collect::<DFResult<Vec<_>>>()?;

                Ok(LogicalPlan::Window(Window {
                    input: input.clone(),
                    window_expr: new_exprs,
                    schema: schema.clone(),
                }))
            }
            LogicalPlan::Aggregate(Aggregate {
                group_expr,
                aggr_expr,
                input,
                schema,
                ..
            }) => {
                let new_group_expr = group_expr
                    .iter()
                    .cloned()
                    .map(|e| e.rewrite(&mut replacer))
                    .collect::<DFResult<Vec<_>>>()?;
                let new_aggr_expr = aggr_expr
                    .iter()
                    .cloned()
                    .map(|e| e.rewrite(&mut replacer))
                    .collect::<DFResult<Vec<_>>>()?;

                Ok(LogicalPlan::Aggregate(Aggregate::try_new_with_schema(
                    input.clone(),
                    new_group_expr,
                    new_aggr_expr,
                    schema.clone(),
                )?))
            }
            LogicalPlan::Sort(Sort { expr, input, fetch }) => {
                let new_exprs = expr
                    .iter()
                    .cloned()
                    .map(|e| e.rewrite(&mut replacer))
                    .collect::<DFResult<Vec<_>>>()?;

                Ok(LogicalPlan::Sort(Sort {
                    expr: new_exprs,
                    input: input.clone(),
                    fetch: *fetch,
                }))
            }
            LogicalPlan::Extension(extension) => {
                // would be nice to avoid this copy -- maybe can
                // update extension to just observer Exprs
                let new_exprs = extension
                    .node
                    .expressions()
                    .into_iter()
                    .map(|e| e.rewrite(&mut replacer))
                    .collect::<DFResult<Vec<_>>>()?;
                let inputs = extension
                    .node
                    .inputs()
                    .into_iter()
                    .cloned()
                    .collect::<Vec<_>>();
                let ext_plan = extension.node.from_template(&new_exprs, &inputs);
                Ok(LogicalPlan::Extension(Extension { node: ext_plan }))
            }
            // plans without expressions
            LogicalPlan::EmptyRelation(_)
            | LogicalPlan::Subquery(_)
            | LogicalPlan::SubqueryAlias(_)
            | LogicalPlan::Limit(_)
            | LogicalPlan::CreateExternalTable(_)
            | LogicalPlan::CreateMemoryTable(_)
            | LogicalPlan::CreateView(_)
            | LogicalPlan::CreateCatalogSchema(_)
            | LogicalPlan::CreateCatalog(_)
            | LogicalPlan::DropTable(_)
            | LogicalPlan::SetVariable(_)
            | LogicalPlan::DropView(_)
            | LogicalPlan::CrossJoin(_)
            | LogicalPlan::Analyze(_)
            | LogicalPlan::Explain(_)
            | LogicalPlan::Union(_)
            | LogicalPlan::Distinct(_)
            | LogicalPlan::Dml(_)
            | LogicalPlan::DescribeTable(_)
            | LogicalPlan::TableScan(_)
            | LogicalPlan::Values(_)
            | LogicalPlan::Join(_)
            | LogicalPlan::Unnest(_)
            | LogicalPlan::Prepare(_) => Ok(self.clone()),
        }
    }
}
