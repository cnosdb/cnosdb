//! Projection Push Down optimizer rule ensures that only referenced columns are
//! loaded into memory

use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::Arc;

use datafusion::arrow::datatypes::Field;
use datafusion::arrow::error::Result as ArrowResult;
use datafusion::common::{
    Column, DFField, DFSchema, DFSchemaRef, DataFusionError, Result, ToDFSchema,
};
use datafusion::datasource::source_as_provider;
use datafusion::logical_expr::logical_plan::Analyze;
use datafusion::logical_expr::utils::{
    expr_to_columns, exprlist_to_columns, find_sort_exprs, from_plan, grouping_set_to_exprlist,
};
use datafusion::logical_expr::{
    build_join_schema, Aggregate, Join, LogicalPlan, LogicalPlanBuilder, Projection, SubqueryAlias,
    TableScan, Union, Window,
};
use datafusion::optimizer::{OptimizerConfig, OptimizerRule};
use datafusion::prelude::Expr;

use crate::data_source::table_provider::tskv::ClusterTable;

/// Optimizer that removes unused projections and aggregations from plans
/// This reduces both scans and
#[derive(Default)]
pub struct PushDownProjectionAdapter {}

impl OptimizerRule for PushDownProjectionAdapter {
    fn try_optimize(
        &self,
        plan: &LogicalPlan,
        config: &dyn OptimizerConfig,
    ) -> Result<Option<LogicalPlan>> {
        // set of all columns referred by the plan (and thus considered required by the root)
        let required_columns = plan
            .schema()
            .fields()
            .iter()
            .map(|f| f.qualified_column())
            .collect::<HashSet<Column>>();
        Ok(Some(optimize_plan(
            self,
            plan,
            &required_columns,
            false,
            config,
        )?))
    }

    fn name(&self) -> &str {
        "push_down_projection"
    }
}

impl PushDownProjectionAdapter {
    #[allow(missing_docs)]
    pub fn new() -> Self {
        Self {}
    }
}

/// Recursively transverses the logical plan removing expressions and that are not needed.
fn optimize_plan(
    _optimizer: &PushDownProjectionAdapter,
    plan: &LogicalPlan,
    required_columns: &HashSet<Column>, // set of columns required up to this step
    has_projection: bool,
    _config: &dyn OptimizerConfig,
) -> Result<LogicalPlan> {
    let mut new_required_columns = required_columns.clone();
    let new_plan = match plan {
        LogicalPlan::Projection(Projection {
            input,
            expr,
            schema,
            ..
        }) => {
            // projection:
            // * remove any expression that is not required
            // * construct the new set of required columns

            let mut new_expr = Vec::new();
            let mut new_fields = Vec::new();
            // When meet projection, its expr must contain all columns that its child need.
            // So we need create a empty required_columns instead use original new_required_columns.
            // Otherwise it cause redundant columns.
            let mut new_required_columns = HashSet::new();

            // Gather all columns needed for expressions in this Projection
            schema.fields().iter().enumerate().for_each(|(i, field)| {
                if required_columns.contains(&field.qualified_column()) {
                    new_expr.push(expr[i].clone());
                    new_fields.push(field.clone());
                }
            });

            for e in new_expr.iter() {
                expr_to_columns(e, &mut new_required_columns)?
            }

            let new_input = optimize_plan(_optimizer, input, &new_required_columns, true, _config)?;

            let new_required_columns_optimized = new_input
                .schema()
                .fields()
                .iter()
                .map(|f| f.qualified_column())
                .collect::<HashSet<Column>>();

            let all_column_exprs = new_expr.iter().all(|e| matches!(e, Expr::Column(_)));

            if new_fields.is_empty()
                || (has_projection
                    && all_column_exprs
                    && &new_required_columns_optimized == required_columns)
            {
                // no need for an expression at all
                Ok(new_input)
            } else {
                let metadata = new_input.schema().metadata().clone();
                Ok(LogicalPlan::Projection(Projection::try_new_with_schema(
                    new_expr,
                    Arc::new(new_input),
                    DFSchemaRef::new(DFSchema::new_with_metadata(new_fields, metadata)?),
                )?))
            }
        }
        LogicalPlan::Join(Join {
            left,
            right,
            on,
            filter,
            join_type,
            join_constraint,
            null_equals_null,
            ..
        }) => {
            for (l, r) in on {
                new_required_columns.extend(l.to_columns()?);
                new_required_columns.extend(r.to_columns()?);
            }

            if let Some(expr) = filter {
                expr_to_columns(expr, &mut new_required_columns)?;
            }

            let optimized_left = Arc::new(optimize_plan(
                _optimizer,
                left,
                &new_required_columns,
                true,
                _config,
            )?);

            let optimized_right = Arc::new(optimize_plan(
                _optimizer,
                right,
                &new_required_columns,
                true,
                _config,
            )?);

            let schema =
                build_join_schema(optimized_left.schema(), optimized_right.schema(), join_type)?;

            Ok(LogicalPlan::Join(Join {
                left: optimized_left,
                right: optimized_right,
                join_type: *join_type,
                join_constraint: *join_constraint,
                on: on.clone(),
                filter: filter.clone(),
                schema: DFSchemaRef::new(schema),
                null_equals_null: *null_equals_null,
            }))
        }
        LogicalPlan::Window(Window {
            window_expr, input, ..
        }) => {
            // Gather all columns needed for expressions in this Window
            let mut new_window_expr = Vec::new();
            {
                window_expr.iter().try_for_each(|expr| {
                    let name = &expr.display_name()?;
                    let column = Column::from_name(name);
                    if required_columns.contains(&column) {
                        new_window_expr.push(expr.clone());
                        new_required_columns.insert(column);
                        // add to the new set of required columns
                        expr_to_columns(expr, &mut new_required_columns)
                    } else {
                        Ok(())
                    }
                })?;
            }

            // none columns in window expr are needed, remove the window expr
            if new_window_expr.is_empty() {
                return LogicalPlanBuilder::from(optimize_plan(
                    _optimizer,
                    input,
                    required_columns,
                    true,
                    _config,
                )?)
                .build();
            };

            // for all the retained window expr, find their sort expressions if any, and retain these
            exprlist_to_columns(
                &find_sort_exprs(&new_window_expr),
                &mut new_required_columns,
            )?;

            LogicalPlanBuilder::from(optimize_plan(
                _optimizer,
                input,
                &new_required_columns,
                true,
                _config,
            )?)
            .window(new_window_expr)?
            .build()
        }
        LogicalPlan::Aggregate(Aggregate {
            group_expr,
            aggr_expr,
            input,
            ..
        }) => {
            // aggregate:
            // * remove any aggregate expression that is not required
            // * construct the new set of required columns

            // Find distinct group by exprs in the case where we have a grouping set
            let all_group_expr: Vec<Expr> = grouping_set_to_exprlist(group_expr)?;
            exprlist_to_columns(&all_group_expr, &mut new_required_columns)?;

            // Gather all columns needed for expressions in this Aggregate
            let mut new_aggr_expr = Vec::new();
            aggr_expr.iter().try_for_each(|expr| {
                let name = &expr.display_name()?;
                let column = Column::from_name(name);
                if required_columns.contains(&column) {
                    new_aggr_expr.push(expr.clone());
                    new_required_columns.insert(column);

                    // add to the new set of required columns
                    expr_to_columns(expr, &mut new_required_columns)
                } else {
                    Ok(())
                }
            })?;

            Ok(LogicalPlan::Aggregate(Aggregate::try_new(
                Arc::new(optimize_plan(
                    _optimizer,
                    input,
                    &new_required_columns,
                    true,
                    _config,
                )?),
                group_expr.clone(),
                new_aggr_expr,
            )?))
        }
        // scans:
        // * remove un-used columns from the scan projection
        LogicalPlan::TableScan(scan) if scan.agg_with_grouping.is_none() => {
            // filter expr may not exist in expr in projection.
            // like: TableScan: t1 projection=[bool_col, int_col], full_filters=[t1.id = Int32(1)]
            // projection=[bool_col, int_col] don't contain `ti.id`.
            exprlist_to_columns(&scan.filters, &mut new_required_columns)?;
            push_down_scan(scan, &new_required_columns, has_projection)
        }
        LogicalPlan::Explain { .. } => Err(DataFusionError::Internal(
            "Unsupported logical plan: Explain must be root of the plan".to_string(),
        )),
        LogicalPlan::Analyze(a) => {
            // make sure we keep all the columns from the input plan
            let required_columns = a
                .input
                .schema()
                .fields()
                .iter()
                .map(|f| f.qualified_column())
                .collect::<HashSet<Column>>();

            Ok(LogicalPlan::Analyze(Analyze {
                input: Arc::new(optimize_plan(
                    _optimizer,
                    &a.input,
                    &required_columns,
                    false,
                    _config,
                )?),
                verbose: a.verbose,
                schema: a.schema.clone(),
            }))
        }
        LogicalPlan::Union(Union { inputs, schema }) => {
            // UNION inputs will reference the same column with different identifiers, so we need
            // to populate new_required_columns by unqualified column name based on required fields
            // from the resulting UNION output
            let union_required_fields = schema
                .fields()
                .iter()
                .filter(|f| new_required_columns.contains(&f.qualified_column()))
                .map(|f| f.field())
                .collect::<HashSet<&Field>>();
            let new_inputs = inputs
                .iter()
                .map(|input_plan| {
                    input_plan
                        .schema()
                        .fields()
                        .iter()
                        .filter(|f| union_required_fields.contains(f.field()))
                        .for_each(|f| {
                            new_required_columns.insert(f.qualified_column());
                        });
                    optimize_plan(
                        _optimizer,
                        input_plan,
                        &new_required_columns,
                        has_projection,
                        _config,
                    )
                })
                .collect::<Result<Vec<_>>>()?;
            let new_schema = DFSchema::new_with_metadata(
                schema
                    .fields()
                    .iter()
                    .filter(|f| union_required_fields.contains(f.field()))
                    .cloned()
                    .collect(),
                schema.metadata().clone(),
            )?;
            Ok(LogicalPlan::Union(Union {
                inputs: new_inputs.iter().cloned().map(Arc::new).collect(),
                schema: Arc::new(new_schema),
            }))
        }
        LogicalPlan::SubqueryAlias(SubqueryAlias { input, alias, .. }) => {
            let new_required_columns = replace_alias(required_columns, alias, input.schema());
            let child = optimize_plan(
                _optimizer,
                input,
                &new_required_columns,
                has_projection,
                _config,
            )?;
            from_plan(plan, &plan.expressions(), &[child])
        }
        // at a distinct, all columns are required
        LogicalPlan::Distinct(distinct) => {
            let new_required_columns = distinct
                .input
                .schema()
                .fields()
                .iter()
                .map(|f| f.qualified_column())
                .collect();
            let child = optimize_plan(
                _optimizer,
                distinct.input.as_ref(),
                &new_required_columns,
                has_projection,
                _config,
            )?;
            from_plan(plan, &[], &[child])
        }
        // all other nodes: Add any additional columns used by
        // expressions in this node to the list of required columns
        LogicalPlan::Limit(_)
        | LogicalPlan::Filter { .. }
        | LogicalPlan::Repartition(_)
        | LogicalPlan::EmptyRelation(_)
        | LogicalPlan::Subquery(_)
        | LogicalPlan::Values(_)
        | LogicalPlan::Sort { .. }
        | LogicalPlan::CreateExternalTable(_)
        | LogicalPlan::CreateMemoryTable(_)
        | LogicalPlan::CreateView(_)
        | LogicalPlan::CreateCatalogSchema(_)
        | LogicalPlan::CreateCatalog(_)
        | LogicalPlan::DropTable(_)
        | LogicalPlan::DropView(_)
        | LogicalPlan::SetVariable(_)
        | LogicalPlan::DescribeTable(_)
        | LogicalPlan::CrossJoin(_)
        | LogicalPlan::Dml(_)
        | LogicalPlan::Unnest(_)
        | LogicalPlan::Extension { .. }
        | LogicalPlan::TableScan(_)
        | LogicalPlan::Prepare(_) => {
            let expr = plan.expressions();
            // collect all required columns by this plan
            exprlist_to_columns(&expr, &mut new_required_columns)?;

            // apply the optimization to all inputs of the plan
            let inputs = plan.inputs();
            let new_inputs = inputs
                .iter()
                .map(|input_plan| {
                    optimize_plan(
                        _optimizer,
                        input_plan,
                        &new_required_columns,
                        has_projection,
                        _config,
                    )
                })
                .collect::<Result<Vec<_>>>()?;

            from_plan(plan, &expr, &new_inputs)
        }
    };

    // when this rule is applied multiple times it will insert duplicate nested projections,
    // so we catch this here
    let with_dupe_projection_removed = match new_plan? {
        LogicalPlan::Projection(p) => match p.input.as_ref() {
            LogicalPlan::Projection(p2) if projection_equal(&p, p2) => {
                LogicalPlan::Projection(p2.clone())
            }
            _ => LogicalPlan::Projection(p),
        },
        other => other,
    };

    Ok(with_dupe_projection_removed)
}

fn projection_equal(p: &Projection, p2: &Projection) -> bool {
    p.expr.len() == p2.expr.len() && p.expr.iter().zip(&p2.expr).all(|(l, r)| l == r)
}

fn replace_alias(
    required_columns: &HashSet<Column>,
    alias: &str,
    input_schema: &DFSchemaRef,
) -> HashSet<Column> {
    let mut map = HashMap::new();
    for field in input_schema.fields() {
        let col = field.qualified_column();
        let alias_col = Column {
            relation: Some(alias.to_owned()),
            name: col.name.clone(),
        };
        map.insert(alias_col, col);
    }
    required_columns
        .iter()
        .map(|col| map.get(col).unwrap_or(col).clone())
        .collect::<HashSet<_>>()
}

fn push_down_scan(
    scan: &TableScan,
    required_columns: &HashSet<Column>,
    has_projection: bool,
) -> Result<LogicalPlan> {
    // once we reach the table scan, we can use the accumulated set of column
    // names to construct the set of column indexes in the scan
    //
    // we discard non-existing columns because some column names are not part of the schema,
    // e.g. when the column derives from an aggregation
    //
    // Use BTreeSet to remove potential duplicates (e.g. union) as
    // well as to sort the projection to ensure deterministic behavior
    let schema = scan.source.schema();
    let mut projection: BTreeSet<usize> = required_columns
        .iter()
        .filter(|c| c.relation.is_none() || c.relation.as_ref().unwrap() == &scan.table_name)
        .map(|c| schema.index_of(&c.name))
        .filter_map(ArrowResult::ok)
        .collect();

    if projection.is_empty() {
        if has_projection && !schema.fields().is_empty() {
            let table_provider = source_as_provider(&scan.source)?;
            // Ensure that we are reading at least one column from the table in case the query
            // does not reference any columns directly such as "SELECT COUNT(1) FROM table",
            // except when the table is empty (no column)
            let table_writer = table_provider.as_any().downcast_ref::<ClusterTable>();
            let idx = table_writer
                .map(|e| {
                    e.table_schema()
                        .fields()
                        .get(0)
                        .map(|f| {
                            let schema = e.table_schema();
                            schema.column_index(&f.name).unwrap_or(0)
                        })
                        .unwrap_or(0)
                })
                .unwrap_or(0);
            projection.insert(idx);
        } else {
            // for table scan without projection, we default to return all columns
            projection = scan
                .source
                .schema()
                .fields()
                .iter()
                .enumerate()
                .map(|(i, _)| i)
                .collect::<BTreeSet<usize>>();
        }
    }

    // create the projected schema
    let projected_fields: Vec<DFField> = projection
        .iter()
        .map(|i| DFField::from_qualified(&scan.table_name, schema.fields()[*i].clone()))
        .collect();

    let projection = projection.into_iter().collect::<Vec<_>>();
    let projected_schema = projected_fields.to_dfschema_ref()?;

    // return the table scan with projection
    Ok(LogicalPlan::TableScan(TableScan {
        table_name: scan.table_name.clone(),
        source: scan.source.clone(),
        projection: Some(projection),
        projected_schema,
        filters: scan.filters.clone(),
        fetch: scan.fetch,
        agg_with_grouping: scan.agg_with_grouping.clone(),
    }))
}
