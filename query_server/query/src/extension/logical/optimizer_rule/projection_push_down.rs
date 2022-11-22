//! Projection Push Down optimizer rule ensures that only referenced columns are
//! loaded into memory

use datafusion::arrow::datatypes::Field;
use datafusion::arrow::error::Result as ArrowResult;
use datafusion::common::{
    Column, DFField, DFSchema, DFSchemaRef, DataFusionError, Result, ToDFSchema,
};
use datafusion::datasource::source_as_provider;
use datafusion::datasource::TableProvider;
use datafusion::logical_expr::utils::grouping_set_to_exprlist;
use datafusion::logical_expr::Extension;
use datafusion::logical_expr::{
    logical_plan::{
        builder::{build_join_schema, LogicalPlanBuilder},
        Aggregate, Analyze, Join, LogicalPlan, Projection, SubqueryAlias, TableScan, Union, Window,
    },
    utils::{expr_to_columns, exprlist_to_columns, find_sort_exprs, from_plan},
    Expr,
};
use datafusion::optimizer::{OptimizerConfig, OptimizerRule};
use std::{
    collections::{BTreeSet, HashSet},
    sync::Arc,
};

use crate::extension::logical::plan_node::table_writer::{
    as_table_writer_plan_node, TableWriterPlanNode,
};
use crate::table::ClusterTable;

/// An adapter to [ProjectionPushDown].
///
/// the query does not reference any columns directly such as "SELECT COUNT(1) FROM table", auto assign the first field column
#[derive(Default)]
pub struct ProjectionPushDownAdapter {}

impl OptimizerRule for ProjectionPushDownAdapter {
    fn optimize(
        &self,
        plan: &LogicalPlan,
        optimizer_config: &mut OptimizerConfig,
    ) -> Result<LogicalPlan> {
        // set of all columns refered by the plan (and thus considered required by the root)
        let required_columns = plan
            .schema()
            .fields()
            .iter()
            .map(|f| f.qualified_column())
            .collect::<HashSet<Column>>();
        optimize_plan(self, plan, &required_columns, false, optimizer_config)
    }

    fn name(&self) -> &str {
        "projection_push_down"
    }
}

impl ProjectionPushDownAdapter {
    #[allow(missing_docs)]
    pub fn new() -> Self {
        Self {}
    }
}

fn get_projected_schema(
    table_name: Option<&String>,
    table_provider: Arc<dyn TableProvider>,
    required_columns: &HashSet<Column>,
    has_projection: bool,
) -> Result<(Vec<usize>, DFSchemaRef)> {
    let schema = table_provider.schema();
    // once we reach the table scan, we can use the accumulated set of column
    // names to construct the set of column indexes in the scan
    //
    // we discard non-existing columns because some column names are not part of the schema,
    // e.g. when the column derives from an aggregation
    //
    // Use BTreeSet to remove potential duplicates (e.g. union) as
    // well as to sort the projection to ensure deterministic behavior
    let mut projection: BTreeSet<usize> = required_columns
        .iter()
        .filter(|c| c.relation.is_none() || c.relation.as_ref() == table_name)
        .map(|c| schema.index_of(&c.name))
        .filter_map(ArrowResult::ok)
        .collect();

    if projection.is_empty() {
        if has_projection && !schema.fields().is_empty() {
            // Ensure that we are reading at least one column from the table in case the query
            // does not reference any columns directly such as "SELECT COUNT(1) FROM table",
            // except when the table is empty (no column)
            let table_writer = table_provider.as_any().downcast_ref::<ClusterTable>();
            let idx = table_writer
                .map(|e| {
                    e.table_schema()
                        .fields()
                        .get(0)
                        .map(|f| e.table_schema().column_index(&f.name).unwrap_or(&0))
                        .unwrap_or(&0)
                })
                .unwrap_or(&0);

            projection.insert(*idx);
        } else {
            // for table scan without projection, we default to return all columns
            projection = schema
                .fields()
                .iter()
                .enumerate()
                .map(|(i, _)| i)
                .collect::<BTreeSet<usize>>();
        }
    }

    // create the projected schema
    let projected_fields: Vec<DFField> = match table_name {
        Some(qualifer) => projection
            .iter()
            .map(|i| DFField::from_qualified(qualifer, schema.fields()[*i].clone()))
            .collect(),
        None => projection
            .iter()
            .map(|i| DFField::from(schema.fields()[*i].clone()))
            .collect(),
    };

    let projection = projection.into_iter().collect::<Vec<_>>();
    Ok((projection, projected_fields.to_dfschema_ref()?))
}

/// Recursively transverses the logical plan removing expressions and that are not needed.
fn optimize_plan(
    _optimizer: &ProjectionPushDownAdapter,
    plan: &LogicalPlan,
    required_columns: &HashSet<Column>, // set of columns required up to this step
    has_projection: bool,
    _optimizer_config: &OptimizerConfig,
) -> Result<LogicalPlan> {
    let mut new_required_columns = required_columns.clone();
    let new_plan = match plan {
        LogicalPlan::Projection(Projection {
            input,
            expr,
            schema,
            alias,
        }) => {
            // projection:
            // * remove any expression that is not required
            // * construct the new set of required columns

            let mut new_expr = Vec::new();
            let mut new_fields = Vec::new();

            // Gather all columns needed for expressions in this Projection
            schema
                .fields()
                .iter()
                .enumerate()
                .try_for_each(|(i, field)| {
                    if required_columns.contains(&field.qualified_column()) {
                        new_expr.push(expr[i].clone());
                        new_fields.push(field.clone());

                        // gather the new set of required columns
                        expr_to_columns(&expr[i], &mut new_required_columns)
                    } else {
                        Ok(())
                    }
                })?;

            let new_input = optimize_plan(
                _optimizer,
                input,
                &new_required_columns,
                true,
                _optimizer_config,
            )?;

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
                    alias.clone(),
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
                new_required_columns.insert(l.clone());
                new_required_columns.insert(r.clone());
            }

            if let Some(expr) = filter {
                expr_to_columns(expr, &mut new_required_columns)?;
            }

            let optimized_left = Arc::new(optimize_plan(
                _optimizer,
                left,
                &new_required_columns,
                true,
                _optimizer_config,
            )?);

            let optimized_right = Arc::new(optimize_plan(
                _optimizer,
                right,
                &new_required_columns,
                true,
                _optimizer_config,
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
                    _optimizer_config,
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
                _optimizer_config,
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
                    _optimizer_config,
                )?),
                group_expr.clone(),
                new_aggr_expr,
            )?))
        }
        // scans:
        // * remove un-used columns from the scan projection
        LogicalPlan::TableScan(TableScan {
            table_name,
            source,
            filters,
            fetch: limit,
            ..
        }) => {
            let provider = source_as_provider(source)?;

            let (projection, projected_schema) =
                get_projected_schema(Some(table_name), provider, required_columns, has_projection)?;
            // return the table scan with projection
            Ok(LogicalPlan::TableScan(TableScan {
                table_name: table_name.clone(),
                source: source.clone(),
                projection: Some(projection),
                projected_schema,
                filters: filters.clone(),
                fetch: *limit,
            }))
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
                    _optimizer_config,
                )?),
                verbose: a.verbose,
                schema: a.schema.clone(),
            }))
        }
        LogicalPlan::Union(Union {
            inputs,
            schema,
            alias,
        }) => {
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
                        _optimizer_config,
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
                alias: alias.clone(),
            }))
        }
        LogicalPlan::SubqueryAlias(SubqueryAlias { input, alias, .. }) => match input.as_ref() {
            LogicalPlan::TableScan(TableScan { table_name, .. }) => {
                let new_required_columns = new_required_columns
                    .iter()
                    .map(|c| match &c.relation {
                        Some(q) if q == alias => Column {
                            relation: Some(table_name.clone()),
                            name: c.name.clone(),
                        },
                        _ => c.clone(),
                    })
                    .collect();
                let new_inputs = vec![optimize_plan(
                    _optimizer,
                    input,
                    &new_required_columns,
                    has_projection,
                    _optimizer_config,
                )?];
                let expr = vec![];
                from_plan(plan, &expr, &new_inputs)
            }
            _ => Err(DataFusionError::Plan(
                "SubqueryAlias should only wrap TableScan".to_string(),
            )),
        },
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
        | LogicalPlan::CrossJoin(_)
        | LogicalPlan::Distinct(_)
        | LogicalPlan::Extension { .. } => {
            if let LogicalPlan::Extension(Extension { node }) = plan {
                if let Some(TableWriterPlanNode { input, .. }) =
                    as_table_writer_plan_node(node.as_ref())
                {
                    // table write node need all schema fields
                    input.schema().fields().iter().for_each(|e| {
                        new_required_columns.insert(e.qualified_column());
                    });
                }
            }

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
                        _optimizer_config,
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
    p.expr.len() == p2.expr.len()
        && p.alias == p2.alias
        && p.expr.iter().zip(&p2.expr).all(|(l, r)| l == r)
}
