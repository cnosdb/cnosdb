use std::sync::Arc;

use datafusion::common::{DFField, DFSchema, Result as DFResult};
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::{
    Extension, LogicalPlan, LogicalPlanBuilder, Projection, TableSource,
};
use datafusion::prelude::{cast, lit, Expr};
use datafusion::scalar::ScalarValue;
use models::schema::Watermark;
use spi::query::datasource::stream::StreamProviderRef;
use spi::query::logical_planner::{affected_row_expr, merge_affected_row_expr};
use spi::QueryError;
use trace::debug;

use super::plan_node::expand::ExpandNode;
use super::plan_node::stream_scan::StreamScanPlanNode;
use super::plan_node::table_writer_merge::TableWriterMergePlanNode;
use super::plan_node::watermark::WatermarkNode;
use crate::extension::logical::plan_node::table_writer::TableWriterPlanNode;

/// Used to extend the function of datafusion's [`LogicalPlanBuilder`]
pub trait LogicalPlanBuilderExt: Sized {
    /// Apply a expand with specific projections
    fn expand(self, projections: Vec<Vec<Expr>>) -> Result<Self>;
    fn watermark(self, watermark: Watermark) -> Result<Self>;
    fn stream_scan(table_name: impl Into<String>, table_source: StreamProviderRef) -> Result<Self>;
    fn write(
        self,
        target_table: Arc<dyn TableSource>,
        table_name: &str,
        insert_columns: &[String],
    ) -> DFResult<Self>;
}

impl LogicalPlanBuilderExt for LogicalPlanBuilder {
    fn expand(self, projections: Vec<Vec<Expr>>) -> Result<Self> {
        let input = Arc::new(self.build()?);

        let expand = Arc::new(ExpandNode::try_new(projections, input)?);

        let plan = LogicalPlan::Extension(Extension { node: expand });

        Ok(Self::from(plan))
    }

    fn watermark(self, watermark: Watermark) -> Result<Self> {
        let input = Arc::new(self.build()?);

        let watermark_node = Arc::new(WatermarkNode::try_new(watermark, input)?);

        let plan = LogicalPlan::Extension(Extension {
            node: watermark_node,
        });

        Ok(Self::from(plan))
    }

    /// Convert a stream provider into a builder with a [`StreamScanPlanNode`]
    fn stream_scan(table_name: impl Into<String>, table_source: StreamProviderRef) -> Result<Self> {
        let table_name = table_name.into();

        if table_name.is_empty() {
            return Err(DataFusionError::Plan(
                "table_name cannot be empty".to_string(),
            ));
        }

        let schema = table_source.schema();

        let projected_schema = DFSchema::try_from_qualified_schema(&table_name, &schema)?;

        let node = Arc::new(StreamScanPlanNode {
            table_name,
            source: table_source,
            projected_schema: Arc::new(projected_schema),
            projection: None,
            filters: vec![],
            agg_with_grouping: None,
        });

        Ok(Self::from(LogicalPlan::Extension(Extension { node })))
    }

    fn write(
        self,
        target_table: Arc<dyn TableSource>,
        table_name: &str,
        insert_columns: &[String],
    ) -> DFResult<Self> {
        let insert_columns = extract_column_names(insert_columns, target_table.clone());

        let source_plan = self.build()?;

        debug!("Build writer plan: target table schema: {:?}, insert_columns: {:?}, source schema: {:?}",
            target_table.schema(),
            insert_columns,
            source_plan.schema(),
        );

        debug!(
            "Build writer plan: source plan:\n{}",
            source_plan.display_indent_schema(),
        );

        // Check if the plan is legal
        semantic_check(insert_columns.as_ref(), &source_plan, target_table.clone())
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        let final_source_logical_plan = add_projection_between_source_and_insert_node_if_necessary(
            target_table.clone(),
            source_plan,
            insert_columns.as_ref(),
        )?;

        let plan = table_write_plan_node(table_name, target_table, final_source_logical_plan)?;

        debug!(
            "Build writer plan: final plan:\n{}",
            plan.display_indent_schema(),
        );

        Ok(Self::from(plan))
    }
}

fn extract_column_names(
    columns: &[String],
    target_table_source_ref: Arc<dyn TableSource>,
) -> Vec<String> {
    if columns.is_empty() {
        target_table_source_ref
            .schema()
            .fields()
            .iter()
            .map(|e| e.name().clone())
            .collect()
    } else {
        columns.to_vec()
    }
}

fn semantic_check(
    insert_columns: &[String],
    source_plan: &LogicalPlan,
    target_table: Arc<dyn TableSource>,
) -> Result<(), QueryError> {
    let target_table_schema = target_table.schema();
    let target_table_fields = target_table_schema.fields();

    let source_field_num = source_plan.schema().fields().len();
    let insert_field_num = insert_columns.len();
    let target_table_field_num = target_table_fields.len();

    if insert_field_num > source_field_num {
        return Err(QueryError::MismatchColumns);
    }

    if insert_field_num == 0 && source_field_num != target_table_field_num {
        return Err(QueryError::MismatchColumns);
    }
    // The target table must contain all insert fields
    for insert_col in insert_columns {
        target_table_fields
            .iter()
            .find(|e| e.name() == insert_col)
            .ok_or_else(|| QueryError::MissingColumn {
                insert_col: insert_col.to_string(),
                fields: target_table_fields
                    .iter()
                    .map(|e| e.name().as_str())
                    .collect::<Vec<&str>>()
                    .join(","),
            })?;
    }

    Ok(())
}

/// Add a projection operation (if necessary)
/// 1. Iterate over all fields of the table
///   1.1. Construct the col expression
///   1.2. Check if the current field exists in columns
///     1.2.1. does not exist: add cast(null as target_type) expression to save
///     1.2.1. Exist: save if the type matches, add cast(expr as target_type) to save if it does not exist
fn add_projection_between_source_and_insert_node_if_necessary(
    target_table: Arc<dyn TableSource>,
    source_plan: LogicalPlan,
    insert_columns: &[String],
) -> DFResult<LogicalPlan> {
    let insert_col_name_with_source_field_tuples: Vec<(&String, &DFField)> = insert_columns
        .iter()
        .zip(source_plan.schema().fields())
        .collect();

    debug!(
        "Insert col name with source field tuples: {:?}",
        insert_col_name_with_source_field_tuples
    );
    debug!("Target table: {:?}", target_table.schema());

    let assignments: Vec<Expr> = target_table
        .schema()
        .fields()
        .iter()
        .map(|column| {
            let target_column_name = column.name();
            let target_column_data_type = column.data_type();

            let expr = if let Some((_, source_field)) = insert_col_name_with_source_field_tuples
                .iter()
                .find(|(insert_col_name, _)| *insert_col_name == target_column_name)
            {
                // insert column exists in the target table
                if source_field.data_type() == target_column_data_type {
                    // save if type matches col(source_field_name)
                    Expr::Column(source_field.qualified_column())
                } else {
                    // Add cast(source_col as target_type) if it doesn't exist
                    cast(
                        Expr::Column(source_field.qualified_column()),
                        target_column_data_type.clone(),
                    )
                }
            } else {
                // The specified column in the target table is missing from the insert
                // then add cast(null as target_type)
                cast(lit(ScalarValue::Null), target_column_data_type.clone())
            };

            expr.alias(target_column_name)
        })
        .collect();

    debug!("assignments: {:?}", &assignments);

    Ok(LogicalPlan::Projection(Projection::try_new(
        assignments,
        Arc::new(source_plan),
    )?))
}

fn table_write_plan_node(
    table_name: impl Into<String>,
    target_table: Arc<dyn TableSource>,
    input: LogicalPlan,
) -> DFResult<LogicalPlan> {
    let input_exprs = input
        .schema()
        .fields()
        .iter()
        .map(|f| Expr::Column(f.qualified_column()))
        .collect::<Vec<Expr>>();
    let affected_row_expr = affected_row_expr(input_exprs);

    // construct table writer logical node
    let plan = TableWriterPlanNode::try_new(
        table_name.into(),
        target_table,
        Arc::new(input),
        vec![affected_row_expr],
    )?;

    let plan =
        TableWriterMergePlanNode::try_new(Arc::new(plan.into()), vec![merge_affected_row_expr()])?;

    Ok(plan.into())

    // let group_expr: Vec<Expr> = vec![];

    // LogicalPlanBuilder::from(df_plan)
    //     .aggregate(group_expr, vec![merge_affected_row_expr()])?
    //     .build()
}
