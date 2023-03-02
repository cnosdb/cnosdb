use std::any::Any;
use std::fmt::{self, Debug};
use std::sync::Arc;

use datafusion::common::{DFField, DFSchema, DFSchemaRef};
use datafusion::error::DataFusionError;
use datafusion::logical_expr::utils::exprlist_to_fields;
use datafusion::logical_expr::{LogicalPlan, UserDefinedLogicalNode};
use datafusion::prelude::Expr;

#[derive(Clone)]
pub struct ExpandNode {
    pub projections: Vec<Vec<Expr>>,
    /// The incoming logical plan
    pub input: Arc<LogicalPlan>,
    pub expressions: Vec<Expr>,
    /// The schema description of the output
    pub schema: DFSchemaRef,
}

impl ExpandNode {
    /// Create a new ExpandNode
    pub fn try_new(
        projections: Vec<Vec<Expr>>,
        input: Arc<LogicalPlan>,
    ) -> Result<Self, DataFusionError> {
        // Check whether the output schemas of multiple projections are consistent,
        // and if they are inconsistent, an error will be reported
        let mut fields: Option<Vec<DFField>> = None;

        for proj in &projections {
            let proj_schema = exprlist_to_fields(proj, &input)?;

            if let Some(ref fields) = fields {
                if fields != &proj_schema {
                    return Err(DataFusionError::Internal(
                        format!("Expand's projections must have same output schema, but found: {fields:?} and {proj_schema:?}")));
                }
            } else {
                let _ = fields.insert(proj_schema);
            }
        }

        let fields = fields.ok_or_else(|| {
            DataFusionError::Internal("Projections of Expand not be empty".to_string())
        })?;

        let schema = Arc::new(DFSchema::new_with_metadata(
            fields,
            input.schema().metadata().clone(),
        )?);

        let expressions = projections.iter().flatten().cloned().collect::<Vec<_>>();
        Ok(Self {
            projections,
            input,
            expressions,
            schema,
        })
    }
}

impl Debug for ExpandNode {
    /// For ExpandNode, use explain format for the Debug format. Other types
    /// of nodes may
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.fmt_for_explain(f)
    }
}

impl UserDefinedLogicalNode for ExpandNode {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![self.input.as_ref()]
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }

    fn expressions(&self) -> Vec<Expr> {
        self.expressions.clone()
    }

    fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let projections = self
            .projections
            .iter()
            .map(|e| {
                let proj = e
                    .iter()
                    .map(|e| format!("{e}"))
                    .collect::<Vec<_>>()
                    .join(", ");
                format!("[{}]", proj)
            })
            .collect::<Vec<_>>()
            .join(", ");

        write!(f, "Expand: [{projections}]")
    }

    /// TODO [`PushDownProjection`] has no effect on this node
    fn from_template(
        &self,
        _exprs: &[Expr],
        inputs: &[LogicalPlan],
    ) -> Arc<dyn UserDefinedLogicalNode> {
        assert_eq!(inputs.len(), 1, "input size inconsistent");
        Arc::new(self.clone())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use datafusion::error::DataFusionError;
    use datafusion::logical_expr::logical_plan::table_scan;
    use datafusion::logical_expr::{
        Extension, LogicalPlan, LogicalPlanBuilder, UserDefinedLogicalNode,
    };
    use datafusion::prelude::{cast, col, lit, Expr};

    use super::ExpandNode;
    use crate::extension::expr::expr_fn::{minus, modulo, multiply, plus};

    fn test_table_scan_with_name(name: &str) -> Result<LogicalPlan, DataFusionError> {
        let schema = Schema::new(vec![
            Field::new(
                "time",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            ),
            Field::new("b", DataType::UInt32, false),
            Field::new("c", DataType::UInt32, false),
        ]);
        table_scan(Some(name), &schema, None)?.build()
    }

    fn make_window(
        time: Expr,
        i: u64,
        window_duration: u64,
        slide_duration: u64,
        start_time: u64,
    ) -> Vec<Expr> {
        let ns_time = cast(time, DataType::Timestamp(TimeUnit::Nanosecond, None));
        let u64_time = cast(ns_time, DataType::UInt64);

        let last_start = minus(
            u64_time.clone(),
            modulo(
                plus(minus(u64_time, lit(start_time)), lit(slide_duration)),
                lit(slide_duration),
            ),
        );
        let window_start = minus(last_start, multiply(lit(i), lit(slide_duration)));
        let window_end = plus(window_start.clone(), lit(window_duration));

        let window_start = cast(
            window_start,
            DataType::Timestamp(TimeUnit::Nanosecond, None),
        );
        let window_end = cast(window_end, DataType::Timestamp(TimeUnit::Nanosecond, None));

        vec![window_start.alias("$start"), window_end.alias("$end")]
    }

    #[test]
    fn test_new() -> Result<(), DataFusionError> {
        // timeColumn: Expression,
        // windowDuration: Long,
        // slideDuration: Long,
        // startTime: Long
        let time_column = "time";
        let window_duration = 10_000_000_000_u64; // 10s > 1_000_000_u32 1ms
        let slide_duration = 5_000_000_000_u64; // 5s > 1_000_000_u32 1ms
        let start_time = 0_u64;

        let input = test_table_scan_with_name("test")?;
        let input_exprs = input
            .schema()
            .field_names()
            .into_iter()
            .map(col)
            .collect::<Vec<_>>();

        // prevent window_duration + slide_duration from overflowing
        let overlapping_windows = (window_duration + slide_duration - 1) / slide_duration;

        let windows = (0..overlapping_windows).map(|i| {
            make_window(
                col(time_column),
                i,
                window_duration,
                slide_duration,
                start_time,
            )
        });

        // Generate project exprs for each window
        let projections = windows
            .map(|e| {
                let mut result: Vec<Expr> = Vec::with_capacity(input_exprs.len());
                result.extend(e);
                result.extend(input_exprs.clone());
                result
            })
            .collect::<Vec<Vec<_>>>();

        // Expand: [$start, $end, <child exprs>]
        let node = ExpandNode::try_new(projections, Arc::new(input))?;

        let expand_schema = node.schema();
        let field_names = node.schema().field_names();

        // Projection: [Struct<$start, $end> as window, <child exprs>]
        let start_field = expand_schema.field_with_unqualified_name("$start")?;
        let end_field = expand_schema.field_with_unqualified_name("$end")?;

        let window_expr = Expr::ScalarVariable(
            DataType::Struct(vec![start_field.field().clone(), end_field.field().clone()]),
            vec!["window".to_string()],
        );

        let project = field_names
            .into_iter()
            .filter(|n| n != start_field.name() && n != end_field.name())
            .map(col)
            .chain(vec![window_expr])
            .collect::<Vec<_>>();

        let result = LogicalPlanBuilder::from(LogicalPlan::Extension(Extension {
            node: Arc::new(node),
        }))
        .project(project)?
        .build()?;

        let str = result.display_indent().to_string();

        assert_eq!("Projection: test.time, test.b, test.c, window\
        \n  Expand: [[CAST(CAST(CAST(time AS Timestamp(Nanosecond, None)) AS UInt64) - (CAST(CAST(time AS Timestamp(Nanosecond, None)) AS UInt64) - UInt64(0) + UInt64(5000000000)) % UInt64(5000000000) - UInt64(0) * UInt64(5000000000) AS Timestamp(Nanosecond, None)) AS $start, CAST(CAST(CAST(time AS Timestamp(Nanosecond, None)) AS UInt64) - (CAST(CAST(time AS Timestamp(Nanosecond, None)) AS UInt64) - UInt64(0) + UInt64(5000000000)) % UInt64(5000000000) - UInt64(0) * UInt64(5000000000) + UInt64(10000000000) AS Timestamp(Nanosecond, None)) AS $end, test.time, test.b, test.c], [CAST(CAST(CAST(time AS Timestamp(Nanosecond, None)) AS UInt64) - (CAST(CAST(time AS Timestamp(Nanosecond, None)) AS UInt64) - UInt64(0) + UInt64(5000000000)) % UInt64(5000000000) - UInt64(1) * UInt64(5000000000) AS Timestamp(Nanosecond, None)) AS $start, CAST(CAST(CAST(time AS Timestamp(Nanosecond, None)) AS UInt64) - (CAST(CAST(time AS Timestamp(Nanosecond, None)) AS UInt64) - UInt64(0) + UInt64(5000000000)) % UInt64(5000000000) - UInt64(1) * UInt64(5000000000) + UInt64(10000000000) AS Timestamp(Nanosecond, None)) AS $end, test.time, test.b, test.c]]\
        \n    TableScan: test", str);

        Ok(())
    }
}
