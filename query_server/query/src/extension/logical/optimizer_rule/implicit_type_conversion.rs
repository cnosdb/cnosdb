use std::mem;
use std::sync::Arc;

use datafusion::arrow::compute;
use datafusion::arrow::compute::kernels::cast_utils::string_to_timestamp_nanos;
use datafusion::arrow::compute::CastOptions;
use datafusion::arrow::datatypes::{DataType, TimeUnit};
use datafusion::common::DFSchemaRef;
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::expr_rewriter::{ExprRewritable, ExprRewriter};
use datafusion::logical_expr::{
    utils, Between, BinaryExpr, Expr, ExprSchemable, Filter, LogicalPlan, Operator, TableScan,
};
use datafusion::optimizer::optimizer::OptimizerRule;
use datafusion::optimizer::OptimizerConfig;
use datafusion::prelude::lit;
use datafusion::scalar::ScalarValue;
use trace::debug;

/// Optimizer that cast literal value to target column's type
///
/// # Support operator:
/// * binary op
/// * between and
/// * in list
///
/// # Transformations that are applied:
/// * utf8 to Timestamp(s/ms/us/ns)
///     Examples of accepted utf8 inputs:
///     * `1997-01-31T09:26:56.123Z`        # RCF3339
///     * `1997-01-31T09:26:56.123-05:00`   # RCF3339
///     * `1997-01-31 09:26:56.123-05:00`   # close to RCF3339 but with a space rather than T
///     * `1997-01-31T09:26:56.123`         # close to RCF3339 but no timezone offset specified
///     * `1997-01-31 09:26:56.123`         # close to RCF3339 but uses a space and no timezone offset
///     * `1997-01-31 09:26:56`             # close to RCF3339, no fractional seconds
/// * other reference: `datafusion::arrow::compute::cast_with_options`
pub struct ImplicitTypeConversion;

impl OptimizerRule for ImplicitTypeConversion {
    fn try_optimize(
        &self,
        plan: &LogicalPlan,
        _optimizer_config: &dyn OptimizerConfig,
    ) -> Result<Option<LogicalPlan>> {
        let mut rewriter = DataTypeRewriter {
            schemas: plan.all_schemas(),
        };

        match plan {
            LogicalPlan::Filter(filter) => {
                let input = self
                    .try_optimize(filter.input.as_ref(), _optimizer_config)?
                    .map(Arc::new)
                    .unwrap_or_else(|| filter.input.clone());

                Ok(Some(LogicalPlan::Filter(Filter::try_new(
                    filter.predicate.clone().rewrite(&mut rewriter)?,
                    input,
                )?)))
            }
            LogicalPlan::TableScan(TableScan {
                table_name,
                source,
                projection,
                projected_schema,
                filters,
                fetch,
                agg_with_grouping,
            }) => {
                let rewrite_filters = filters
                    .clone()
                    .into_iter()
                    .map(|e| e.rewrite(&mut rewriter))
                    .collect::<Result<Vec<_>>>()?;
                Ok(Some(LogicalPlan::TableScan(TableScan {
                    table_name: table_name.clone(),
                    source: source.clone(),
                    projection: projection.clone(),
                    projected_schema: projected_schema.clone(),
                    filters: rewrite_filters,
                    fetch: *fetch,
                    agg_with_grouping: agg_with_grouping.clone(),
                })))
            }
            LogicalPlan::Projection { .. }
            | LogicalPlan::Window { .. }
            | LogicalPlan::Aggregate { .. }
            | LogicalPlan::Repartition { .. }
            | LogicalPlan::CreateExternalTable { .. }
            | LogicalPlan::Extension { .. }
            | LogicalPlan::Sort { .. }
            | LogicalPlan::Explain { .. }
            | LogicalPlan::Limit { .. }
            | LogicalPlan::Union { .. }
            | LogicalPlan::Join { .. }
            | LogicalPlan::CrossJoin { .. }
            | LogicalPlan::CreateMemoryTable { .. }
            | LogicalPlan::DropTable { .. }
            | LogicalPlan::DropView { .. }
            | LogicalPlan::Values { .. }
            | LogicalPlan::Distinct { .. }
            | LogicalPlan::SetVariable { .. }
            | LogicalPlan::Prepare { .. }
            | LogicalPlan::Unnest { .. }
            | LogicalPlan::Analyze { .. } => {
                let inputs = plan.inputs();
                let new_inputs = inputs
                    .iter()
                    .map(|plan| {
                        self.try_optimize(plan, _optimizer_config)
                            .transpose()
                            .unwrap_or_else(|| Ok((*plan).clone()))
                    })
                    .collect::<Result<Vec<_>>>()?;

                let expr = plan
                    .expressions()
                    .into_iter()
                    .map(|e| e.rewrite(&mut rewriter))
                    .collect::<Result<Vec<_>>>()?;

                Ok(Some(utils::from_plan(plan, &expr, &new_inputs)?))
            }

            LogicalPlan::Subquery(_)
            | LogicalPlan::SubqueryAlias(_)
            | LogicalPlan::CreateView(_)
            | LogicalPlan::CreateCatalogSchema(_)
            | LogicalPlan::CreateCatalog(_)
            | LogicalPlan::DescribeTable(_)
            | LogicalPlan::Dml(_)
            | LogicalPlan::EmptyRelation { .. } => Ok(None),
        }
    }

    fn name(&self) -> &str {
        "implicit_type_conversion"
    }
}

struct DataTypeRewriter<'a> {
    /// input schemas
    schemas: Vec<&'a DFSchemaRef>,
}

impl<'a> DataTypeRewriter<'a> {
    /// If expr is Column, get its data type; otherwise, return None
    fn extract_column_data_type(&self, expr: &Expr) -> Option<DataType> {
        if let Expr::Column(_) = expr {
            for schema in &self.schemas {
                if let Ok(v) = expr.get_type(schema) {
                    return Some(v);
                }
            }
        }

        None
    }

    fn convert_data_type_if_necessary(
        &self,
        mut left: Box<Expr>,
        mut right: Box<Expr>,
    ) -> Result<(Box<Expr>, Box<Expr>)> {
        let left_type = self.extract_column_data_type(&left);
        let right_type = self.extract_column_data_type(&right);

        // Ensure that the left side of the op is column
        let mut reverse = false;
        let left_type = match (&left_type, &right_type) {
            (Some(v), None) => v,
            (None, Some(v)) => {
                reverse = true;
                mem::swap(&mut left, &mut right);
                v
            }
            _ => return Ok((left, right)),
        };

        // Only processing of column op literal
        let (left, right) = match (left.as_ref(), right.as_ref()) {
            // Convert the data on the right of op to the data type corresponding to the left column
            (Expr::Column(col), Expr::Literal(value)) if !value.is_null() => {
                let casted_right = Self::cast_scalar_value(value, left_type)?;
                debug!(
                    "DataTypeRewriter convert type, origin_left:{:?}, type:{}, right:{:?}, casted_right:{:?}",
                    col, left_type, value, casted_right
                );
                if casted_right.is_null() {
                    return Err(DataFusionError::Plan(format!(
                        "column:{:?} value:{:?} is invalid",
                        col, value
                    )));
                }
                let casted_right = expr_changed_with_alias(right.as_ref(), lit(casted_right))?;

                (left, Box::new(casted_right))
            }
            _ => (left, right),
        };

        if reverse {
            Ok((right, left))
        } else {
            Ok((left, right))
        }
    }

    /// Cast `array` to the provided data type and return a new Array with
    /// type `to_type`, if possible.
    fn cast_scalar_value(value: &ScalarValue, data_type: &DataType) -> Result<ScalarValue> {
        if let DataType::Timestamp(unit, _) = data_type {
            if let ScalarValue::Utf8(Some(v)) = value {
                return match unit {
                    TimeUnit::Second => string_to_timestamp_s(v),
                    TimeUnit::Millisecond => string_to_timestamp_ms(v),
                    TimeUnit::Microsecond => string_to_timestamp_us(v),
                    TimeUnit::Nanosecond => string_to_timestamp_ns(v),
                };
            }
        }

        let array = value.to_array();
        ScalarValue::try_from_array(
            &compute::cast_with_options(&array, data_type, &CastOptions { safe: false })
                .map_err(DataFusionError::ArrowError)?,
            // index: Converts a value in `array` at `index` into a ScalarValue
            0,
        )
    }
}

impl<'a> ExprRewriter for DataTypeRewriter<'a> {
    fn mutate(&mut self, expr: Expr) -> Result<Expr> {
        let new_expr = match expr {
            Expr::BinaryExpr(BinaryExpr { left, op, right }) => match op {
                Operator::Eq
                | Operator::NotEq
                | Operator::Lt
                | Operator::LtEq
                | Operator::Gt
                | Operator::GtEq => {
                    let (left, right) = self.convert_data_type_if_necessary(left, right)?;
                    Expr::BinaryExpr(BinaryExpr { left, op, right })
                }
                _ => Expr::BinaryExpr(BinaryExpr { left, op, right }),
            },
            Expr::Between(Between {
                expr,
                negated,
                low,
                high,
            }) => {
                let (expr, low) = self.convert_data_type_if_necessary(expr, low)?;
                let (expr, high) = self.convert_data_type_if_necessary(expr, high)?;
                Expr::Between(Between {
                    expr,
                    negated,
                    low,
                    high,
                })
            }
            Expr::InList {
                expr,
                list,
                negated,
            } => {
                let mut list_expr = Vec::with_capacity(list.len());
                for e in list {
                    let (_, expr_conversion) =
                        self.convert_data_type_if_necessary(expr.clone(), Box::new(e))?;
                    list_expr.push(*expr_conversion);
                }
                Expr::InList {
                    expr,
                    list: list_expr,
                    negated,
                }
            }
            expr => {
                // no rewrite possible
                expr
            }
        };
        Ok(new_expr)
    }
}

fn string_to_timestamp_s(string: &str) -> Result<ScalarValue> {
    Ok(ScalarValue::TimestampSecond(
        Some(
            string_to_timestamp_nanos(string)
                .map(|t| t / 1_000_000_000)
                .map_err(DataFusionError::from)?,
        ),
        None,
    ))
}

fn string_to_timestamp_ms(string: &str) -> Result<ScalarValue> {
    Ok(ScalarValue::TimestampMillisecond(
        Some(
            string_to_timestamp_nanos(string)
                .map(|t| t / 1_000_000)
                .map_err(DataFusionError::from)?,
        ),
        None,
    ))
}

fn string_to_timestamp_us(string: &str) -> Result<ScalarValue> {
    Ok(ScalarValue::TimestampMicrosecond(
        Some(
            string_to_timestamp_nanos(string)
                .map(|t| t / 1_000)
                .map_err(DataFusionError::from)?,
        ),
        None,
    ))
}

fn string_to_timestamp_ns(string: &str) -> Result<ScalarValue> {
    Ok(ScalarValue::TimestampNanosecond(
        Some(string_to_timestamp_nanos(string).map_err(DataFusionError::from)?),
        None,
    ))
}

/// when an expr changed from old to new, compare their name,
/// if unequal then new_expr.alias(old_expr)
fn expr_changed_with_alias(old: &Expr, new: Expr) -> Result<Expr> {
    let old_expr_name = old.display_name();
    let new_expr_name = new.display_name();

    match (old_expr_name, new_expr_name) {
        (Ok(old_name), Ok(new_name)) if old_name.ne(&new_name) => Ok(new.alias(old_name)),
        _ => Ok(new),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use datafusion::arrow::datatypes::TimeUnit;
    use datafusion::common::{DFField, DFSchema};
    use datafusion::logical_expr::expr_rewriter::ExprRewritable;
    use datafusion::prelude::col;

    use super::*;

    fn expr_test_schema() -> DFSchemaRef {
        Arc::new(
            DFSchema::new_with_metadata(
                vec![
                    DFField::new(None, "c1", DataType::Utf8, true),
                    DFField::new(None, "c2", DataType::Int64, true),
                    DFField::new(None, "c3", DataType::Float64, true),
                    DFField::new(None, "c4", DataType::Float32, true),
                    DFField::new(None, "c5", DataType::Boolean, true),
                    DFField::new(
                        None,
                        "c6",
                        DataType::Timestamp(TimeUnit::Millisecond, None),
                        false,
                    ),
                    DFField::new(
                        None,
                        "c7",
                        DataType::Timestamp(TimeUnit::Second, None),
                        false,
                    ),
                    DFField::new(
                        None,
                        "c8",
                        DataType::Timestamp(TimeUnit::Microsecond, None),
                        false,
                    ),
                    DFField::new(
                        None,
                        "c9",
                        DataType::Timestamp(TimeUnit::Nanosecond, None),
                        false,
                    ),
                ],
                HashMap::new(),
            )
            .unwrap(),
        )
    }

    fn assert_expr_type_conversion(expr: Expr, expected: &str) -> Result<()> {
        let schema = expr_test_schema();
        let mut rewriter = DataTypeRewriter {
            schemas: vec![&schema],
        };
        let rewrite_expr = expr.rewrite(&mut rewriter)?;

        let rewrite_expr = format!("{rewrite_expr:?}");

        assert_eq!(rewrite_expr, expected);
        Ok(())
    }

    #[test]
    fn test_type_conversion_int64() {
        let int_value = 100;
        let int_str = int_value.to_string();
        let not_int_str = "100ss".to_string();

        // Int64 c2 > "100" success
        let expr = col("c2").gt(lit(&int_str));
        assert_expr_type_conversion(expr, "c2 > Int64(100) AS Utf8(\"100\")").unwrap();

        // Int64 "100" > c2 success
        let expr = lit(int_str).gt(col("c2"));
        assert_expr_type_conversion(expr, "Int64(100) AS Utf8(\"100\") > c2").unwrap();

        // Int64 c2 > "100ss" fail
        let expr = col("c2").gt(lit(not_int_str));
        assert_expr_type_conversion(expr, "").err().unwrap();
    }

    #[test]
    fn test_type_conversion_float() {
        let double_value = 100.1;
        let double_str = double_value.to_string();
        let not_int_str = "100ss".to_string();

        // Float64 c3 > "100" success
        let expr = col("c3").gt(lit(&double_str));
        assert_expr_type_conversion(expr, "c3 > Float64(100.1) AS Utf8(\"100.1\")").unwrap();

        // Float64 c3 > "100ss" fail
        let expr = col("c3").gt(lit(&not_int_str));
        assert_expr_type_conversion(expr, "c4 > Float32(100.1) AS Utf8(\"100.1\")")
            .err()
            .unwrap();

        // Float32 c4 > "100" success
        let expr = col("c4").gt(lit(double_str));
        assert_expr_type_conversion(expr, "c4 > Float32(100.1) AS Utf8(\"100.1\")").unwrap();

        // Float32 c4 > "100ss" fail
        let expr = col("c4").gt(lit(not_int_str));
        assert_expr_type_conversion(expr, "").err().unwrap();
    }

    #[test]
    fn test_type_conversion_boolean() {
        let bool_value = true;
        let bool_str = bool_value.to_string();
        let not_int_str = "100ss".to_string();

        // Boolean c5 > "100ss" fail
        let expr = col("c5").gt(lit(not_int_str));
        assert_expr_type_conversion(expr, "c5 > Boolean(true) AS Utf8(\"true\")")
            .err()
            .unwrap();

        // Boolean c5 > "true" success
        let expr = col("c5").gt(lit(bool_str));
        assert_expr_type_conversion(expr, "c5 > Boolean(true) AS Utf8(\"true\")").unwrap();

        // Boolean c5 > true success
        let expr = col("c5").gt(lit(ScalarValue::Boolean(Some(bool_value))));
        assert_expr_type_conversion(expr, "c5 > Boolean(true)").unwrap();
    }

    #[test]
    fn test_type_conversion_timestamp_with_binary_op() {
        let date_string = "2021-09-07 16:00:00".to_string();
        // Timestamp(ms) c6 > "2021-09-07 16:00:00"
        let expr = col("c6").gt(lit(date_string.clone()));
        assert_expr_type_conversion(
            expr,
            "c6 > TimestampMillisecond(1631030400000, None) AS Utf8(\"2021-09-07 16:00:00\")",
        )
        .unwrap();

        // Timestamp(s) c7 > "2021-09-07 16:00:00"
        let expr = col("c7").gt(lit(date_string.clone()));
        assert_expr_type_conversion(
            expr,
            "c7 > TimestampSecond(1631030400, None) AS Utf8(\"2021-09-07 16:00:00\")",
        )
        .unwrap();

        // Timestamp(us) c8 > "2021-09-07 16:00:00"
        let expr = col("c8").gt(lit(date_string.clone()));
        assert_expr_type_conversion(
            expr,
            "c8 > TimestampMicrosecond(1631030400000000, None) AS Utf8(\"2021-09-07 16:00:00\")",
        )
        .unwrap();

        // Timestamp(ns) c9 > "2021-09-07 16:00:00"
        let expr = col("c9").gt(lit(date_string.clone()));
        assert_expr_type_conversion(
            expr,
            "c9 > TimestampNanosecond(1631030400000000000, None) AS Utf8(\"2021-09-07 16:00:00\")",
        )
        .unwrap();

        // "2021-09-07 16:00:00" > Timestamp(ms) c6
        let expr = lit(date_string.clone()).gt(col("c6"));
        assert_expr_type_conversion(
            expr,
            "TimestampMillisecond(1631030400000, None) AS Utf8(\"2021-09-07 16:00:00\") > c6",
        )
        .unwrap();

        // "2021-09-07 16:00:00" > Timestamp(s) c7
        let expr = lit(date_string.clone()).gt(col("c7"));
        assert_expr_type_conversion(
            expr,
            "TimestampSecond(1631030400, None) AS Utf8(\"2021-09-07 16:00:00\") > c7",
        )
        .unwrap();

        // "2021-09-07 16:00:00" > Timestamp(us) c8
        let expr = lit(date_string.clone()).gt(col("c8"));
        assert_expr_type_conversion(
            expr,
            "TimestampMicrosecond(1631030400000000, None) AS Utf8(\"2021-09-07 16:00:00\") > c8",
        )
        .unwrap();

        // "2021-09-07 16:00:00" > Timestamp(ns) c9
        let expr = lit(date_string).gt(col("c9"));
        assert_expr_type_conversion(
            expr,
            "TimestampNanosecond(1631030400000000000, None) AS Utf8(\"2021-09-07 16:00:00\") > c9",
        )
        .unwrap();

        // Timestamp(ms) c6 > 1642141472
        let timestamp_int = 1642141472;
        let expr = col("c6").gt(lit(ScalarValue::Int64(Some(timestamp_int))));
        assert_expr_type_conversion(
            expr,
            "c6 > TimestampMillisecond(1642141472, None) AS Int64(1642141472)",
        )
        .unwrap();

        // Timestamp(s) c7 > 1642141472
        let timestamp_int = 1642141472;
        let expr = col("c7").gt(Expr::Literal(ScalarValue::Int64(Some(timestamp_int))));
        assert_expr_type_conversion(
            expr,
            "c7 > TimestampSecond(1642141472, None) AS Int64(1642141472)",
        )
        .unwrap();

        // Timestamp(us) c8 > 1642141472
        let timestamp_int = 1642141472;
        let expr = col("c8").gt(lit(ScalarValue::Int64(Some(timestamp_int))));
        assert_expr_type_conversion(
            expr,
            "c8 > TimestampMicrosecond(1642141472, None) AS Int64(1642141472)",
        )
        .unwrap();

        // Timestamp(ns) c9 > 1642141472
        let timestamp_int = 1642141472;
        let expr = col("c9").gt(lit(ScalarValue::Int64(Some(timestamp_int))));
        assert_expr_type_conversion(
            expr,
            "c9 > TimestampNanosecond(1642141472, None) AS Int64(1642141472)",
        )
        .unwrap();
    }

    #[test]
    fn test_type_conversion_timestamp_with_between_and() {
        // Timestamp(ms) c6 between "2021-09-07 16:00:00" and "2021-09-07 17:00:00"
        let date_string = "2021-09-07 16:00:00".to_string();
        let date_string2 = "2021-09-07 17:00:00".to_string();
        let expr = Expr::Between(Between::new(
            col("c6").into(),
            false,
            lit(&date_string).into(),
            lit(&date_string2).into(),
        ));

        assert_expr_type_conversion(expr, "c6 BETWEEN TimestampMillisecond(1631030400000, None) AS Utf8(\"2021-09-07 16:00:00\") AND TimestampMillisecond(1631034000000, None) AS Utf8(\"2021-09-07 17:00:00\")").unwrap();

        // Timestamp(ms) c6 between 1642141472 and 1642141472
        let timestamp_int_low = 1642141472;
        let timestamp_int_high = 1642141474;
        let expr = Expr::Between(Between::new(
            col("c6").into(),
            false,
            lit(&date_string).into(),
            lit(&date_string2).into(),
        ));
        assert_expr_type_conversion(expr, "c6 BETWEEN TimestampMillisecond(1631030400000, None) AS Utf8(\"2021-09-07 16:00:00\") AND TimestampMillisecond(1631034000000, None) AS Utf8(\"2021-09-07 17:00:00\")").unwrap();

        let expr = Expr::Between(Between::new(
            col("c6").into(),
            false,
            lit(ScalarValue::Int64(Some(timestamp_int_low))).into(),
            lit(ScalarValue::Int64(Some(timestamp_int_high))).into(),
        ));
        assert_expr_type_conversion(expr, "c6 BETWEEN TimestampMillisecond(1642141472, None) AS Int64(1642141472) AND TimestampMillisecond(1642141474, None) AS Int64(1642141474)").unwrap();
    }

    #[test]
    fn test_type_conversion_timestamp_with_in_list() {
        // Timestamp(ms) c6 in ('2021-09-07 16:00:00', '2021-09-07 17:00:00')
        let date_string = "2021-09-07 16:00:00".to_string();
        let date_string2 = "2021-09-07 17:00:00".to_string();
        let expr = Expr::InList {
            expr: col("c6").into(),
            negated: false,
            list: vec![lit(date_string), lit(date_string2)],
        };
        assert_expr_type_conversion(expr, "c6 IN ([TimestampMillisecond(1631030400000, None) AS Utf8(\"2021-09-07 16:00:00\"), TimestampMillisecond(1631034000000, None) AS Utf8(\"2021-09-07 17:00:00\")])").unwrap();
    }
}
