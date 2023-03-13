use std::collections::VecDeque;
use std::result;

use datafusion::arrow::datatypes::DataType;
use datafusion::error::DataFusionError;
use datafusion::logical_expr::expr_visitor::{ExprVisitable, ExpressionVisitor, Recursion};
use datafusion::logical_expr::{BinaryExpr, Operator};
use datafusion::prelude::{Column, Expr};
use datafusion::scalar::ScalarValue;

use super::domain::{ColumnDomains, Domain, Range};

type Result<T> = result::Result<T, DataFusionError>;

#[derive(Clone, Default)]
struct RowExpressionToDomainsVisitorContext {
    current_domain_stack: VecDeque<ColumnDomains<Column>>,
}

struct NormalizedSimpleComparison {
    column: Column,
    op: Operator,
    value: ScalarValue,
}

impl NormalizedSimpleComparison {
    fn reverse_op(op: Operator) -> Operator {
        match op {
            Operator::Lt => Operator::Gt,
            Operator::LtEq => Operator::GtEq,
            Operator::Gt => Operator::Lt,
            Operator::GtEq => Operator::LtEq,
            _ => op,
        }
    }
    /// Extract a normalized simple comparison between a Column and a ScalarValue if possible
    /// otherwise return None
    fn of(left: Expr, op: Operator, right: Expr) -> Option<NormalizedSimpleComparison> {
        if !Self::is_comparison_op(op) {
            return None;
        }

        match (left, op, right) {
            (Expr::Column(column), _, Expr::Literal(value)) => Some(Self { column, op, value }),
            (Expr::Literal(value), _, Expr::Column(column)) => Some(Self {
                column,
                op: Self::reverse_op(op),
                value,
            }),
            (_, _, _) => None,
        }
        .and_then(|e| {
            // not support null with column of binary op
            if matches!(e.value, ScalarValue::Null | ScalarValue::Utf8(None)) {
                return None;
            }
            Some(e)
        })
    }
    /// Determine if a data type is sortable
    fn is_orderable(&self) -> bool {
        match self.value {
            ScalarValue::Boolean(_)
            | ScalarValue::Null
            | ScalarValue::List(_, _)
            | ScalarValue::IntervalYearMonth(_)
            | ScalarValue::IntervalDayTime(_)
            | ScalarValue::IntervalMonthDayNano(_)
            | ScalarValue::Dictionary(_, _)
            | ScalarValue::Struct(_, _) => false,

            ScalarValue::Float32(_)
            | ScalarValue::Float64(_)
            | ScalarValue::Decimal128(_, _, _)
            | ScalarValue::Int8(_)
            | ScalarValue::Int16(_)
            | ScalarValue::Int32(_)
            | ScalarValue::Int64(_)
            | ScalarValue::UInt8(_)
            | ScalarValue::UInt16(_)
            | ScalarValue::UInt32(_)
            | ScalarValue::UInt64(_)
            | ScalarValue::Utf8(_)
            | ScalarValue::LargeUtf8(_)
            | ScalarValue::Binary(_)
            | ScalarValue::LargeBinary(_)
            | ScalarValue::FixedSizeBinary(_, _)
            | ScalarValue::Date32(_)
            | ScalarValue::Date64(_)
            | ScalarValue::Time32Second(_)
            | ScalarValue::Time32Millisecond(_)
            | ScalarValue::Time64Microsecond(_)
            | ScalarValue::Time64Nanosecond(_)
            | ScalarValue::TimestampSecond(_, _)
            | ScalarValue::TimestampMillisecond(_, _)
            | ScalarValue::TimestampMicrosecond(_, _)
            | ScalarValue::TimestampNanosecond(_, _) => true,
        }
    }
    // Determine if data types are comparable
    fn _is_comparable(&self) -> bool {
        true
    }

    fn is_eq_op(&self) -> bool {
        self.op.eq(&Operator::Eq)
    }

    fn is_comparison_op(op: Operator) -> bool {
        matches!(
            op,
            Operator::Eq
                | Operator::NotEq
                | Operator::Lt
                | Operator::LtEq
                | Operator::Gt
                | Operator::GtEq
        )
    }
}

pub struct RowExpressionToDomainsVisitor<'a> {
    ctx: &'a mut RowExpressionToDomainsVisitorContext,
}

type GetRangeFromDataTypeAndValue = fn(data_type: &DataType, scalar_value: &ScalarValue) -> Range;

/// Get the corresponding range constructor according to the comparison operator
fn get_get_range_fn(op: &Operator) -> Option<GetRangeFromDataTypeAndValue> {
    match op {
        Operator::Eq => Some(Range::eq),
        Operator::Lt => Some(Range::lt),
        Operator::LtEq => Some(Range::le),
        Operator::Gt => Some(Range::gt),
        Operator::GtEq => Some(Range::ge),
        _ => None,
    }
}

impl ExpressionVisitor for RowExpressionToDomainsVisitor<'_> {
    fn pre_visit(self, expr: &Expr) -> Result<Recursion<Self>> {
        // Note: expressions that can be used as and/or child nodes need to be processed.
        // Expressions returning boolean need to be processed, If the expression is not supported, push ColumnDomains::all() onto the stack.
        // If the expression supports it, return Continue directly.
        match expr {
            // Expr::Alias(_, _)
            // | Expr::Negative(_)
            // | Expr::ScalarVariable(_, _)
            // | Expr::Case { .. }
            // | Expr::Cast { .. }
            // | Expr::TryCast { .. }
            // | Expr::Sort { .. }
            // | Expr::ScalarFunction { .. }
            // | Expr::ScalarUDF { .. }
            // | Expr::WindowFunction { .. }
            // | Expr::AggregateFunction { .. }
            // | Expr::GroupingSet(_)
            // | Expr::AggregateUDF { .. }
            // | Expr::Exists { .. }
            // | Expr::InSubquery { .. }
            // | Expr::ScalarSubquery(_)
            // | Expr::Wildcard
            // | Expr::QualifiedWildcard { .. }
            // | Expr::GetIndexedField { .. } => {}
            Expr::Column(_) | Expr::Literal(_) => Ok(Recursion::Continue(self)),
            Expr::BinaryExpr(BinaryExpr {
                left: _,
                op,
                right: _,
            }) => {
                match op {
                    Operator::Eq
                    | Operator::NotEq
                    | Operator::Lt
                    | Operator::LtEq
                    | Operator::Gt
                    | Operator::GtEq
                    | Operator::And
                    | Operator::Or => {
                        // support
                        Ok(Recursion::Continue(self))
                    }
                    _ => {
                        // not support
                        self.ctx
                            .current_domain_stack
                            .push_back(ColumnDomains::all());
                        Ok(Recursion::Stop(self))
                    }
                }
            }
            // TODO Currently not supported, follow-up support needs to implement the corresponding expression in post_visit
            Expr::Like(_)
            | Expr::ILike(_)
            | Expr::SimilarTo(_)
            | Expr::Not(_)
            | Expr::IsNotNull(_)
            | Expr::IsNull(_)
            | Expr::IsTrue(_)
            | Expr::IsFalse(_)
            | Expr::IsUnknown(_)
            | Expr::IsNotTrue(_)
            | Expr::IsNotFalse(_)
            | Expr::IsNotUnknown(_)
            | Expr::Between { .. }
            | Expr::InList { .. } => {
                self.ctx
                    .current_domain_stack
                    .push_back(ColumnDomains::all());
                Ok(Recursion::Stop(self))
            }
            _ => Ok(Recursion::Stop(self)),
        }
    }

    fn post_visit(self, expr: &Expr) -> datafusion::common::Result<Self> {
        match expr {
            Expr::BinaryExpr(BinaryExpr { left, op, right }) => {
                match op {
                    // stack中为expr，清空，生成domain
                    Operator::Eq
                    | Operator::NotEq
                    | Operator::Lt
                    | Operator::LtEq
                    | Operator::Gt
                    | Operator::GtEq => {
                        Self::construct_value_set_and_push_current_domain_stack(
                            self.ctx, left, op, right,
                        );
                    }
                    // The stack is domain, pop it, and generate a new domain
                    Operator::And => {
                        let domain1_opt = self.ctx.current_domain_stack.pop_back();
                        let domain2_opt = self.ctx.current_domain_stack.pop_back();

                        // Executed to this point, it means that all child nodes are valid and exist, and there is no need to deal with the case of None
                        if let (Some(ref mut d1), Some(d2)) = (domain1_opt, domain2_opt) {
                            d1.intersect(&d2);
                            self.ctx.current_domain_stack.push_back(d1.to_owned());
                        }
                    }
                    Operator::Or => {
                        let domain1_opt = self.ctx.current_domain_stack.pop_back();
                        let domain2_opt = self.ctx.current_domain_stack.pop_back();

                        // Executed to this point, it means that all child nodes are valid and exist, and there is no need to deal with the case of None
                        if let (Some(ref mut d1), Some(d2)) = (domain1_opt, domain2_opt) {
                            d1.column_wise_union(&d2);
                            self.ctx.current_domain_stack.push_back(d1.to_owned());
                        }
                    }
                    _ => {}
                }
            }
            // TODO The stack is the domain, and the domain is generated
            Expr::Not(_) | Expr::Between { .. } | Expr::InList { .. } => {}
            _ => {}
        }

        Ok(self)
    }
}

impl RowExpressionToDomainsVisitor<'_> {
    pub fn expr_to_column_domains(expr: &Expr) -> Result<ColumnDomains<Column>> {
        let mut ctx = RowExpressionToDomainsVisitorContext::default();
        expr.accept(RowExpressionToDomainsVisitor { ctx: &mut ctx })?;
        Ok(ctx
            .current_domain_stack
            .pop_back()
            .unwrap_or_else(ColumnDomains::all))
    }

    /// Convert nsc to RangeValueSet
    ///
    /// Note: nsc must supports ordering, i.e. is_orderable == true
    fn nsc_to_column_domains_with_range(nsc: &NormalizedSimpleComparison) -> ColumnDomains<Column> {
        let col = &nsc.column;
        let value = &nsc.value;
        let op = &nsc.op;
        // Get the function that constructs Range by dataType and ScalarValue.
        // If op does not support it, it will return None, but it must be supported here (because op is obtained from nsc)
        let val_set = get_get_range_fn(op)
            // Construct Range
            .map(|f| f(&value.get_datatype(), value))
            // Construct ValueSet by Range, where of_ranges will not return an exception, because the parameter has only one range
            .map(|r| Domain::of_ranges(&[r]).unwrap())
            // Normally this is not triggered (unless there is a bug), but returns ValueSet::All for safety
            .unwrap_or(Domain::All);

        ColumnDomains::of(col.to_owned(), &val_set)
    }
    /// Convert nsc to EqutableValueSet
    ///
    /// Note: nsc does not support ordering, i.e. is_orderable == false
    fn nsc_to_domains_with_equtable(nsc: &NormalizedSimpleComparison) -> ColumnDomains<Column> {
        let col = &nsc.column;
        let value = &nsc.value;
        let is_eq_op = nsc.is_eq_op();
        let val_set = Domain::of_values(&value.get_datatype(), is_eq_op, &[value]);
        ColumnDomains::of(col.to_owned(), &val_set)
    }
    /// Construct comparison operations as simple column-value comparison data structures nsc.
    ///
    /// Choose a different NscToValueSet function based on whether the data type supports sorting.
    ///
    /// Convert nsc to ValueSet via NscToValueSet function
    ///
    /// Push the domain into the stack
    fn construct_value_set_and_push_current_domain_stack(
        ctx: &mut RowExpressionToDomainsVisitorContext,
        left: &Expr,
        op: &Operator,
        right: &Expr,
    ) {
        let domains_opt = NormalizedSimpleComparison::of(left.clone(), *op, right.clone())
            .map(|ref nsc| {
                if nsc.is_orderable() {
                    return Self::nsc_to_column_domains_with_range(nsc);
                }
                Self::nsc_to_domains_with_equtable(nsc)
            })
            .or_else(|| Some(ColumnDomains::all()));

        if let Some(domains) = domains_opt {
            ctx.current_domain_stack.push_back(domains);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::ops::Add;

    use chrono::{Duration, NaiveDate};
    use datafusion::logical_expr::expr_fn::col;
    use datafusion::logical_expr::{binary_expr, Like};
    use datafusion::prelude::{and, in_list, lit, or, random};

    use super::*;

    /// resolves the domain of the specified expression
    fn get_domains(expr: &Expr) -> Result<ColumnDomains<Column>> {
        RowExpressionToDomainsVisitor::expr_to_column_domains(expr)
    }
    fn get_tuple_int_and_expr_with_except_column_domains() -> (Expr, ColumnDomains<Column>) {
        let filter1 = binary_expr(col("i1"), Operator::Lt, lit(-1000000));
        let filter2 = binary_expr(col("i2"), Operator::Eq, lit(2147483647));
        let filter3 = binary_expr(col("i3"), Operator::Gt, lit(3_333_333_333_333_333_i64));

        let result_expr = and(and(filter1, filter2), filter3);

        // build except result
        //   i1: (_, -1000000)
        //   i2: [2147483647, 2147483647]
        //   i3: (3333333333333333, _)
        let i1 = Range::lt(&DataType::Int32, &ScalarValue::Int32(Some(-1000000_i32)));
        let i2 = Range::eq(&DataType::Int32, &ScalarValue::Int32(Some(2147483647_i32)));
        let i3 = Range::gt(
            &DataType::Int64,
            &ScalarValue::Int64(Some(3333333333333333_i64)),
        );

        let i1_domain = Domain::of_ranges(&[i1]).unwrap();
        let i2_domain = Domain::of_ranges(&[i2]).unwrap();
        let i3_domain = Domain::of_ranges(&[i3]).unwrap();

        let except_column_domains = &mut ColumnDomains::of(Column::from_name("i1"), &i1_domain);
        except_column_domains.insert_or_intersect(Column::from_name("i2"), &i2_domain);
        except_column_domains.insert_or_intersect(Column::from_name("i3"), &i3_domain);

        (result_expr, except_column_domains.to_owned())
    }

    fn get_tuple_float_and_expr_with_except_column_domains() -> (Expr, ColumnDomains<Column>) {
        let filter_f1 = binary_expr(col("f1"), Operator::Lt, lit(-1000000.1));
        let filter_f2 = binary_expr(col("f2"), Operator::Eq, lit(2.2));
        let filter_f3 = binary_expr(col("f3"), Operator::Gt, lit(3333333333333333.3));

        let result_expr = and(and(filter_f1, filter_f2), filter_f3);

        // build except result
        //   f1: (_, -1000000.1)
        //   f2: [2.2, 2.2]
        //   f3: (3333333333333333.3, _)
        let f1 = Range::lt(&DataType::Float64, &ScalarValue::Float64(Some(-1000000.1)));
        let f2 = Range::eq(&DataType::Float64, &ScalarValue::Float64(Some(2.2)));
        let f3 = Range::gt(
            &DataType::Float64,
            &ScalarValue::Float64(Some(3333333333333333.3)),
        );

        let f1_domain = Domain::of_ranges(&[f1]).unwrap();
        let f2_domain = Domain::of_ranges(&[f2]).unwrap();
        let f3_domain = Domain::of_ranges(&[f3]).unwrap();

        let except_column_domains = &mut ColumnDomains::of(Column::from_name("f1"), &f1_domain);
        except_column_domains.insert_or_intersect(Column::from_name("f2"), &f2_domain);
        except_column_domains.insert_or_intersect(Column::from_name("f3"), &f3_domain);

        (result_expr, except_column_domains.to_owned())
    }

    fn get_tuple_bool_and_expr_with_except_column_domains() -> (Expr, ColumnDomains<Column>) {
        let filter_b1 = binary_expr(col("b1"), Operator::Eq, lit(true));
        let filter_b2 = binary_expr(col("b2"), Operator::Eq, lit(true));
        let filter_b3 = binary_expr(col("b3"), Operator::Eq, lit(true));

        let result_expr = and(and(filter_b1, filter_b2), filter_b3);

        // build except result
        //   b1: true
        //   b2: true
        //   b3: true
        let b = ScalarValue::Boolean(Some(true));

        let domain = Domain::of_values(&DataType::Boolean, true, &[&b, &b, &b]);

        let except_column_domains = &mut ColumnDomains::of(Column::from_name("b1"), &domain);
        except_column_domains.insert_or_intersect(Column::from_name("b2"), &domain);
        except_column_domains.insert_or_intersect(Column::from_name("b3"), &domain);

        (result_expr, except_column_domains.to_owned())
    }

    fn get_tuple_string_and_expr_with_except_column_domains() -> (Expr, ColumnDomains<Column>) {
        let filter_s1 = binary_expr(col("s1"), Operator::Lt, lit("true"));
        let filter_s2 = binary_expr(col("s2"), Operator::Eq, lit(" 99 0 _ *"));
        let filter_s3 = binary_expr(col("s3"), Operator::Gt, lit(""));

        let result_expr = and(and(filter_s1, filter_s2), filter_s3);

        // build except result
        //   s1: (_, true)
        //   s2: [" 99 0 _ *", " 99 0 _ *"]
        //   s3: ("", _)
        let s1 = Range::lt(
            &DataType::Utf8,
            &ScalarValue::Utf8(Some("true".to_string())),
        );
        let s2 = Range::eq(
            &DataType::Utf8,
            &ScalarValue::Utf8(Some(" 99 0 _ *".to_string())),
        );
        let s3 = Range::gt(&DataType::Utf8, &ScalarValue::Utf8(Some("".to_string())));

        let s1_domain = Domain::of_ranges(&[s1]).unwrap();
        let s2_domain = Domain::of_ranges(&[s2]).unwrap();
        let s3_domain = Domain::of_ranges(&[s3]).unwrap();

        let except_column_domains = &mut ColumnDomains::of(Column::from_name("s1"), &s1_domain);
        except_column_domains.insert_or_intersect(Column::from_name("s2"), &s2_domain);
        except_column_domains.insert_or_intersect(Column::from_name("s3"), &s3_domain);

        (result_expr, except_column_domains.to_owned())
    }

    fn get_tuple_date64_and_expr_with_except_column_domains() -> (Expr, ColumnDomains<Column>) {
        let epoch_l = NaiveDate::from_ymd_opt(1970, 1, 1)
            .expect("invalid or out-of-range date")
            .and_hms_opt(1, 1, 1)
            .expect("invalid time")
            .add(Duration::milliseconds(1000_i64));

        let epoch_m = NaiveDate::from_ymd_opt(2022, 12, 31)
            .expect("invalid or out-of-range date")
            .and_hms_opt(1, 1, 1)
            .expect("invalid time")
            .add(Duration::milliseconds(1000_i64));

        let epoch_h = NaiveDate::from_ymd_opt(2000, 1, 1)
            .expect("invalid or out-of-range date")
            .and_hms_opt(1, 1, 1)
            .expect("invalid time")
            .add(Duration::milliseconds(1000_i64));

        let epoch_h_expr = Expr::Literal(ScalarValue::Date64(Some(epoch_h.timestamp())));
        let epoch_m_expr = Expr::Literal(ScalarValue::Date64(Some(epoch_m.timestamp_micros())));
        let epoch_l_expr = Expr::Literal(ScalarValue::Date64(Some(epoch_l.timestamp_millis())));

        let filter_d1 = binary_expr(col("d1"), Operator::Lt, epoch_h_expr);
        let filter_d2 = binary_expr(col("d2"), Operator::Eq, epoch_m_expr);
        let filter_d3 = binary_expr(col("d3"), Operator::Gt, epoch_l_expr);

        let result_expr = and(and(filter_d1, filter_d2), filter_d3);

        // build except result
        //   d1: (_, Date64(946688462))
        //   d2: [Date64(1672448462000000), Date64(1672448462000000)]
        //   d3: (Date64(3662000), _)
        let d1 = Range::lt(
            &DataType::Date64,
            &ScalarValue::Date64(Some(epoch_h.timestamp())),
        );
        let d2 = Range::eq(
            &DataType::Date64,
            &ScalarValue::Date64(Some(epoch_m.timestamp_micros())),
        );
        let d3 = Range::gt(
            &DataType::Date64,
            &ScalarValue::Date64(Some(epoch_l.timestamp_millis())),
        );

        let d1_domain = Domain::of_ranges(&[d1]).unwrap();
        let d2_domain = Domain::of_ranges(&[d2]).unwrap();
        let d3_domain = Domain::of_ranges(&[d3]).unwrap();

        let except_column_domains = &mut ColumnDomains::of(Column::from_name("d1"), &d1_domain);
        except_column_domains.insert_or_intersect(Column::from_name("d2"), &d2_domain);
        except_column_domains.insert_or_intersect(Column::from_name("d3"), &d3_domain);

        (result_expr, except_column_domains.to_owned())
    }

    /// simple and test - 0
    /// eg.
    ///   s1 like '%上证180' and time >= '2022-10-10 00:00:00'
    ///   ===>
    ///   time: ['2022-10-10 00:00:00', _)
    #[test]
    fn test_simple_and_to_domain_0() {
        let filter1 = Expr::Like(Like::new(
            false,
            Box::new(col("s1")),
            Box::new(lit("%上证180")),
            None,
        ));
        let filter2 = binary_expr(col("time"), Operator::GtEq, lit("2022-10-10 00:00:00"));

        let and = and(filter1, filter2);

        // build except result
        //   time: ['2022-10-10 00:00:00', _)
        let i1 = Range::gt(
            &DataType::Utf8,
            &ScalarValue::Utf8(Some("2022-10-10 00:00:00".to_string())),
        );

        let i1_domain = Domain::of_ranges(&[i1]).unwrap();

        let except_column_domains = ColumnDomains::of(Column::from_name("time"), &i1_domain);

        let result = get_domains(&and);

        assert!(
            result.is_ok(),
            "convert expr {} to column domains err",
            &and
        );

        let column_domain = result.as_ref().unwrap();

        assert!(
            except_column_domains.eq(column_domain),
            "convert expr {} to column domains err, excepted {:?}, found {:?}",
            &and,
            except_column_domains,
            column_domain,
        );
    }

    /// simple and test - 1
    /// eg.
    ///   i1 < -1000000 and i2 = 2147483647 and i3 > 3333333333333333
    ///   ===>
    ///   i1: (_, -1000000)
    ///   i2: [2147483647, 2147483647]
    ///   i3: (3333333333333333, _)
    #[test]
    fn test_simple_and_to_domain_1() {
        let (and, except_column_domains) = get_tuple_int_and_expr_with_except_column_domains();

        let result = get_domains(&and);

        assert!(
            result.is_ok(),
            "convert expr {} to column domains err",
            &and
        );

        // println!("{:#?}", result.as_ref().unwrap());

        let column_domain = result.as_ref().unwrap();

        assert!(
            except_column_domains.eq(column_domain),
            "convert expr {} to column domains err, excepted {:?}, found {:?}",
            &and,
            except_column_domains,
            column_domain,
        );
    }

    /// simple and test - 2
    /// eg.
    /// i1 < -1000000 and i2 = 2147483647 and i3 > 3333333333333333
    /// f1 < -1000000.1 and f2 = 2.2 and f3 > 3333333333333333.3
    ///   ===>
    ///   i1: (_, -1000000)
    ///   i2: [2147483647, 2147483647]
    ///   i3: (3333333333333333, _)
    ///   f1: (_, -1000000.1)
    ///   f2: [2.2, 2.2]
    ///   f3: (3333333333333333.3, _)
    #[test]
    fn test_simple_and_to_domain_2() {
        let (and_i, ref mut except_column_domains) =
            get_tuple_int_and_expr_with_except_column_domains();
        let (and_f, and_f_except_column_domains) =
            get_tuple_float_and_expr_with_except_column_domains();

        let and = and(and_i, and_f);

        let result = get_domains(&and);

        assert!(
            result.is_ok(),
            "convert expr {} to column domains err",
            &and
        );

        // println!("{:#?}", result.as_ref().unwrap());

        let column_domain = result.as_ref().unwrap();

        // // build except result
        except_column_domains.intersect(&and_f_except_column_domains);

        assert!(
            except_column_domains.eq(&column_domain),
            "convert expr {} to column domains err, excepted {:?}, found {:?}",
            &and,
            except_column_domains,
            column_domain,
        );
    }

    /// simple and test - 3
    /// eg.
    /// i1 < 1 and i2 = 2 and i3 > -3 and \
    /// f1 < 1.1 and f2 = 2.2 and f3 > 3.3 and \
    /// b1 < 1.1 and b2 = 2.2 and b3 > 3.3 and \
    ///   ===>
    ///   i1: (_, -1000000)
    ///   i2: [2147483647, 2147483647]
    ///   i3: (3333333333333333, _)
    ///   f1: (_, -1000000.1)
    ///   f2: [2.2, 2.2]
    ///   f3: (3333333333333333.3, _)
    ///   b1: [true, true]
    ///   b2: [true, true]
    ///   b3: [true, true]
    #[test]
    fn test_simple_and_to_domain_3() {
        let (and_i, ref mut except_column_domains) =
            get_tuple_int_and_expr_with_except_column_domains();
        let (and_f, and_f_except_column_domains) =
            get_tuple_float_and_expr_with_except_column_domains();
        let (and_b, and_b_except_column_domains) =
            get_tuple_bool_and_expr_with_except_column_domains();

        let and = and(and(and_i, and_f), and_b);

        let result = get_domains(&and);

        assert!(
            result.is_ok(),
            "convert expr {} to column domains err",
            &and
        );

        // println!("{:#?}", result.as_ref().unwrap());

        let column_domain = result.as_ref().unwrap();

        // build except result
        except_column_domains.intersect(&and_f_except_column_domains);
        except_column_domains.intersect(&and_b_except_column_domains);

        assert!(
            except_column_domains.eq(&column_domain),
            "convert expr {} to column domains err, excepted {:?}, found {:?}",
            &and,
            except_column_domains,
            column_domain,
        );
    }

    /// simple and test - 4
    /// eg.
    /// i1 < 1 and i2 = 2 and i3 > -3 and \
    /// f1 < 1.1 and f2 = 2.2 and f3 > 3.3 and \
    /// b1 < 1.1 and b2 = 2.2 and b3 > 3.3 and \
    /// s1 < 1.1 and s2 = 2.2 and s3 > 3.3 and \
    ///   ===>
    ///   i1: (_, -1000000)
    ///   i2: [2147483647, 2147483647]
    ///   i3: (3333333333333333, _)
    ///   f1: (_, -1000000.1)
    ///   f2: [2.2, 2.2]
    ///   f3: (3333333333333333.3, _)
    ///   b1: [true, true]
    ///   b2: [true, true]
    ///   b3: [true, true]
    ///   s1: (_, true)
    ///   s2: [" 99 0 _ *", " 99 0 _ *"]
    ///   s3: ("", _)
    #[test]
    fn test_simple_and_to_domain_4() {
        let (and_i, ref mut except_column_domains) =
            get_tuple_int_and_expr_with_except_column_domains();
        let (and_f, and_f_except_column_domains) =
            get_tuple_float_and_expr_with_except_column_domains();
        let (and_b, and_b_except_column_domains) =
            get_tuple_bool_and_expr_with_except_column_domains();
        let (and_s, and_s_except_column_domains) =
            get_tuple_string_and_expr_with_except_column_domains();

        let and = and(and(and(and_i, and_f), and_b), and_s);

        let result = get_domains(&and);

        assert!(
            result.is_ok(),
            "convert expr {} to column domains err",
            &and
        );

        // println!("{:#?}", result.as_ref().unwrap());

        let column_domain = result.as_ref().unwrap();

        // build except result
        except_column_domains.intersect(&and_f_except_column_domains);
        except_column_domains.intersect(&and_b_except_column_domains);
        except_column_domains.intersect(&and_s_except_column_domains);

        assert!(
            except_column_domains.eq(&column_domain),
            "convert expr {} to column domains err, excepted {:?}, found {:?}",
            &and,
            except_column_domains,
            column_domain,
        );
    }

    /// simple and test - 5
    /// eg.
    /// i1 < 1 and i2 = 2 and i3 > -3 and \
    /// f1 < 1.1 and f2 = 2.2 and f3 > 3.3 and \
    /// b1 < 1.1 and b2 = 2.2 and b3 > 3.3 and \
    /// s1 < 1.1 and s2 = 2.2 and s3 > 3.3 and \
    /// d1 < 1.1 and d2 = 2.2 and d3 > 3.3
    ///   ===>
    ///   i1: (_, -1000000)
    ///   i2: [2147483647, 2147483647]
    ///   i3: (3333333333333333, _)
    ///   f1: (_, -1000000.1)
    ///   f2: [2.2, 2.2]
    ///   f3: (3333333333333333.3, _)
    ///   b1: [true, true]
    ///   b2: [true, true]
    ///   b3: [true, true]
    ///   s1: (_, true)
    ///   s2: [" 99 0 _ *", " 99 0 _ *"]
    ///   s3: ("", _)
    ///   d1: (_, Date64(946688462))
    ///   d2: [Date64(1672448462000000), Date64(1672448462000000)]
    ///   d3: (Date64(3662000), _)
    #[test]
    fn test_simple_and_to_domain_5() {
        let (and_i, ref mut except_column_domains) =
            get_tuple_int_and_expr_with_except_column_domains();
        let (and_f, and_f_except_column_domains) =
            get_tuple_float_and_expr_with_except_column_domains();
        let (and_b, and_b_except_column_domains) =
            get_tuple_bool_and_expr_with_except_column_domains();
        let (and_s, and_s_except_column_domains) =
            get_tuple_string_and_expr_with_except_column_domains();
        let (and_d, and_d_except_column_domains) =
            get_tuple_date64_and_expr_with_except_column_domains();

        let and = and(and(and(and(and_i, and_f), and_b), and_s), and_d);

        let result = get_domains(&and);

        assert!(
            result.is_ok(),
            "convert expr {} to column domains err",
            &and
        );

        // println!("{:#?}", result.as_ref().unwrap());

        let column_domain = result.as_ref().unwrap();

        // build except result
        except_column_domains.intersect(&and_f_except_column_domains);
        except_column_domains.intersect(&and_b_except_column_domains);
        except_column_domains.intersect(&and_s_except_column_domains);
        except_column_domains.intersect(&and_d_except_column_domains);

        assert!(
            except_column_domains.eq(&column_domain),
            "convert expr {} to column domains err, excepted {:?}, found {:?}",
            &and,
            except_column_domains,
            column_domain,
        );
    }

    /// and & or
    /// eg.
    ///   i1 < 1 and i2 = 2 and i3 > -3 or \
    ///   i1 <-10 and i2 = 3 and i3 < 3
    ///   ===>
    ///   i1: (_, 1)
    ///   i2: [2, 2], [3, 3]
    ///   i3: (_, _)
    #[test]
    fn test_and_or_to_domain_1() {
        let filter1_1 = binary_expr(col("i1"), Operator::Lt, lit(1));
        let filter2_1 = binary_expr(col("i2"), Operator::Eq, lit(2));
        let filter3_1 = binary_expr(col("i3"), Operator::Gt, lit(-3));

        let filter1_2 = binary_expr(col("i1"), Operator::Lt, lit(-10));
        let filter2_2 = binary_expr(col("i2"), Operator::Eq, lit(3));
        let filter3_2 = binary_expr(col("i3"), Operator::Lt, lit(3));

        let and_1 = and(and(filter1_1, filter2_1), filter3_1);
        let and_2 = and(and(filter1_2, filter2_2), filter3_2);

        let or = or(and_1, and_2);

        let result = get_domains(&or);

        assert!(result.is_ok(), "convert expr {} to column domains err", &or);

        // println!("{:#?}", result.as_ref().unwrap());

        let column_domain = result.as_ref().unwrap();

        // build except result
        //   i1: (_, 1)
        //   i2: [2, 2], [3, 3]
        //   i3: (_, _)
        let i1 = Range::lt(&DataType::Int32, &ScalarValue::Int32(Some(1_i32)));

        let i2_1 = Range::eq(&DataType::Int32, &ScalarValue::Int32(Some(2_i32)));
        let i2_2 = Range::eq(&DataType::Int32, &ScalarValue::Int32(Some(3_i32)));

        let i3 = Range::all(&DataType::Int32);

        let i1_domain = Domain::of_ranges(&[i1]).unwrap();
        let i2_domain = Domain::of_ranges(&[i2_1, i2_2]).unwrap();
        let i3_domain = Domain::of_ranges(&[i3]).unwrap();

        let except_column_domains = &mut ColumnDomains::of(Column::from_name("i1"), &i1_domain);

        except_column_domains.insert_or_intersect(Column::from_name("i2"), &i2_domain);
        except_column_domains.insert_or_intersect(Column::from_name("i3"), &i3_domain);

        assert!(
            except_column_domains.eq(&column_domain),
            "convert expr {} to column domains err, excepted {:?}, found {:?}",
            &or,
            except_column_domains,
            column_domain,
        );
    }

    /// and & or
    /// eg.
    ///   1 < i1 and 2 = i2 and -3 > i3 or \
    ///   -10 < i1 and 3 = i2 and 3 < i3
    ///   ===>
    ///   i1: (-10, _)
    ///   i2: [2, 2], [3, 3]
    ///   i3: (_, -3), (3, _)
    #[test]
    fn test_and_or_to_domain_1_reverse() {
        let filter1_1 = binary_expr(lit(1), Operator::Lt, col("i1"));
        let filter2_1 = binary_expr(lit(2), Operator::Eq, col("i2"));
        let filter3_1 = binary_expr(lit(-3), Operator::Gt, col("i3"));

        let filter1_2 = binary_expr(lit(-10), Operator::Lt, col("i1"));
        let filter2_2 = binary_expr(lit(3), Operator::Eq, col("i2"));
        let filter3_2 = binary_expr(lit(3), Operator::Lt, col("i3"));

        let and_1 = and(and(filter1_1, filter2_1), filter3_1);
        let and_2 = and(and(filter1_2, filter2_2), filter3_2);

        let or = or(and_1, and_2);

        let result = get_domains(&or);

        assert!(result.is_ok(), "convert expr {} to column domains err", &or);

        // println!("{:#?}", result.as_ref().unwrap());

        let column_domain = result.as_ref().unwrap();

        // build except result
        //   i1: (-10, _)
        //   i2: [2, 2], [3, 3]
        //   i3: (_, -3), (3, _)
        let i1 = Range::gt(&DataType::Int32, &ScalarValue::Int32(Some(-10_i32)));

        let i2_1 = Range::eq(&DataType::Int32, &ScalarValue::Int32(Some(2_i32)));
        let i2_2 = Range::eq(&DataType::Int32, &ScalarValue::Int32(Some(3_i32)));

        let i3_1 = Range::lt(&DataType::Int32, &ScalarValue::Int32(Some(-3_i32)));
        let i3_2 = Range::gt(&DataType::Int32, &ScalarValue::Int32(Some(3_i32)));

        let i1_domain = Domain::of_ranges(&[i1]).unwrap();
        let i2_domain = Domain::of_ranges(&[i2_1, i2_2]).unwrap();
        let i3_domain = Domain::of_ranges(&[i3_1, i3_2]).unwrap();

        let except_column_domains = &mut ColumnDomains::of(Column::from_name("i1"), &i1_domain);

        except_column_domains.insert_or_intersect(Column::from_name("i2"), &i2_domain);
        except_column_domains.insert_or_intersect(Column::from_name("i3"), &i3_domain);

        assert!(
            except_column_domains.eq(&column_domain),
            "convert expr {} to column domains err, excepted {:?}, found {:?}",
            &or,
            except_column_domains,
            column_domain,
        );
    }

    /// Expression simplification
    /// eg.
    ///   i1 < 1 and i1 < 2 and \
    ///   i2 = 2 and i2 > -3 and \
    ///   (i3 = 1 or i3 = 2 or i3 = 2)
    ///   ===>
    ///   i1: (_, 1)
    ///   i2: [2, 2]
    ///   i3: [1, 1], [2, 2]
    #[test]
    fn test_simplify_expr_to_domain() {
        // and
        let filter1_1 = binary_expr(col("i1"), Operator::Lt, lit(1));
        let filter1_2 = binary_expr(col("i1"), Operator::Lt, lit(2));
        // and
        let filter2_1 = binary_expr(col("i2"), Operator::Eq, lit(2));
        let filter2_2 = binary_expr(col("i2"), Operator::Gt, lit(-3));
        // or
        let filter3_1 = binary_expr(col("i3"), Operator::Eq, lit(1));
        let filter3_2 = binary_expr(col("i3"), Operator::Eq, lit(2));
        let filter3_3 = binary_expr(col("i3"), Operator::Eq, lit(2));

        let and_1 = and(filter1_1, filter1_2);
        let and_2 = and(filter2_1, filter2_2);
        let or_3 = or(or(filter3_1, filter3_2), filter3_3);

        let and = and(and(and_1, and_2), or_3);

        let result = get_domains(&and);

        assert!(
            result.is_ok(),
            "convert expr {} to column domains err",
            &and
        );

        // println!("{:#?}", result.as_ref().unwrap());

        let column_domain = result.as_ref().unwrap();

        // build except result
        //   i1: (_, 1)
        //   i2: [2, 2]
        //   i3: [1, 1], [2, 2]
        let i1 = Range::lt(&DataType::Int32, &ScalarValue::Int32(Some(1_i32)));
        let i2 = Range::eq(&DataType::Int32, &ScalarValue::Int32(Some(2_i32)));

        let i3_1 = Range::eq(&DataType::Int32, &ScalarValue::Int32(Some(1_i32)));
        let i3_2 = Range::eq(&DataType::Int32, &ScalarValue::Int32(Some(2_i32)));

        let i1_domain = Domain::of_ranges(&[i1]).unwrap();
        let i2_domain = Domain::of_ranges(&[i2]).unwrap();
        let i3_domain = Domain::of_ranges(&[i3_1, i3_2]).unwrap();

        let except_column_domains = &mut ColumnDomains::of(Column::from_name("i1"), &i1_domain);

        except_column_domains.insert_or_intersect(Column::from_name("i2"), &i2_domain);
        except_column_domains.insert_or_intersect(Column::from_name("i3"), &i3_domain);

        assert!(
            except_column_domains.eq(&column_domain),
            "convert expr {} to column domains err, excepted {:?}, found {:?}",
            &and,
            except_column_domains,
            column_domain,
        );
    }

    /// complex application scenarios - 1
    /// eg.
    ///   host = "192.168.1.222" and port > 10000 and port < 20000 and \
    ///   time < "2022/12/31 01:01:01" and time >= "1970/01/01 01:01:01"
    ///   ===>
    ///   host: "192.168.1.222"
    ///   port: [10000, 20000]
    ///   time: ["1970/01/01 01:01:01", "2022/12/31 01:01:01")
    #[test]
    fn test_complex_application_to_domain_1() {
        let host = binary_expr(col("host"), Operator::Eq, lit("192.168.1.222"));
        let port_low = binary_expr(col("port"), Operator::LtEq, lit(20000));
        let port_high = binary_expr(col("port"), Operator::GtEq, lit(10000));

        let epoch_l = NaiveDate::from_ymd_opt(1970, 1, 1)
            .expect("invalid or out-of-range date")
            .and_hms_opt(1, 1, 1)
            .expect("invalid time")
            .add(Duration::milliseconds(1000_i64));

        let epoch_m = NaiveDate::from_ymd_opt(2022, 12, 31)
            .expect("invalid or out-of-range date")
            .and_hms_opt(1, 1, 1)
            .expect("invalid time")
            .add(Duration::milliseconds(1000_i64));

        let filter_d2 = binary_expr(col("time"), Operator::Lt, lit(epoch_m.timestamp_micros()));
        let filter_d3 = binary_expr(col("time"), Operator::GtEq, lit(epoch_l.timestamp_millis()));

        let and_1 = and(and(host, port_low), port_high);
        let and_2 = and(filter_d2, filter_d3);

        let and = and(and_1, and_2);

        let result = get_domains(&and);

        assert!(
            result.is_ok(),
            "convert expr {} to column domains err",
            &and
        );

        // println!("{:#?}", result.as_ref().unwrap());

        let column_domain = result.as_ref().unwrap();

        // build except result
        //   host: "192.168.1.222"
        //   port: [10000, 20000]
        //   time: ["1970/01/01 01:01:01", "2022/12/31 01:01:01")
        let host = Range::eq(
            &DataType::Utf8,
            &ScalarValue::Utf8(Some("192.168.1.222".to_string())),
        );
        let port_1 = Range::ge(&DataType::Int32, &ScalarValue::Int32(Some(10000_i32)));
        let port_2 = Range::le(&DataType::Int32, &ScalarValue::Int32(Some(20000_i32)));

        let time_1 = Range::ge(&DataType::Int64, &ScalarValue::Int64(Some(3662000_i64)));
        let time_2 = Range::lt(
            &DataType::Int64,
            &ScalarValue::Int64(Some(1672448462000000_i64)),
        );

        let host_domain = Domain::of_ranges(&[host]).unwrap();
        let port_1_domain = Domain::of_ranges(&[port_1]).unwrap();
        let port_2_domain = Domain::of_ranges(&[port_2]).unwrap();
        let time_1_domain = Domain::of_ranges(&[time_1]).unwrap();
        let time_2_domain = Domain::of_ranges(&[time_2]).unwrap();

        let except_column_domains = &mut ColumnDomains::of(Column::from_name("host"), &host_domain);

        except_column_domains.insert_or_intersect(Column::from_name("port"), &port_1_domain);
        except_column_domains.insert_or_intersect(Column::from_name("port"), &port_2_domain);
        except_column_domains.insert_or_intersect(Column::from_name("time"), &time_1_domain);
        except_column_domains.insert_or_intersect(Column::from_name("time"), &time_2_domain);

        assert!(
            except_column_domains.eq(&column_domain),
            "convert expr {} to column domains err, excepted {:?}, found {:?}",
            &and,
            except_column_domains,
            column_domain,
        );
    }

    /// complex application scenarios - 2
    /// eg.
    ///   region = "hangzhou" and \
    ///   (host >= host200 or host <= host099) and \
    ///   time < "2022/12/31 01:01:01" and time >= "1970/01/01 01:01:01"
    ///   ===>
    ///   region: "hangzhou"
    ///   host: (_, host099], [host200, _)
    ///   time: ["1970/01/01 01:01:01", "2022/12/31 01:01:01")
    #[test]
    fn test_complex_application_to_domain_2() {
        let region = binary_expr(col("region"), Operator::Eq, lit("hangzhou"));
        let host_low = binary_expr(col("host"), Operator::LtEq, lit("host099"));
        let host_high = binary_expr(col("host"), Operator::GtEq, lit("host200"));

        let epoch_l = NaiveDate::from_ymd_opt(1970, 1, 1)
            .expect("invalid or out-of-range date")
            .and_hms_opt(1, 1, 1)
            .expect("invalid time")
            .add(Duration::milliseconds(1000_i64));

        let epoch_m = NaiveDate::from_ymd_opt(2022, 12, 31)
            .expect("invalid or out-of-range date")
            .and_hms_opt(1, 1, 1)
            .expect("invalid time")
            .add(Duration::milliseconds(1000_i64));

        let filter_d2 = binary_expr(col("time"), Operator::Lt, lit(epoch_m.timestamp_micros()));
        let filter_d3 = binary_expr(col("time"), Operator::GtEq, lit(epoch_l.timestamp_millis()));

        let host_or = or(host_low, host_high);
        let time_and = and(filter_d2, filter_d3);

        let and = and(and(region, host_or), time_and);

        let result = get_domains(&and);

        assert!(
            result.is_ok(),
            "convert expr {} to column domains err",
            &and
        );

        // println!("{:#?}", result.as_ref().unwrap());

        let column_domain = result.as_ref().unwrap();

        // build except result
        //   region: "hangzhou"
        //   host: (_, host099], [host200, _)
        //   time: ["1970/01/01 01:01:01", "2022/12/31 01:01:01")
        let region = Range::eq(
            &DataType::Utf8,
            &ScalarValue::Utf8(Some("hangzhou".to_string())),
        );
        let host_1 = Range::le(
            &DataType::Utf8,
            &ScalarValue::Utf8(Some("host099".to_string())),
        );
        let host_2 = Range::ge(
            &DataType::Utf8,
            &ScalarValue::Utf8(Some("host200".to_string())),
        );

        let time_1 = Range::ge(&DataType::Int64, &ScalarValue::Int64(Some(3662000_i64)));
        let time_2 = Range::lt(
            &DataType::Int64,
            &ScalarValue::Int64(Some(1672448462000000_i64)),
        );

        println!("{:#?}", &time_1);
        println!("{:#?}", &time_2);

        let region_domain = Domain::of_ranges(&[region]).unwrap();
        let host_domain = Domain::of_ranges(&[host_1, host_2]).unwrap();
        let time_1_domain = Domain::of_ranges(&[time_1]).unwrap();
        let time_2_domain = Domain::of_ranges(&[time_2]).unwrap();

        let except_column_domains =
            &mut ColumnDomains::of(Column::from_name("region"), &region_domain);

        except_column_domains.insert_or_intersect(Column::from_name("host"), &host_domain);
        except_column_domains.insert_or_intersect(Column::from_name("time"), &time_1_domain);
        except_column_domains.insert_or_intersect(Column::from_name("time"), &time_2_domain);

        assert!(
            except_column_domains.eq(&column_domain),
            "convert expr {} to column domains err, excepted {:?}, found {:?}",
            &and,
            except_column_domains,
            column_domain,
        );
    }

    /// partial support push down
    /// eg.
    ///   c1 > 1 and c2 > 1 or \
    ///   c1 > -1
    ///   ===>
    ///   c1: (-1, _)
    #[test]
    fn test_partial_support_expr_to_domain_1() {
        let c1_1 = binary_expr(col("c1"), Operator::Gt, lit(1));
        let c2_1 = binary_expr(col("c2"), Operator::Gt, lit(1));

        let c1_2 = binary_expr(col("c1"), Operator::Gt, lit(-1));

        let and_1 = and(c1_1, c2_1);

        let and = or(and_1, c1_2);

        let result = get_domains(&and);

        assert!(
            result.is_ok(),
            "convert expr {} to column domains err",
            &and
        );

        // println!("{:#?}", result.as_ref().unwrap());

        let column_domain = result.as_ref().unwrap();

        // build except result
        let range = Range::gt(&DataType::Int32, &ScalarValue::Int32(Some(-1_i32)));
        let domain = Domain::of_ranges(&[range]).unwrap();
        let except_column_domains: ColumnDomains<Column> =
            ColumnDomains::of(Column::from_name("c1"), &domain);

        assert!(
            except_column_domains.eq(column_domain),
            "convert expr {} to column domains err, excepted {:?}, found {:?}",
            &and,
            except_column_domains,
            column_domain,
        );
    }

    /// not support push down - 1
    /// eg.
    ///   c1 > 1 or \
    ///   c2 > 1
    ///   ===>
    ///   All
    #[test]
    fn test_not_support_expr_to_domain_1() {
        let c1 = binary_expr(col("c1"), Operator::Gt, lit(1));
        let c2 = binary_expr(col("c2"), Operator::Gt, lit(1));

        let or = or(c1, c2);

        let result = get_domains(&or);

        assert!(
            result.is_ok(),
            "convert expr {} to column domains err: {}",
            &or,
            result.unwrap_err()
        );

        // println!("{:#?}", result.as_ref().unwrap());

        let column_domain = result.as_ref().unwrap();

        assert!(
            column_domain.is_all(),
            "convert expr {} to column domains err, excepted ColumnDomains::All",
            &or
        );
    }

    /// not support push down - 2
    /// eg.
    ///   c1 in Values(1), (2), (3)
    ///   ===>
    ///   All
    #[test]
    fn test_not_support_expr_to_domain_2() {
        let list = vec![lit(1), lit(2), lit(3)];

        let in_list = in_list(col("c1"), list, false);

        let result = get_domains(&in_list);

        assert!(
            result.is_ok(),
            "convert expr {} to column domains err",
            &in_list
        );

        // println!("{:#?}", result.as_ref().unwrap());

        let column_domain = result.as_ref().unwrap();

        assert!(
            column_domain.is_all(),
            "convert expr {} to column domains err, excepted ColumnDomains::All",
            &in_list
        );
    }

    /// not support push down - 3
    /// eg.
    ///   not c1 > 1
    ///   ===>
    ///   All
    #[test]
    fn test_not_support_expr_to_domain_3() {
        let c1 = binary_expr(col("c1"), Operator::Gt, lit(1));

        let not = Expr::Not(Box::new(c1));

        let result = get_domains(&not);

        assert!(
            result.is_ok(),
            "convert expr {} to column domains err",
            &not
        );

        // println!("{:#?}", result.as_ref().unwrap());

        let column_domain = result.as_ref().unwrap();

        assert!(
            column_domain.is_all(),
            "convert expr {} to column domains err, excepted ColumnDomains::All",
            &not
        );
    }

    /// not support push down - 4
    /// eg.
    ///   c1 is null
    ///   ===>
    ///   All
    #[test]
    fn test_not_support_expr_to_domain_4() {
        let is_null = Expr::IsNull(Box::new(col("c1")));

        let result = get_domains(&is_null);

        assert!(
            result.is_ok(),
            "convert expr {} to column domains err: {}",
            &is_null,
            result.unwrap_err()
        );

        // println!("{:#?}", result.as_ref().unwrap());

        let column_domain = result.as_ref().unwrap();

        assert!(
            column_domain.is_all(),
            "convert expr {} to column domains err, excepted ColumnDomains::All",
            &is_null
        );
    }

    /// not support push down - 5
    /// eg.
    ///   c1 > random()
    ///   ===>
    ///   All
    #[test]
    fn test_not_support_expr_to_domain_5() {
        let with_func = binary_expr(col("c1"), Operator::Gt, random());

        let result = get_domains(&with_func);

        assert!(
            result.is_ok(),
            "convert expr {} to column domains err: {}",
            &with_func,
            result.unwrap_err()
        );

        // println!("{:#?}", result.as_ref().unwrap());

        let column_domain = result.as_ref().unwrap();

        assert!(
            column_domain.is_all(),
            "convert expr {} to column domains err, excepted ColumnDomains::All",
            &with_func
        );
    }

    #[cfg(test)]
    mod test_normalized_simple_comparison {
        use super::*;

        #[test]
        fn test_of() {
            let c1 = "c1";
            let val = 1_i32;
            let nsc_option = NormalizedSimpleComparison::of(col(c1), Operator::Gt, lit(val));

            assert!(nsc_option.is_some());
            let nsc = nsc_option.unwrap();

            assert_eq!(nsc.column, Column::from_name(c1));
            assert_eq!(nsc.op, Operator::Gt);
            assert_eq!(nsc.value, ScalarValue::Int32(Some(val)));

            let nsc_option = NormalizedSimpleComparison::of(lit(val), Operator::Gt, col(c1));

            assert!(nsc_option.is_some());
            let nsc = nsc_option.unwrap();

            assert_eq!(nsc.column, Column::from_name(c1));
            assert_eq!(nsc.op, Operator::Lt);
            assert_eq!(nsc.value, ScalarValue::Int32(Some(val)));
        }

        #[test]
        fn test_reverse() {
            let wait_reverse = vec![
                Operator::Lt,
                Operator::LtEq,
                Operator::Gt,
                Operator::GtEq,
                Operator::Eq,
                Operator::NotEq,
            ];
            let except_reverse = vec![
                Operator::Gt,
                Operator::GtEq,
                Operator::Lt,
                Operator::LtEq,
                Operator::Eq,
                Operator::NotEq,
            ];

            let after_reverse: Vec<Operator> = wait_reverse
                .iter()
                .map(|e| NormalizedSimpleComparison::reverse_op(*e))
                .collect();

            assert_eq!(except_reverse, after_reverse);
        }
    }
}
