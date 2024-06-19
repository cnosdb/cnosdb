use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use datafusion::physical_optimizer::pruning::PruningPredicate;

use super::column_group::statistics::ColumnGroupsStatisticsWrapper;
use super::Predicate;
use crate::reader::utils::reassign_predicate_columns;
use crate::tsm::column_group::ColumnGroup;
use crate::TskvResult;

pub fn filter_column_groups(
    cgs: Vec<Arc<ColumnGroup>>,
    predicate: &Option<Arc<Predicate>>,
    chunk_schema: SchemaRef,
) -> TskvResult<Vec<Arc<ColumnGroup>>> {
    if let Some(indices) = filter_column_groups_indices(&cgs, predicate, chunk_schema)? {
        let cgs = indices
            .into_iter()
            .zip(cgs)
            .filter(|(b, _)| *b)
            .map(|(_, cg)| cg)
            .collect::<Vec<_>>();
        return Ok(cgs);
    }

    Ok(cgs)
}

fn filter_column_groups_indices(
    cgs: &[Arc<ColumnGroup>],
    predicate: &Option<Arc<Predicate>>,
    chunk_schema: SchemaRef,
) -> TskvResult<Option<Vec<bool>>> {
    if let Some(predicate) = predicate {
        let new_predicate = reassign_predicate_columns(predicate.clone(), chunk_schema.clone())?;
        let statistics = ColumnGroupsStatisticsWrapper(cgs);

        if let Some(expr) = new_predicate {
            let pruning_predicate = PruningPredicate::try_new(expr, chunk_schema)?;
            let indices = pruning_predicate.prune(&statistics)?;
            Ok(Some(indices))
        } else {
            Ok(None)
        }
    } else {
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
    use datafusion::logical_expr::{BuiltinScalarFunction, Operator};
    use datafusion::physical_expr::execution_props::ExecutionProps;
    use datafusion::physical_plan::expressions::{lit, BinaryExpr, Column};
    use datafusion::physical_plan::functions::create_physical_expr;
    use datafusion::scalar::ScalarValue;
    use models::schema::tskv_table_schema::{ColumnType, TableColumn};
    use models::ValueType;

    use crate::reader::chunk::filter_column_groups_indices;
    use crate::reader::Predicate;
    use crate::tsm::column_group::ColumnGroup;
    use crate::tsm::page::{PageMeta, PageStatistics, PageWriteSpec};
    use crate::tsm::statistics::ValueStatistics;

    /// ```text
    ///                     time            tag1            field1
    ///                  ┌──────────────┬───────────────┬───────────────┐
    /// column_group 1   │  [1, 3]      │   [00, 88]    │   [0, 5]      │
    ///                  ├──────────────┼───────────────┼───────────────┤
    /// column_group 2   │  [4, 5]      │   [11, 22]    │   [4, 6]      │
    ///                  ├──────────────┼───────────────┼───────────────┤
    /// column_group 3   │  [7, 8]      │   [33, 77]    │   [null, 4]   │
    ///                  ├──────────────┼───────────────┼───────────────┤
    /// column_group 4   │  [9, 14]     │   [44, 99]    │   [3, null]   │
    ///                  └──────────────┴───────────────┴───────────────┘
    /// ```
    fn data() -> Vec<Arc<ColumnGroup>> {
        let time_column = TableColumn::new_time_column(0, TimeUnit::Second);
        let time_statistics = vec![
            PageStatistics::I64(ValueStatistics::new(Some(1), Some(3), None, 1)),
            PageStatistics::I64(ValueStatistics::new(Some(4), Some(5), None, 1)),
            PageStatistics::I64(ValueStatistics::new(Some(7), Some(8), None, 1)),
            PageStatistics::I64(ValueStatistics::new(Some(9), Some(14), None, 1)),
        ];

        let tag_column = TableColumn::new_tag_column(1, "tag1".to_string());
        let tag_statistics = vec![
            PageStatistics::Bytes(ValueStatistics::new(
                Some("00".as_bytes().to_vec()),
                Some("88".as_bytes().to_vec()),
                None,
                1,
            )),
            PageStatistics::Bytes(ValueStatistics::new(
                Some("11".as_bytes().to_vec()),
                Some("22".as_bytes().to_vec()),
                None,
                1,
            )),
            PageStatistics::Bytes(ValueStatistics::new(
                Some("33".as_bytes().to_vec()),
                Some("77".as_bytes().to_vec()),
                None,
                1,
            )),
            PageStatistics::Bytes(ValueStatistics::new(
                Some("44".as_bytes().to_vec()),
                Some("99".as_bytes().to_vec()),
                None,
                1,
            )),
        ];

        let field_column = TableColumn::new(
            2,
            "field1".to_string(),
            ColumnType::Field(ValueType::Integer),
            Default::default(),
        );
        let field_statistics = vec![
            PageStatistics::I64(ValueStatistics::new(Some(0), Some(5), None, 1)),
            PageStatistics::I64(ValueStatistics::new(Some(4), Some(6), None, 1)),
            PageStatistics::I64(ValueStatistics::new(None, Some(4), None, 1)),
            PageStatistics::I64(ValueStatistics::new(Some(3), None, None, 1)),
        ];

        let mut cgs = vec![];
        for (idx, ((time_s, tag_s), field_s)) in time_statistics
            .into_iter()
            .zip(tag_statistics.into_iter())
            .zip(field_statistics.into_iter())
            .enumerate()
        {
            let mut cg = ColumnGroup::new(idx as u64);
            cg.push(PageWriteSpec::new(
                0,
                0,
                PageMeta {
                    num_values: 1,
                    column: time_column.clone(),
                    statistics: time_s,
                },
            ));
            cg.push(PageWriteSpec::new(
                0,
                0,
                PageMeta {
                    num_values: 1,
                    column: tag_column.clone(),
                    statistics: tag_s,
                },
            ));
            cg.push(PageWriteSpec::new(
                0,
                0,
                PageMeta {
                    num_values: 1,
                    column: field_column.clone(),
                    statistics: field_s,
                },
            ));
            cgs.push(Arc::new(cg))
        }

        cgs
    }

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new(
                "time",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                true,
            ),
            Field::new("tag1", DataType::Utf8, true),
            Field::new("field1", DataType::Int64, true),
        ]))
    }

    #[test]
    fn test_filter_time_column_groups_indices() {
        let schema = schema();
        let data = data();
        let expr = Arc::new(BinaryExpr::new(
            Arc::new(Column::new("time", 0)),
            Operator::Gt,
            lit(ScalarValue::TimestampMillisecond(Some(5), None)),
        ));
        let predicate = Arc::new(Predicate::new(Some(expr), schema.clone(), None));

        let cgs = filter_column_groups_indices(&data, &Some(predicate), schema)
            .unwrap()
            .unwrap();

        assert_eq!(cgs, vec![false, false, true, true]);
    }

    #[test]
    fn test_filter_tag_column_groups_indices() {
        let schema = schema();
        let data = data();
        let expr = Arc::new(BinaryExpr::new(
            Arc::new(Column::new("tag1", 0)),
            Operator::Eq,
            lit(ScalarValue::Utf8(Some("33".to_string()))),
        ));
        let predicate = Arc::new(Predicate::new(Some(expr), schema.clone(), None));

        let cgs = filter_column_groups_indices(&data, &Some(predicate), schema)
            .unwrap()
            .unwrap();

        assert_eq!(cgs, vec![true, false, true, false]);
    }

    #[test]
    fn test_filter_multi_column_groups_indices() {
        let schema = schema();
        let data = data();
        let time_expr = Arc::new(BinaryExpr::new(
            Arc::new(Column::new("time", 0)),
            Operator::Lt,
            lit(ScalarValue::TimestampMillisecond(Some(10), None)),
        ));
        let tag_expr = Arc::new(BinaryExpr::new(
            Arc::new(Column::new("tag1", 0)),
            Operator::Eq,
            lit(ScalarValue::Utf8(Some("40".to_string()))),
        ));
        let field_expr = Arc::new(BinaryExpr::new(
            Arc::new(Column::new("field1", 0)),
            Operator::Eq,
            lit(ScalarValue::Int64(Some(10))),
        ));

        let expr = Arc::new(BinaryExpr::new(
            Arc::new(BinaryExpr::new(time_expr, Operator::And, tag_expr)),
            Operator::Or,
            field_expr,
        ));

        let predicate = Arc::new(Predicate::new(Some(expr), schema.clone(), None));

        let cgs = filter_column_groups_indices(&data, &Some(predicate), schema)
            .unwrap()
            .unwrap();

        assert_eq!(cgs, vec![true, false, true, true]);
    }

    #[test]
    fn test_filter_field_column_groups_indices_with_func() {
        let schema = schema();
        let data = data();

        let func_expr = create_physical_expr(
            &BuiltinScalarFunction::Abs,
            &[Arc::new(Column::new("field1", 2))],
            schema.as_ref(),
            &ExecutionProps::default(),
        )
        .unwrap();
        let expr = Arc::new(BinaryExpr::new(
            Arc::new(Column::new("field1", 0)),
            Operator::Eq,
            func_expr,
        ));

        let predicate = Arc::new(Predicate::new(Some(expr), schema.clone(), None));

        let cgs = filter_column_groups_indices(&data, &Some(predicate), schema)
            .unwrap()
            .unwrap();

        assert_eq!(cgs, vec![true, true, true, true]);
    }

    #[test]
    fn test_filter_not_exists_column_groups_indices() {
        let schema = schema();
        let data = data();

        let expr = Arc::new(BinaryExpr::new(
            Arc::new(Column::new("field_not_exsists", 0)),
            Operator::Eq,
            lit(ScalarValue::Int64(Some(10))),
        ));

        let predicate = Arc::new(Predicate::new(Some(expr), schema.clone(), None));

        let cgs = filter_column_groups_indices(&data, &Some(predicate), schema);

        assert!(cgs.is_err());
    }
}
