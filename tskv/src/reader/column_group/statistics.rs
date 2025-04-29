use std::collections::HashSet;
use std::sync::Arc;

use arrow::datatypes::DataType;
use arrow_array::{ArrayRef, BooleanArray};
use datafusion::common::Column;
use datafusion::physical_optimizer::pruning::PruningStatistics;
use datafusion::scalar::ScalarValue;

use crate::tsm::column_group::ColumnGroup;
use crate::tsm::page::PageStatistics;

pub struct ColumnGroupsStatisticsWrapper<'a>(pub &'a [Arc<ColumnGroup>]);

impl PruningStatistics for ColumnGroupsStatisticsWrapper<'_> {
    fn min_values(&self, column: &datafusion::prelude::Column) -> Option<ArrayRef> {
        let data_type = self.0.first().and_then(|cg| {
            cg.pages()
                .iter()
                .find(|e| e.meta.column.name == column.name)
                .map(|e| DataType::from(e.meta().column.column_type.clone()))
        })?;

        let null_value = ScalarValue::try_from(data_type).ok()?;

        let values = self.0.iter().map(|cg| {
            cg.pages()
                .iter()
                .find(|e| e.meta.column.name == column.name)
                .map(|e| match &e.meta().statistics {
                    PageStatistics::Bool(v) => ScalarValue::from(*v.min()),
                    PageStatistics::F64(v) => ScalarValue::from(*v.min()),
                    PageStatistics::I64(v) => ScalarValue::from(*v.min()),
                    PageStatistics::U64(v) => ScalarValue::from(*v.min()),
                    PageStatistics::Bytes(v) => {
                        let str = v.min().as_ref().and_then(|e| std::str::from_utf8(e).ok());
                        ScalarValue::from(str)
                    }
                })
                .unwrap_or(null_value.clone())
        });

        ScalarValue::iter_to_array(values).ok()
    }

    fn max_values(&self, column: &datafusion::prelude::Column) -> Option<ArrayRef> {
        let data_type = self.0.first().and_then(|cg| {
            cg.pages()
                .iter()
                .find(|e| e.meta.column.name == column.name)
                .map(|e| DataType::from(e.meta().column.column_type.clone()))
        })?;

        let null_value = ScalarValue::try_from(data_type).ok()?;

        let values = self.0.iter().map(|cg| {
            cg.pages()
                .iter()
                .find(|e| e.meta.column.name == column.name)
                .map(|e| match &e.meta().statistics {
                    PageStatistics::Bool(v) => ScalarValue::from(*v.max()),
                    PageStatistics::F64(v) => ScalarValue::from(*v.max()),
                    PageStatistics::I64(v) => ScalarValue::from(*v.max()),
                    PageStatistics::U64(v) => ScalarValue::from(*v.max()),
                    PageStatistics::Bytes(v) => {
                        let str = v.max().as_ref().and_then(|e| std::str::from_utf8(e).ok());
                        ScalarValue::from(str)
                    }
                })
                .unwrap_or(null_value.clone())
        });

        ScalarValue::iter_to_array(values).ok()
    }

    fn num_containers(&self) -> usize {
        self.0.len()
    }

    fn null_counts(&self, _column: &datafusion::prelude::Column) -> Option<ArrayRef> {
        None
    }

    fn row_counts(&self, _column: &Column) -> Option<ArrayRef> {
        None
    }

    fn contained(&self, _column: &Column, _values: &HashSet<ScalarValue>) -> Option<BooleanArray> {
        None
    }
}
