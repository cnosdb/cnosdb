use std::cmp::{min, Ordering};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use arrow::datatypes::SchemaRef;
use arrow_array::RecordBatch;
use datafusion::common::tree_node::{TreeNode, TreeNodeRewriter, TreeNodeVisitor, VisitRecursion};
use datafusion::error::DataFusionError;
use datafusion::logical_expr::Operator;
use datafusion::physical_plan::expressions::{BinaryExpr, Column, Literal};
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use datafusion::physical_plan::PhysicalExpr;
use datafusion::scalar::ScalarValue;
use futures::{Stream, StreamExt};
use models::predicate::domain::TimeRange;
use models::SeriesId;

use super::metrics::BaselineMetrics;
use super::{Predicate, SchemableTskvRecordBatchStream, SendableSchemableTskvRecordBatchStream};
use crate::error::MismatchedSchemaSnafu;
use crate::TskvResult;

pub trait DataReferenceExt {
    fn series_id(&self) -> SeriesId;
    fn time_range(&self) -> TimeRange;
    fn data_id(&self) -> u64;
    fn set_data_id(&mut self, data_id: u64);
    fn file_id(&self) -> u64;
}

impl PartialEq for dyn DataReferenceExt {
    fn eq(&self, other: &Self) -> bool {
        self.file_id() == other.file_id()
            && self.series_id() == other.series_id()
            && self.time_range() == other.time_range()
    }
}

impl Eq for dyn DataReferenceExt {}

impl PartialOrd for dyn DataReferenceExt {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for dyn DataReferenceExt {
    fn cmp(&self, other: &Self) -> Ordering {
        self.data_id()
            .cmp(&other.data_id())
            .then_with(|| self.series_id().cmp(&other.series_id()))
            .then_with(|| self.file_id().cmp(&other.file_id()))
            .then_with(|| self.time_range().cmp(&other.time_range()))
    }
}

pub fn preprocess_metadata<T: DataReferenceExt + 'static>(metadata_list: &mut [T]) {
    metadata_list.sort_by(|a, b| {
        a.series_id()
            .cmp(&b.series_id())
            .then_with(|| a.time_range().cmp(&b.time_range()))
    });

    for i in 0..metadata_list.len() {
        for j in (i + 1)..metadata_list.len() {
            if metadata_list[i].series_id() == metadata_list[j].series_id()
                && metadata_list[i].time_range().max_ts > metadata_list[j].time_range().min_ts
            {
                let data_id_i = metadata_list[i].data_id();
                let data_id_j = metadata_list[j].data_id();
                metadata_list[j].set_data_id(min(data_id_i, data_id_j));
            }
        }
    }
    metadata_list.sort_by(|a, b| {
        let a = a as &dyn DataReferenceExt;
        let b = b as &dyn DataReferenceExt;
        a.cmp(b)
    });
}

/// CombinedRecordBatchStream can be used to combine a Vec of SendableRecordBatchStreams into one
pub struct CombinedRecordBatchStream {
    /// Schema wrapped by Arc
    schema: SchemaRef,
    /// Stream entries
    entries: Vec<SendableSchemableTskvRecordBatchStream>,
    metrics: BaselineMetrics,
}

impl CombinedRecordBatchStream {
    /// 依次从 entries 中取出 stream，直到取到第一个不为空的 stream 为止。
    /// 要求所有 steram 的 schema一致。
    /// 只有从一个 stream 中取完所有元素时，才会进行下一个 stream 的取值。
    ///
    /// 注意：为了性能考虑，会反向遍历 entries 列表中的 stream。
    pub fn try_new(
        schema: SchemaRef,
        entries: Vec<SendableSchemableTskvRecordBatchStream>,
        metrics: &ExecutionPlanMetricsSet,
    ) -> TskvResult<Self> {
        for s in &entries {
            if s.schema().fields() != schema.fields() {
                return Err(MismatchedSchemaSnafu {
                    msg: format!(
                        "in combined stream. Expected: {:?}, got {:?}",
                        schema,
                        s.schema()
                    ),
                }
                .build());
            }
        }

        Ok(Self {
            schema,
            entries,
            metrics: BaselineMetrics::new(metrics),
        })
    }
}

impl SchemableTskvRecordBatchStream for CombinedRecordBatchStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

impl Stream for CombinedRecordBatchStream {
    type Item = TskvResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        use Poll::*;

        let poll;
        loop {
            match self.entries.last_mut() {
                Some(s) => {
                    match s.poll_next_unpin(cx) {
                        Ready(Some(val)) => {
                            poll = Ready(Some(val));
                            break;
                        }
                        Ready(None) => {
                            // Remove the entry
                            let _ = self.entries.pop();
                            continue;
                        }
                        Pending => {
                            poll = Pending;
                            break;
                        }
                    }
                }
                None => {
                    poll = Ready(None);
                    break;
                }
            }
        }
        self.metrics.record_poll(poll)
    }
}

/// Re-assign column indices referenced in predicate according to given schema.
/// If a column is not found in the schema, it will be replaced.
///
/// if we have a file, has [time, c1, c2],
/// If the filter is 'c3 = 1', then it will be replaced by a expr 'true'.
pub fn reassign_predicate_columns(
    pred: Arc<Predicate>,
    file_schema: SchemaRef,
) -> TskvResult<Option<Arc<dyn PhysicalExpr>>, DataFusionError> {
    let full_schema = pred.schema();
    let mut rewriter = PredicateColumnsReassigner {
        file_schema: file_schema.clone(),
        full_schema,
    };

    match pred.expr() {
        None => Ok(None),
        Some(expr) => {
            let new_expr = expr.clone().rewrite(&mut rewriter)?;
            let mut vistor = ColumnVistor {
                part_schema: file_schema,
                has_col_not_in_file: false,
            };
            new_expr.visit(&mut vistor)?;
            if vistor.has_col_not_in_file {
                return Ok(None);
            }
            Ok(Some(new_expr))
        }
    }
}

struct PredicateColumnsReassigner {
    file_schema: SchemaRef,
    full_schema: SchemaRef,
}

struct ColumnVistor {
    part_schema: SchemaRef,
    has_col_not_in_file: bool,
}

impl TreeNodeVisitor for ColumnVistor {
    type N = Arc<dyn PhysicalExpr>;

    fn pre_visit(&mut self, node: &Self::N) -> datafusion::common::Result<VisitRecursion> {
        if let Some(column) = node.as_any().downcast_ref::<Column>() {
            if self.part_schema.index_of(column.name()).is_err() {
                self.has_col_not_in_file = true;
                return Ok(VisitRecursion::Stop);
            }
        }
        Ok(VisitRecursion::Continue)
    }
}

impl TreeNodeRewriter for PredicateColumnsReassigner {
    type N = Arc<dyn PhysicalExpr>;

    fn mutate(
        &mut self,
        expr: Arc<dyn PhysicalExpr>,
    ) -> TskvResult<Arc<dyn PhysicalExpr>, DataFusionError> {
        if let Some(column) = expr.as_any().downcast_ref::<Column>() {
            // find the column index in file schema
            let index = match self.file_schema.index_of(column.name()) {
                Ok(idx) => idx,
                Err(_) => {
                    // the column expr must be in the full schema
                    return match self.full_schema.field_with_name(column.name()) {
                        Ok(_) => Ok(expr),
                        Err(e) => {
                            // If the column is not in the full schema, should throw the error
                            Err(DataFusionError::ArrowError(e))
                        }
                    };
                }
            };
            return Ok(Arc::new(Column::new(column.name(), index)));
        } else if let Some(b_expr) = expr.as_any().downcast_ref::<BinaryExpr>() {
            let mut visitor = ColumnVistor {
                part_schema: self.file_schema.clone(),
                has_col_not_in_file: false,
            };
            b_expr.left().visit(&mut visitor)?;
            let left = visitor.has_col_not_in_file;

            visitor.has_col_not_in_file = false;
            b_expr.right().visit(&mut visitor)?;
            let right = visitor.has_col_not_in_file;

            if matches!(b_expr.op(), Operator::And) {
                return match (left, right) {
                    (false, false) => Ok(expr),
                    (true, true) => Ok(Arc::new(Literal::new(ScalarValue::Boolean(Some(true))))),
                    (true, _) => Ok(b_expr.right().clone()),
                    (_, true) => Ok(b_expr.left().clone()),
                };
            }
            if matches!(b_expr.op(), Operator::Or) {
                return match (left, right) {
                    (false, false) => Ok(expr),
                    _ => Ok(Arc::new(Literal::new(ScalarValue::Boolean(Some(true))))),
                };
            }
        }

        Ok(expr)
    }
}

#[cfg(test)]
mod tests {
    use models::predicate::domain::TimeRange;
    use models::SeriesId;

    use crate::reader::utils::{preprocess_metadata, DataReferenceExt};

    #[derive(Debug, Eq, Ord, PartialOrd, PartialEq)]
    struct TestDataReference {
        series_id: SeriesId,
        time_range: TimeRange,
        data_id: u64,
        file_id: u64,
    }

    impl DataReferenceExt for TestDataReference {
        fn series_id(&self) -> SeriesId {
            self.series_id
        }

        fn time_range(&self) -> TimeRange {
            self.time_range
        }

        fn data_id(&self) -> u64 {
            self.data_id
        }

        fn set_data_id(&mut self, data_id: u64) {
            self.data_id = data_id;
        }

        fn file_id(&self) -> u64 {
            self.file_id
        }
    }

    impl TestDataReference {
        pub fn new(series_id: SeriesId, time_range: TimeRange, data_id: u64, file_id: u64) -> Self {
            Self {
                series_id,
                time_range,
                data_id,
                file_id,
            }
        }
    }

    #[test]
    fn test_group_overlapping_segments() {
        let mut meta_data_list = vec![
            TestDataReference::new(3, TimeRange::new(1, 3), 1, 1),
            TestDataReference::new(4, TimeRange::new(2, 4), 1, 1),
            TestDataReference::new(5, TimeRange::new(3, 5), 1, 1),
            TestDataReference::new(3, TimeRange::new(4, 6), 2, 2),
            TestDataReference::new(4, TimeRange::new(3, 7), 2, 2),
            TestDataReference::new(5, TimeRange::new(1, 3), 2, 2),
            TestDataReference::new(3, TimeRange::new(1, 3), u64::MAX, u64::MAX),
            TestDataReference::new(4, TimeRange::new(2, 4), u64::MAX, u64::MAX),
            TestDataReference::new(5, TimeRange::new(3, 5), u64::MAX, u64::MAX),
        ];
        preprocess_metadata(&mut meta_data_list);
        let expected_list = vec![
            TestDataReference::new(3, TimeRange::new(1, 3), 1, 1),
            TestDataReference::new(3, TimeRange::new(1, 3), 1, u64::MAX),
            TestDataReference::new(4, TimeRange::new(2, 4), 1, 1),
            TestDataReference::new(4, TimeRange::new(3, 7), 1, 2),
            TestDataReference::new(4, TimeRange::new(2, 4), 1, u64::MAX),
            TestDataReference::new(5, TimeRange::new(3, 5), 1, 1),
            TestDataReference::new(5, TimeRange::new(3, 5), 1, u64::MAX),
            TestDataReference::new(3, TimeRange::new(4, 6), 2, 2),
            TestDataReference::new(5, TimeRange::new(1, 3), 2, 2),
        ];
        assert_eq!(meta_data_list, expected_list);
        // println!("{:#?}", meta_data_list);
    }
}
