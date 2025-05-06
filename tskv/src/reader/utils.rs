use std::cmp;
use std::fmt::{Debug, Formatter};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use arrow::datatypes::SchemaRef;
use arrow_array::RecordBatch;
use datafusion::common::tree_node::{
    Transformed, TreeNode, TreeNodeRecursion, TreeNodeRewriter, TreeNodeVisitor,
};
use datafusion::error::DataFusionError;
use datafusion::logical_expr::Operator;
use datafusion::physical_plan::expressions::{BinaryExpr, Column, Literal};
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use datafusion::physical_plan::PhysicalExpr;
use datafusion::scalar::ScalarValue;
use futures::{Stream, StreamExt};
use models::predicate::domain::TimeRange;

use super::metrics::BaselineMetrics;
use super::{Predicate, SchemableTskvRecordBatchStream, SendableSchemableTskvRecordBatchStream};
use crate::error::MismatchedSchemaSnafu;
use crate::TskvResult;

pub struct OverlappingSegments<T: TimeRangeProvider> {
    segments: Vec<T>,
}

impl<T: TimeRangeProvider> OverlappingSegments<T> {
    pub fn segments(self) -> Vec<T> {
        self.segments
    }
}

impl<T: PartialOrd + Ord + TimeRangeProvider> OverlappingSegments<T> {
    pub fn sort(&mut self) {
        self.segments.sort();
    }
}

impl<T: TimeRangeProvider> IntoIterator for OverlappingSegments<T> {
    type Item = T;
    type IntoIter = std::vec::IntoIter<T>;

    fn into_iter(self) -> Self::IntoIter {
        self.segments.into_iter()
    }
}

impl<T: TimeRangeProvider> Debug for OverlappingSegments<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_list()
            .entries(self.segments.iter().map(|t| t.time_range()))
            .finish()
    }
}

pub trait TimeRangeProvider {
    fn time_range(&self) -> TimeRange;
}

/// Given a slice of range-like items `ordered_chunks`, this function groups overlapping segments
/// and returns a vector of `OverlappingSegments`. It checks for overlaps between the segments
/// and groups them accordingly.
///
/// # Arguments
///
/// * `ordered_chunks` - A slice of range-like items that need to be grouped based on overlaps.
///
/// # Type Parameters
///
/// * `R` - Type representing a range-like item.
/// * `T` - Type that is used within the range-like item and implements `Ord` and `Copy` traits.
///
/// # Returns
///
/// A vector of `OverlappingSegments`, where each element contains a group of overlapping segments.
pub fn group_overlapping_segments<R: TimeRangeProvider + Clone>(
    ordered_chunks: &[R],
) -> Vec<OverlappingSegments<R>> {
    let mut result: Vec<OverlappingSegments<R>> = Vec::new();

    let mut global_min_ts = i64::MIN;

    for chunk in ordered_chunks {
        let mut found = false;

        // Check if the current segment overlaps any existing grouping
        if let Some(segment_group) = result.last_mut() {
            if chunk.time_range().min_ts <= global_min_ts {
                segment_group.segments.push(chunk.clone());
                found = true;
            }
        }

        if !found {
            // If there is no overlap, create a new grouping
            let new_segment_group = OverlappingSegments {
                segments: vec![chunk.clone()],
            };
            result.push(new_segment_group);
        }

        global_min_ts = cmp::max(chunk.time_range().max_ts, global_min_ts);
    }

    result
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
            let mut vistor = ColumnVisitor {
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

struct ColumnVisitor {
    part_schema: SchemaRef,
    has_col_not_in_file: bool,
}

impl<'a> TreeNodeVisitor<'a> for ColumnVisitor {
    type Node = Arc<dyn PhysicalExpr>;

    fn f_down(&mut self, node: &Self::Node) -> datafusion::common::Result<TreeNodeRecursion> {
        if let Some(column) = node.as_any().downcast_ref::<Column>() {
            if self.part_schema.index_of(column.name()).is_err() {
                self.has_col_not_in_file = true;
                return Ok(TreeNodeRecursion::Stop);
            }
        }
        Ok(TreeNodeRecursion::Continue)
    }
}

impl TreeNodeRewriter for PredicateColumnsReassigner {
    type Node = Arc<dyn PhysicalExpr>;

    fn f_up(&mut self, expr: Self::Node) -> TskvResult<Transformed<Self::Node>, DataFusionError> {
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
                            Err(DataFusionError::ArrowError(e, None))
                        }
                    };
                }
            };
            return Ok(Arc::new(Column::new(column.name(), index)));
        } else if let Some(b_expr) = expr.as_any().downcast_ref::<BinaryExpr>() {
            let mut visitor = ColumnVisitor {
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

    use super::{group_overlapping_segments, TimeRangeProvider};

    #[derive(Clone)]
    struct TestTimeRangeProvider {
        tr: TimeRange,
    }

    impl TestTimeRangeProvider {
        fn new(tr: TimeRange) -> Self {
            Self { tr }
        }
    }

    impl TimeRangeProvider for TestTimeRangeProvider {
        fn time_range(&self) -> TimeRange {
            self.tr
        }
    }

    fn test_time_range_provider(min_ts: i64, max_ts: i64) -> TestTimeRangeProvider {
        TestTimeRangeProvider::new(TimeRange { min_ts, max_ts })
    }

    #[test]
    fn test_group_overlapping_segments() {
        let trs = vec![
            test_time_range_provider(0, 10),
            test_time_range_provider(1, 3),
            test_time_range_provider(4, 7),
            test_time_range_provider(6, 10),
            test_time_range_provider(11, 14),
            test_time_range_provider(12, 15),
            test_time_range_provider(16, 18),
        ];

        let grouped_trs = group_overlapping_segments(&trs);

        assert_eq!(grouped_trs.len(), 3);

        let g4 = &grouped_trs[0];
        let g2 = &grouped_trs[1];
        let g1 = &grouped_trs[2];

        assert_eq!(g4.segments.len(), 4);
        assert_eq!(g2.segments.len(), 2);
        assert_eq!(g1.segments.len(), 1);
    }
}
