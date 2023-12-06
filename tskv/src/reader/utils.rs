use std::cmp;
use std::fmt::{Debug, Formatter};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use arrow::datatypes::SchemaRef;
use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::{ArrowError, Field, Schema};
use datafusion::common::tree_node::{TreeNode, TreeNodeRewriter};
use datafusion::error::DataFusionError;
use datafusion::physical_plan::expressions::{BinaryExpr, Column, Literal};
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use datafusion::physical_plan::PhysicalExpr;
use datafusion::scalar::ScalarValue;
use futures::{Stream, StreamExt};
use models::predicate::domain::TimeRange;
use models::schema::TIME_FIELD;

use super::metrics::BaselineMetrics;
use super::{Predicate, SchemableTskvRecordBatchStream, SendableSchemableTskvRecordBatchStream};
use crate::{Error, Result};

pub struct OverlappingSegments<T: TimeRangeProvider> {
    segments: Vec<T>,
}

impl<T: TimeRangeProvider> OverlappingSegments<T> {
    pub fn new(segments: Vec<T>) -> Self {
        Self { segments }
    }

    pub fn segments_ref(&self) -> &[T] {
        &self.segments
    }

    pub fn segments(self) -> Vec<T> {
        self.segments
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
    fn time_range(&self) -> &TimeRange;
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
    /// Create an CombinedRecordBatchStream
    pub fn try_new(
        schema: SchemaRef,
        entries: Vec<SendableSchemableTskvRecordBatchStream>,
        metrics: &ExecutionPlanMetricsSet,
    ) -> Result<Self> {
        for s in &entries {
            if s.schema().fields() != schema.fields() {
                return Err(Error::MismatchedSchema {
                    msg: format!(
                        "in combined stream. Expected: {:?}, got {:?}",
                        schema,
                        s.schema()
                    ),
                });
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
    type Item = Result<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        use Poll::*;

        let poll;
        loop {
            match self.entries.first_mut() {
                Some(s) => {
                    match s.poll_next_unpin(cx) {
                        Ready(Some(val)) => {
                            poll = Ready(Some(val));
                            break;
                        }
                        Ready(None) => {
                            // Remove the entry
                            self.entries.remove(0);
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
) -> Result<Option<Arc<dyn PhysicalExpr>>, DataFusionError> {
    let full_schema = pred.schema();
    let mut rewriter = PredicateColumnsReassigner {
        file_schema,
        full_schema,
        has_col_not_in_file: false,
    };

    match pred.expr() {
        None => Ok(None),
        Some(expr) => {
            let new_expr = expr.rewrite(&mut rewriter)?;
            if rewriter.has_col_not_in_file {
                return Ok(Some(Arc::new(Literal::new(ScalarValue::Boolean(Some(
                    true,
                ))))));
            }
            Ok(Some(new_expr))
        }
    }
}

struct PredicateColumnsReassigner {
    file_schema: SchemaRef,
    full_schema: SchemaRef,
    has_col_not_in_file: bool,
}

impl TreeNodeRewriter for PredicateColumnsReassigner {
    type N = Arc<dyn PhysicalExpr>;

    fn mutate(
        &mut self,
        expr: Arc<dyn PhysicalExpr>,
    ) -> Result<Arc<dyn PhysicalExpr>, DataFusionError> {
        if let Some(column) = expr.as_any().downcast_ref::<Column>() {
            // find the column index in file schema
            let index = match self.file_schema.index_of(column.name()) {
                Ok(idx) => idx,
                Err(_) => {
                    // the column expr must be in the full schema
                    return match self.full_schema.field_with_name(column.name()) {
                        Ok(_) => {
                            // 标记 predicate 中含有文件中不存在的列
                            self.has_col_not_in_file = true;
                            Ok(expr)
                        }
                        Err(e) => {
                            // If the column is not in the full schema, should throw the error
                            Err(DataFusionError::ArrowError(e))
                        }
                    };
                }
            };

            return Ok(Arc::new(Column::new(column.name(), index)));
        } else if expr.as_any().downcast_ref::<BinaryExpr>().is_some() && self.has_col_not_in_file {
            let true_value = ScalarValue::Boolean(Some(true));
            // 重置标记
            self.has_col_not_in_file = false;
            return Ok(Arc::new(Literal::new(true_value)));
        }

        Ok(expr)
    }
}

pub fn time_field_from_schema(schema: &Schema) -> std::result::Result<(usize, &Field), ArrowError> {
    schema.column_with_name(TIME_FIELD).ok_or_else(|| {
        ArrowError::SchemaError(format!("Unable to get field named \"{TIME_FIELD}\"."))
    })
}

pub fn time_column_from_record_batch(record_batch: &RecordBatch) -> Result<&ArrayRef, ArrowError> {
    record_batch.column_by_name(TIME_FIELD).ok_or_else(|| {
        ArrowError::SchemaError(format!("Unable to get field named \"{TIME_FIELD}\"."))
    })
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
        fn time_range(&self) -> &TimeRange {
            &self.tr
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
