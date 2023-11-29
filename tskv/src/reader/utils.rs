use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use arrow::datatypes::SchemaRef;
use arrow_array::RecordBatch;
use datafusion::common::tree_node::{TreeNode, TreeNodeRewriter};
use datafusion::error::DataFusionError;
use datafusion::physical_plan::expressions::{BinaryExpr, Column, Literal};
use datafusion::physical_plan::PhysicalExpr;
use datafusion::scalar::ScalarValue;
use futures::{Stream, StreamExt};
use models::predicate::domain::TimeRange;

use super::{Predicate, SchemableTskvRecordBatchStream, SendableSchemableTskvRecordBatchStream};
use crate::{Error, Result};

pub struct OverlappingSegments<T> {
    segments: Vec<T>,
}

impl<T> OverlappingSegments<T> {
    pub fn new(segments: Vec<T>) -> Self {
        Self { segments }
    }

    pub fn segments(&self) -> &[T] {
        &self.segments
    }
}

impl<T> IntoIterator for OverlappingSegments<T> {
    type Item = T;
    type IntoIter = std::vec::IntoIter<T>;

    fn into_iter(self) -> Self::IntoIter {
        self.segments.into_iter()
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

    for chunk in ordered_chunks {
        let mut found = false;

        // Check if the current segment overlaps any existing grouping
        for segment_group in result.iter_mut() {
            if let Some(last_segment) = segment_group.segments().last() {
                let last = last_segment.time_range().max_ts;
                let c = chunk.time_range().min_ts;

                // Check if there is overlap
                if c <= last {
                    segment_group.segments.push(chunk.clone());
                    found = true;
                    break;
                }
            }
        }

        if !found {
            // If there is no overlap, create a new grouping
            let new_segment_group = OverlappingSegments {
                segments: vec![chunk.clone()],
            };
            result.push(new_segment_group);
        }
    }

    result
}

/// CombinedRecordBatchStream can be used to combine a Vec of SendableRecordBatchStreams into one
pub struct CombinedRecordBatchStream {
    /// Schema wrapped by Arc
    schema: SchemaRef,
    /// Stream entries
    entries: Vec<SendableSchemableTskvRecordBatchStream>,
}

impl CombinedRecordBatchStream {
    /// Create an CombinedRecordBatchStream
    pub fn try_new(
        schema: SchemaRef,
        entries: Vec<SendableSchemableTskvRecordBatchStream>,
    ) -> Result<Self> {
        for s in &entries {
            if s.schema() != schema {
                return Err(Error::MismatchedSchema {
                    msg: format!(
                        "in combined stream. Expected: {:?}, got {:?}",
                        schema,
                        s.schema()
                    ),
                });
            }
        }

        Ok(Self { schema, entries })
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

        loop {
            match self.entries.first_mut() {
                Some(s) => {
                    match s.poll_next_unpin(cx) {
                        Ready(Some(val)) => return Ready(Some(val)),
                        Ready(None) => {
                            // Remove the entry
                            self.entries.remove(0);
                            continue;
                        }
                        Pending => return Pending,
                    }
                }
                None => return Ready(None),
            }
        }
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
) -> Result<Arc<dyn PhysicalExpr>, DataFusionError> {
    let full_schema = pred.schema();
    let expr = pred.expr();

    let mut rewriter = PredicateColumnsReassigner {
        file_schema,
        full_schema,
        has_col_not_in_file: false,
    };

    expr.rewrite(&mut rewriter)
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
