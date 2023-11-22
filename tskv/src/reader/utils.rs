use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use arrow::datatypes::SchemaRef;
use arrow_array::RecordBatch;
use futures::{Stream, StreamExt};
use models::predicate::domain::TimeRange;

use super::{SchemableTskvRecordBatchStream, SendableSchemableTskvRecordBatchStream};
use crate::tsm2::page::Chunk;
use crate::tsm2::reader::TSM2Reader;
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

impl TimeRangeProvider for (Arc<Chunk>, Arc<TSM2Reader>) {
    fn time_range(&self) -> &TimeRange {
        self.0.time_range()
    }
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
