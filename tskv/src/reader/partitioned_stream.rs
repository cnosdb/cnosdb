use std::marker::PhantomData;
use std::task::{ready, Context, Poll};

use arrow_array::RecordBatch;
use datafusion::physical_plan::sorts::cursor::CursorArray;
use futures::stream::Fuse;
use futures::StreamExt;
use snafu::ResultExt;

use crate::error::ArrowSnafu;
use crate::reader::sort_merge::ColumnCursor;
use crate::reader::SendableSchemableTskvRecordBatchStream;
use crate::TskvResult;

/// A [`Stream`](futures::Stream) that has multiple partitions that can
/// be polled separately but not concurrently
///
/// Used by sort preserving merge to decouple the cursor merging logic from
/// the source of the cursors, the intention being to allow preserving
/// any row encoding performed for intermediate sorts
pub trait PartitionedStream: std::fmt::Debug + Send {
    type Output;

    /// Returns the number of partitions
    fn partitions(&self) -> usize;

    fn poll_next(&mut self, cx: &mut Context<'_>, stream_idx: usize) -> Poll<Option<Self::Output>>;
}

struct FusedStreams(Vec<Fuse<SendableSchemableTskvRecordBatchStream>>);

impl std::fmt::Debug for FusedStreams {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FusedStreams")
            .field("num_streams", &self.0.len())
            .finish()
    }
}

impl FusedStreams {
    fn poll_next(
        &mut self,
        cx: &mut Context<'_>,
        stream_idx: usize,
    ) -> Poll<Option<TskvResult<RecordBatch>>> {
        loop {
            match ready!(self.0[stream_idx].poll_next_unpin(cx)) {
                Some(Ok(b)) if b.num_rows() == 0 => continue,
                r => return Poll::Ready(r),
            }
        }
    }
}

pub struct ColumnCursorStream<T: CursorArray> {
    streams: FusedStreams,
    time_column_idx: Vec<usize>,
    phantom: PhantomData<fn(T) -> T>,
}

impl<T: CursorArray> std::fmt::Debug for ColumnCursorStream<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PrimitiveCursorStream")
            .field("num_streams", &self.streams)
            .finish()
    }
}

impl<T: CursorArray> ColumnCursorStream<T> {
    pub fn new(
        streams: Vec<SendableSchemableTskvRecordBatchStream>,
        column_name: &str,
    ) -> TskvResult<Self> {
        let idxs = streams
            .iter()
            .map(|a| a.schema().index_of(column_name).context(ArrowSnafu))
            .collect::<TskvResult<Vec<usize>>>()?;

        let streams = streams.into_iter().map(|s| s.fuse()).collect();

        Ok(Self {
            streams: FusedStreams(streams),
            time_column_idx: idxs,
            phantom: Default::default(),
        })
    }

    fn convert_batch(
        &mut self,
        batch: &RecordBatch,
        idx: usize,
    ) -> TskvResult<ColumnCursor<T::Values>> {
        let array = batch.column(idx);
        let array = array.as_any().downcast_ref::<T>().expect("field values");
        Ok(ColumnCursor::new(array, idx))
    }
}

impl<T: CursorArray> PartitionedStream for ColumnCursorStream<T> {
    type Output = TskvResult<(ColumnCursor<T::Values>, RecordBatch)>;

    fn partitions(&self) -> usize {
        self.streams.0.len()
    }

    fn poll_next(&mut self, cx: &mut Context<'_>, stream_idx: usize) -> Poll<Option<Self::Output>> {
        Poll::Ready(ready!(self.streams.poll_next(cx, stream_idx)).map(|r| {
            r.and_then(|batch| {
                let cursor = self.convert_batch(&batch, self.time_column_idx[stream_idx])?;
                Ok((cursor, batch))
            })
        }))
    }
}
