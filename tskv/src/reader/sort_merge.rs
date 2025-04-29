use std::cmp::Ordering;
use std::pin::Pin;
use std::task::{Context, Poll};

use arrow::datatypes::SchemaRef;
use arrow_array::RecordBatch;
use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, MetricBuilder, Time};
use datafusion::physical_plan::sorts::cursor::{CursorArray, CursorValues};
use futures::{ready, Stream};
use snafu::ResultExt;

use crate::error::ArrowSnafu;
use crate::reader::batch_builder::BatchMergeBuilder;
use crate::reader::metrics::BaselineMetrics;
use crate::reader::partitioned_stream::{ColumnCursorStream, PartitionedStream};
use crate::reader::{SchemableTskvRecordBatchStream, SendableSchemableTskvRecordBatchStream};
use crate::TskvResult;

macro_rules! primitive_merge_helper {
    ($t:ty, $($v:ident),+) => {
        merge_helper!(arrow_array::PrimitiveArray<$t>, $($v), +)
    };
}

macro_rules! merge_helper {
    ($t:ty, $streams:ident, $schema:ident, $batch_size:ident, $sort_column:ident, $metrics:ident) => {{
        let streams = ColumnCursorStream::<$t>::new($streams, $sort_column)?;
        return Ok(Box::pin(SortPreservingMergeStream::<$t>::new(
            streams,
            $schema,
            $batch_size,
            $metrics,
        )));
    }};
}

pub fn sort_merge(
    streams: Vec<SendableSchemableTskvRecordBatchStream>,
    schema: SchemaRef,
    batch_size: usize,
    column_name: &str,
    metrics: &ExecutionPlanMetricsSet,
) -> TskvResult<SendableSchemableTskvRecordBatchStream> {
    use arrow_array::*;

    // Special case single column comparisons with optimized cursor implementations
    let data_type = schema
        .field_with_name(column_name)
        .context(ArrowSnafu)?
        .data_type();
    downcast_primitive! {
        data_type => (primitive_merge_helper, streams, schema, batch_size, column_name, metrics),
        _ => {}
    }

    Err(crate::error::TskvError::Unimplemented {
        msg: format!("Not implement batch merge for {}", data_type),
    })
}

/// A cursor over sorted, nullable [`CursorValues`]
///
/// Note: comparing cursors with different `SortOptions` will yield an arbitrary ordering
#[derive(Debug)]
pub struct ColumnCursor<T: CursorValues> {
    values: T,
    // 标记来自record batch的哪一列
    column_index: usize,
    offset: usize,
}

impl<T: CursorValues> ColumnCursor<T> {
    /// Create a new [`ColumnCursor`] from the provided `values` sorted according to `options`
    pub fn new<A: CursorArray<Values = T>>(array: &A, column_idx: usize) -> Self {
        Self {
            values: array.values(),
            column_index: column_idx,
            offset: 0,
        }
    }

    fn is_finished(&self) -> bool {
        self.offset == self.values.len()
    }

    fn advance(&mut self) -> usize {
        let t = self.offset;
        self.offset += 1;
        t
    }
}

impl<T: CursorValues> PartialEq for ColumnCursor<T> {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other).is_eq()
    }
}

impl<T: CursorValues> Eq for ColumnCursor<T> {}

impl<T: CursorValues> PartialOrd for ColumnCursor<T> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<T: CursorValues> Ord for ColumnCursor<T> {
    fn cmp(&self, other: &Self) -> Ordering {
        T::compare(&self.values, self.offset, &other.values, other.offset)
    }
}

/// Stores metrics about the table writer execution.
#[derive(Debug, Clone)]
pub struct SortMergeMetrics {
    elapsed_data_merge_time: Time,
    inner: BaselineMetrics,
}

impl SortMergeMetrics {
    /// Create new metrics
    pub fn new(metrics: &ExecutionPlanMetricsSet) -> Self {
        let elapsed_data_merge_time =
            MetricBuilder::new(metrics).subset_time("elapsed_data_merge_time", 0);

        let inner = BaselineMetrics::new(metrics);

        Self {
            elapsed_data_merge_time,
            inner,
        }
    }

    pub fn elapsed_data_merge_time(&self) -> &Time {
        &self.elapsed_data_merge_time
    }
    pub fn elapsed_compute(&self) -> &Time {
        self.inner.elapsed_compute()
    }

    pub fn record_poll(
        &self,
        poll: Poll<Option<TskvResult<RecordBatch>>>,
    ) -> Poll<Option<TskvResult<RecordBatch>>> {
        self.inner.record_poll(poll)
    }
}

#[derive(Debug)]
pub struct SortPreservingMergeStream<T: CursorArray> {
    in_progress: BatchMergeBuilder<T>,

    streams: ColumnCursorStream<T>,

    /// The sorted input streams to merge together

    /// A loser tree that always produces the minimum cursor
    ///
    /// Node 0 stores the top winner, Nodes 1..num_streams store
    /// the loser nodes
    ///
    /// This implements a "Tournament Tree" (aka Loser Tree) to keep
    /// track of the current smallest element at the top. When the top
    /// record is taken, the tree structure is not modified, and only
    /// the path from bottom to top is visited, keeping the number of
    /// comparisons close to the theoretical limit of `log(S)`.
    ///
    /// The current implementation uses a vector to store the tree.
    /// Conceptually, it looks like this (assuming 8 streams):
    ///
    /// ```text
    ///     0 (winner)
    ///
    ///     1
    ///    / \
    ///   2   3
    ///  / \ / \
    /// 4  5 6  7
    /// ```
    ///
    /// Where element at index 0 in the vector is the current winner. Element
    /// at index 1 is the root of the loser tree, element at index 2 is the
    /// left child of the root, and element at index 3 is the right child of
    /// the root and so on.
    ///
    /// reference: <https://en.wikipedia.org/wiki/K-way_merge_algorithm#Tournament_Tree>
    loser_tree: Vec<usize>,

    /// If the most recently yielded overall winner has been replaced
    /// within the loser tree. A value of `false` indicates that the
    /// overall winner has been yielded but the loser tree has not
    /// been updated
    loser_tree_adjusted: bool,

    /// target batch size
    batch_size: usize,

    /// Vector that holds cursors for each non-exhausted input partition
    cursors: Vec<Option<ColumnCursor<T::Values>>>,

    metrics: SortMergeMetrics,
}

impl<T: CursorArray + Unpin> SortPreservingMergeStream<T> {
    pub fn new(
        streams: ColumnCursorStream<T>,
        schema: SchemaRef,
        batch_size: usize,
        metrics: &ExecutionPlanMetricsSet,
    ) -> SortPreservingMergeStream<T> {
        let stream_count = streams.partitions();
        Self {
            in_progress: BatchMergeBuilder::new(schema, stream_count, batch_size),
            batch_size,
            streams,
            cursors: (0..stream_count).map(|_| None).collect(),
            loser_tree: vec![],
            loser_tree_adjusted: false,
            metrics: SortMergeMetrics::new(metrics),
        }
    }

    fn maybe_poll_stream(&mut self, cx: &mut Context<'_>, idx: usize) -> Poll<TskvResult<()>> {
        if self.cursors[idx].is_some() {
            // Cursor is not finished - don't need a new RecordBatch yet
            return Poll::Ready(Ok(()));
        }

        match futures::ready!(self.streams.poll_next(cx, idx)) {
            None => Poll::Ready(Ok(())),
            Some(Err(e)) => Poll::Ready(Err(e)),
            Some(Ok((cursor, batch))) => {
                self.cursors[idx] = Some(cursor);
                self.in_progress.push_batch(idx, batch);
                Poll::Ready(Ok(()))
            }
        }
    }

    fn poll_next_inner(&mut self, cx: &mut Context<'_>) -> Poll<Option<TskvResult<RecordBatch>>> {
        // try to initialize the loser tree
        if self.loser_tree.is_empty() {
            // Ensure all non-exhausted streams have a cursor from which
            // rows can be pulled
            for i in 0..self.streams.partitions() {
                if let Err(e) = ready!(self.maybe_poll_stream(cx, i)) {
                    return Poll::Ready(Some(Err(e)));
                }
            }
            self.init_loser_tree();
        }

        // NB timer records time taken on drop, so there are no
        // calls to `timer.done()` below.

        loop {
            // Adjust the loser tree if necessary, returning control if needed
            if !self.loser_tree_adjusted {
                let winner = self.loser_tree[0];
                if let Err(e) = ready!(self.maybe_poll_stream(cx, winner)) {
                    return Poll::Ready(Some(Err(e)));
                }
                self.update_loser_tree();
            }

            // winner
            let stream_idx = self.loser_tree[0];

            let value = self
                .cursors
                .get(stream_idx)
                .and_then(|a| a.as_ref().map(|f| f.column_index));

            if self.advance(stream_idx) {
                self.loser_tree_adjusted = false;
                if let Err(e) = self.in_progress.push_row(stream_idx, value.unwrap()) {
                    return Poll::Ready(Some(Err(e)));
                }
                if self.in_progress.len() < self.batch_size {
                    continue;
                }
            } else {
                self.in_progress.take_last_and_merge()?;
            }

            return Poll::Ready(self.in_progress.build_record_batch().transpose());
        }
    }

    fn advance(&mut self, stream_idx: usize) -> bool {
        let slot = &mut self.cursors[stream_idx];
        match slot.as_mut() {
            Some(c) => {
                c.advance();
                if c.is_finished() {
                    *slot = None;
                }
                true
            }
            None => false,
        }
    }

    /// Returns `true` if the cursor at index `a` is greater than at index `b`
    #[inline]
    fn is_gt(&self, a: usize, b: usize) -> bool {
        match (&self.cursors[a], &self.cursors[b]) {
            (None, _) => true,
            (_, None) => false,
            (Some(ac), Some(bc)) => ac.cmp(bc).then_with(|| a.cmp(&b)).is_gt(),
        }
    }

    /// Find the leaf node index in the loser tree for the given cursor index
    ///
    /// Note that this is not necessarily a leaf node in the tree, but it can
    /// also be a half-node (a node with only one child). This happens when the
    /// number of cursors/streams is not a power of two. Thus, the loser tree
    /// will be unbalanced, but it will still work correctly.
    ///
    /// For example, with 5 streams, the loser tree will look like this:
    ///
    /// ```text
    ///           0 (winner)
    ///
    ///           1
    ///        /     \
    ///       2       3
    ///     /  \     / \
    ///    4    |   |   |
    ///   / \   |   |   |
    /// -+---+--+---+---+---- Below is not a part of loser tree
    ///  S3 S4 S0   S1  S2
    /// ```
    ///
    /// S0, S1, ... S4 are the streams (read: stream at index 0, stream at
    /// index 1, etc.)
    ///
    /// Zooming in at node 2 in the loser tree as an example, we can see that
    /// it takes as input the next item at (S0) and the loser of (S3, S4).
    ///
    #[inline]
    fn lt_leaf_node_index(&self, cursor_index: usize) -> usize {
        (self.cursors.len() + cursor_index) / 2
    }

    /// Find the parent node index for the given node index
    #[inline]
    fn lt_parent_node_index(&self, node_idx: usize) -> usize {
        node_idx / 2
    }

    /// Attempts to initialize the loser tree with one value from each
    /// non exhausted input, if possible
    fn init_loser_tree(&mut self) {
        // Init loser tree
        self.loser_tree = vec![usize::MAX; self.cursors.len()];
        for i in 0..self.cursors.len() {
            let mut winner = i;
            let mut cmp_node = self.lt_leaf_node_index(i);
            while cmp_node != 0 && self.loser_tree[cmp_node] != usize::MAX {
                let challenger = self.loser_tree[cmp_node];
                if self.is_gt(winner, challenger) {
                    self.loser_tree[cmp_node] = winner;
                    winner = challenger;
                }

                cmp_node = self.lt_parent_node_index(cmp_node);
            }
            self.loser_tree[cmp_node] = winner;
        }
        self.loser_tree_adjusted = true;
    }

    /// Attempts to update the loser tree, following winner replacement, if possible
    fn update_loser_tree(&mut self) {
        let mut winner = self.loser_tree[0];
        // Replace overall winner by walking tree of losers
        let mut cmp_node = self.lt_leaf_node_index(winner);
        while cmp_node != 0 {
            let challenger = self.loser_tree[cmp_node];
            if self.is_gt(winner, challenger) {
                self.loser_tree[cmp_node] = winner;
                winner = challenger;
            }
            cmp_node = self.lt_parent_node_index(cmp_node);
        }
        self.loser_tree[0] = winner;
        self.loser_tree_adjusted = true;
    }
}
impl<T: Unpin + CursorArray> Unpin for SortPreservingMergeStream<T> {}

impl<T: CursorArray + Unpin> Stream for SortPreservingMergeStream<T> {
    type Item = TskvResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let res = self.poll_next_inner(cx);
        self.metrics.record_poll(res)
    }
}

impl<T: CursorArray + Unpin> SchemableTskvRecordBatchStream for SortPreservingMergeStream<T> {
    fn schema(&self) -> SchemaRef {
        self.in_progress.schema().clone()
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use arrow_array::{Int64Array, RecordBatch};
    use arrow_schema::{DataType, Field, Fields, Schema, SchemaRef};
    use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
    use datafusion::physical_plan::sorts::cursor::CursorArray;
    use futures::StreamExt;

    use crate::reader::partitioned_stream::ColumnCursorStream;
    use crate::reader::sort_merge::SortPreservingMergeStream;
    use crate::reader::{SchemableMemoryBatchReaderStream, SendableSchemableTskvRecordBatchStream};

    pub fn new_with_batches<T: CursorArray + Unpin>(
        schema: SchemaRef,
        batches: Vec<RecordBatch>,
        batch_size: usize,
        sort_column: &str,
    ) -> crate::TskvResult<SortPreservingMergeStream<T>> {
        let streams = batches
            .into_iter()
            .map(|b| {
                Box::pin(SchemableMemoryBatchReaderStream::new(
                    schema.clone(),
                    vec![b],
                )) as SendableSchemableTskvRecordBatchStream
            })
            .collect::<Vec<_>>();
        let cursor_stream = ColumnCursorStream::<T>::new(streams, sort_column)?;
        Ok(SortPreservingMergeStream::new(
            cursor_stream,
            schema.clone(),
            batch_size,
            &ExecutionPlanMetricsSet::new(),
        ))
    }
    #[tokio::test]
    async fn test_merge_tree() {
        let fields = Fields::from(vec![
            Field::new("column1", DataType::Int64, true),
            Field::new("column2", DataType::Int64, true),
        ]);
        let schema = SchemaRef::new(Schema::new(fields));

        let array11 = Arc::new(Int64Array::from_iter_values([1, 1, 1]));
        let array12 = Arc::new(Int64Array::from_iter_values([1, 2, 3]));
        let record_batch1 = RecordBatch::try_new(schema.clone(), vec![array11, array12]).unwrap();
        let array21 = Arc::new(Int64Array::from_iter_values([1, 1, 2]));
        let array22 = Arc::new(Int64Array::from_iter_values([4, 5, 6]));
        let record_batch2 = RecordBatch::try_new(schema.clone(), vec![array21, array22]).unwrap();
        let array31 = Arc::new(Int64Array::from_iter_values([1, 2, 2]));
        let array32 = Arc::new(Int64Array::from_iter_values([7, 8, 9]));
        let record_batch3 = RecordBatch::try_new(schema.clone(), vec![array31, array32]).unwrap();

        let batches = vec![record_batch1, record_batch2, record_batch3];

        let mut stream =
            new_with_batches::<Int64Array>(schema.clone(), batches, 4096, "column1").unwrap();
        let res = stream.next().await.unwrap().unwrap();

        let array1 = Arc::new(Int64Array::from_iter_values([1, 2]));
        let array2 = Arc::new(Int64Array::from_iter_values([7, 9]));

        let expected_batch = RecordBatch::try_new(schema.clone(), vec![array1, array2]).unwrap();

        assert_eq!(res, expected_batch);
        assert!(stream.next().await.is_none());
    }

    #[tokio::test]
    async fn test_merge_column() {
        let fields = Fields::from(vec![
            Field::new("column1", DataType::Int64, true),
            Field::new("column2", DataType::Int64, true),
        ]);
        let schema = SchemaRef::new(Schema::new(fields));

        let array11 = Arc::new(Int64Array::from_iter_values([1, 1, 1]));
        let array12 = Arc::new(Int64Array::from(vec![Some(1), None, Some(3)]));
        let record_batch1 = RecordBatch::try_new(schema.clone(), vec![array11, array12]).unwrap();
        let array21 = Arc::new(Int64Array::from_iter_values([1, 1, 2]));
        let array22 = Arc::new(Int64Array::from(vec![None, Some(5), None]));
        let record_batch2 = RecordBatch::try_new(schema.clone(), vec![array21, array22]).unwrap();
        let array31 = Arc::new(Int64Array::from_iter_values([1, 2, 2]));
        let array32 = Arc::new(Int64Array::from(vec![None, Some(8), None]));
        let record_batch3 = RecordBatch::try_new(schema.clone(), vec![array31, array32]).unwrap();

        let batches = vec![record_batch1, record_batch2, record_batch3];

        let mut stream =
            new_with_batches::<Int64Array>(schema.clone(), batches, 4096, "column1").unwrap();
        let res = stream.next().await.unwrap().unwrap();

        let array1 = Arc::new(Int64Array::from_iter_values([1, 2]));
        let array2 = Arc::new(Int64Array::from_iter_values([5, 8]));

        let expected_batch = RecordBatch::try_new(schema.clone(), vec![array1, array2]).unwrap();

        assert_eq!(res, expected_batch);
        assert!(stream.next().await.is_none());
    }

    #[tokio::test]
    async fn test_merge_time() {
        let fields = Fields::from(vec![Field::new("column1", DataType::Int64, true)]);
        let schema = SchemaRef::new(Schema::new(fields));

        let array11 = Arc::new(Int64Array::from_iter_values([1, 1, 1]));
        let record_batch1 = RecordBatch::try_new(schema.clone(), vec![array11]).unwrap();
        let array21 = Arc::new(Int64Array::from_iter_values([1, 1, 2]));
        let record_batch2 = RecordBatch::try_new(schema.clone(), vec![array21]).unwrap();
        let array31 = Arc::new(Int64Array::from_iter_values([1, 2, 2]));
        let record_batch3 = RecordBatch::try_new(schema.clone(), vec![array31]).unwrap();

        let batches = vec![record_batch1, record_batch2, record_batch3];

        let mut stream =
            new_with_batches::<Int64Array>(schema.clone(), batches, 4096, "column1").unwrap();
        let res = stream.next().await.unwrap().unwrap();

        let array1 = Arc::new(Int64Array::from_iter_values([1, 2]));

        let expected_batch = RecordBatch::try_new(schema.clone(), vec![array1]).unwrap();

        assert_eq!(res, expected_batch);
        assert!(stream.next().await.is_none());
    }
}
