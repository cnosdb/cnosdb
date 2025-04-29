use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use datafusion::arrow::record_batch::RecordBatch;
use futures::{Stream, StreamExt};
use tokio::runtime::Runtime;
use tokio::sync::mpsc;
use tokio::task::JoinSet;

pub type BoxStream<T> = Pin<Box<dyn Stream<Item = T> + Send>>;

pub struct ParallelMergeStream<E> {
    /// Stream entries
    receiver: mpsc::Receiver<Result<RecordBatch, E>>,
    #[allow(unused)]
    drop_helper: JoinSet<()>,
}

impl<E> ParallelMergeStream<E>
where
    E: Send + 'static,
{
    pub fn new(
        runtime: Option<Arc<Runtime>>,
        streams: Vec<BoxStream<Result<RecordBatch, E>>>,
    ) -> Self {
        let mut join_set = JoinSet::new();
        let (sender, receiver) = mpsc::channel::<Result<RecordBatch, E>>(streams.len());

        for mut stream in streams {
            let sender = sender.clone();
            let task = async move {
                while let Some(item) = stream.next().await {
                    let exit = item.is_err();
                    // If send fails, stream being torn down,
                    // there is no place to send the error.
                    if sender.send(item).await.is_err() || exit {
                        return;
                    }
                }
            };

            if let Some(rt) = &runtime {
                join_set.spawn_on(task, rt.handle());
            } else {
                join_set.spawn(task);
            }
        }

        Self {
            receiver,
            drop_helper: join_set,
        }
    }
}

impl<E> Stream for ParallelMergeStream<E> {
    type Item = Result<RecordBatch, E>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.receiver.poll_recv(cx)
    }
}

/// Iterator over batches
pub struct MemoryRecordBatchStream<E> {
    /// Vector of record batches
    data: Vec<RecordBatch>,
    /// Index into the data
    index: usize,
    _err: PhantomData<E>,
}

impl<E> MemoryRecordBatchStream<E> {
    /// Create an iterator for a vector of record batches
    pub fn new(data: Vec<RecordBatch>) -> Self {
        Self {
            data,
            index: 0,
            _err: PhantomData::<E>,
        }
    }
}

impl<E> Stream for MemoryRecordBatchStream<E> {
    type Item = Result<RecordBatch, E>;

    fn poll_next(mut self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Poll::Ready(if self.index < self.data.len() {
            self.index += 1;
            let batch = &self.data[self.index - 1];
            Some(Ok(batch.to_owned()))
        } else {
            None
        })
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.data.len(), Some(self.data.len()))
    }
}

impl<E> Unpin for MemoryRecordBatchStream<E> {}

#[cfg(test)]
mod tests {
    use datafusion::arrow::datatypes::Schema;
    use futures::TryStreamExt;

    use super::*;

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_parallel_merge_stream() {
        let schema = Arc::new(Schema::empty());

        let batches = vec![
            RecordBatch::new_empty(schema.clone()),
            RecordBatch::new_empty(schema.clone()),
            RecordBatch::new_empty(schema),
        ];

        let streams: Vec<BoxStream<Result<RecordBatch, String>>> = vec![
            Box::pin(MemoryRecordBatchStream::new(batches.clone())),
            Box::pin(MemoryRecordBatchStream::new(batches.clone())),
            Box::pin(MemoryRecordBatchStream::new(batches)),
        ];

        let parallel_merge_stream = ParallelMergeStream::new(None, streams);

        let result_batches = parallel_merge_stream.try_collect::<Vec<_>>().await.unwrap();

        assert_eq!(result_batches.len(), 9);
    }
}
