pub mod query_executor;
pub mod replica_selection;
pub mod status_listener;
pub mod table_scan;
pub mod tag_scan;

use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use datafusion::arrow::record_batch::RecordBatch;
use datafusion::physical_plan::common::AbortOnDropMany;
use futures::future::BoxFuture;
use futures::{ready, FutureExt, Stream, StreamExt, TryFutureExt};
use models::meta_data::VnodeInfo;
pub use query_executor::*;
use tokio::runtime::Runtime;
use tokio::sync::mpsc;
use tracing::{info, warn};
use tskv::query_iterator::QueryOption;

use crate::errors::{CoordinatorError, CoordinatorResult};
use crate::{CoordinatorRecordBatchStream, SendableCoordinatorRecordBatchStream};

/// A fallible future that reads to a stream of [`RecordBatch`]
pub type VnodeOpenFuture =
    BoxFuture<'static, CoordinatorResult<SendableCoordinatorRecordBatchStream>>;
/// A fallible future that checks the vnode query operation is available
pub type CheckFuture = BoxFuture<'static, CoordinatorResult<()>>;

/// Generic API for connect a vnode and reading to a stream of [`RecordBatch`]
pub trait VnodeOpener: Unpin {
    /// Asynchronously open the specified vnode and return a stream of [`RecordBatch`]
    fn open(&self, vnode: &VnodeInfo, option: &QueryOption) -> CoordinatorResult<VnodeOpenFuture>;
}

pub struct CheckedCoordinatorRecordBatchStream<O: VnodeOpener> {
    opener: O,
    vnode: VnodeInfo,
    option: QueryOption,
    state: StreamState,
}

impl<O: VnodeOpener> CheckedCoordinatorRecordBatchStream<O> {
    pub fn new(option: QueryOption, opener: O, checker: CheckFuture) -> Self {
        Self {
            option,
            opener,
            vnode: VnodeInfo::default(),
            state: StreamState::Check(checker),
        }
    }

    fn poll_inner(&mut self, cx: &mut Context<'_>) -> Poll<Option<CoordinatorResult<RecordBatch>>> {
        loop {
            match &mut self.state {
                StreamState::Check(checker) => {
                    // TODO record time used
                    match ready!(checker.try_poll_unpin(cx)) {
                        Ok(_) => {
                            self.vnode = self.option.split.pop_front().ok_or(
                                CoordinatorError::NoValidReplica {
                                    id: self.option.split.replica_id(),
                                },
                            )?;

                            self.state = StreamState::Idle;
                        }

                        Err(err) => return Poll::Ready(Some(Err(err))),
                    };
                }
                StreamState::Idle => {
                    // TODO record time used
                    let future = match self.opener.open(&self.vnode, &self.option) {
                        Ok(future) => future,
                        Err(err) => return Poll::Ready(Some(Err(err))),
                    };
                    self.state = StreamState::Open(future);
                }
                StreamState::Open(future) => {
                    // TODO record time used
                    match ready!(future.poll_unpin(cx)) {
                        Ok(stream) => {
                            self.state = StreamState::Scan(stream);
                        }
                        Err(err) => {
                            if let CoordinatorError::FailoverNode { id: _ } = err {
                                if let Some(vnode) = self.option.split.pop_front() {
                                    info!("failover reader try to read another vnode: {:?}", vnode);
                                    self.vnode = vnode;
                                    self.state = StreamState::Idle;
                                } else {
                                    return Poll::Ready(Some(Err(err)));
                                }
                            } else {
                                return Poll::Ready(Some(Err(err)));
                            }
                        }
                    };
                }
                StreamState::Scan(stream) => return stream.poll_next_unpin(cx),
            }
        }
    }
}

impl<O: VnodeOpener> Stream for CheckedCoordinatorRecordBatchStream<O> {
    type Item = Result<RecordBatch, CoordinatorError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.poll_inner(cx)
    }
}

impl<O: VnodeOpener> CoordinatorRecordBatchStream for CheckedCoordinatorRecordBatchStream<O> {}

enum StreamState {
    Check(CheckFuture),
    Idle,
    Open(VnodeOpenFuture),
    Scan(SendableCoordinatorRecordBatchStream),
}

pub struct ParallelMergeStream {
    /// Stream entries
    receiver: mpsc::Receiver<CoordinatorResult<RecordBatch>>,
    #[allow(unused)]
    drop_helper: AbortOnDropMany<()>,
}

impl ParallelMergeStream {
    pub fn new(
        runtime: Option<Arc<Runtime>>,
        streams: Vec<SendableCoordinatorRecordBatchStream>,
    ) -> Self {
        let mut join_handles = Vec::with_capacity(streams.len());
        let (sender, receiver) = mpsc::channel::<CoordinatorResult<RecordBatch>>(streams.len());

        for mut stream in streams {
            let sender = sender.clone();
            let task = async move {
                while let Some(item) = stream.next().await {
                    // If send fails, stream being torn down,
                    // there is no place to send the error.
                    if sender.send(item).await.is_err() {
                        warn!("Stopping execution: output is gone, ParallelMergeStream cancelling");
                        return;
                    }
                }
            };

            let join_handle = if let Some(rt) = &runtime {
                rt.spawn(task)
            } else {
                tokio::spawn(task)
            };

            join_handles.push(join_handle);
        }

        Self {
            receiver,
            drop_helper: AbortOnDropMany(join_handles),
        }
    }
}

impl Stream for ParallelMergeStream {
    type Item = CoordinatorResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.receiver.poll_recv(cx)
    }
}

impl CoordinatorRecordBatchStream for ParallelMergeStream {}

/// Iterator over batches
pub struct MemoryRecordBatchStream {
    /// Vector of record batches
    data: Vec<RecordBatch>,
    /// Index into the data
    index: usize,
}

impl MemoryRecordBatchStream {
    /// Create an iterator for a vector of record batches
    pub fn new(data: Vec<RecordBatch>) -> Self {
        Self { data, index: 0 }
    }
}

impl Stream for MemoryRecordBatchStream {
    type Item = CoordinatorResult<RecordBatch>;

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

impl CoordinatorRecordBatchStream for MemoryRecordBatchStream {}

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

        let streams: Vec<SendableCoordinatorRecordBatchStream> = vec![
            Box::pin(MemoryRecordBatchStream::new(batches.clone())),
            Box::pin(MemoryRecordBatchStream::new(batches.clone())),
            Box::pin(MemoryRecordBatchStream::new(batches)),
        ];

        let parallel_merge_stream = ParallelMergeStream::new(None, streams);

        let result_batches = parallel_merge_stream.try_collect::<Vec<_>>().await.unwrap();

        assert_eq!(result_batches.len(), 9);
    }
}
