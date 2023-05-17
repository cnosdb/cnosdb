use std::collections::HashMap;
use std::ops::Deref;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::Result as DFResult;
use datafusion::physical_plan::{RecordBatchStream, SendableRecordBatchStream};
use futures::{Stream, StreamExt};
use parking_lot::RwLock;
use spi::query::dispatcher::{QueryInfo, QueryStatus};
use spi::query::execution::{Output, QueryExecution, QueryExecutionRef, QueryType};
use spi::service::protocol::QueryId;
use spi::{QueryError, Result};
use trace::{debug, warn};

use super::persister::QueryPersisterRef;

pub struct QueryTracker {
    queries: RwLock<HashMap<QueryId, Arc<dyn QueryExecution>>>,
    query_limit: usize,
    query_persister: QueryPersisterRef,
}

impl QueryTracker {
    pub fn new(query_limit: usize, query_persister: QueryPersisterRef) -> Self {
        Self {
            queries: RwLock::new(HashMap::new()),
            query_limit,
            query_persister,
        }
    }
}

impl QueryTracker {
    /// track a query
    ///
    /// Returns [`TrackedQuery`], which holds a [`QueryExecutionTrackedProxy`] internally.
    ///
    /// Errors:
    ///     [`QueryError::RequestLimit`]
    pub async fn try_track_query(
        self: &Arc<Self>,
        query_id: QueryId,
        query: Arc<dyn QueryExecution>,
    ) -> Result<TrackedQuery> {
        debug!(
            "total query count: {}, current query info {:?} status {:?}",
            self.queries.read().len(),
            query.info(),
            query.status(),
        );

        self.save_query(query_id, query.clone()).await?;

        let query = match query.query_type() {
            // 封装一层代理，用于在释放query result stream时，从tracker中移除
            QueryType::Batch => Arc::new(QueryExecutionTrackedProxy {
                inner: query,
                query_id,
                tracker: self.clone(),
            }),
            QueryType::Stream => {
                // 流任务是常驻任务，需要手动kill，不需要在这里做代理
                query
            }
        };

        Ok(TrackedQuery { query })
    }

    pub fn query(&self, id: &QueryId) -> Option<Arc<dyn QueryExecution>> {
        self.queries.read().get(id).cloned()
    }

    pub fn _running_query_count(&self) -> usize {
        self.queries.read().len()
    }

    /// all running queries
    pub fn running_queries(&self) -> Vec<Arc<dyn QueryExecution>> {
        self.queries.read().values().cloned().collect()
    }

    /// all persistent queries
    pub async fn persistent_queries(&self) -> Result<Vec<QueryInfo>> {
        self.query_persister.queries().await
    }

    /// Once closed, no new requests will be accepted
    ///
    /// After closing, tracking new requests through [`QueryTracker::try_track_query`] will return [`QueryError::Closed`]
    pub fn _close(&self) {
        // pass
    }

    pub fn expire_query(&self, id: &QueryId) -> Option<Arc<dyn QueryExecution>> {
        self.queries.write().remove(id).map(|q| {
            if q.need_persist() {
                let _ = self.query_persister.remove(id).map_err(|err| {
                    warn!("Remove query from persister failed: {:?}", err);
                });
            }
            q
        })
    }

    async fn save_query(&self, query_id: QueryId, query: Arc<dyn QueryExecution>) -> Result<()> {
        if self.queries.read().len() >= self.query_limit {
            warn!("simultaneous request limit exceeded - dropping request");
            return Err(QueryError::RequestLimit);
        }

        {
            // store the query in memory
            let mut wqueries = self.queries.write();
            if wqueries.len() >= self.query_limit {
                warn!("simultaneous request limit exceeded - dropping request");
                return Err(QueryError::RequestLimit);
            }
            let _ = wqueries.insert(query_id, query.clone());
        }

        // persist the query if necessary
        if query.need_persist() {
            self.query_persister.save(query_id, query.info()).await?;
        }

        Ok(())
    }
}

pub struct TrackedQuery {
    query: QueryExecutionRef,
}

impl Deref for TrackedQuery {
    type Target = dyn QueryExecution;

    fn deref(&self) -> &Self::Target {
        self.query.deref()
    }
}

/// A proxy for [`QueryExecution`] that tracks the query's lifecycle
///
/// When the query result is a stream, it will be wrapped with [`TrackedRecordBatchStream`] to track the record batch stream
pub struct QueryExecutionTrackedProxy {
    inner: QueryExecutionRef,
    query_id: QueryId,
    tracker: Arc<QueryTracker>,
}

#[async_trait]
impl QueryExecution for QueryExecutionTrackedProxy {
    fn query_type(&self) -> QueryType {
        self.inner.query_type()
    }

    async fn start(&self) -> Result<Output> {
        match self.inner.start().await {
            Ok(Output::StreamData(stream)) => {
                debug!("Track RecordBatchStream: {:?}", self.query_id);
                Ok(Output::StreamData(Box::pin(TrackedRecordBatchStream {
                    inner: stream,
                    query_id: self.query_id,
                    tracker: self.tracker.clone(),
                })))
            }
            Ok(nil @ Output::Nil(_)) => {
                debug!("Query drop: {:?}", self.query_id);
                let _ = self.tracker.expire_query(&self.query_id);
                Ok(nil)
            }
            Err(err) => {
                // query error, remove from tracker
                debug!(
                    "Query {:?} error: {:?}, remove from tracker",
                    self.query_id, err
                );
                let _ = self.tracker.expire_query(&self.query_id);
                Err(err)
            }
        }
    }

    fn cancel(&self) -> Result<()> {
        self.inner.cancel()
    }

    fn info(&self) -> QueryInfo {
        self.inner.info()
    }

    fn status(&self) -> QueryStatus {
        self.inner.status()
    }

    fn need_persist(&self) -> bool {
        self.inner.need_persist()
    }
}

pub struct TrackedRecordBatchStream {
    inner: SendableRecordBatchStream,
    query_id: QueryId,
    tracker: Arc<QueryTracker>,
}

impl RecordBatchStream for TrackedRecordBatchStream {
    fn schema(&self) -> SchemaRef {
        self.inner.schema()
    }
}

impl Stream for TrackedRecordBatchStream {
    type Item = DFResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.inner.poll_next_unpin(cx)
    }
}

impl Drop for TrackedRecordBatchStream {
    fn drop(&mut self) {
        debug!("Query drop: {:?}", self.query_id);
        let _ = self.tracker.expire_query(&self.query_id);
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use async_trait::async_trait;
    use datafusion::arrow::datatypes::Schema;
    use datafusion::physical_plan::EmptyRecordBatchStream;
    use models::auth::user::{UserDesc, UserOptions};
    use spi::query::dispatcher::{QueryInfo, QueryStatus};
    use spi::query::execution::{Output, QueryExecution, QueryState, RUNNING};
    use spi::service::protocol::QueryId;
    use spi::QueryError;

    use super::QueryTracker;
    use crate::dispatcher::persister::LocalQueryPersister;

    struct QueryExecutionMock {}

    #[async_trait]
    impl QueryExecution for QueryExecutionMock {
        async fn start(&self) -> std::result::Result<Output, QueryError> {
            Ok(Output::StreamData(Box::pin(EmptyRecordBatchStream::new(
                Arc::new(Schema::empty()),
            ))))
        }
        fn cancel(&self) -> std::result::Result<(), QueryError> {
            Ok(())
        }
        fn info(&self) -> QueryInfo {
            let options = UserOptions::default();
            let desc = UserDesc::new(0_u128, "user".to_string(), options, true);
            QueryInfo::new(
                1_u64.into(),
                "test".to_string(),
                0_u128,
                "tenant".to_string(),
                desc,
            )
        }
        fn status(&self) -> QueryStatus {
            QueryStatus::new(
                QueryState::RUNNING(RUNNING::SCHEDULING),
                Duration::new(0, 1),
            )
        }
    }

    fn new_query_tracker(limit: usize) -> QueryTracker {
        QueryTracker::new(
            limit,
            Arc::new(LocalQueryPersister::try_new("/tmp/cnosdb/query").unwrap()),
        )
    }

    #[tokio::test]
    async fn test_track_and_drop() {
        let query_id = QueryId::next_id();
        let query = Arc::new(QueryExecutionMock {});
        let tracker = Arc::new(new_query_tracker(10));

        let _tq = tracker
            .try_track_query(query_id, query.clone())
            .await
            .unwrap();
        assert_eq!(tracker._running_query_count(), 1);

        let query_id = QueryId::next_id();
        let _tq = tracker
            .try_track_query(query_id, query.clone())
            .await
            .unwrap();
        assert_eq!(tracker._running_query_count(), 2);

        {
            // 作用域结束时，不会结束对当前query的追踪
            let query_id = QueryId::next_id();
            let _tq = tracker
                .try_track_query(query_id, query.clone())
                .await
                .unwrap();
            assert_eq!(tracker._running_query_count(), 3);
            // 需有主动调用expire_query
            let _ = tracker.expire_query(&query_id);
        }

        assert_eq!(tracker._running_query_count(), 2);

        let query_id = QueryId::next_id();
        let _tq = tracker.try_track_query(query_id, query).await.unwrap();
        assert_eq!(tracker._running_query_count(), 3);
    }

    #[tokio::test]
    async fn test_track_stream_result_and_drop() {
        let query = Arc::new(QueryExecutionMock {});
        let tracker = Arc::new(new_query_tracker(10));

        let output = {
            let query_id = QueryId::next_id();
            let tq = tracker
                .try_track_query(query_id, query.clone())
                .await
                .unwrap();
            tq.start().await.unwrap()
        };
        assert_eq!(tracker._running_query_count(), 1);
        // 释放output，结束对当前query的追踪
        drop(output);
        assert_eq!(tracker._running_query_count(), 0);
    }

    #[tokio::test]
    async fn test_exceed_query_limit() {
        let query_id = QueryId::next_id();
        let query = Arc::new(QueryExecutionMock {});
        let tracker = Arc::new(new_query_tracker(2));

        let _tq = tracker
            .try_track_query(query_id, query.clone())
            .await
            .unwrap();
        assert_eq!(tracker._running_query_count(), 1);

        let query_id = QueryId::next_id();
        let _tq = tracker
            .try_track_query(query_id, query.clone())
            .await
            .unwrap();
        assert_eq!(tracker._running_query_count(), 2);

        let query_id = QueryId::next_id();
        assert!(tracker.try_track_query(query_id, query).await.is_err())
    }

    #[tokio::test]
    async fn test_get_query_info() {
        let query_id = QueryId::next_id();
        let query = Arc::new(QueryExecutionMock {});
        let tracker = Arc::new(new_query_tracker(2));

        let _tq = tracker
            .try_track_query(query_id, query.clone())
            .await
            .unwrap();

        let exe = tracker.query(&query_id).unwrap();

        let info_actual = query.info();
        let info_found = exe.info();

        assert_eq!(info_actual, info_found);
    }
}
