use std::borrow::Cow;
use std::collections::HashMap;
use std::ops::Deref;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use async_trait::async_trait;
use coordinator::service::CoordinatorRef;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::Result as DFResult;
use datafusion::physical_plan::{RecordBatchStream, SendableRecordBatchStream};
use futures::{Stream, StreamExt};
use models::meta_data::NodeId;
use models::schema::query_info::{QueryId, QueryInfo};
use models::schema::{CLUSTER_SCHEMA, DEFAULT_CATALOG};
use models::utils::now_timestamp_nanos;
use parking_lot::RwLock;
use protocol_parser::Line;
use protos::FieldValue;
use spi::query::dispatcher::QueryStatus;
use spi::query::execution::{Output, QueryExecution, QueryExecutionRef, QueryType};
use spi::{QueryError, QueryResult};
use trace::{debug, warn};
use utils::precision::Precision;

use super::persister::QueryPersisterRef;

const SQL_HISTORY: &str = "sql_history";

pub struct QueryTracker {
    queries: RwLock<HashMap<QueryId, Arc<dyn QueryExecution>>>,
    query_limit: usize,
    query_persister: QueryPersisterRef,
    coord: CoordinatorRef,
}

impl QueryTracker {
    pub fn new(
        query_limit: usize,
        query_persister: QueryPersisterRef,
        coord: CoordinatorRef,
    ) -> Self {
        Self {
            queries: RwLock::new(HashMap::new()),
            query_limit,
            query_persister,
            coord,
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
    ) -> QueryResult<TrackedQuery> {
        debug!(
            "total query count: {}, status {:?}",
            self.queries.read().len(),
            query.status(),
        );

        // debug!(
        //     "total query count: {}, current query info {:?} status {:?}",
        //     self.queries.read().len(),
        //     query.info(),
        //     query.status(),
        // );

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
    pub async fn persistent_queries(&self, node_id: NodeId) -> QueryResult<Vec<QueryInfo>> {
        self.query_persister.queries(node_id).await
    }

    /// Once closed, no new requests will be accepted
    ///
    /// After closing, tracking new requests through [`QueryTracker::try_track_query`] will return [`QueryError::Closed`]
    pub fn _close(&self) {
        // pass
    }

    pub fn expire_query(&self, id: &QueryId) -> Option<Arc<dyn QueryExecution>> {
        self.queries.write().remove(id).inspect(|query| {
            if query.need_persist() {
                let _ = self.query_persister.remove(id).map_err(|err| {
                    warn!("Remove query from persister failed: {:?}", err);
                });
            }
            if *query.status().duration() >= self.coord.get_config().query.sql_record_timeout {
                let info = query.info();
                let status = query.status();

                let query_text = info.query();
                let user_id = info.user_id().to_string();
                let user_name = info.user_name();
                let tenant_id = info.tenant_id().to_string();
                let tenant_name = info.tenant_name();

                let state = status.query_state();
                let duration = status.duration().as_secs_f64();
                let processed_count = status.processed_count();
                let error_count = status.error_count();
                let line = Line {
                    hash_id: 0,
                    table: Cow::Owned(SQL_HISTORY.to_string()),
                    tags: vec![
                        (
                            Cow::Owned("user_id".to_string()),
                            Cow::Owned(user_id.to_string()),
                        ),
                        (
                            Cow::Owned("user_name".to_string()),
                            Cow::Owned(user_name.to_string()),
                        ),
                        (
                            Cow::Owned("tenant_id".to_string()),
                            Cow::Owned(tenant_id.to_string()),
                        ),
                        (
                            Cow::Owned("tenant_name".to_string()),
                            Cow::Owned(tenant_name.to_string()),
                        ),
                    ],
                    fields: vec![
                        (
                            Cow::Owned("query_id".to_string()),
                            FieldValue::Str(id.to_string().as_bytes().to_owned()),
                        ),
                        (
                            Cow::Owned("query_type".to_string()),
                            FieldValue::Str(query.query_type().to_string().as_bytes().to_owned()),
                        ),
                        (
                            Cow::Owned("query_text".to_string()),
                            FieldValue::Str(query_text.as_bytes().to_owned()),
                        ),
                        (
                            Cow::Owned("state".to_string()),
                            FieldValue::Str(state.as_ref().as_bytes().to_owned()),
                        ),
                        (
                            Cow::Owned("duration".to_string()),
                            FieldValue::F64(duration),
                        ),
                        (
                            Cow::Owned("processed_count".to_string()),
                            FieldValue::U64(processed_count),
                        ),
                        (
                            Cow::Owned("error_count".to_string()),
                            FieldValue::U64(error_count),
                        ),
                    ],
                    timestamp: now_timestamp_nanos(),
                };
                let coord = self.coord.clone();
                tokio::spawn(async move {
                    let _ = coord
                        .write_lines(
                            DEFAULT_CATALOG,
                            CLUSTER_SCHEMA,
                            Precision::NS,
                            vec![line],
                            None,
                        )
                        .await
                        .map_err(|err| {
                            warn!("write_lines failed: {:?}", err);
                        });
                });
            }
        })
    }

    async fn save_query(
        &self,
        query_id: QueryId,
        query: Arc<dyn QueryExecution>,
    ) -> QueryResult<()> {
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

    async fn start(&self) -> QueryResult<Output> {
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

    fn cancel(&self) -> QueryResult<()> {
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
    use std::collections::HashSet;
    use std::sync::Arc;
    use std::time::Duration;

    use async_trait::async_trait;
    use coordinator::service_mock::MockCoordinator;
    use datafusion::arrow::datatypes::Schema;
    use datafusion::physical_plan::EmptyRecordBatchStream;
    use meta::model::meta_admin::AdminMeta;
    use models::auth::user::{User, UserDesc, UserOptions};
    use models::schema::query_info::{QueryId, QueryInfo};
    use spi::query::dispatcher::QueryStatus;
    use spi::query::execution::{Output, QueryExecution, QueryState, RUNNING};
    use spi::QueryError;

    use super::QueryTracker;
    use crate::dispatcher::persister::MetaQueryPersister;

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
            let user = User::new(desc, HashSet::new(), None);
            QueryInfo::new(
                1_u64.into(),
                "test".to_string(),
                0_u128,
                "tenant".to_string(),
                "db".to_string(),
                user,
                1001,
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
            Arc::new(MetaQueryPersister::new(Arc::new(AdminMeta::mock()))),
            Arc::new(MockCoordinator {}),
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
