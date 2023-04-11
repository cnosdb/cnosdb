use std::collections::HashMap;
use std::ops::Deref;
use std::sync::Arc;

use parking_lot::RwLock;
use spi::query::execution::{QueryExecution, QueryType};
use spi::service::protocol::QueryId;
use spi::QueryError;
use tokio::sync::{Semaphore, SemaphorePermit, TryAcquireError};
use trace::{debug, warn};

pub struct QueryTracker {
    queries: RwLock<HashMap<QueryId, Arc<dyn QueryExecution>>>,
    query_limit_semaphore: Semaphore,
}

impl QueryTracker {
    pub fn new(query_limit: usize) -> Self {
        let query_limit_semaphore = Semaphore::new(query_limit);

        Self {
            queries: RwLock::new(HashMap::new()),
            query_limit_semaphore,
        }
    }
}

impl QueryTracker {
    /// track a query
    ///
    /// [`QueryError::RequestLimit`]
    ///
    /// [`QueryError::Closed`] after call [`Self::close`]
    pub fn try_track_query(
        &self,
        query_id: QueryId,
        query: Arc<dyn QueryExecution>,
    ) -> std::result::Result<TrackedQuery, QueryError> {
        debug!(
            "total query count: {}, current query info {:?} status {:?}",
            self.queries.read().len(),
            query.info(),
            query.status(),
        );

        let _ = self.queries.write().insert(query_id, query.clone());

        let _permit = match self.query_limit_semaphore.try_acquire() {
            Ok(p) => p,
            Err(TryAcquireError::NoPermits) => {
                warn!("simultaneous request limit exceeded - dropping request");
                return Err(QueryError::RequestLimit);
            }
            Err(TryAcquireError::Closed) => {
                return Err(QueryError::Closed);
            }
        };

        Ok(TrackedQuery {
            _permit,
            tracker: self,
            query_id,
            query,
        })
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

    /// Once closed, no new requests will be accepted
    ///
    /// After closing, tracking new requests through [`QueryTracker::try_track_query`] will return [`QueryError::Closed`]
    pub fn _close(&self) {
        self.query_limit_semaphore.close();
    }

    pub fn expire_query(&self, id: &QueryId) -> Option<Arc<dyn QueryExecution>> {
        self.queries.write().remove(id)
    }
}

pub struct TrackedQuery<'a> {
    _permit: SemaphorePermit<'a>,
    tracker: &'a QueryTracker,
    query_id: QueryId,
    query: Arc<dyn QueryExecution>,
}

impl Deref for TrackedQuery<'_> {
    type Target = dyn QueryExecution;

    fn deref(&self) -> &Self::Target {
        self.query.deref()
    }
}

impl Drop for TrackedQuery<'_> {
    fn drop(&mut self) {
        match self.query.query_type() {
            QueryType::Batch => {
                debug!("TrackedQuery drop: {:?}", &self.query_id);
                let _ = self.tracker.expire_query(&self.query_id);
            }
            QueryType::Stream => {
                // 流任务是常驻任务，需要手动kill
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use async_trait::async_trait;
    use models::auth::user::{UserDesc, UserOptions};
    use spi::query::dispatcher::{QueryInfo, QueryStatus};
    use spi::query::execution::{Output, QueryExecution, QueryState, RUNNING};
    use spi::service::protocol::QueryId;
    use spi::QueryError;

    use super::QueryTracker;

    struct QueryExecutionMock {}

    #[async_trait]
    impl QueryExecution for QueryExecutionMock {
        async fn start(&self) -> std::result::Result<Output, QueryError> {
            Ok(Output::Nil(()))
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
        QueryTracker::new(limit)
    }

    #[test]
    fn test_track_and_drop() {
        let query_id = QueryId::next_id();
        let query = Arc::new(QueryExecutionMock {});
        let tracker = new_query_tracker(10);

        let _tq = tracker.try_track_query(query_id, query.clone()).unwrap();
        assert_eq!(tracker._running_query_count(), 1);

        let query_id = QueryId::next_id();
        let _tq = tracker.try_track_query(query_id, query.clone()).unwrap();
        assert_eq!(tracker._running_query_count(), 2);

        {
            // 作用域结束时，结束对当前query的追踪
            let query_id = QueryId::next_id();
            let _tq = tracker.try_track_query(query_id, query.clone()).unwrap();
            assert_eq!(tracker._running_query_count(), 3);
        }

        assert_eq!(tracker._running_query_count(), 2);

        let query_id = QueryId::next_id();
        let _tq = tracker.try_track_query(query_id, query).unwrap();
        assert_eq!(tracker._running_query_count(), 3);
    }

    #[test]
    fn test_exceed_query_limit() {
        let query_id = QueryId::next_id();
        let query = Arc::new(QueryExecutionMock {});
        let tracker = new_query_tracker(2);

        let _tq = tracker.try_track_query(query_id, query.clone()).unwrap();
        assert_eq!(tracker._running_query_count(), 1);

        let query_id = QueryId::next_id();
        let _tq = tracker.try_track_query(query_id, query.clone()).unwrap();
        assert_eq!(tracker._running_query_count(), 2);

        let query_id = QueryId::next_id();
        assert!(tracker.try_track_query(query_id, query).is_err())
    }

    #[test]
    fn test_get_query_info() {
        let query_id = QueryId::next_id();
        let query = Arc::new(QueryExecutionMock {});
        let tracker = new_query_tracker(2);

        let _tq = tracker.try_track_query(query_id, query.clone()).unwrap();

        let exe = tracker.query(&query_id).unwrap();

        let info_actual = query.info();
        let info_found = exe.info();

        assert_eq!(info_actual, info_found);
    }
}
