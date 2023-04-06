use std::collections::HashMap;
use std::sync::Arc;

use meta::model::MetaClientRef;
use models::schema::StreamTable;

use crate::QueryError;

pub type StreamCheckerManagerRef = Arc<StreamCheckerManager>;

/// Maintain and manage all registered streaming data sources
#[derive(Default)]
pub struct StreamCheckerManager {
    checkers: HashMap<String, StreamTableCheckerRef>,
}

impl StreamCheckerManager {
    pub fn register_stream_checker(
        &mut self,
        stream_type: impl Into<String>,
        checker: StreamTableCheckerRef,
    ) -> Result<(), QueryError> {
        let stream_type = stream_type.into();

        if self.checkers.contains_key(&stream_type) {
            return Err(QueryError::StreamTableCheckerAlreadyExists { stream_type });
        }

        let _ = self.checkers.insert(stream_type, checker);

        Ok(())
    }

    pub fn checker(&self, stream_type: &str) -> Option<StreamTableCheckerRef> {
        self.checkers.get(stream_type).cloned()
        // .ok_or_else(|| QueryError::UnsupportedStreamType {
        //     stream_type: stream_type.to_string(),
        // })
    }
}

pub type StreamTableCheckerRef = Arc<dyn SchemaChecker<StreamTable> + Send + Sync>;

pub trait SchemaChecker<T> {
    /// Check whether [`T`] meets the conditions
    fn check(&self, client: &MetaClientRef, element: &T) -> Result<(), QueryError>;
}
