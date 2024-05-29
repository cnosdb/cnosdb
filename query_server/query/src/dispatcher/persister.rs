use std::sync::Arc;

use async_trait::async_trait;
use meta::model::meta_admin::AdminMeta;
use snafu::ResultExt;
use spi::query::dispatcher::QueryInfo;
use spi::service::protocol::QueryId;
use spi::{MetaSnafu, PersistQuerySnafu, QueryError};
use trace::{debug, warn};

use super::QueryPersister;

pub type QueryPersisterRef = Arc<dyn QueryPersister + Send + Sync>;

pub struct MetaQueryPersister {
    admin_meta: Arc<AdminMeta>,
}

impl MetaQueryPersister {
    pub fn new(admin_meta: Arc<AdminMeta>) -> Self {
        Self { admin_meta }
    }
}

#[async_trait]
impl QueryPersister for MetaQueryPersister {
    fn remove(&self, query_id: &QueryId) -> Result<(), QueryError> {
        let admin_meta = self.admin_meta.clone();
        let query_id = query_id.get();

        tokio::spawn(async move {
            let _ = admin_meta
                .remove_queryinfo(admin_meta.node_id(), query_id)
                .await
                .map_err(|err| warn!("Remove query dir failed: {:?}", err));
        });

        Ok(())
    }

    async fn save(&self, query_id: QueryId, query: QueryInfo) -> Result<(), QueryError> {
        debug!("Save query: {}, {:?}", query_id, query);

        let body = serde_json::to_vec(&query).map_err(|err| {
            trace::error!("Failed to serialize query info: {}", err);
            PersistQuerySnafu {
                reason: format!("Failed to serialize query info: {}", err),
            }
            .build()
        })?;

        self.admin_meta
            .write_queryinfo(self.admin_meta.node_id(), query_id.get(), body)
            .await
            .map_err(|err| {
                PersistQuerySnafu {
                    reason: format!(
                        "Failed to save query info into file at {}: {:?}",
                        self.admin_meta.node_id(),
                        err
                    ),
                }
                .build()
            })?;

        Ok(())
    }

    async fn queries(&self) -> Result<Vec<QueryInfo>, QueryError> {
        let query_infos = self
            .admin_meta
            .read_queryinfos(self.admin_meta.node_id())
            .await
            .context(MetaSnafu)?;

        let mut result = vec![];
        for query_info in query_infos {
            match serde_json::from_slice::<QueryInfo>(&query_info) {
                Ok(query_info) => {
                    result.push(query_info);
                }
                Err(err) => {
                    trace::warn!("Failed to deserialize query, error: {}", err,);
                }
            }
        }

        Ok(result)
    }
}
