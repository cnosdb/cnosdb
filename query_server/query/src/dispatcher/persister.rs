use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;
use spi::query::dispatcher::QueryInfo;
use spi::service::protocol::QueryId;
use spi::QueryError;
use tokio::fs::{self, File};
use tokio::io::AsyncWriteExt;
use trace::{debug, warn};

use super::QueryPersister;

pub type QueryPersisterRef = Arc<dyn QueryPersister + Send + Sync>;

const QUERY_INFO_FILE_NAME: &str = "query_info";

pub struct LocalQueryPersister {
    path: PathBuf,
}

impl LocalQueryPersister {
    pub fn try_new(path: impl Into<PathBuf>) -> Result<Self, QueryError> {
        let path: PathBuf = path.into();
        std::fs::create_dir_all(path.as_path())?;

        Ok(Self { path })
    }

    fn file_path(&self, query_id: &QueryId) -> PathBuf {
        let mut path = PathBuf::from(self.path.as_path());
        path.push(&format!("{}", query_id));
        path.push(QUERY_INFO_FILE_NAME);
        path
    }

    fn query_dir(&self, query_id: &QueryId) -> PathBuf {
        let mut path = PathBuf::from(self.path.as_path());
        path.push(&format!("{}", query_id));
        path
    }

    fn root_dir(&self) -> PathBuf {
        PathBuf::from(self.path.as_path())
    }
}

#[async_trait]
impl QueryPersister for LocalQueryPersister {
    fn remove(&self, query_id: &QueryId) -> Result<(), QueryError> {
        let path = self.query_dir(query_id);

        debug!("Remove query dir: {:?}", path.as_path());

        // Asynchronously delete the metadata file of the query.
        // note:
        //   Since deleting folders is asynchronous,
        //   the system crashes before deleting files,
        //   and the query still exists when restarting.
        tokio::spawn(async {
            let _ = fs::remove_dir_all(path)
                .await
                .map_err(|err| warn!("Remove query dir failed: {:?}", err));
        });

        Ok(())
    }

    async fn save(&self, query_id: QueryId, query: QueryInfo) -> Result<(), QueryError> {
        debug!("Save query: {}, {:?}", query_id, query);

        let path = self.file_path(&query_id);
        let mut file = File::create(path)
            .await
            .map_err(|err| QueryError::PersistQuery {
                reason: format!(
                    "Failed to create query info file at {}: {}",
                    self.path.display(),
                    err
                ),
            })?;

        let body = serde_json::to_vec(&query).map_err(|err| {
            trace::error!("Failed to serialize query info: {}", err);
            QueryError::BincodeSerialize {
                source: Box::new(err),
            }
        })?;

        file.write_all(&body)
            .await
            .map_err(|err| QueryError::PersistQuery {
                reason: format!(
                    "Failed to save query info into file at {}: {:?}",
                    self.path.display(),
                    err
                ),
            })?;

        Ok(())
    }

    async fn queries(&self) -> Result<Vec<QueryInfo>, QueryError> {
        let mut result = vec![];

        let mut entries = fs::read_dir(self.root_dir()).await?;
        while let Some(entry) = entries.next_entry().await? {
            let bytes = fs::read(entry.path().join(QUERY_INFO_FILE_NAME)).await?;
            match serde_json::from_slice::<QueryInfo>(&bytes) {
                Ok(query_info) => {
                    result.push(query_info);
                }
                Err(err) => {
                    trace::warn!(
                        "Failed to deserialize query info from file: {:?}, error: {}",
                        entry.path(),
                        err,
                    );
                }
            }
        }

        Ok(result)
    }
}
