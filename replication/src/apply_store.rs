use std::collections::HashMap;
use std::fs;
use std::path::Path;
use std::sync::Arc;

use async_trait::async_trait;
use heed::types::*;
use heed::{Database, Env};
use serde::{Deserialize, Serialize};

use crate::errors::{ReplicationError, ReplicationResult};
use crate::{Request, Response};

#[async_trait]
pub trait ApplyStorage: Send + Sync {
    async fn apply(&self, req: &Request) -> ReplicationResult<Response>;
    async fn snapshot(&self) -> ReplicationResult<Vec<u8>>;
    async fn restore(&self, snapshot: &[u8]) -> ReplicationResult<()>;

    async fn test_get_value(&self, key: &str) -> ReplicationResult<Option<String>>;
}

pub type ApplyStorageRef = Arc<dyn ApplyStorage>;

#[derive(Serialize, Deserialize)]
struct ExampleSnapshotData {
    pub map: HashMap<String, String>,
}

pub struct ExampleApplyStorage {
    env: Env,
    db: Database<Str, Str>,
}

impl ExampleApplyStorage {
    pub fn open(path: impl AsRef<Path>) -> ReplicationResult<Self> {
        fs::create_dir_all(&path)?;

        let env = heed::EnvOpenOptions::new()
            .map_size(1024 * 1024 * 1024)
            .max_dbs(128)
            .open(path)?;

        let db: Database<Str, Str> = env.create_database(Some("data"))?;
        let storage = Self { env, db };

        Ok(storage)
    }

    pub fn get(&self, key: &str) -> ReplicationResult<Option<String>> {
        let reader = self.env.read_txn()?;
        if let Some(data) = self.db.get(&reader, key)? {
            Ok(Some(data.to_owned()))
        } else {
            Ok(None)
        }
    }
}

#[async_trait]
impl ApplyStorage for ExampleApplyStorage {
    async fn apply(&self, req: &Request) -> ReplicationResult<Response> {
        match req {
            Request::Set { key, value } => {
                let mut writer = self.env.write_txn()?;
                self.db.put(&mut writer, key, value)?;
                writer.commit()?;

                Ok(Response {
                    value: Some(value.to_string()),
                })
            }

            _ => Err(ReplicationError::MsgInvalid {
                msg: format!("Unknow apply message: {:?}", req),
            }),
        }
    }

    async fn snapshot(&self) -> ReplicationResult<Vec<u8>> {
        let mut hash_map = HashMap::new();

        let reader = self.env.read_txn()?;
        let iter = self.db.iter(&reader)?;
        for pair in iter {
            let (key, val) = pair?;
            hash_map.insert(key.to_string(), val.to_string());
        }

        let data = ExampleSnapshotData { map: hash_map };
        let json_str = serde_json::to_string(&data).unwrap();

        Ok(json_str.as_bytes().to_vec())
    }

    async fn restore(&self, snapshot: &[u8]) -> ReplicationResult<()> {
        let data: ExampleSnapshotData = serde_json::from_slice(snapshot).unwrap();

        let mut writer = self.env.write_txn()?;
        self.db.clear(&mut writer)?;
        for (key, val) in data.map.iter() {
            self.db.put(&mut writer, key, val)?;
        }
        writer.commit()?;

        Ok(())
    }

    async fn test_get_value(&self, key: &str) -> ReplicationResult<Option<String>> {
        let reader = self.env.read_txn()?;
        if let Some(data) = self.db.get(&reader, key)? {
            return Ok(Some(data.to_string()));
        } else {
            Ok(None)
        }
    }
}
