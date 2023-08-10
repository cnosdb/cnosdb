use std::collections::HashMap;
use std::fs;
use std::path::Path;
use std::sync::Arc;

use async_trait::async_trait;
use heed::types::*;
use heed::{Database, Env};
use serde::{Deserialize, Serialize};

use crate::errors::ReplicationResult;

#[async_trait]
pub trait ApplyStorage: Send + Sync {
    async fn apply(&self, message: &[u8]) -> ReplicationResult<()>;
    async fn snapshot(&self) -> ReplicationResult<Vec<u8>>;
    async fn restore(&self, snapshot: &[u8]) -> ReplicationResult<()>;

    async fn test_get_kv(&self, key: &str) -> ReplicationResult<Option<String>>;
    async fn test_set_kv(&self, key: &str, val: &str) -> ReplicationResult<()>;
}

pub type ApplyStorageRef = Arc<dyn ApplyStorage>;

//-------------------------Example-----------------------------
struct ExampleRequestMessage {
    pub key: String,
    pub val: String,
}

impl ExampleRequestMessage {
    pub fn encode(&self) -> String {
        format!("{} {}", self.key, self.val)
    }

    pub fn decode(data: &[u8]) -> Self {
        let data_str = String::from_utf8_lossy(data).to_string();
        let strs: Vec<&str> = data_str.split(' ').collect();

        Self {
            key: strs[0].to_owned(),
            val: strs[1].to_owned(),
        }
    }
}

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
    async fn apply(&self, message: &[u8]) -> ReplicationResult<()> {
        let request = ExampleRequestMessage::decode(message);
        let mut writer = self.env.write_txn()?;
        self.db.put(&mut writer, &request.key, &request.val)?;
        writer.commit()?;

        Ok(())
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

    async fn test_set_kv(&self, key: &str, val: &str) -> ReplicationResult<()> {
        let mut writer = self.env.write_txn()?;
        self.db.put(&mut writer, key, val)?;
        writer.commit()?;

        Ok(())
    }

    async fn test_get_kv(&self, key: &str) -> ReplicationResult<Option<String>> {
        let reader = self.env.read_txn()?;
        if let Some(data) = self.db.get(&reader, key)? {
            return Ok(Some(data.to_string()));
        } else {
            Ok(None)
        }
    }
}
