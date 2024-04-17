use std::collections::HashMap;
use std::fs;
use std::path::Path;

use async_trait::async_trait;
use heed::types::*;
use heed::{Database, Env};
use serde::{Deserialize, Serialize};

use crate::errors::{ReplicationError, ReplicationResult};
use crate::{ApplyContext, ApplyStorage, EngineMetrics, Request, Response, SnapshotMode};

const LAST_APPLIED_ID_KEY: &str = "last_applied_id";
// --------------------------------------------------------------------------- //
#[derive(Serialize, Deserialize)]
pub struct HashMapSnapshotData {
    pub map: HashMap<String, String>,
}

pub struct HeedApplyStorage {
    env: Env,
    db: Database<Str, Str>,
}

impl HeedApplyStorage {
    pub fn open(path: impl AsRef<Path>, size: usize) -> ReplicationResult<Self> {
        fs::create_dir_all(&path)?;

        let env = heed::EnvOpenOptions::new()
            .map_size(size)
            .max_dbs(1)
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

    fn get_last_applied_id(&self) -> ReplicationResult<Option<u64>> {
        let reader = self.env.read_txn()?;
        if let Some(data) = self.db.get(&reader, LAST_APPLIED_ID_KEY)? {
            let id = data
                .parse::<u64>()
                .map_err(|err| ReplicationError::MsgInvalid {
                    msg: err.to_string(),
                })?;

            Ok(Some(id))
        } else {
            Ok(None)
        }
    }
}

#[async_trait]
impl ApplyStorage for HeedApplyStorage {
    async fn apply(&mut self, ctx: &ApplyContext, req: &Request) -> ReplicationResult<Response> {
        #[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
        struct RequestCommand {
            key: String,
            value: String,
        }

        let req: RequestCommand = serde_json::from_slice(req)?;
        let mut writer = self.env.write_txn()?;
        self.db.put(&mut writer, &req.key, &req.value)?;
        self.db
            .put(&mut writer, LAST_APPLIED_ID_KEY, &ctx.index.to_string())?;
        writer.commit()?;

        Ok(req.value.into())
    }

    async fn snapshot(&mut self, _mode: SnapshotMode) -> ReplicationResult<(Vec<u8>, Option<u64>)> {
        let mut hash_map = HashMap::new();
        let reader = self.env.read_txn()?;
        let iter = self.db.iter(&reader)?;
        for pair in iter {
            let (key, val) = pair?;
            hash_map.insert(key.to_string(), val.to_string());
        }

        let data = HashMapSnapshotData { map: hash_map };
        let json_str = serde_json::to_string(&data).unwrap();

        Ok((json_str.as_bytes().to_vec(), None))
    }

    async fn restore(&mut self, snapshot: &[u8]) -> ReplicationResult<()> {
        let data: HashMapSnapshotData = serde_json::from_slice(snapshot).unwrap();

        let mut writer = self.env.write_txn()?;
        self.db.clear(&mut writer)?;
        for (key, val) in data.map.iter() {
            self.db.put(&mut writer, key, val)?;
        }
        writer.commit()?;

        Ok(())
    }

    async fn destory(&mut self) -> ReplicationResult<()> {
        Ok(())
    }

    async fn metrics(&self) -> ReplicationResult<EngineMetrics> {
        let id = self.get_last_applied_id()?.unwrap_or_default();
        Ok(EngineMetrics {
            last_applied_id: id,
            flushed_apply_id: id,
            snapshot_apply_id: id,
        })
    }
}
