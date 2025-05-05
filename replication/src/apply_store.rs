use std::collections::HashMap;
use std::fs;
use std::path::Path;

use async_trait::async_trait;
use heed::types::Str;
use heed::{Database, Env};
use serde::{Deserialize, Serialize};
use snafu::ResultExt;

use crate::errors::{HeedSnafu, IOErrSnafu, MsgInvalidSnafu, ReplicationResult};
use crate::{ApplyContext, ApplyStorage, EngineMetrics, Request, Response};

const LAST_APPLIED_ID_KEY: &str = "last_applied_id";

#[derive(Serialize, Deserialize)]
pub struct HashMapSnapshotData {
    pub map: HashMap<String, String>,
}

pub struct HeedApplyStorage {
    env: Env,
    db: Database<Str, Str>,
    snapshot: Option<(Vec<u8>, u64)>,
}

impl HeedApplyStorage {
    pub fn open(path: impl AsRef<Path>, size: usize) -> ReplicationResult<Self> {
        fs::create_dir_all(&path).context(IOErrSnafu)?;

        let env = unsafe {
            heed::EnvOpenOptions::new()
                .map_size(size)
                .max_dbs(1)
                .open(path)
        }
        .context(HeedSnafu)?;

        let mut wtxn = env.write_txn().context(HeedSnafu)?;
        let db: Database<Str, Str> = env
            .create_database(&mut wtxn, Some("data"))
            .context(HeedSnafu)?;
        wtxn.commit().context(HeedSnafu)?;

        Ok(Self {
            env,
            db,
            snapshot: None,
        })
    }

    pub fn get(&self, key: &str) -> ReplicationResult<Option<String>> {
        let reader = self.env.read_txn().context(HeedSnafu)?;
        if let Some(data) = self.db.get(&reader, key).context(HeedSnafu)? {
            Ok(Some(data.to_owned()))
        } else {
            Ok(None)
        }
    }

    fn get_last_applied_id(&self) -> ReplicationResult<Option<u64>> {
        let reader = self.env.read_txn().context(HeedSnafu)?;
        if let Some(data) = self
            .db
            .get(&reader, LAST_APPLIED_ID_KEY)
            .context(HeedSnafu)?
        {
            let id = data.parse::<u64>().map_err(|err| {
                MsgInvalidSnafu {
                    msg: err.to_string(),
                }
                .build()
            })?;

            Ok(Some(id))
        } else {
            Ok(None)
        }
    }
}

#[async_trait]
impl ApplyStorage for HeedApplyStorage {
    async fn apply(&mut self, _ctx: &ApplyContext, req: &Request) -> ReplicationResult<Response> {
        #[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
        struct RequestCommand {
            key: String,
            value: String,
        }

        let req: RequestCommand = serde_json::from_slice(req)
            .map_err(|e| MsgInvalidSnafu { msg: e.to_string() }.build())?;
        let mut writer = self.env.write_txn().context(HeedSnafu)?;
        self.db
            .put(&mut writer, &req.key, &req.value)
            .context(HeedSnafu)?;
        writer.commit().context(HeedSnafu)?;

        Ok(req.value.into())
    }

    async fn get_snapshot(&mut self) -> ReplicationResult<Option<(Vec<u8>, u64)>> {
        Ok(self.snapshot.clone())
    }

    async fn create_snapshot(&mut self, applied_id: u64) -> ReplicationResult<(Vec<u8>, u64)> {
        let mut hash_map = HashMap::new();
        let reader = self.env.read_txn().context(HeedSnafu)?;
        let iter = self.db.iter(&reader).context(HeedSnafu)?;
        for pair in iter {
            let (key, val) = pair.context(HeedSnafu)?;
            hash_map.insert(key.to_string(), val.to_string());
        }

        let data = HashMapSnapshotData { map: hash_map };
        let bytes = serde_json::to_vec(&data)
            .map_err(|e| MsgInvalidSnafu { msg: e.to_string() }.build())?;

        self.snapshot = Some((bytes.clone(), applied_id));

        Ok((bytes.clone(), applied_id))
    }

    async fn restore(&mut self, snapshot: &[u8]) -> ReplicationResult<()> {
        let data: HashMapSnapshotData = serde_json::from_slice(snapshot)
            .map_err(|e| MsgInvalidSnafu { msg: e.to_string() }.build())?;
        let mut writer = self.env.write_txn().context(HeedSnafu)?;
        self.db.clear(&mut writer).context(HeedSnafu)?;
        for (key, val) in data.map.iter() {
            self.db.put(&mut writer, key, val).context(HeedSnafu)?;
        }
        writer.commit().context(HeedSnafu)?;

        Ok(())
    }

    async fn destory(&mut self) -> ReplicationResult<()> {
        Ok(())
    }

    async fn metrics(&self) -> ReplicationResult<EngineMetrics> {
        let _id = self.get_last_applied_id()?.unwrap_or_default();
        if let Some(snapshot) = &self.snapshot {
            Ok(EngineMetrics {
                last_applied_id: 0,
                flushed_apply_id: 0,
                snapshot_apply_id: snapshot.1,
                write_apply_duration: 0,
                write_build_group_duration: 0,
                write_put_points_duration: 0,
            })
        } else {
            Ok(EngineMetrics::default())
        }
    }
}
