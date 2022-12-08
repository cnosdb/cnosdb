#![allow(clippy::module_inception)]

use std::collections::HashMap;
use std::fmt::Debug;
use std::io::Cursor;
use std::ops::{Bound, RangeBounds};
use std::sync::Arc;
use std::sync::Mutex;

use crate::store::state_machine::CommandResp;
use crate::ExampleTypeConfig;
use crate::NodeId;
use openraft::async_trait::async_trait;
use openraft::storage::LogState;
use openraft::storage::Snapshot;
use openraft::AnyError;
use openraft::EffectiveMembership;
use openraft::Entry;
use openraft::EntryPayload;
use openraft::ErrorSubject;
use openraft::ErrorVerb;
use openraft::LogId;
use openraft::RaftLogReader;
use openraft::RaftSnapshotBuilder;
use openraft::RaftStorage;
use openraft::SnapshotMeta;
use openraft::StateMachineChanges;
use openraft::StorageError;
use openraft::StorageIOError;
use openraft::Vote;
use sled::{Db, IVec};
use tokio::sync::RwLock;
use tracing;

pub mod command;
pub mod config;
pub mod state_machine;
pub mod store;

use crate::store::config::Config;

use self::state_machine::{StateMachine, WatchTenantMetaData};

#[derive(Debug)]
pub struct SnapshotInfo {
    pub meta: SnapshotMeta<NodeId>,
    pub data: Vec<u8>,
}

#[derive(Debug)]
pub struct Store {
    last_purged_log_id: RwLock<Option<LogId<NodeId>>>,

    /// The Raft log.
    pub log: sled::Tree,

    /// The Raft state machine.
    pub state_machine: RwLock<StateMachine>,

    /// The current granted vote.
    vote: sled::Tree,

    snapshot_idx: Arc<Mutex<u64>>,

    current_snapshot: RwLock<Option<SnapshotInfo>>,

    config: Config,

    pub watch: RwLock<HashMap<String, WatchTenantMetaData>>,

    pub node_id: NodeId,
}

fn get_sled_db(config: Config, node_id: NodeId) -> Db {
    let db_path = format!(
        "{}/{}-{}.binlog",
        config.journal_path, config.instance_prefix, node_id
    );
    let db = sled::open(db_path.clone()).unwrap();
    tracing::debug!("get_sled_db: created log at: {:?}", db_path);
    db
}

impl Store {
    pub fn open_create(node_id: NodeId) -> Store {
        tracing::info!("open_create, node_id: {}", node_id);

        let config = Config::default();

        let db = get_sled_db(config.clone(), node_id);

        let log = db
            .open_tree(format!("journal_entities_{}", node_id))
            .unwrap();

        let vote = db.open_tree(format!("votes_{}", node_id)).unwrap();

        let current_snapshot = RwLock::new(None);

        Store {
            last_purged_log_id: Default::default(),
            config,
            node_id,
            log,
            state_machine: Default::default(),
            vote,
            snapshot_idx: Arc::new(Mutex::new(0)),
            current_snapshot,
            watch: RwLock::new(HashMap::new()),
        }
    }
}

//Store trait for restore things from snapshot and log
#[async_trait]
pub trait Restore {
    async fn restore(&mut self);
}

#[async_trait]
impl Restore for Arc<Store> {
    #[tracing::instrument(level = "trace", skip(self))]
    async fn restore(&mut self) {
        tracing::debug!("restore");
        let log = &self.log;

        let first = log
            .iter()
            .rev()
            .next()
            .map(|res| res.unwrap())
            .map(|(_, val)| {
                serde_json::from_slice::<Entry<ExampleTypeConfig>>(&val)
                    .unwrap()
                    .log_id
            });

        if let Some(x) = first {
            tracing::debug!("restore: first log id = {:?}", x);
            let mut ld = self.last_purged_log_id.write().await;
            *ld = Some(x);
        }

        let snapshot = self.get_current_snapshot().await.unwrap();

        if let Some(ss) = snapshot {
            self.install_snapshot(&ss.meta, ss.snapshot).await.unwrap();
        }
    }
}

#[async_trait]
impl RaftLogReader<ExampleTypeConfig> for Arc<Store> {
    #[tracing::instrument(level = "trace", skip(self))]
    async fn get_log_state(&mut self) -> Result<LogState<ExampleTypeConfig>, StorageError<NodeId>> {
        let log = &self.log;
        let last = log
            .iter()
            .rev()
            .next()
            .map(|res| res.unwrap())
            .map(|(_, val)| {
                serde_json::from_slice::<Entry<ExampleTypeConfig>>(&val)
                    .unwrap()
                    .log_id
            });

        let last_purged = *self.last_purged_log_id.read().await;

        let last = match last {
            None => last_purged,
            Some(x) => Some(x),
        };
        tracing::debug!(
            "get_log_state: last_purged = {:?}, last = {:?}",
            last_purged,
            last
        );
        Ok(LogState {
            last_purged_log_id: last_purged,
            last_log_id: last,
        })
    }

    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + Send + Sync>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry<ExampleTypeConfig>>, StorageError<NodeId>> {
        let log = &self.log;
        let response = log
            .range(transform_range_bound(range))
            .map(|res| res.unwrap())
            .map(|(_, val)| serde_json::from_slice::<Entry<ExampleTypeConfig>>(&val).unwrap())
            .collect();

        Ok(response)
    }
}

fn transform_range_bound<RB: RangeBounds<u64> + Clone + Debug + Send + Sync>(
    range: RB,
) -> (Bound<IVec>, Bound<IVec>) {
    (
        serialize_bound(&range.start_bound()),
        serialize_bound(&range.end_bound()),
    )
}

fn serialize_bound(v: &Bound<&u64>) -> Bound<IVec> {
    match v {
        Bound::Included(v) => Bound::Included(IVec::from(&v.to_be_bytes())),
        Bound::Excluded(v) => Bound::Excluded(IVec::from(&v.to_be_bytes())),
        Bound::Unbounded => Bound::Unbounded,
    }
}

#[async_trait]
impl RaftSnapshotBuilder<ExampleTypeConfig, Cursor<Vec<u8>>> for Arc<Store> {
    #[tracing::instrument(level = "trace", skip(self))]
    async fn build_snapshot(
        &mut self,
    ) -> Result<Snapshot<ExampleTypeConfig, Cursor<Vec<u8>>>, StorageError<NodeId>> {
        let (data, last_applied_log);

        {
            // Serialize the data of the state machine.
            let state_machine = self.state_machine.read().await;
            data = serde_json::to_vec(&*state_machine).map_err(|e| {
                StorageIOError::new(
                    ErrorSubject::StateMachine,
                    ErrorVerb::Read,
                    AnyError::new(&e),
                )
            })?;

            last_applied_log = state_machine.last_applied_log;
        }

        let last_applied_log = match last_applied_log {
            None => {
                panic!("can not compact empty state machine");
            }
            Some(x) => x,
        };

        let snapshot_idx = {
            let mut l = self.snapshot_idx.lock().unwrap();
            *l += 1;
            *l
        };

        let snapshot_id = format!(
            "{}-{}-{}",
            last_applied_log.leader_id, last_applied_log.index, snapshot_idx
        );

        let meta = SnapshotMeta {
            last_log_id: last_applied_log,
            snapshot_id,
        };

        let snapshot = SnapshotInfo {
            meta: meta.clone(),
            data: data.clone(),
        };

        {
            let mut current_snapshot = self.current_snapshot.write().await;
            *current_snapshot = Some(snapshot);
        }

        self.write_snapshot().await.unwrap();

        Ok(Snapshot {
            meta,
            snapshot: Box::new(Cursor::new(data)),
        })
    }
}

#[async_trait]
impl RaftStorage<ExampleTypeConfig> for Arc<Store> {
    type SnapshotData = Cursor<Vec<u8>>;
    type LogReader = Self;
    type SnapshotBuilder = Self;

    #[tracing::instrument(level = "trace", skip(self))]
    async fn save_vote(&mut self, vote: &Vote<NodeId>) -> Result<(), StorageError<NodeId>> {
        self.vote
            .insert(b"vote", IVec::from(serde_json::to_vec(vote).unwrap()))
            .unwrap();
        Ok(())
    }

    async fn read_vote(&mut self) -> Result<Option<Vote<NodeId>>, StorageError<NodeId>> {
        let value = self.vote.get(b"vote").unwrap();
        match value {
            None => Ok(None),
            Some(val) => Ok(Some(serde_json::from_slice::<Vote<NodeId>>(&val).unwrap())),
        }
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    #[tracing::instrument(level = "trace", skip(self, entries))]
    async fn append_to_log(
        &mut self,
        entries: &[&Entry<ExampleTypeConfig>],
    ) -> Result<(), StorageError<NodeId>> {
        let log = &self.log;
        for entry in entries {
            log.insert(
                entry.log_id.index.to_be_bytes(),
                IVec::from(serde_json::to_vec(entry).unwrap()),
            )
            .unwrap();
        }
        Ok(())
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn delete_conflict_logs_since(
        &mut self,
        log_id: LogId<NodeId>,
    ) -> Result<(), StorageError<NodeId>> {
        tracing::debug!("delete_log: [{:?}, +oo)", log_id);

        let log = &self.log;
        let keys = log
            .range(transform_range_bound(log_id.index..))
            .map(|res| res.unwrap())
            .map(|(k, _v)| k); //TODO Why originally used collect instead of the iter.
        for key in keys {
            log.remove(&key).unwrap();
        }

        Ok(())
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn purge_logs_upto(&mut self, log_id: LogId<NodeId>) -> Result<(), StorageError<NodeId>> {
        tracing::debug!("delete_log: [{:?}, +oo)", log_id);

        {
            let mut ld = self.last_purged_log_id.write().await;
            assert!(*ld <= Some(log_id));
            *ld = Some(log_id);
        }

        {
            let log = &self.log;

            let keys = log
                .range(transform_range_bound(..=log_id.index))
                .map(|res| res.unwrap())
                .map(|(k, _)| k);
            for key in keys {
                log.remove(&key).unwrap();
            }
        }

        Ok(())
    }

    async fn last_applied_state(
        &mut self,
    ) -> Result<(Option<LogId<NodeId>>, EffectiveMembership<NodeId>), StorageError<NodeId>> {
        let state_machine = self.state_machine.read().await;
        Ok((
            state_machine.last_applied_log,
            state_machine.last_membership.clone(),
        ))
    }

    #[tracing::instrument(level = "trace", skip(self, entries))]
    async fn apply_to_state_machine(
        &mut self,
        entries: &[&Entry<ExampleTypeConfig>],
    ) -> Result<Vec<CommandResp>, StorageError<NodeId>> {
        let mut res = Vec::with_capacity(entries.len());

        let mut watch = self.watch.write().await;
        let mut sm = self.state_machine.write().await;

        for entry in entries {
            tracing::debug!(%entry.log_id, "replicate to sm");

            sm.last_applied_log = Some(entry.log_id);

            match entry.payload {
                EntryPayload::Blank => res.push(CommandResp::default()),
                EntryPayload::Membership(ref mem) => {
                    sm.last_membership = EffectiveMembership::new(Some(entry.log_id), mem.clone());
                    res.push(CommandResp::default());
                }

                EntryPayload::Normal(ref req) => {
                    res.push(sm.process_write_command(req, &mut watch));
                }
            };
        }
        Ok(res)
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        self.clone()
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<Box<Self::SnapshotData>, StorageError<NodeId>> {
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    #[tracing::instrument(level = "trace", skip(self, snapshot))]
    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<NodeId>,
        snapshot: Box<Self::SnapshotData>,
    ) -> Result<StateMachineChanges<ExampleTypeConfig>, StorageError<NodeId>> {
        tracing::info!(
            { snapshot_size = snapshot.get_ref().len() },
            "decoding snapshot for installation"
        );

        let new_snapshot = SnapshotInfo {
            meta: meta.clone(),
            data: snapshot.into_inner(),
        };

        // Update the state machine.
        {
            let updated_state_machine: StateMachine = serde_json::from_slice(&new_snapshot.data)
                .map_err(|e| {
                    StorageIOError::new(
                        ErrorSubject::Snapshot(new_snapshot.meta.clone()),
                        ErrorVerb::Read,
                        AnyError::new(&e),
                    )
                })?;
            let mut state_machine = self.state_machine.write().await;
            *state_machine = updated_state_machine;
        }

        // Update current snapshot.
        let mut current_snapshot = self.current_snapshot.write().await;
        *current_snapshot = Some(new_snapshot);
        Ok(StateMachineChanges {
            last_applied: meta.last_log_id,
            is_snapshot: true,
        })
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<ExampleTypeConfig, Self::SnapshotData>>, StorageError<NodeId>> {
        tracing::debug!("get_current_snapshot: start");

        match &*self.current_snapshot.read().await {
            Some(snapshot) => {
                let data = snapshot.data.clone();
                Ok(Some(Snapshot {
                    meta: snapshot.meta.clone(),
                    snapshot: Box::new(Cursor::new(data)),
                }))
            }
            None => {
                let data = self.read_snapshot_file().await;
                let data = match data {
                    Ok(c) => c,
                    Err(_e) => return Ok(None),
                };

                let content: StateMachine = serde_json::from_slice(&data).unwrap();

                let last_applied_log = content.last_applied_log.unwrap();
                tracing::debug!(
                    "get_current_snapshot: last_applied_log = {:?}",
                    last_applied_log
                );

                let snapshot_idx = {
                    let mut l = self.snapshot_idx.lock().unwrap();
                    *l += 1;
                    *l
                };

                let snapshot_id = format!(
                    "{}-{}-{}",
                    last_applied_log.leader_id, last_applied_log.index, snapshot_idx
                );

                let meta = SnapshotMeta {
                    last_log_id: last_applied_log,
                    snapshot_id,
                };

                tracing::debug!("get_current_snapshot: meta {:?}", meta);

                Ok(Some(Snapshot {
                    meta,
                    snapshot: Box::new(Cursor::new(data)),
                }))
            }
        }
    }
}
