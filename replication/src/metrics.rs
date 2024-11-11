use std::collections::HashMap;
use std::sync::Arc;

use maplit::hashmap;
use metrics::gauge::U64Gauge;
use metrics::metric_register::MetricsRegister;
use models::meta_data::ReplicationSetId;

use crate::raft_node::RaftNodeMetrics;
use crate::RaftNodeId;

#[derive(Debug, Clone)]
pub struct ReplicationMetrics {
    pub applied_id: U64Gauge,
    pub snapshot_id: U64Gauge,
    pub flushed_id: U64Gauge,
    pub wal_index_min: U64Gauge,
    pub wal_index_max: U64Gauge,
    pub wal_avg_write_time: U64Gauge,
    pub repication_delay: HashMap<RaftNodeId, U64Gauge>,
    pub write_apply_duration: U64Gauge,
    pub write_build_group_duration: U64Gauge,
    pub write_put_points_duration: U64Gauge,

    id: RaftNodeId,
    tenant: String,
    db_name: String,
    replica_id: ReplicationSetId,
    register: Arc<MetricsRegister>,
}

impl ReplicationMetrics {
    pub fn new(
        register: Arc<MetricsRegister>,
        tenant: &str,
        db_name: &str,
        replica_id: ReplicationSetId,
        vnode_id: u64,
    ) -> Self {
        let vnode_id_str = vnode_id.to_string();
        let replica_id_str = replica_id.to_string();
        let lables = [
            ("tenant", tenant),
            ("database", db_name),
            ("replica_id", replica_id_str.as_str()),
            ("vnode_id", vnode_id_str.as_str()),
        ];

        let metric = register.metric::<U64Gauge>("raft_applied_index", "raft applied index");
        let applied_id = metric.recorder(lables);

        let metric = register.metric::<U64Gauge>("raft_snapshot_index", "raft snapshot index");
        let snapshot_id = metric.recorder(lables);

        let metric = register.metric::<U64Gauge>("raft_flushed_index", "raft flushed index");
        let flushed_id = metric.recorder(lables);

        let metric = register.metric::<U64Gauge>("raft_wal_index_min", "raft wal min index");
        let wal_index_min = metric.recorder(lables);

        let metric = register.metric::<U64Gauge>("raft_wal_index_max", "raft wal max index");
        let wal_index_max = metric.recorder(lables);

        let metric = register.metric::<U64Gauge>("write_apply_duration", "write apply duration");
        let write_apply_duration = metric.recorder(lables);

        let metric =
            register.metric::<U64Gauge>("write_build_group_duration", "write build group duration");
        let write_build_group_duration = metric.recorder(lables);

        let metric =
            register.metric::<U64Gauge>("write_put_points_duration", "write put points duration");
        let write_put_points_duration = metric.recorder(lables);

        let metric = register
            .metric::<U64Gauge>("raft_wal_avg_write_time", "raft wal average write time(ms)");
        let wal_avg_write_time = metric.recorder(lables);

        Self {
            applied_id,
            snapshot_id,
            flushed_id,
            wal_index_min,
            wal_index_max,
            wal_avg_write_time,
            write_apply_duration,
            write_build_group_duration,
            write_put_points_duration,
            repication_delay: hashmap! {},

            id: vnode_id,
            replica_id,
            register,
            tenant: tenant.to_string(),
            db_name: db_name.to_string(),
        }
    }

    pub fn update_values(&mut self, metrics: RaftNodeMetrics) {
        self.applied_id.set(metrics.engine.last_applied_id);
        self.flushed_id.set(metrics.engine.flushed_apply_id);
        self.snapshot_id.set(metrics.engine.snapshot_apply_id);
        self.wal_index_min.set(metrics.entries.min_seq);
        self.wal_index_max.set(metrics.entries.max_seq);
        self.wal_avg_write_time.set(metrics.entries.avg_write_time);
        self.write_apply_duration
            .set(metrics.engine.write_apply_duration);
        self.write_build_group_duration
            .set(metrics.engine.write_build_group_duration);
        self.write_put_points_duration
            .set(metrics.engine.write_put_points_duration);

        let mut clears: Vec<RaftNodeId> = self.repication_delay.keys().cloned().collect();
        if metrics.raft.state == openraft::ServerState::Leader {
            if let Some(replications) = metrics.raft.replication {
                let last_log_index = metrics.raft.last_log_index.unwrap_or_default();
                for (id, status) in replications {
                    clears.retain(|&v| v != id);

                    let repl_delay = self.repication_delay.entry(id).or_insert({
                        let metric = self.register.metric::<U64Gauge>(
                            "raft_replication_delay",
                            "raft replication behind leader",
                        );

                        metric.recorder([
                            ("tenant", self.tenant.to_string()),
                            ("database", self.db_name.to_string()),
                            ("replica_id", self.replica_id.to_string()),
                            ("vnode_id", id.to_string()),
                        ])
                    });

                    let value = if last_log_index >= status.unwrap_or_default().index {
                        last_log_index - status.unwrap_or_default().index
                    } else {
                        0
                    };

                    repl_delay.set(value)
                }
            }
        }

        for id in clears {
            self.repication_delay.remove(&id);
            let metric = self
                .register
                .metric::<U64Gauge>("raft_replication_delay", "raft replication behind leader");
            metric.remove([
                ("tenant", self.tenant.as_str()),
                ("database", self.db_name.as_str()),
                ("replica_id", self.replica_id.to_string().as_str()),
                ("vnode_id", id.to_string().as_str()),
            ]);
        }
    }

    pub fn drop(&self) {
        let register = self.register.clone();
        let vnode_id_str = self.id.to_string();
        let replica_id_str = self.replica_id.to_string();
        let lables = [
            ("tenant", self.tenant.as_str()),
            ("database", self.db_name.as_str()),
            ("replica_id", replica_id_str.as_str()),
            ("vnode_id", vnode_id_str.as_str()),
        ];

        let metric = register.metric::<U64Gauge>("raft_applied_index", "raft applied index");
        metric.remove(lables);

        let metric = register.metric::<U64Gauge>("raft_snapshot_index", "raft snapshot index");
        metric.remove(lables);

        let metric = register.metric::<U64Gauge>("raft_flushed_index", "raft flushed index");
        metric.remove(lables);

        let metric = register.metric::<U64Gauge>("raft_wal_index_min", "raft wal min index");
        metric.remove(lables);

        let metric = register.metric::<U64Gauge>("raft_wal_index_max", "raft wal max index");
        metric.remove(lables);

        for (id, _) in self.repication_delay.iter() {
            let metric = register
                .metric::<U64Gauge>("raft_replication_delay", "raft replication behind leader");
            metric.remove([
                ("tenant", self.tenant.as_str()),
                ("database", self.db_name.as_str()),
                ("replica_id", self.replica_id.to_string().as_str()),
                ("vnode_id", id.to_string().as_str()),
            ]);
        }
    }
}
