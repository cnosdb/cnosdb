use std::sync::Arc;
use std::time::Duration;

use memory_pool::MemoryPoolRef;
use meta::model::MetaRef;
use models::meta_data::*;
use protos::kv_service::{raft_write_command, RaftWriteCommand};
use protos::models_helper::to_prost_bytes;
use protos::{tskv_service_time_out_client, DEFAULT_GRPC_SERVER_MESSAGE_LEN};
use replication::errors::ReplicationResult;
use replication::raft_node::RaftNode;
use trace::{debug, info, SpanContext, SpanRecorder};
use tskv::EngineRef;

use super::manager::RaftNodesManager;
use crate::errors::*;

pub struct RaftWriter {
    meta: MetaRef,
    config: config::Config,
    kv_inst: Option<EngineRef>,
    memory_pool: MemoryPoolRef,
    raft_manager: Arc<RaftNodesManager>,
}

impl RaftWriter {
    pub fn new(
        meta: MetaRef,
        config: config::Config,
        kv_inst: Option<EngineRef>,
        memory_pool: MemoryPoolRef,
        raft_manager: Arc<RaftNodesManager>,
    ) -> Self {
        Self {
            meta,
            config,
            kv_inst,
            memory_pool,
            raft_manager,
        }
    }

    pub async fn write_to_replica(
        &self,
        replica: &ReplicationSet,
        request: RaftWriteCommand,
        span_recorder: SpanRecorder,
    ) -> CoordinatorResult<()> {
        let node_id = self.config.global.node_id;
        let leader_id = replica.leader_node_id;
        if leader_id == node_id && self.kv_inst.is_some() {
            let span_recorder = span_recorder.child("write to local node or forward");
            let result = self
                .write_to_local_or_forward(replica, request, span_recorder.span_ctx())
                .await;

            debug!("write to local {} {:?} {:?}", node_id, replica, result);

            result
        } else {
            let span_recorder = span_recorder.child("write to remote node");
            let result = self
                .write_to_remote(leader_id, request.clone(), span_recorder.span_ctx())
                .await;
            debug!("write to remote {} {:?} {:?}", leader_id, replica, result);

            if let Err(CoordinatorError::FailoverNode { .. }) = result {
                for vnode in replica.vnodes.iter() {
                    if vnode.node_id == leader_id {
                        continue;
                    }

                    let result = self
                        .write_to_remote(vnode.node_id, request.clone(), span_recorder.span_ctx())
                        .await;
                    debug!(
                        "try write to remote {} {:?} {:?}",
                        vnode.node_id, replica, result
                    );

                    if result.is_ok() {
                        return Ok(());
                    }

                    if let Err(CoordinatorError::FailoverNode { .. }) = result {
                        continue;
                    } else {
                        return result;
                    }
                }
            }

            result
        }
    }

    pub async fn write_to_local_or_forward(
        &self,
        replica: &ReplicationSet,
        request: RaftWriteCommand,
        span_ctx: Option<&SpanContext>,
    ) -> CoordinatorResult<()> {
        let raft = self
            .raft_manager
            .get_node_or_build(&request.tenant, &request.db_name, replica)
            .await?;

        self.pre_check_write_to_raft(&request).await?;
        let raft_data = to_prost_bytes(request.clone());
        let result = self.write_to_raft(raft, raft_data).await;
        if let Err(CoordinatorError::ForwardToLeader {
            replica_id: _,
            leader_vnode_id,
        }) = result
        {
            self.process_leader_change(leader_vnode_id, request, span_ctx)
                .await
        } else {
            result
        }
    }

    async fn pre_check_write_to_raft(&self, request: &RaftWriteCommand) -> CoordinatorResult<()> {
        if let Some(command) = &request.command {
            match command {
                raft_write_command::Command::WriteData(request) => {
                    let fb_points = flatbuffers::root::<protos::models::Points>(&request.data)
                        .map_err(|err| CoordinatorError::TskvError {
                            source: tskv::Error::InvalidFlatbuffer { source: err },
                        })?;

                    let _ = fb_points.tables().ok_or(CoordinatorError::TskvError {
                        source: tskv::Error::InvalidPointTable,
                    })?;

                    let total_memory = self.config.deployment.memory * 1024 * 1024 * 1024;
                    if request.data.len() > total_memory.saturating_sub(self.memory_pool.reserved())
                    {
                        return Err(CoordinatorError::TskvError {
                            source: tskv::Error::MemoryExhausted,
                        });
                    }
                }

                raft_write_command::Command::DropTable(_request) => {}
                raft_write_command::Command::DropColumn(_request) => {}
                raft_write_command::Command::UpdateTags(_request) => {}
                raft_write_command::Command::DeleteFromTable(_request) => {}
            }
        }

        Ok(())
    }

    async fn process_leader_change(
        &self,
        leader_vnode_id: VnodeId,
        request: RaftWriteCommand,
        span_ctx: Option<&SpanContext>,
    ) -> CoordinatorResult<()> {
        let vnode_id = leader_vnode_id;
        let all_info =
            crate::get_vnode_all_info(self.meta.clone(), &request.tenant, vnode_id).await?;

        let rsp = self
            .meta
            .tenant_meta(&request.tenant)
            .await
            .ok_or(CoordinatorError::TenantNotFound {
                name: request.tenant.clone(),
            })?
            .change_repl_set_leader(
                &all_info.db_name,
                all_info.bucket_id,
                request.replica_id,
                all_info.node_id,
                vnode_id,
            )
            .await;

        info!(
            "change replica set({}) leader to vnode({}); {:?}",
            request.replica_id, vnode_id, rsp
        );

        self.write_to_remote(all_info.node_id, request, span_ctx)
            .await
    }

    async fn write_to_remote(
        &self,
        leader_id: u64,
        request: RaftWriteCommand,
        span_ctx: Option<&SpanContext>,
    ) -> CoordinatorResult<()> {
        let channel = self.meta.get_node_conn(leader_id).await.map_err(|error| {
            CoordinatorError::FailoverNode {
                id: leader_id,
                error: error.to_string(),
            }
        })?;
        let timeout = self.config.query.write_timeout_ms;
        let mut client = tskv_service_time_out_client(
            channel,
            Duration::from_millis(timeout),
            DEFAULT_GRPC_SERVER_MESSAGE_LEN,
            self.config.service.grpc_enable_gzip,
        );

        let mut cmd = tonic::Request::new(request);
        trace_http::ctx::append_trace_context(span_ctx, cmd.metadata_mut()).map_err(|_| {
            CoordinatorError::CommonError {
                msg: "Parse trace_id, this maybe a bug".to_string(),
            }
        })?;

        let begin_time = models::utils::now_timestamp_millis();
        let response = client
            .exec_raft_write_command(cmd)
            .await
            .map_err(|err| match err.code() {
                tonic::Code::Internal => CoordinatorError::TskvError { source: err.into() },
                _ => CoordinatorError::FailoverNode {
                    id: leader_id,
                    error: format!("{err:?}"),
                },
            })?
            .into_inner();

        let use_time = models::utils::now_timestamp_millis() - begin_time;
        if use_time > 200 {
            debug!(
                "write points to node:{}, use time too long {}",
                leader_id, use_time
            )
        }

        crate::status_response_to_result(&response)
    }

    async fn write_to_raft(&self, raft: Arc<RaftNode>, data: Vec<u8>) -> CoordinatorResult<()> {
        match raft.raw_raft().client_write(data).await {
            Err(err) => {
                if let Some(openraft::error::ForwardToLeader {
                    leader_id: Some(leader_id),
                    leader_node: Some(leader_node),
                }) = err.forward_to_leader()
                {
                    Err(CoordinatorError::ForwardToLeader {
                        leader_vnode_id: (*leader_id) as u32,
                        replica_id: leader_node.group_id,
                    })
                } else {
                    Err(CoordinatorError::RaftWriteError {
                        msg: err.to_string(),
                    })
                }
            }

            Ok(resp) => {
                let apply_result =
                    bincode::deserialize::<ReplicationResult<replication::Response>>(&resp.data)?;

                let _data = apply_result?;

                Ok(())
            }
        }
    }
}
