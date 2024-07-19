use std::collections::VecDeque;
use std::time::Duration;

use meta::model::MetaRef;
use models::meta_data::*;
use protos::kv_service::AdminCommand;
use protos::{tskv_service_time_out_client, DEFAULT_GRPC_SERVER_MESSAGE_LEN};
use snafu::ResultExt;
use trace::info;

use crate::errors::*;
use crate::TskvLeaderCaller;

pub struct TskvAdminRequest {
    pub meta: MetaRef,
    pub timeout: Duration,
    pub enable_gzip: bool,
    pub request: AdminCommand,
}

impl TskvAdminRequest {
    pub async fn do_request(&self, node_id: u64) -> CoordinatorResult<Vec<u8>> {
        let result = self.warp_do_request(node_id).await;
        if let Err(err) = &result {
            info!(
                "Call node {}, admin request failed: {:?} {}",
                node_id, self.request, err
            );
        } else {
            info!(
                "Call node {}, admin request success: {:?} ",
                node_id, self.request
            );
        }

        result
    }

    async fn warp_do_request(&self, node_id: u64) -> CoordinatorResult<Vec<u8>> {
        let channel = self.meta.get_node_conn(node_id).await.map_err(|error| {
            CoordinatorError::PreExecution {
                error: error.to_string(),
            }
        })?;
        let mut client = tskv_service_time_out_client(
            channel,
            self.timeout,
            DEFAULT_GRPC_SERVER_MESSAGE_LEN,
            self.enable_gzip,
        );

        info!("Call node {}, admin request: {:?}", node_id, self.request);
        let request = tonic::Request::new(self.request.clone());
        let response = client.admin_request(request).await?.into_inner();

        decode_grpc_response(response)
    }
}

#[async_trait::async_trait]
impl TskvLeaderCaller for TskvAdminRequest {
    async fn call(&self, _replica: &ReplicationSet, node_id: u64) -> CoordinatorResult<Vec<u8>> {
        self.do_request(node_id).await
    }
}

pub struct TskvLeaderExecutor {
    pub meta: MetaRef,
}

impl TskvLeaderExecutor {
    pub async fn do_request(
        &self,
        tenant: &str,
        replica: &ReplicationSet,
        caller: &impl TskvLeaderCaller,
    ) -> CoordinatorResult<Vec<u8>> {
        let leader_id = replica.leader_node_id;
        let mut vnode_list = replica.vnodes.clone();
        vnode_list.sort_by_key(|vnode| if vnode.node_id == leader_id { 0 } else { 1 });
        let mut node_list = VecDeque::from(vnode_list);

        let mut result = Err(CoordinatorError::NoValidReplica { id: replica.id });
        loop {
            let vnode = if let Some(vnode) = node_list.pop_front() {
                vnode
            } else {
                return result;
            };

            result = caller.call(replica, vnode.node_id).await;
            if let Err(CoordinatorError::PreExecution { .. }) = &result {
                continue;
            } else if let Err(CoordinatorError::RaftForwardToLeader {
                replica_id: _,
                leader_vnode_id: new_leader,
            }) = &result
            {
                result = self
                    .redirect_request(tenant, replica, *new_leader, caller)
                    .await;
                if let Ok(data) = result {
                    return Ok(data);
                }
            } else if let Ok(data) = result {
                if vnode.node_id != leader_id {
                    let _ = self.update_new_leader_to_meta(tenant, vnode.id).await;
                }
                return Ok(data);
            } else {
                return result;
            }
        }
    }

    async fn redirect_request(
        &self,
        tenant: &str,
        replica: &ReplicationSet,
        new_leader: VnodeId,
        caller: &impl TskvLeaderCaller,
    ) -> CoordinatorResult<Vec<u8>> {
        let new_leader_node_id = self.update_new_leader_to_meta(tenant, new_leader).await?;
        info!(
            "Redirect replica: {} request to new_leader(node: {}, vnode id: {})",
            replica.id, new_leader_node_id, new_leader
        );

        caller.call(replica, new_leader_node_id).await
    }

    async fn update_new_leader_to_meta(
        &self,
        tenant: &str,
        new_leader: VnodeId,
    ) -> CoordinatorResult<NodeId> {
        let new_leader_node_id = self
            .meta
            .tenant_meta(tenant)
            .await
            .ok_or_else(|| CoordinatorError::TenantNotFound {
                name: tenant.to_string(),
            })?
            .replica_new_leader(new_leader)
            .await
            .context(MetaSnafu)?;

        Ok(new_leader_node_id)
    }
}
