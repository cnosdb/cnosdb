use std::collections::{BTreeSet, HashSet};

use actix_web::web::Json;
use models::auth::role::{SystemTenantRole, TenantRoleIdentifier};
use models::auth::user::{UserDesc, UserOptionsBuilder};
use models::oid::Identifier;
use models::schema::TenantOptionsBuilder;
use openraft::async_trait::async_trait;
use openraft::error::{
    AddLearnerError, AppendEntriesError, ClientWriteError, Infallible, InitializeError,
    InstallSnapshotError, VoteError,
};
use openraft::raft::{
    AddLearnerResponse, AppendEntriesRequest, AppendEntriesResponse, ClientWriteResponse,
    InstallSnapshotRequest, InstallSnapshotResponse, VoteRequest, VoteResponse,
};
use openraft::RaftMetrics;
use tracing::error;

use crate::store::command::{CommonResp, ReadCommand};
use crate::store::config::MetaInit;
use crate::store::key_path::KeyPath;
use crate::{ClusterNode, ClusterNodeId, CommandResp, MetaApp, TypeConfig, WriteCommand};

pub mod api;
pub mod connection;
pub mod raft_api;

#[async_trait]
pub trait MetaRaftApi {
    async fn vote(
        &self,
        req: Json<VoteRequest<ClusterNodeId>>,
    ) -> Result<VoteResponse<ClusterNodeId>, VoteError<ClusterNodeId>>;

    async fn append(
        &self,
        req: Json<AppendEntriesRequest<TypeConfig>>,
    ) -> Result<AppendEntriesResponse<ClusterNodeId>, AppendEntriesError<ClusterNodeId>>;

    async fn snapshot(
        &self,
        req: Json<InstallSnapshotRequest<TypeConfig>>,
    ) -> Result<InstallSnapshotResponse<ClusterNodeId>, InstallSnapshotError<ClusterNodeId>>;

    async fn add_learner(
        &self,
        req: Json<(ClusterNodeId, String)>,
    ) -> Result<AddLearnerResponse<ClusterNodeId>, AddLearnerError<ClusterNodeId, ClusterNode>>;

    async fn change_membership(
        &self,
        req: Json<BTreeSet<ClusterNodeId>>,
    ) -> Result<ClientWriteResponse<TypeConfig>, ClientWriteError<ClusterNodeId, ClusterNode>>;

    async fn init(&self) -> Result<(), InitializeError<ClusterNodeId, ClusterNode>>;

    async fn metrics(&self) -> Result<RaftMetrics<ClusterNodeId, ClusterNode>, Infallible>;
}

#[async_trait]
pub trait MetaApi {
    async fn read(&self, req: Json<ReadCommand>) -> Result<CommandResp, Infallible>;
    async fn write(
        &self,
        req: Json<WriteCommand>,
    ) -> Result<ClientWriteResponse<TypeConfig>, ClientWriteError<ClusterNodeId, ClusterNode>>;
    async fn dump(&self) -> String;
    async fn restore(&self, data: String) -> String;
    async fn watch(
        &self,
        req: Json<(String, String, HashSet<String>, u64)>,
    ) -> Result<CommandResp, Infallible>;
    async fn debug(&self) -> String;
    async fn cpu_pprof(&self) -> String;
    async fn backtrace(&self) -> String;
}

pub async fn init_meta(app: &MetaApp) {
    // init user
    let user_opt_res = UserOptionsBuilder::default()
        .must_change_password(true)
        .comment("system admin")
        .build();
    let user_opt = if user_opt_res.is_err() {
        error!(
            "failed init admin user {}, exit init meta",
            app.meta_init.admin_user
        );
        return;
    } else {
        user_opt_res.unwrap()
    };
    let req = WriteCommand::CreateUser(
        app.meta_init.cluster_name.clone(),
        app.meta_init.admin_user.clone(),
        user_opt,
        true,
    );
    if app.raft.client_write(req).await.is_err() {
        error!(
            "failed init admin user {}, exit init meta",
            app.meta_init.admin_user
        );
        return;
    }

    // init tenant
    let tenant_opt = TenantOptionsBuilder::default()
        .comment("system tenant")
        .build()
        .expect("failed to init system tenant.");
    let req = WriteCommand::CreateTenant(
        app.meta_init.cluster_name.clone(),
        app.meta_init.system_tenant.clone(),
        tenant_opt,
    );
    if app.raft.client_write(req).await.is_err() {
        error!(
            "failed init system tenant {}, exit init meta",
            app.meta_init.system_tenant
        );
        return;
    }

    // init role
    let req = ReadCommand::User(
        app.meta_init.cluster_name.clone(),
        app.meta_init.admin_user.to_string(),
    );
    let sm_r = app.store.state_machine.read().await;
    let user_resp =
        serde_json::from_str::<CommonResp<Option<UserDesc>>>(&sm_r.process_read_command(&req));
    drop(sm_r);
    let user = if user_resp.is_err() {
        error!(
            "failed get admin user {}, exit init meta",
            app.meta_init.admin_user
        );
        return;
    } else {
        user_resp.unwrap()
    };
    if let CommonResp::Ok(Some(user_desc)) = user {
        let role = TenantRoleIdentifier::System(SystemTenantRole::Owner);
        let req = WriteCommand::AddMemberToTenant(
            app.meta_init.cluster_name.clone(),
            *user_desc.id(),
            role,
            app.meta_init.system_tenant.to_string(),
        );
        if app.raft.client_write(req).await.is_err() {
            error!(
                "failed add admin user {} to system tenant {}, exist init meta",
                app.meta_init.admin_user, app.meta_init.system_tenant
            );
            return;
        }
    }

    // init database
    for db in app.meta_init.default_database.iter() {
        let req = WriteCommand::Set {
            key: KeyPath::tenant_db_name(
                &app.meta_init.cluster_name,
                &app.meta_init.system_tenant,
                db,
            ),
            value: MetaInit::default_db_config(&app.meta_init.system_tenant, db),
        };
        if app.raft.client_write(req).await.is_err() {
            error!("failed create default database {}, exist init meta", db);
        }
    }
}
