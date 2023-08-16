use std::sync::Arc;

use models::auth::role::{SystemTenantRole, TenantRoleIdentifier};
use models::auth::user::{UserDesc, UserOptionsBuilder};
use models::oid::{Identifier, UuidGenerator};
use models::schema::{Tenant, TenantOptionsBuilder};
use replication::raft_node::RaftNode;
use tracing::error;

use crate::error::MetaResult;
use crate::store::command::{ReadCommand, WriteCommand};
use crate::store::config::MetaInit;
use crate::store::key_path::KeyPath;
use crate::store::storage::StateMachine;

pub async fn init_meta(node: Arc<RaftNode>, storage: Arc<StateMachine>, init_data: Arc<MetaInit>) {
    // init user
    let user_opt_res = UserOptionsBuilder::default()
        .must_change_password(true)
        .comment("system admin")
        .build();
    let user_opt = if user_opt_res.is_err() {
        error!(
            "failed init admin user {}, exit init meta",
            init_data.admin_user
        );
        return;
    } else {
        user_opt_res.unwrap()
    };
    let oid = UuidGenerator::default().next_id();
    let user_desc = UserDesc::new(oid, init_data.admin_user.clone(), user_opt, true);
    let req = WriteCommand::CreateUser(init_data.cluster_name.clone(), user_desc);
    let data = serde_json::to_vec(&req).unwrap();
    if node.raw_raft().client_write(data).await.is_err() {
        error!(
            "failed init admin user {}, exit init meta",
            init_data.admin_user
        );
        return;
    }

    // init tenant
    let tenant_opt = TenantOptionsBuilder::default()
        .comment("system tenant")
        .build()
        .expect("failed to init system tenant.");
    let oid = UuidGenerator::default().next_id();
    let tenant = Tenant::new(oid, init_data.system_tenant.clone(), tenant_opt);
    let req = WriteCommand::CreateTenant(init_data.cluster_name.clone(), tenant);
    let data = serde_json::to_vec(&req).unwrap();
    if node.raw_raft().client_write(data).await.is_err() {
        error!(
            "failed init system tenant {}, exit init meta",
            init_data.system_tenant
        );
        return;
    }

    // init role
    let req = ReadCommand::User(
        init_data.cluster_name.clone(),
        init_data.admin_user.to_string(),
    );

    let user =
        serde_json::from_str::<MetaResult<Option<UserDesc>>>(&storage.process_read_command(&req))
            .unwrap()
            .unwrap();

    if let Some(user_desc) = user {
        let role = TenantRoleIdentifier::System(SystemTenantRole::Owner);
        let req = WriteCommand::AddMemberToTenant(
            init_data.cluster_name.clone(),
            *user_desc.id(),
            role,
            init_data.system_tenant.to_string(),
        );
        let data = serde_json::to_vec(&req).unwrap();
        if node.raw_raft().client_write(data).await.is_err() {
            error!(
                "failed add admin user {} to system tenant {}, exist init meta",
                init_data.admin_user, init_data.system_tenant
            );
            return;
        }
    }

    // init database
    for db in init_data.default_database.iter() {
        let req = WriteCommand::Set {
            key: KeyPath::tenant_db_name(&init_data.cluster_name, &init_data.system_tenant, db),
            value: MetaInit::default_db_config(&init_data.system_tenant, db),
        };

        let data = serde_json::to_vec(&req).unwrap();
        if node.raw_raft().client_write(data).await.is_err() {
            error!("failed create default database {}, exist init meta", db);
        }
    }
}
