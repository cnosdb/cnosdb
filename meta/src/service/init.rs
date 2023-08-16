use std::sync::Arc;

use models::auth::role::{SystemTenantRole, TenantRoleIdentifier};
use models::auth::user::{UserDesc, UserOptionsBuilder};
use models::oid::Identifier;
use models::schema::{Tenant, TenantOptionsBuilder};
use replication::apply_store::ApplyStorage;

use crate::store::command::WriteCommand;
use crate::store::config::MetaInit;
use crate::store::key_path::KeyPath;
use crate::store::storage::StateMachine;

pub async fn init_meta(storage: Arc<StateMachine>, init_data: MetaInit) {
    // init tenant
    let tenant_opt = TenantOptionsBuilder::default()
        .comment("system tenant")
        .build()
        .expect("failed to init system tenant.");
    let oid = 78322384368497284380257291774744000001;
    let tenant = Tenant::new(oid, init_data.system_tenant.clone(), tenant_opt);
    let req = WriteCommand::CreateTenant(init_data.cluster_name.clone(), tenant);
    let data = serde_json::to_vec(&req).unwrap();
    storage.apply(&data).await.expect("init expect success");

    // init user
    let user_opt = UserOptionsBuilder::default()
        .must_change_password(true)
        .comment("system admin")
        .build()
        .expect("failed to init user option.");
    let oid = 78322384368497284380257291774744000002;
    let user_desc = UserDesc::new(oid, init_data.admin_user.clone(), user_opt, true);
    let req = WriteCommand::CreateUser(init_data.cluster_name.clone(), user_desc.clone());
    let data = serde_json::to_vec(&req).unwrap();
    storage.apply(&data).await.expect("init expect success");

    // init role
    let role = TenantRoleIdentifier::System(SystemTenantRole::Owner);
    let req = WriteCommand::AddMemberToTenant(
        init_data.cluster_name.clone(),
        *user_desc.id(),
        role,
        init_data.system_tenant.to_string(),
    );
    let data = serde_json::to_vec(&req).unwrap();
    storage.apply(&data).await.expect("init expect success");

    // init database
    for db in init_data.default_database.iter() {
        let req = WriteCommand::Set {
            key: KeyPath::tenant_db_name(&init_data.cluster_name, &init_data.system_tenant, db),
            value: MetaInit::default_db_config(&init_data.system_tenant, db),
        };

        let data = serde_json::to_vec(&req).unwrap();
        storage.apply(&data).await.expect("init expect success");
    }
}