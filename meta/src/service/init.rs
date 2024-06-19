use config::meta::MetaInit;
use models::auth::user::{UserDesc, UserOptionsBuilder};
use models::schema::tenant::{Tenant, TenantOptionsBuilder};
use replication::{ApplyContext, ApplyStorage, APPLY_TYPE_WRITE};

use crate::store::command::WriteCommand;
use crate::store::key_path::KeyPath;
use crate::store::storage::StateMachine;

pub async fn init_meta(storage: &mut StateMachine, init_data: MetaInit) {
    if storage.is_meta_init().unwrap() {
        return;
    }

    let ctx = ApplyContext {
        index: 0,
        raft_id: 0,
        apply_type: APPLY_TYPE_WRITE,
    };

    // init tenant
    let tenant_opt = TenantOptionsBuilder::default()
        .comment("system tenant")
        .build()
        .expect("failed to init system tenant.");
    let oid = 78322384368497284380257291774744000001;
    let tenant = Tenant::new(oid, init_data.system_tenant.clone(), tenant_opt);
    let req = WriteCommand::CreateTenant(init_data.cluster_name.clone(), tenant);
    let data = serde_json::to_vec(&req).unwrap();
    storage.apply(&ctx, &data).await.expect("expect success");

    // init user
    let user_opt = UserOptionsBuilder::default()
        .must_change_password(true)
        .password(init_data.admin_pwd)
        .expect("failed to init user option.")
        .comment("system admin")
        .build()
        .expect("failed to init user option.");
    let oid = 78322384368497284380257291774744000002;
    let user_desc = UserDesc::new(oid, init_data.admin_user.clone(), user_opt, true);
    let req = WriteCommand::CreateUser(init_data.cluster_name.clone(), user_desc.clone());
    let data = serde_json::to_vec(&req).unwrap();
    storage.apply(&ctx, &data).await.expect("expect success");

    // init database
    for db in init_data.default_database.iter() {
        let req = WriteCommand::Set {
            key: KeyPath::tenant_db_name(&init_data.cluster_name, &init_data.system_tenant, db),
            value: MetaInit::default_db_config(&init_data.system_tenant, db),
        };

        let data = serde_json::to_vec(&req).unwrap();
        storage.apply(&ctx, &data).await.expect("expect success");
    }

    storage.set_already_init().unwrap();
}
