use actix_web::web::Data;
use models::auth::role::{SystemTenantRole, TenantRoleIdentifier};
use models::auth::user::{UserDesc, UserOptionsBuilder};
use models::oid::Identifier;
use models::schema::TenantOptionsBuilder;
use tracing::error;

use crate::error::MetaResult;
use crate::store::command::ReadCommand;
use crate::store::config::MetaInit;
use crate::store::key_path::KeyPath;
use crate::{MetaApp, WriteCommand};

pub mod api;
pub mod connection;
pub mod raft_api;

pub async fn init_meta(app: &Data<MetaApp>) {
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
    let user =
        serde_json::from_str::<MetaResult<Option<UserDesc>>>(&sm_r.process_read_command(&req))
            .unwrap()
            .unwrap();
    drop(sm_r);

    if let Some(user_desc) = user {
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
