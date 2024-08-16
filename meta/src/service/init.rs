use models::auth::user::{UserDesc, UserOptionsBuilder};
use models::schema::database_schema::{DatabaseConfig, DatabaseOptions, DatabaseSchema};
use models::schema::tenant::{Tenant, TenantOptionsBuilder};
use replication::{ApplyContext, ApplyStorage, APPLY_TYPE_WRITE};
use serde::{Deserialize, Serialize};

use crate::store::command::WriteCommand;
use crate::store::key_path::KeyPath;
use crate::store::storage::{value_encode, StateMachine};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MetaInit {
    pub cluster_name: String,
    pub admin_user: String,
    pub admin_pwd: String,
    pub system_tenant: String,
    pub default_database: Vec<(String, DatabaseConfig)>,
}

impl MetaInit {
    pub fn new(
        cluster_name: String,
        admin_user: String,
        admin_pwd: String,
        system_tenant: String,
        default_database: Vec<(String, DatabaseConfig)>,
    ) -> Self {
        Self {
            cluster_name,
            admin_user,
            admin_pwd,
            system_tenant,
            default_database,
        }
    }

    pub async fn init_meta(self, storage: &mut StateMachine) {
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
        let tenant = Tenant::new(oid, self.system_tenant.clone(), tenant_opt);
        let req = WriteCommand::CreateTenant(self.cluster_name.clone(), tenant);
        let data = serde_json::to_vec(&req).unwrap();
        storage.apply(&ctx, &data).await.expect("expect success");

        // init user
        let user_opt = UserOptionsBuilder::default()
            .must_change_password(true)
            .password(self.admin_pwd)
            .expect("failed to init user option.")
            .comment("system admin")
            .build()
            .expect("failed to init user option.");
        let oid = 78322384368497284380257291774744000002;
        let user_desc = UserDesc::new(oid, self.admin_user.clone(), user_opt, true);
        let req = WriteCommand::CreateUser(self.cluster_name.clone(), user_desc.clone());
        let data = serde_json::to_vec(&req).unwrap();
        storage.apply(&ctx, &data).await.expect("expect success");

        // init database
        for (db, config) in self.default_database.into_iter() {
            let req = WriteCommand::Set {
                key: KeyPath::tenant_db_name(&self.cluster_name, &self.system_tenant, &db),
                value: Self::db_config_schema_init(&self.system_tenant, &db, config),
            };

            let data = serde_json::to_vec(&req).unwrap();
            storage.apply(&ctx, &data).await.expect("expect success");
        }

        storage.set_already_init().unwrap();
    }

    fn db_config_schema_init(tenant: &str, db: &str, config: DatabaseConfig) -> String {
        let mut db_opt = DatabaseOptions::default();
        db_opt.set_replica(config.replica());
        let db_schema = DatabaseSchema::new(tenant, db, db_opt, config.into());
        value_encode(&db_schema).unwrap()
    }
}
