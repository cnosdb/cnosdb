use macros::EnvKeys;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, EnvKeys)]
pub struct MetaInit {
    pub cluster_name: String,
    pub admin_user: String,
    pub admin_pwd: String,
    pub system_tenant: String,
    pub default_database: Vec<String>,
}

impl Default for MetaInit {
    fn default() -> Self {
        Self {
            cluster_name: String::from("cluster_xxx"),
            admin_user: String::from("root"),
            admin_pwd: String::from("root"),
            system_tenant: String::from("cnosdb"),
            default_database: vec![String::from("public"), String::from("usage_schema")],
        }
    }
}

impl MetaInit {
    pub fn default_db_config(tenant: &str, db: &str) -> String {
        format!(
            "{{\"tenant\":\"{}\",\"database\":\"{}\",\"config\":{{\"ttl\":null,\"shard_num\":null,\"vnode_duration\":null,\"replica\":null,\"precision\":null,\"db_is_hidden\":false}}}}",
            tenant, db
        )
    }
}
