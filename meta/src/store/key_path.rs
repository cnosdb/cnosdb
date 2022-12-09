use models::oid::Oid;

// **    /cluster_name/auto_incr_id -> id
// **    /cluster_name/data_nodes/node_id -> [NodeInfo] 集群、数据节点等信息
// **    /cluster_name/tenant_name/users/name -> [UserInfo] 租户下用户信息、访问权限等
// **    /cluster_name/tenant_name/dbs/db_name -> [DatabaseInfo] db相关信息、保留策略等
// **    /cluster_name/tenant_name/dbs/db_name/buckets/id -> [BucketInfo] bucket相关信息
// **    /cluster_name/tenant_name/dbs/db_name/schemas/name -> [TskvTableSchema] schema相关信息
pub struct KeyPath {}

impl KeyPath {
    pub fn incr_id(cluster: &str) -> String {
        format!("/{}/auto_incr_id", cluster)
    }

    pub fn data_nodes(cluster: &str) -> String {
        format!("/{}/data_nodes", cluster)
    }

    pub fn data_node_id(cluster: &str, id: u64) -> String {
        format!("/{}/data_nodes/{}", cluster, id)
    }

    pub fn tenant_users(cluster: &str, tenant: &str) -> String {
        format!("/{}/{}/users", cluster, tenant)
    }

    // pub fn tenant_user_name(cluster: &str, tenant: &str, name: &str) -> String {
    //     format!("/{}/{}/users/{}", cluster, tenant, name)
    // }

    pub fn tenant_dbs(cluster: &str, tenant: &str) -> String {
        format!("/{}/{}/dbs", cluster, tenant)
    }

    // pub fn tenant_version(cluster: &str, tenant: &str) -> String {
    //     format!("/{}/{}/version", cluster, tenant)
    // }

    pub fn tenant_db_name(cluster: &str, tenant: &str, db: &str) -> String {
        format!("/{}/{}/dbs/{}", cluster, tenant, db)
    }

    pub fn tenant_db_buckets(cluster: &str, tenant: &str, db: &str) -> String {
        format!("/{}/{}/dbs/{}/buckets", cluster, tenant, db)
    }

    pub fn tenant_bucket_id(cluster: &str, tenant: &str, db: &str, id: u32) -> String {
        format!("/{}/{}/dbs/{}/buckets/{}", cluster, tenant, db, id)
    }

    pub fn tenant_schemas(cluster: &str, tenant: &str, db: &str) -> String {
        format!("/{}/{}/dbs/{}/schemas", cluster, tenant, db)
    }

    pub fn tenant_schema_name(cluster: &str, tenant: &str, db: &str, name: &str) -> String {
        format!("/{}/{}/dbs/{}/schemas/{}", cluster, tenant, db, name)
    }

    pub fn users(cluster: &str) -> String {
        format!("/{}/users", cluster)
    }

    pub fn user(cluster: &str, user: &str) -> String {
        format!("/{}/users/{}", cluster, user)
    }

    pub fn tenants(cluster: &str) -> String {
        format!("/{}/tenants", cluster)
    }

    pub fn tenant(cluster: &str, name: &str) -> String {
        format!("/{}/tenants/{}", cluster, name)
    }

    pub fn role(cluster: &str, tenant_name: &str, role_name: &str) -> String {
        format!("/{}/tenants/{}/roles/{}", cluster, tenant_name, role_name)
    }

    pub fn roles(cluster: &str, tenant_name: &str) -> String {
        format!("/{}/tenants/{}/roles", cluster, tenant_name)
    }

    pub fn member(cluster: &str, tenant_name: &str, user_id: &Oid) -> String {
        format!("/{}/tenants/{}/members/{}", cluster, tenant_name, user_id)
    }

    pub fn members(cluster: &str, tenant_name: &str) -> String {
        format!("/{}/tenants/{}/members", cluster, tenant_name)
    }
}

