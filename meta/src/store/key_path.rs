use models::oid::Oid;

// **    /cluster_name/users ->
// **    /cluster_name/users/user ->
// **    /cluster_name/tenants/tenant ->
// **    /cluster_name/tenants/tenant/roles/roles ->
// **    /cluster_name/tenants/tenant/members/user_id ->
// **    /cluster_name/tenants/tenant/limiter ->
// **    /cluster_name/auto_incr_id -> id
// **    /cluster_name/data_nodes/node_id -> [NodeInfo] 集群、数据节点等信息

// **    /cluster_name/tenant_name/dbs/db_name -> [DatabaseInfo] db相关信息、保留策略等
// **    /cluster_name/tenant_name/dbs/db_name/buckets/id -> [BucketInfo] bucket相关信息
// **    /cluster_name/tenant_name/dbs/db_name/schemas/name -> [TskvTableSchema] schema相关信息

pub const DBS: &str = "dbs";
pub const USERS: &str = "users";
pub const ROLES: &str = "roles";
pub const BUCKETS: &str = "buckets";
pub const SCHEMAS: &str = "schemas";
pub const TENANTS: &str = "tenants";
pub const MEMBERS: &str = "members";
pub const DATA_NODES: &str = "data_nodes";
pub const AUTO_INCR_ID: &str = "auto_incr_id";
pub const LIMITER: &str = "limiter";

pub struct KeyPath {}

impl KeyPath {
    pub fn cluster_prefix(cluster: &str) -> String {
        format!("/{}/", cluster)
    }

    pub fn users(cluster: &str) -> String {
        format!("/{}/users", cluster)
    }

    pub fn user(cluster: &str, user: &str) -> String {
        format!("/{}/users/{}", cluster, user)
    }
    pub fn incr_id(cluster: &str) -> String {
        format!("/{}/auto_incr_id", cluster)
    }

    pub fn version() -> String {
        "/data_version".to_string()
    }

    pub fn already_init() -> String {
        "/already_init_key".to_string()
    }

    pub fn data_nodes(cluster: &str) -> String {
        format!("/{}/data_nodes", cluster)
    }

    pub fn data_node_id(cluster: &str, id: u64) -> String {
        format!("/{}/data_nodes/{}", cluster, id)
    }

    pub fn data_nodes_metrics(cluster: &str) -> String {
        format!("/{}/data_nodes_metrics", cluster)
    }

    pub fn data_node_metrics(cluster: &str, id: u64) -> String {
        format!("/{}/data_nodes_metrics/{}", cluster, id)
    }

    pub fn tenant_dbs(cluster: &str, tenant: &str) -> String {
        format!("/{}/tenants/{}/dbs", cluster, tenant)
    }

    // pub fn tenant_version(cluster: &str, tenant: &str) -> String {
    //     format!("/{}/{}/version", cluster, tenant)
    // }

    pub fn tenant_db_name(cluster: &str, tenant: &str, db: &str) -> String {
        format!("/{}/tenants/{}/dbs/{}", cluster, tenant, db)
    }

    pub fn tenant_db_buckets(cluster: &str, tenant: &str, db: &str) -> String {
        format!("/{}/tenants/{}/dbs/{}/buckets", cluster, tenant, db)
    }

    pub fn tenant_bucket_id(cluster: &str, tenant: &str, db: &str, id: u32) -> String {
        format!("/{}/tenants/{}/dbs/{}/buckets/{}", cluster, tenant, db, id)
    }

    pub fn tenant_schemas(cluster: &str, tenant: &str, db: &str) -> String {
        format!("/{}/tenants/{}/dbs/{}/schemas", cluster, tenant, db)
    }

    pub fn tenant_schema_name(cluster: &str, tenant: &str, db: &str, name: &str) -> String {
        format!(
            "/{}/tenants/{}/dbs/{}/schemas/{}",
            cluster, tenant, db, name
        )
    }

    pub fn tenants(cluster: &str) -> String {
        format!("/{}/tenants/", cluster)
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

    pub fn limiter(cluster: &str, tenant_name: &str) -> String {
        format!("/{cluster}/tenants/{tenant_name}/limiter")
    }
}
