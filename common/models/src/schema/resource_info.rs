use std::fmt::{self};

use serde::{Deserialize, Serialize};

use crate::meta_data::{NodeId, ReplicationSet};
use crate::oid::Oid;
use crate::schema::tskv_table_schema::{TableColumn, TskvTableSchema};
use crate::schema::utils::Duration;
use crate::utils::now_timestamp_nanos;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum ResourceOperator {
    // tenant_name
    DropTenant(String),

    // tenant_name, db_name
    DropDatabase(String, String),

    // tenant_name, db_name, table_name
    DropTable(String, String, String),

    // table_schema, new_table_column
    AddColumn(TskvTableSchema, TableColumn),

    // drop_table_column_name, table_schema
    DropColumn(String, TskvTableSchema),

    // column_name, table_schema, new_table_column
    AlterColumn(String, TskvTableSchema, TableColumn),

    // tenant_name, db_name, new_tags_vec, series_keys, shards
    UpdateTagValue(
        String,
        String,
        Vec<(Vec<u8>, Option<Vec<u8>>)>,
        Vec<Vec<u8>>,
        Vec<ReplicationSet>,
    ),
}

impl fmt::Display for ResourceOperator {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            ResourceOperator::DropTenant(..) => write!(f, "DropTenant"),
            ResourceOperator::DropDatabase(..) => write!(f, "DropDatabase"),
            ResourceOperator::DropTable(..) => write!(f, "DropTable"),
            ResourceOperator::DropColumn(..) => write!(f, "DropColumn"),
            ResourceOperator::AddColumn(..) => write!(f, "AddColumn"),
            ResourceOperator::AlterColumn(..) => write!(f, "AlterColumn"),
            ResourceOperator::UpdateTagValue(..) => write!(f, "UpdateTagValue"),
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub enum ResourceStatus {
    Schedule,
    Executing,
    Successed,
    Failed,
    Cancel,
    Fatal,
}

impl fmt::Display for ResourceStatus {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            ResourceStatus::Schedule => write!(f, "Schedule"),
            ResourceStatus::Executing => write!(f, "Executing"),
            ResourceStatus::Successed => write!(f, "Successed"),
            ResourceStatus::Failed => write!(f, "Failed"),
            ResourceStatus::Cancel => write!(f, "Cancel"),
            ResourceStatus::Fatal => write!(f, "Fatal"),
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ResourceInfo {
    time: i64,
    tenant_id_and_db: (Oid, String),
    name: String,
    operator: ResourceOperator,
    try_count: u64,
    after: Option<Duration>,
    // None means now
    status: ResourceStatus,
    comment: String,
    execute_node_id: NodeId,
    is_new_add: bool,
}

impl ResourceInfo {
    pub fn new(
        tenant_id_and_db: (Oid, String),
        name: String,
        operator: ResourceOperator,
        after: &Option<Duration>,
        execute_node_id: NodeId,
    ) -> Self {
        let mut res_info = ResourceInfo {
            time: now_timestamp_nanos(),
            tenant_id_and_db,
            name,
            operator,
            try_count: 0,
            after: after.clone(),
            status: ResourceStatus::Executing,
            comment: String::default(),
            execute_node_id,
            is_new_add: true,
        };
        if let Some(after) = after {
            let after_nanos = after.to_nanoseconds();
            if after_nanos > 0 {
                res_info.status = ResourceStatus::Schedule;
                res_info.time += after_nanos;
            }
        }
        res_info
    }

    pub fn get_time(&self) -> i64 {
        self.time
    }

    pub fn get_tenant_id_and_db(&self) -> &(Oid, String) {
        &self.tenant_id_and_db
    }

    pub fn get_name(&self) -> &str {
        &self.name
    }

    pub fn get_operator(&self) -> &ResourceOperator {
        &self.operator
    }

    pub fn get_try_count(&self) -> u64 {
        self.try_count
    }

    pub fn get_status(&self) -> &ResourceStatus {
        &self.status
    }

    pub fn get_comment(&self) -> &String {
        &self.comment
    }

    pub fn get_execute_node_id(&self) -> &NodeId {
        &self.execute_node_id
    }

    pub fn get_is_new_add(&self) -> bool {
        self.is_new_add
    }

    pub fn increase_try_count(&mut self) {
        self.try_count += 1;
    }

    pub fn set_status(&mut self, status: ResourceStatus) {
        self.status = status;
    }

    pub fn set_comment(&mut self, comment: &str) {
        self.comment = comment.to_string();
    }

    pub fn set_execute_node_id(&mut self, execute_node_id: NodeId) {
        self.execute_node_id = execute_node_id;
    }

    pub fn set_is_new_add(&mut self, is_new_add: bool) {
        self.is_new_add = is_new_add;
    }
}
