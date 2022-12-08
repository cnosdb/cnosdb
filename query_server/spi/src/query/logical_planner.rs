use crate::service::protocol::QueryId;

use super::{
    ast::{parse_bool_value, parse_string_value, ExtStatement},
    session::IsiphoSessionCtx,
    AFFECTED_ROWS,
};

use datafusion::{
    error::DataFusionError,
    logical_expr::{AggregateFunction, CreateExternalTable, LogicalPlan as DFPlan},
    prelude::{col, Expr},
    sql::sqlparser::ast::{Ident, ObjectName, SqlOption},
};
use meta::meta_client::MetaError;
use models::{
    auth::{
        privilege::{DatabasePrivilege, Privilege},
        role::{SystemTenantRole, TenantRoleIdentifier},
        user::{UserOptions, UserOptionsBuilder},
    },
    oid::Oid,
    schema::{DatabaseOptions, TenantOptions, TenantOptionsBuilder},
};
use models::{define_result, schema::TableColumn};
use snafu::Snafu;

define_result!(LogicalPlannerError);

pub const MISSING_COLUMN: &str = "Insert column name does not exist in target table: ";
pub const DUPLICATE_COLUMN_NAME: &str = "Insert column name is specified more than once: ";
pub const MISMATCHED_COLUMNS: &str = "Insert columns and Source columns not match";

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum LogicalPlannerError {
    #[snafu(display("External err: {}", source))]
    External { source: DataFusionError },

    #[snafu(display("Semantic err: {}", err))]
    Semantic { err: String },

    #[snafu(display("Insufficient privileges, expected [{}]", privilege))]
    InsufficientPrivileges { privilege: String },

    #[snafu(display("Metadata err: {}", source))]
    Metadata { source: MetaError },

    #[snafu(display("This feature is not implemented: {}", err))]
    NotImplemented { err: String },
}

#[derive(Clone)]
pub struct PlanWithPrivileges {
    pub plan: Plan,
    pub privileges: Vec<Privilege<Oid>>,
}

#[derive(Clone)]
pub enum Plan {
    /// Query plan
    Query(QueryPlan),
    /// Query plan
    DDL(DDLPlan),
    /// Query plan
    SYSTEM(SYSPlan),
}

#[derive(Debug, Clone)]
pub struct QueryPlan {
    pub df_plan: DFPlan,
}

#[derive(Clone)]
pub enum DDLPlan {
    // e.g. drop table
    DropDatabaseObject(DropDatabaseObject),
    // e.g. drop user/tenant
    DropGlobalObject(DropGlobalObject),
    // e.g. drop database/role
    DropTenantObject(DropTenantObject),

    /// Create external table. such as parquet\csv...
    CreateExternalTable(CreateExternalTable),

    CreateTable(CreateTable),

    CreateDatabase(CreateDatabase),

    CreateTenant(CreateTenant),

    CreateUser(CreateUser),

    CreateRole(CreateRole),

    DescribeTable(DescribeTable),

    DescribeDatabase(DescribeDatabase),

    ShowTables(Option<String>),

    ShowDatabases(),

    AlterDatabase(AlterDatabase),

    AlterTable(AlterTable),

    AlterTenant(AlterTenant),

    AlterUser(AlterUser),

    GrantRevoke(GrantRevoke),
}

#[derive(Debug, Clone)]
pub enum SYSPlan {
    ShowQueries,
    KillQuery(QueryId),
}

#[derive(Debug, Clone)]
pub struct DropDatabaseObject {
    pub tenant_id: Oid,
    /// object name
    /// e.g. database_name.table_name
    pub object_name: String,
    /// If exists
    pub if_exist: bool,
    ///ObjectType
    pub obj_type: DatabaseObjectType,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DatabaseObjectType {
    Table,
}

#[derive(Debug, Clone)]
pub struct DropTenantObject {
    pub tenant_id: Oid,
    pub name: String,
    pub if_exist: bool,
    pub obj_type: TenantObjectType,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TenantObjectType {
    Role,
    Database,
}

#[derive(Debug, Clone)]
pub struct DropGlobalObject {
    pub name: String,
    pub if_exist: bool,
    pub obj_type: GlobalObjectType,
}

#[derive(Debug, Clone)]
pub enum GlobalObjectType {
    User,
    Tenant,
}

// #[derive(Debug, Clone)]
// pub struct CreateExternalTable {
//     /// The table schema
//     pub schema: DFSchemaRef,
//     /// The table name
//     pub name: String,
//     /// The physical location
//     pub location: String,
//     /// The file type of physical file
//     pub file_descriptor: FileDescriptor,
//     /// Partition Columns
//     pub table_partition_cols: Vec<String>,
//     /// Option to not error if table already exists
//     pub if_not_exists: bool,
// }

// #[derive(Debug, Clone, Copy, PartialEq, Eq)]
// pub enum FileDescriptor {
//     /// Newline-delimited JSON
//     NdJson,
//     /// Apache Parquet columnar storage
//     Parquet,
//     /// Comma separated values
//     CSV(CSVOptions),
//     /// Avro binary records
//     Avro,
// }

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CSVOptions {
    /// Whether the CSV file contains a header
    pub has_header: bool,
    /// Delimiter for CSV
    pub delimiter: char,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateTable {
    /// The table schema
    pub schema: Vec<TableColumn>,
    /// The table name
    pub name: String,
    /// Option to not error if table already exists
    pub if_not_exists: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateDatabase {
    pub name: String,

    pub if_not_exists: bool,

    pub options: DatabaseOptions,
}

#[derive(Debug, Clone)]
pub struct CreateTenant {
    pub name: String,
    pub if_not_exists: bool,
    pub options: TenantOptions,
}

pub fn sql_options_to_tenant_options(
    options: Vec<SqlOption>,
) -> std::result::Result<TenantOptions, String> {
    let mut builder = TenantOptionsBuilder::default();

    for SqlOption { ref name, value } in options {
        match normalize_ident(name).as_str() {
            "comment" => {
                builder.comment(parse_string_value(value)?);
            }
            _ => return Err(format!("Expected option [comment], found [{}]", name)),
        }
    }

    builder.build().map_err(|e| e.to_string())
}

#[derive(Debug, Clone)]
pub struct CreateUser {
    pub name: String,
    pub if_not_exists: bool,
    pub options: UserOptions,
}

pub fn sql_options_to_user_options(
    with_options: Vec<SqlOption>,
) -> std::result::Result<UserOptions, String> {
    let mut builder = UserOptionsBuilder::default();

    for SqlOption { ref name, value } in with_options {
        match normalize_ident(name).as_str() {
            "password" => {
                builder.password(parse_string_value(value)?);
            }
            "must_change_password" => {
                builder.must_change_password(parse_bool_value(value)?);
            }
            "rsa_public_key" => {
                builder.rsa_public_key(parse_string_value(value)?);
            }
            "comment" => {
                builder.comment(parse_string_value(value)?);
            }
            _ => return Err(format!("Expected option [comment], found [{}]", name)),
        }
    }

    builder.build().map_err(|e| e.to_string())
}

#[derive(Debug, Clone)]
pub struct CreateRole {
    pub tenant_id: Oid,
    pub name: String,
    pub if_not_exists: bool,
    pub inherit_tenant_role: SystemTenantRole,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeDatabase {
    pub database_name: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeTable {
    pub table_name: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShowTables {
    pub database_name: String,
}

#[derive(Debug, Clone)]
pub struct GrantRevoke {
    pub is_grant: bool,
    pub database_privileges: Vec<(DatabasePrivilege, Oid)>,
    pub tenant_id: Oid,
    pub role_name: String,
}

#[derive(Debug, Clone)]
pub struct AlterUser {
    pub user_id: Oid,
    pub alter_user_action: AlterUserAction,
}

#[derive(Debug, Clone)]
pub enum AlterUserAction {
    RenameTo(String),
    Set(UserOptions),
}

#[derive(Debug, Clone)]
pub struct AlterTenant {
    pub tenant_id: Oid,
    pub alter_tenant_action: AlterTenantAction,
}

#[derive(Debug, Clone)]
pub enum AlterTenantAction {
    AddUser(AlterTenantAddUser),
    SetUser(AlterTenantSetUser),
    RemoveUser(Oid),
    Set(TenantOptions),
}

#[derive(Debug, Clone)]
pub struct AlterTenantAddUser {
    pub user_id: Oid,
    pub role: TenantRoleIdentifier,
    pub tenant_id: Oid,
}

#[derive(Debug, Clone)]
pub struct AlterTenantSetUser {
    pub user_id: Oid,
    pub role: TenantRoleIdentifier,
    pub tenant_id: Oid,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterDatabase {
    pub database_name: String,
    pub database_options: DatabaseOptions,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterTable {
    pub table_name: String,
    pub alter_action: AlterTableAction,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AlterTableAction {
    AddColumn {
        table_column: TableColumn,
    },
    AlterColumn {
        column_name: String,
        new_column: TableColumn,
    },
    DropColumn {
        column_name: String,
    },
}

pub trait LogicalPlanner {
    fn create_logical_plan(
        &self,
        statement: ExtStatement,
        session: &IsiphoSessionCtx,
    ) -> Result<Plan>;
}

/// TODO Additional output information
pub fn affected_row_expr(arg: Expr) -> Expr {
    // col(AFFECTED_ROWS.0)
    Expr::AggregateFunction {
        fun: AggregateFunction::Count,
        args: vec![arg],
        distinct: false,
        filter: None,
    }
    .alias(AFFECTED_ROWS.0)
}

pub fn merge_affected_row_expr() -> Expr {
    // lit(ScalarValue::Null).alias("COUNT")
    Expr::AggregateFunction {
        fun: AggregateFunction::Sum,
        args: vec![col(AFFECTED_ROWS.0)],
        distinct: false,
        filter: None,
    }
    .alias(AFFECTED_ROWS.0)
}

/// Normalize a SQL object name
pub fn normalize_sql_object_name(sql_object_name: &ObjectName) -> String {
    sql_object_name
        .0
        .iter()
        .map(normalize_ident)
        .collect::<Vec<String>>()
        .join(".")
}

// Normalize an identifier to a lowercase string unless the identifier is quoted.
pub fn normalize_ident(id: &Ident) -> String {
    match id.quote_style {
        Some(_) => id.value.clone(),
        None => id.value.to_ascii_lowercase(),
    }
}
