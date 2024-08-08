use std::collections::HashMap;
use std::io::Write;
use std::sync::Arc;

use async_trait::async_trait;
use config::common::TenantLimiterConfig;
use datafusion::arrow::array::ArrayRef;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::datasource::file_format::file_type::{FileCompressionType, FileType};
use datafusion::logical_expr::type_coercion::aggregates::{
    DATES, NUMERICS, STRINGS, TIMES, TIMESTAMPS,
};
use datafusion::logical_expr::{
    expr, expr_fn, CreateExternalTable, LogicalPlan as DFPlan, ReturnTypeFunction, ScalarUDF,
    Signature, Volatility,
};
use datafusion::physical_plan::functions::make_scalar_function;
use datafusion::prelude::{col, Expr};
use datafusion::sql::sqlparser::ast::{Ident, ObjectName, SqlOption, Value};
use datafusion::sql::sqlparser::parser::ParserError;
use lazy_static::lazy_static;
use models::auth::privilege::{DatabasePrivilege, GlobalPrivilege, Privilege};
use models::auth::role::{SystemTenantRole, TenantRoleIdentifier};
use models::auth::user::{UserOptions, UserOptionsBuilder};
use models::meta_data::{NodeId, ReplicationSetId, VnodeId};
use models::object_reference::ResolvedTable;
use models::oid::{Identifier, Oid};
use models::schema::database_schema::{DatabaseConfigBuilder, DatabaseOptionsBuilder};
use models::schema::query_info::QueryId;
use models::schema::stream_table_schema::Watermark;
use models::schema::tenant::{Tenant, TenantOptions, TenantOptionsBuilder};
use models::schema::tskv_table_schema::TableColumn;
use snafu::{IntoError, ResultExt};
use tempfile::NamedTempFile;
use utils::duration::CnosDuration;

use super::ast::{parse_bool_value, parse_char_value, parse_string_value, ExtStatement};
use super::datasource::azure::{AzblobStorageConfig, AzblobStorageConfigBuilder};
use super::datasource::gcs::{
    GcsStorageConfig, ServiceAccountCredentials, ServiceAccountCredentialsBuilder,
};
use super::datasource::s3::{S3StorageConfig, S3StorageConfigBuilder};
use super::datasource::UriSchema;
use super::session::SessionCtx;
use super::AFFECTED_ROWS;
use crate::{
    ParserSnafu, QueryError, QueryResult, SerdeJsonSnafu, StdIoSnafu, TenantOptionsBuildFailSnafu,
};

pub const TENANT_OPTION_LIMITER: &str = "_limiter";
pub const TENANT_OPTION_COMMENT: &str = "comment";
pub const TENANT_OPTION_DROP_AFTER: &str = "drop_after";

lazy_static! {
    static ref TABLE_WRITE_UDF: Arc<ScalarUDF> = Arc::new(ScalarUDF::new(
        "rows",
        &Signature::variadic(
            STRINGS
                .iter()
                .chain(NUMERICS)
                .chain(TIMESTAMPS)
                .chain(DATES)
                .chain(TIMES)
                // .chain(iter::once(str_dict_data_type()))
                .cloned()
                .collect::<Vec<_>>(),
            Volatility::Immutable
        ),
        &(Arc::new(move |_: &[DataType]| Ok(Arc::new(DataType::UInt64))) as ReturnTypeFunction),
        &make_scalar_function(|args: &[ArrayRef]| Ok(Arc::clone(&args[0]))),
    ));
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
    /// Ext DML plan
    DML(DMLPlan),
    /// Query plan
    SYSTEM(SYSPlan),
}

impl Plan {
    pub fn schema(&self) -> SchemaRef {
        match self {
            Self::Query(p) => SchemaRef::from(p.df_plan.schema().as_ref()),
            Self::DDL(p) => p.schema(),
            Self::DML(p) => p.schema(),
            Self::SYSTEM(p) => p.schema(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct QueryPlan {
    pub df_plan: DFPlan,
}

impl QueryPlan {
    pub fn is_explain(&self) -> bool {
        matches!(self.df_plan, DFPlan::Explain(_) | DFPlan::Analyze(_))
    }
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

    CreateStreamTable(CreateStreamTable),

    CreateDatabase(CreateDatabase),

    CreateTenant(Box<CreateTenant>),

    CreateUser(CreateUser),

    CreateRole(CreateRole),

    AlterDatabase(AlterDatabase),

    AlterTable(AlterTable),

    AlterTenant(AlterTenant),

    AlterUser(AlterUser),

    GrantRevoke(GrantRevoke),

    DropVnode(DropVnode),

    CopyVnode(CopyVnode),

    MoveVnode(MoveVnode),

    CompactVnode(CompactVnode),

    ChecksumGroup(ChecksumGroup),

    RecoverDatabase(RecoverDatabase),

    RecoverTenant(RecoverTenant),

    ShowReplicas,

    ReplicaDestory(ReplicaDestory),

    ReplicaAdd(ReplicaAdd),

    ReplicaRemove(ReplicaRemove),

    ReplicaPromote(ReplicaPromote),
}

impl DDLPlan {
    pub fn schema(&self) -> SchemaRef {
        match self {
            DDLPlan::ChecksumGroup(_) => Arc::new(Schema::new(vec![
                Field::new("vnode_id", DataType::UInt32, false),
                Field::new("check_sum", DataType::Utf8, false),
            ])),
            _ => Arc::new(Schema::empty()),
        }
    }
}

#[derive(Debug, Clone)]
pub struct ChecksumGroup {
    pub replication_set_id: ReplicationSetId,
}

#[derive(Debug, Clone)]
pub struct CompactVnode {
    pub vnode_ids: Vec<VnodeId>,
}

#[derive(Debug, Clone)]
pub struct MoveVnode {
    pub vnode_id: VnodeId,
    pub node_id: NodeId,
}

#[derive(Debug, Clone)]
pub struct CopyVnode {
    pub vnode_id: VnodeId,
    pub node_id: NodeId,
}

#[derive(Debug, Clone)]
pub struct DropVnode {
    pub vnode_id: VnodeId,
}

#[derive(Debug, Clone)]
pub enum DMLPlan {
    DeleteFromTable(DeleteFromTable),
}

impl DMLPlan {
    pub fn schema(&self) -> SchemaRef {
        Arc::new(Schema::empty())
    }
}

/// TODO implement UserDefinedLogicalNodeCore
#[derive(Debug, Clone)]
pub struct DeleteFromTable {
    pub table_name: ResolvedTable,
    pub selection: Option<Expr>,
}

#[derive(Debug, Clone)]
pub enum SYSPlan {
    KillQuery(QueryId),
}

impl SYSPlan {
    pub fn schema(&self) -> SchemaRef {
        Arc::new(Schema::empty())
    }
}

#[derive(Debug, Clone)]
pub struct DropDatabaseObject {
    /// object name
    /// e.g. database_name.table_name
    pub object_name: ResolvedTable,
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
    pub tenant_name: String,
    pub name: String,
    pub if_exist: bool,
    pub obj_type: TenantObjectType,
    pub after: Option<CnosDuration>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TenantObjectType {
    Role,
    Database,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DropGlobalObject {
    pub name: String,
    pub if_exist: bool,
    pub obj_type: GlobalObjectType,
    pub after: Option<CnosDuration>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GlobalObjectType {
    User,
    Tenant,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecoverTenant {
    pub tenant_name: String,
    pub if_exist: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecoverDatabase {
    pub tenant_name: String,
    pub db_name: String,
    pub if_exist: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecoverTable {
    pub table: ResolvedTable,
    pub if_exist: bool,
}

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
    pub name: ResolvedTable,
    /// Option to not error if table already exists
    pub if_not_exists: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateStreamTable {
    /// Option to not error if table already exists
    pub if_not_exists: bool,
    /// The table name
    pub name: ResolvedTable,
    pub schema: Schema,
    pub watermark: Watermark,
    pub stream_type: String,
    pub extra_options: HashMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateDatabase {
    pub name: String,

    pub if_not_exists: bool,

    pub options: DatabaseOptionsBuilder,

    pub config: DatabaseConfigBuilder,
}

#[derive(Debug, Clone)]
pub struct CreateTenant {
    pub name: String,
    pub if_not_exists: bool,
    pub options: TenantOptions,
}

#[derive(Debug, Clone)]
pub struct ReplicaDestory {
    pub replica_id: ReplicationSetId,
}

#[derive(Debug, Clone)]
pub struct ReplicaAdd {
    pub replica_id: ReplicationSetId,
    pub node_id: NodeId,
}

#[derive(Debug, Clone)]
pub struct ReplicaRemove {
    pub replica_id: ReplicationSetId,
    pub node_id: NodeId,
}

#[derive(Debug, Clone)]
pub struct ReplicaPromote {
    pub replica_id: ReplicationSetId,
    pub node_id: NodeId,
}

pub fn unset_option_to_alter_tenant_action(
    tenant: Tenant,
    ident: Ident,
) -> QueryResult<(AlterTenantAction, Privilege<Oid>)> {
    let tenant_id = *tenant.id();
    let mut tenant_options_builder = TenantOptionsBuilder::from(tenant.to_own_options());

    let privilege = match normalize_ident(&ident).as_str() {
        TENANT_OPTION_COMMENT => {
            tenant_options_builder.unset_comment();
            Privilege::Global(GlobalPrivilege::Tenant(Some(tenant_id)))
        }
        TENANT_OPTION_LIMITER => {
            tenant_options_builder.unset_limiter_config();
            Privilege::Global(GlobalPrivilege::System)
        }
        TENANT_OPTION_DROP_AFTER => {
            tenant_options_builder.unset_drop_after();
            Privilege::Global(GlobalPrivilege::Tenant(Some(tenant_id)))
        }
        _ => {
            let source = ParserError::ParserError(format!(
                "Expected option [{TENANT_OPTION_COMMENT}], [{TENANT_OPTION_LIMITER}], [{TENANT_OPTION_DROP_AFTER}] found [{}]",
                ident
            ));
            return Err(ParserSnafu.into_error(source));
        }
    };
    let tenant_options = tenant_options_builder
        .build()
        .context(TenantOptionsBuildFailSnafu)?;

    Ok((
        AlterTenantAction::SetOption(Box::new(tenant_options)),
        privilege,
    ))
}

pub fn sql_option_to_alter_tenant_action(
    tenant: Tenant,
    option: SqlOption,
) -> std::result::Result<(AlterTenantAction, Privilege<Oid>), QueryError> {
    let SqlOption { name, value } = option;
    let tenant_id = *tenant.id();
    let mut tenant_options_builder = TenantOptionsBuilder::from(tenant.to_own_options());

    let privilege = match normalize_ident(&name).as_str() {
        TENANT_OPTION_COMMENT => {
            let value = parse_string_value(value).context(ParserSnafu)?;
            tenant_options_builder.comment(value);
            Privilege::Global(GlobalPrivilege::Tenant(Some(tenant_id)))
        }
        TENANT_OPTION_LIMITER => {
            let config =
                serde_json::from_str::<TenantLimiterConfig>(parse_string_value(value).context(ParserSnafu)?.as_str())
                    .map_err(|_| ParserError::ParserError("limiter format error".to_string())).context(ParserSnafu)?;
            tenant_options_builder.limiter_config(config);
            Privilege::Global(GlobalPrivilege::System)
        }
        TENANT_OPTION_DROP_AFTER => {
            let drop_after_str = parse_string_value(value).context(ParserSnafu)?;
            let drop_after = CnosDuration::new(&drop_after_str).ok_or_else(|| QueryError::Parser {
                source: ParserError::ParserError(format!("{} is not a valid duration or duration overflow", drop_after_str)),
            })?;
            tenant_options_builder.drop_after(drop_after);
            Privilege::Global(GlobalPrivilege::Tenant(Some(tenant_id)))
        }
        _ => {
            return Err(QueryError::Parser {
                source: ParserError::ParserError(format!(
                "Expected option [{TENANT_OPTION_COMMENT}], [{TENANT_OPTION_LIMITER}], [{TENANT_OPTION_DROP_AFTER}] found [{}]",
                name
            )),
            })
        }
    };
    let tenant_options = tenant_options_builder
        .build()
        .context(TenantOptionsBuildFailSnafu)?;
    Ok((
        AlterTenantAction::SetOption(Box::new(tenant_options)),
        privilege,
    ))
}

pub fn sql_options_to_tenant_options(options: Vec<SqlOption>) -> QueryResult<TenantOptions> {
    let mut builder = TenantOptionsBuilder::default();

    for SqlOption { ref name, value } in options {
        match normalize_ident(name).as_str() {
            TENANT_OPTION_COMMENT => {
                builder.comment(parse_string_value(value).context(ParserSnafu)?);
            }
            TENANT_OPTION_LIMITER => {
                let config = serde_json::from_str::<TenantLimiterConfig>(
                    parse_string_value(value).context(ParserSnafu)?.as_str(),
                ).context(SerdeJsonSnafu)?;
                builder.limiter_config(config);
            }
            TENANT_OPTION_DROP_AFTER => {
                let drop_after_str = parse_string_value(value).context(ParserSnafu)?;
                let drop_after = CnosDuration::new(&drop_after_str).ok_or_else(|| QueryError::Parser {
                    source: ParserError::ParserError(format!("{} is not a valid duration or duration overflow", drop_after_str)),
                })?;
                builder.drop_after(drop_after);
            }
            _ => {
                return Err(QueryError::Parser {
                    source: ParserError::ParserError(format!(
                        "Expected option [{TENANT_OPTION_COMMENT}], [{TENANT_OPTION_LIMITER}], [{TENANT_OPTION_DROP_AFTER}] found [{}]",
                        name
                    )),
                })
            }
        }
    }

    builder.build().map_err(|e| QueryError::Parser {
        source: ParserError::ParserError(e.to_string()),
    })
}

#[derive(Debug, Clone)]
pub struct CreateUser {
    pub name: String,
    pub if_not_exists: bool,
    pub options: UserOptions,
}

pub fn sql_options_to_user_options(
    with_options: Vec<SqlOption>,
) -> std::result::Result<(UserOptions, String), ParserError> {
    let mut ret_str = String::new();
    let mut builder = UserOptionsBuilder::default();

    for SqlOption { ref name, value } in with_options {
        match normalize_ident(name).as_str() {
            "password" => {
                ret_str = parse_string_value(value)?;
                builder
                    .password(ret_str.clone())
                    .map_err(|e| ParserError::ParserError(e.to_string()))?;
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
            "granted_admin" => {
                builder.granted_admin(parse_bool_value(value)?);
            }
            "hash_password" => {
                builder.hash_password(parse_string_value(value)?);
            }
            _ => {
                return Err(ParserError::ParserError(format!(
                "Expected option [password | rsa_public_key | comment | granted_admin], found [{}]",
                name
            )))
            }
        }
    }

    let option = builder
        .build()
        .map_err(|e| ParserError::ParserError(e.to_string()))?;

    Ok((option, ret_str))
}

#[derive(Debug, Clone)]
pub struct CreateRole {
    pub tenant_name: String,
    pub name: String,
    pub if_not_exists: bool,
    pub inherit_tenant_role: Option<SystemTenantRole>,
}

#[derive(Debug, Clone)]
pub struct GrantRevoke {
    pub is_grant: bool,
    // privilege, db name
    pub database_privileges: Vec<(DatabasePrivilege, String)>,
    pub tenant_name: String,
    pub role_name: String,
}

#[derive(Debug, Clone)]
pub struct AlterUser {
    pub user_name: String,
    pub alter_user_action: AlterUserAction,
}

#[derive(Debug, Clone)]
pub enum AlterUserAction {
    RenameTo(String),
    Set(UserOptions),
}

#[derive(Debug, Clone)]
pub struct AlterTenant {
    pub tenant_name: String,
    pub alter_tenant_action: AlterTenantAction,
}

#[derive(Debug, Clone)]
pub enum AlterTenantAction {
    AddUser(AlterTenantAddUser),
    SetUser(AlterTenantSetUser),
    RemoveUser(Oid),
    SetOption(Box<TenantOptions>),
}

#[derive(Debug, Clone)]
pub struct AlterTenantAddUser {
    pub user_id: Oid,
    pub role: TenantRoleIdentifier,
}

#[derive(Debug, Clone)]
pub struct AlterTenantSetUser {
    pub user_id: Oid,
    pub role: TenantRoleIdentifier,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterDatabase {
    pub database_name: String,
    pub database_options: DatabaseOptionsBuilder,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterTable {
    pub table_name: ResolvedTable,
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
    RenameColumn {
        old_column_name: String,
        new_column_name: String,
    },
}

#[async_trait]
pub trait LogicalPlanner {
    async fn create_logical_plan(
        &self,
        statement: ExtStatement,
        session: &SessionCtx,
        auth_enable: bool,
    ) -> QueryResult<Plan>;
}

/// Additional output information
pub fn affected_row_expr(args: Vec<Expr>) -> Expr {
    let udf = expr::ScalarUDF::new(TABLE_WRITE_UDF.clone(), args);

    Expr::ScalarUDF(udf).alias(AFFECTED_ROWS.0)
}

pub fn merge_affected_row_expr() -> Expr {
    expr_fn::sum(col(AFFECTED_ROWS.0)).alias(AFFECTED_ROWS.0)
}

/// Normalize a SQL object name
pub fn normalize_sql_object_name_to_string(sql_object_name: &ObjectName) -> String {
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

pub struct CopyOptions {
    pub auto_infer_schema: bool,
}

#[derive(Default)]
pub struct CopyOptionsBuilder {
    auto_infer_schema: Option<bool>,
}

impl CopyOptionsBuilder {
    // Convert sql options to supported parameters
    // perform value validation
    pub fn apply_options(
        mut self,
        options: Vec<SqlOption>,
    ) -> std::result::Result<Self, QueryError> {
        for SqlOption { ref name, value } in options {
            match normalize_ident(name).as_str() {
                "auto_infer_schema" => {
                    self.auto_infer_schema = Some(parse_bool_value(value).context(ParserSnafu)?);
                }
                option => {
                    return Err(QueryError::Semantic {
                        err: format!("Unsupported option [{}]", option),
                    })
                }
            }
        }

        Ok(self)
    }

    /// Construct CopyOptions and assign default value
    pub fn build(self) -> CopyOptions {
        CopyOptions {
            auto_infer_schema: self.auto_infer_schema.unwrap_or_default(),
        }
    }
}

pub struct FileFormatOptions {
    pub file_type: FileType,
    pub delimiter: char,
    pub with_header: bool,
    pub file_compression_type: FileCompressionType,
}

#[derive(Debug, Default)]
pub struct FileFormatOptionsBuilder {
    file_type: Option<FileType>,
    delimiter: Option<char>,
    with_header: Option<bool>,
    file_compression_type: Option<FileCompressionType>,
}

impl FileFormatOptionsBuilder {
    fn parse_file_type(s: &str) -> QueryResult<FileType> {
        let s = s.to_uppercase();
        match s.as_str() {
            "AVRO" => Ok(FileType::AVRO),
            "PARQUET" => Ok(FileType::PARQUET),
            "CSV" => Ok(FileType::CSV),
            "JSON" => Ok(FileType::JSON),
            _ => Err(QueryError::Semantic {
                err: format!(
                    "Unknown FileType: {}, only support AVRO | PARQUET | CSV | JSON",
                    s
                ),
            }),
        }
    }

    fn parse_file_compression_type(s: &str) -> QueryResult<FileCompressionType> {
        let s = s.to_uppercase();
        match s.as_str() {
            "GZIP" | "GZ" => Ok(FileCompressionType::GZIP),
            "BZIP2" | "BZ2" => Ok(FileCompressionType::BZIP2),
            "" => Ok(FileCompressionType::UNCOMPRESSED),
            _ => Err(QueryError::Semantic {
                err: format!(
                    "Unknown FileCompressionType: {}, only support GZIP | BZIP2",
                    s
                ),
            }),
        }
    }

    // 将sql options转换为受支持的参数
    // 执行值校验
    pub fn apply_options(mut self, options: Vec<SqlOption>) -> QueryResult<Self> {
        for SqlOption { ref name, value } in options {
            match normalize_ident(name).as_str() {
                "type" => {
                    let file_type =
                        Self::parse_file_type(&parse_string_value(value).context(ParserSnafu)?)?;
                    self.file_type = Some(file_type);
                }
                "delimiter" => {
                    // only support csv
                    if let Some(file_type) = &self.file_type {
                        if *file_type != FileType::CSV {
                            return Err(QueryError::Semantic {
                                err: "delimiter and with_header fields are specific to CSV"
                                    .to_string(),
                            });
                        }
                    }
                    self.delimiter = Some(parse_char_value(value).context(ParserSnafu)?);
                }
                "with_header" => {
                    if let Some(file_type) = &self.file_type {
                        if *file_type != FileType::CSV {
                            return Err(QueryError::Semantic {
                                err: "delimiter and with_header fields are specific to CSV"
                                    .to_string(),
                            });
                        }
                    }
                    self.with_header = Some(parse_bool_value(value).context(ParserSnafu)?);
                }
                "file_compression_type" => {
                    let file_compression_type = Self::parse_file_compression_type(
                        &parse_string_value(value).context(ParserSnafu)?,
                    )?;
                    self.file_compression_type = Some(file_compression_type);
                }
                option => {
                    return Err(QueryError::Semantic {
                        err: format!("Unsupported option [{}]", option),
                    })
                }
            }
        }

        Ok(self)
    }

    /// Construct FileFormatOptions and assign default value
    pub fn build(self) -> FileFormatOptions {
        FileFormatOptions {
            file_type: self.file_type.unwrap_or(FileType::CSV),
            delimiter: self.delimiter.unwrap_or(','),
            with_header: self.with_header.unwrap_or(true),
            file_compression_type: self
                .file_compression_type
                .unwrap_or(FileCompressionType::UNCOMPRESSED),
        }
    }
}

pub enum ConnectionOptions {
    S3(S3StorageConfig),
    Gcs(GcsStorageConfig),
    Azblob(AzblobStorageConfig),
    Local,
}

/// Construct ConnectionOptions and assign default value
/// Convert sql options to supported parameters
/// perform value validation
pub fn parse_connection_options(
    url: &UriSchema,
    bucket: Option<&str>,
    options: Vec<SqlOption>,
) -> QueryResult<ConnectionOptions> {
    let parsed_options = match (url, bucket) {
        (UriSchema::S3, Some(bucket)) => ConnectionOptions::S3(parse_s3_options(bucket, options)?),
        (UriSchema::Gcs, Some(bucket)) => {
            ConnectionOptions::Gcs(parse_gcs_options(bucket, options)?)
        }
        (UriSchema::Azblob, Some(bucket)) => {
            ConnectionOptions::Azblob(parse_azure_options(bucket, options)?)
        }
        (UriSchema::Local, _) => ConnectionOptions::Local,
        (UriSchema::Custom(schema), _) => {
            return Err(QueryError::Semantic {
                err: format!("Unsupported url schema [{}]", schema),
            })
        }
        (_, None) => {
            return Err(QueryError::Semantic {
                err: "Lost bucket in url".to_string(),
            })
        }
    };

    Ok(parsed_options)
}

/// s3://<bucket>/<path>
fn parse_s3_options(bucket: &str, options: Vec<SqlOption>) -> QueryResult<S3StorageConfig> {
    let mut builder = S3StorageConfigBuilder::default();

    builder.bucket(bucket);

    for SqlOption { ref name, value } in options {
        match normalize_ident(name).as_str() {
            "endpoint_url" => {
                builder.endpoint_url(parse_string_value(value).context(ParserSnafu)?);
            }
            "region" => {
                builder.region(parse_string_value(value).context(ParserSnafu)?);
            }
            "access_key_id" => {
                builder.access_key_id(parse_string_value(value).context(ParserSnafu)?);
            }
            "secret_key" => {
                builder.secret_access_key(parse_string_value(value).context(ParserSnafu)?);
            }
            "token" => {
                builder.security_token(parse_string_value(value).context(ParserSnafu)?);
            }
            "virtual_hosted_style" => {
                builder.virtual_hosted_style_request(parse_bool_value(value).context(ParserSnafu)?);
            }
            _ => {
                return Err(QueryError::Semantic {
                    err: format!("Unsupported option [{}]", name),
                })
            }
        }
    }

    builder.build().map_err(|err| QueryError::Semantic {
        err: err.to_string(),
    })
}

/// gcs://<bucket>/<path>
fn parse_gcs_options(bucket: &str, options: Vec<SqlOption>) -> QueryResult<GcsStorageConfig> {
    let mut sac_builder = ServiceAccountCredentialsBuilder::default();

    for SqlOption { ref name, value } in options {
        match normalize_ident(name).as_str() {
            "gcs_base_url" => {
                sac_builder.gcs_base_url(parse_string_value(value).context(ParserSnafu)?);
            }
            "disable_oauth" => {
                sac_builder.disable_oauth(parse_bool_value(value).context(ParserSnafu)?);
            }
            "client_email" => {
                sac_builder.client_email(parse_string_value(value).context(ParserSnafu)?);
            }
            "private_key" => {
                sac_builder.private_key(parse_string_value(value).context(ParserSnafu)?);
            }
            _ => {
                return Err(QueryError::Semantic {
                    err: format!("Unsupported option [{}]", name),
                })
            }
        }
    }

    let sac = sac_builder.build().map_err(|err| QueryError::Semantic {
        err: err.to_string(),
    })?;
    let mut temp = NamedTempFile::new().context(StdIoSnafu)?;
    write_tmp_service_account_file(sac, &mut temp)?;

    Ok(GcsStorageConfig {
        bucket: bucket.to_string(),
        service_account_path: temp.into_temp_path(),
    })
}

/// https://<account>.blob.core.windows.net/<container>[/<path>]
/// azblob://<container>/<path>
fn parse_azure_options(bucket: &str, options: Vec<SqlOption>) -> QueryResult<AzblobStorageConfig> {
    let mut builder = AzblobStorageConfigBuilder::default();
    builder.container_name(bucket);

    for SqlOption { ref name, value } in options {
        match normalize_ident(name).as_str() {
            "account" => {
                builder.account_name(parse_string_value(value).context(ParserSnafu)?);
            }
            "access_key" => {
                builder.access_key(parse_string_value(value).context(ParserSnafu)?);
            }
            "bearer_token" => {
                builder.bearer_token(parse_string_value(value).context(ParserSnafu)?);
            }
            "use_emulator" => {
                builder.use_emulator(parse_bool_value(value).context(ParserSnafu)?);
            }
            _ => {
                return Err(QueryError::Semantic {
                    err: format!("Unsupported option [{}]", name),
                })
            }
        }
    }

    builder.build().map_err(|err| QueryError::Semantic {
        err: err.to_string(),
    })
}

fn write_tmp_service_account_file(
    sac: ServiceAccountCredentials,
    tmp: &mut NamedTempFile,
) -> QueryResult<()> {
    let body = serde_json::to_vec(&sac).context(SerdeJsonSnafu)?;
    let _ = tmp.write(&body).context(StdIoSnafu)?;
    tmp.flush().context(StdIoSnafu)?;

    Ok(())
}

/// Convert SqlOption s to map, and convert value to lowercase
pub fn sql_options_to_map(opts: &[SqlOption]) -> HashMap<String, String> {
    let mut map = HashMap::with_capacity(opts.len());
    for SqlOption { name, value } in opts {
        let value_str = match value {
            Value::SingleQuotedString(s) | Value::DoubleQuotedString(s) => s.clone(),
            _ => value.to_string().to_ascii_lowercase(),
        };
        map.insert(normalize_ident(name), value_str);
    }
    map
}
