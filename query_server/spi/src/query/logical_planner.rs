use std::collections::HashMap;
use std::io::Write;
use std::sync::Arc;

use async_trait::async_trait;
use config::TenantLimiterConfig;
use datafusion::arrow::array::ArrayRef;
use datafusion::arrow::datatypes::{DataType, Schema};
use datafusion::datasource::file_format::file_type::{FileCompressionType, FileType};
use datafusion::logical_expr::expr::AggregateFunction as AggregateFunctionExpr;
use datafusion::logical_expr::type_coercion::aggregates::{
    DATES, NUMERICS, STRINGS, TIMES, TIMESTAMPS,
};
use datafusion::logical_expr::{
    AggregateFunction, CreateExternalTable, LogicalPlan as DFPlan, ReturnTypeFunction, ScalarUDF,
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
use models::schema::{
    DatabaseOptions, TableColumn, Tenant, TenantOptions, TenantOptionsBuilder, Watermark,
};
use snafu::ResultExt;
use tempfile::NamedTempFile;

use super::ast::{parse_bool_value, parse_char_value, parse_string_value, ExtStatement};
use super::datasource::azure::{AzblobStorageConfig, AzblobStorageConfigBuilder};
use super::datasource::gcs::{
    GcsStorageConfig, ServiceAccountCredentials, ServiceAccountCredentialsBuilder,
};
use super::datasource::s3::{S3StorageConfig, S3StorageConfigBuilder};
use super::datasource::UriSchema;
use super::session::SessionCtx;
use super::AFFECTED_ROWS;
use crate::service::protocol::QueryId;
use crate::{ParserSnafu, QueryError, Result};

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
    /// Query plan
    SYSTEM(SYSPlan),
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

    DescribeTable(DescribeTable),

    DescribeDatabase(DescribeDatabase),

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
pub enum SYSPlan {
    ShowQueries,
    KillQuery(QueryId),
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
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GlobalObjectType {
    User,
    Tenant,
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

    pub options: DatabaseOptions,
}

#[derive(Debug, Clone)]
pub struct CreateTenant {
    pub name: String,
    pub if_not_exists: bool,
    pub options: TenantOptions,
}

pub fn unset_option_to_alter_tenant_action(
    tenant: Tenant,
    ident: Ident,
) -> Result<(AlterTenantAction, Privilege<Oid>)> {
    let tenant_id = *tenant.id();
    let mut tenant_options_builder = TenantOptionsBuilder::from(tenant.to_own_options());

    let privilege = match normalize_ident(&ident).as_str() {
        "comment" => {
            tenant_options_builder.unset_comment();
            Privilege::Global(GlobalPrivilege::Tenant(Some(tenant_id)))
        }
        "_limiter" => {
            tenant_options_builder.unset_limiter_config();
            Privilege::Global(GlobalPrivilege::System)
        }
        _ => {
            return Err(QueryError::Parser {
                source: ParserError::ParserError(format!(
                    "Expected option [comment], [limiter] found [{}]",
                    ident
                )),
            })
        }
    };
    let tenant_options = tenant_options_builder.build()?;

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
        "comment" => {
            let value = parse_string_value(value)?;
            tenant_options_builder.comment(value);
            Privilege::Global(GlobalPrivilege::Tenant(Some(tenant_id)))
        }
        "_limiter" => {
            let config =
                serde_json::from_str::<TenantLimiterConfig>(parse_string_value(value)?.as_str())
                    .map_err(|_| ParserError::ParserError("limiter format error".to_string()))?;
            tenant_options_builder.limiter_config(config);
            Privilege::Global(GlobalPrivilege::System)
        }
        _ => {
            return Err(QueryError::Parser {
                source: ParserError::ParserError(format!(
                    "Expected option [comment], [limiter] found [{}]",
                    name
                )),
            })
        }
    };
    let tenant_options = tenant_options_builder.build()?;
    Ok((
        AlterTenantAction::SetOption(Box::new(tenant_options)),
        privilege,
    ))
}

pub fn sql_options_to_tenant_options(options: Vec<SqlOption>) -> Result<TenantOptions> {
    let mut builder = TenantOptionsBuilder::default();

    for SqlOption { ref name, value } in options {
        match normalize_ident(name).as_str() {
            "comment" => {
                builder.comment(parse_string_value(value).context(ParserSnafu)?);
            }
            "_limiter" => {
                let config = serde_json::from_str::<TenantLimiterConfig>(
                    parse_string_value(value).context(ParserSnafu)?.as_str(),
                )?;
                builder.limiter_config(config);
            }
            _ => {
                return Err(QueryError::Parser {
                    source: ParserError::ParserError(format!(
                        "Expected option [comment], [limiter] found [{}]",
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
) -> std::result::Result<UserOptions, ParserError> {
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
            _ => {
                return Err(ParserError::ParserError(format!(
                    "Expected option [comment], found [{}]",
                    name
                )))
            }
        }
    }

    builder
        .build()
        .map_err(|e| ParserError::ParserError(e.to_string()))
}

#[derive(Debug, Clone)]
pub struct CreateRole {
    pub tenant_name: String,
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
    pub table_name: ResolvedTable,
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
    pub database_options: DatabaseOptions,
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
}

#[async_trait]
pub trait LogicalPlanner {
    async fn create_logical_plan(
        &self,
        statement: ExtStatement,
        session: &SessionCtx,
    ) -> Result<Plan>;
}

/// Additional output information
pub fn affected_row_expr(args: Vec<Expr>) -> Expr {
    Expr::ScalarUDF {
        fun: TABLE_WRITE_UDF.clone(),
        args,
    }
    .alias(AFFECTED_ROWS.0)
}

pub fn merge_affected_row_expr() -> Expr {
    // lit(ScalarValue::Null).alias("COUNT")
    Expr::AggregateFunction(AggregateFunctionExpr {
        fun: AggregateFunction::Sum,
        args: vec![col(AFFECTED_ROWS.0)],
        distinct: false,
        filter: None,
    })
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
                    self.auto_infer_schema = Some(parse_bool_value(value)?);
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
    fn parse_file_type(s: &str) -> Result<FileType> {
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

    fn parse_file_compression_type(s: &str) -> Result<FileCompressionType> {
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
    pub fn apply_options(mut self, options: Vec<SqlOption>) -> Result<Self> {
        for SqlOption { ref name, value } in options {
            match normalize_ident(name).as_str() {
                "type" => {
                    let file_type = Self::parse_file_type(&parse_string_value(value)?)?;
                    self.file_type = Some(file_type);
                }
                "delimiter" => {
                    self.delimiter = Some(parse_char_value(value)?);
                }
                "with_header" => {
                    self.with_header = Some(parse_bool_value(value)?);
                }
                "file_compression_type" => {
                    let file_compression_type =
                        Self::parse_file_compression_type(&parse_string_value(value)?)?;
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
) -> Result<ConnectionOptions> {
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
fn parse_s3_options(bucket: &str, options: Vec<SqlOption>) -> Result<S3StorageConfig> {
    let mut builder = S3StorageConfigBuilder::default();

    builder.bucket(bucket);

    for SqlOption { ref name, value } in options {
        match normalize_ident(name).as_str() {
            "endpoint_url" => {
                builder.endpoint_url(parse_string_value(value)?);
            }
            "region" => {
                builder.region(parse_string_value(value)?);
            }
            "access_key_id" => {
                builder.access_key_id(parse_string_value(value)?);
            }
            "secret_key" => {
                builder.secret_access_key(parse_string_value(value)?);
            }
            "token" => {
                builder.security_token(parse_string_value(value)?);
            }
            "virtual_hosted_style" => {
                builder.virtual_hosted_style_request(parse_bool_value(value)?);
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
fn parse_gcs_options(bucket: &str, options: Vec<SqlOption>) -> Result<GcsStorageConfig> {
    let mut sac_builder = ServiceAccountCredentialsBuilder::default();

    for SqlOption { ref name, value } in options {
        match normalize_ident(name).as_str() {
            "gcs_base_url" => {
                sac_builder.gcs_base_url(parse_string_value(value)?);
            }
            "disable_oauth" => {
                sac_builder.disable_oauth(parse_bool_value(value)?);
            }
            "client_email" => {
                sac_builder.client_email(parse_string_value(value)?);
            }
            "private_key" => {
                sac_builder.private_key(parse_string_value(value)?);
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
    let mut temp = NamedTempFile::new()?;
    write_tmp_service_account_file(sac, &mut temp)?;

    Ok(GcsStorageConfig {
        bucket: bucket.to_string(),
        service_account_path: temp.into_temp_path(),
    })
}

/// https://<account>.blob.core.windows.net/<container>[/<path>]
/// azblob://<container>/<path>
fn parse_azure_options(bucket: &str, options: Vec<SqlOption>) -> Result<AzblobStorageConfig> {
    let mut builder = AzblobStorageConfigBuilder::default();
    builder.container_name(bucket);

    for SqlOption { ref name, value } in options {
        match normalize_ident(name).as_str() {
            "account" => {
                builder.account_name(parse_string_value(value)?);
            }
            "access_key" => {
                builder.access_key(parse_string_value(value)?);
            }
            "bearer_token" => {
                builder.bearer_token(parse_string_value(value)?);
            }
            "use_emulator" => {
                builder.use_emulator(parse_bool_value(value)?);
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
) -> Result<()> {
    let body = serde_json::to_vec(&sac)?;
    let _ = tmp.write(&body)?;
    tmp.flush()?;

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
