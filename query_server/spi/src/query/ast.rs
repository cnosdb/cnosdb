use std::fmt;

use datafusion::sql::parser::CreateExternalTable;
use datafusion::sql::sqlparser::ast::{
    AnalyzeFormat, DataType, Expr, Ident, ObjectName, Offset, OrderByExpr, Statement, TableFactor,
    Value,
};
use datafusion::sql::sqlparser::parser::ParserError;
use models::codec::Encoding;
use models::meta_data::{NodeId, ReplicationSetId, VnodeId};

use super::logical_planner::{DatabaseObjectType, GlobalObjectType, TenantObjectType};

/// Statement representations
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExtStatement {
    /// ANSI SQL AST node
    SqlStatement(Box<Statement>),

    // bulk load/unload
    Copy(Copy),

    CreateExternalTable(CreateExternalTable),
    CreateTable(CreateTable),
    CreateStreamTable(Statement),
    CreateDatabase(Box<CreateDatabase>),
    CreateTenant(CreateTenant),
    CreateUser(CreateUser),
    CreateRole(CreateRole),

    CreateStream(CreateStream),
    DropStream(DropStream),
    ShowStreams(ShowStreams),

    DropDatabaseObject(DropDatabaseObject),
    DropTenantObject(DropTenantObject),
    DropGlobalObject(DropGlobalObject),

    GrantRevoke(GrantRevoke),

    DescribeTable(DescribeTable),
    DescribeDatabase(DescribeDatabase),
    ShowDatabases(),
    ShowTables(Option<Ident>),
    ShowSeries(Box<ShowSeries>),
    ShowTagValues(Box<ShowTagValues>),
    Explain(Explain),

    // system cmd
    ShowQueries,
    AlterDatabase(Box<AlterDatabase>),
    AlterTable(AlterTable),
    AlterTenant(AlterTenant),
    AlterUser(AlterUser),

    // vnode cmd
    DropVnode(DropVnode),
    CopyVnode(CopyVnode),
    MoveVnode(MoveVnode),
    CompactVnode(CompactVnode),
    CompactDatabase(CompactDatabase),
    ChecksumGroup(ChecksumGroup),

    // recover cmd
    RecoverTenant(RecoverTenant),
    RecoverDatabase(RecoverDatabase),

    // replica cmd
    ShowReplicas,
    ReplicaDestroy(ReplicaDestroy),
    ReplicaAdd(ReplicaAdd),
    ReplicaRemove(ReplicaRemove),
    ReplicaPromote(ReplicaPromote),
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
pub struct SqlOption {
    pub name: Ident,
    pub value: Value,
}

/// Readable file type
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FileType {
    /// Apache Arrow file
    ARROW,
    /// Apache Avro file
    AVRO,
    /// Apache Parquet file
    PARQUET,
    /// CSV file
    CSV,
    /// JSON file
    JSON,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicaDestroy {
    pub replica_id: ReplicationSetId,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicaAdd {
    pub replica_id: ReplicationSetId,
    pub node_id: NodeId,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicaRemove {
    pub replica_id: ReplicationSetId,
    pub node_id: NodeId,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicaPromote {
    pub replica_id: ReplicationSetId,
    pub node_id: NodeId,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChecksumGroup {
    pub replication_set_id: ReplicationSetId,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompactVnode {
    pub vnode_ids: Vec<VnodeId>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompactDatabase {
    pub database_name: Ident,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MoveVnode {
    pub vnode_id: VnodeId,
    pub node_id: NodeId,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CopyVnode {
    pub vnode_id: VnodeId,
    pub node_id: NodeId,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DropVnode {
    pub vnode_id: VnodeId,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Copy {
    pub copy_target: CopyTarget,
    pub file_format_options: Vec<SqlOption>,
    pub copy_options: Vec<SqlOption>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CopyTarget {
    IntoTable(CopyIntoTable),
    IntoLocation(CopyIntoLocation),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CopyIntoTable {
    pub location: UriLocation,
    pub table_name: ObjectName,
    pub columns: Vec<Ident>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CopyIntoLocation {
    pub from: TableFactor,
    pub location: UriLocation,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UriLocation {
    pub path: String,
    pub connection_options: Vec<SqlOption>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterTable {
    pub table_name: ObjectName,
    pub alter_action: AlterTableAction,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Explain {
    pub analyze: bool,
    pub verbose: bool,
    pub ext_statement: Box<ExtStatement>,
    pub format: Option<AnalyzeFormat>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AlterTableAction {
    AddColumn {
        column: ColumnOption,
    },
    AlterColumnEncoding {
        column_name: Ident,
        encoding: Encoding,
    },
    DropColumn {
        column_name: Ident,
    },
    /// `RENAME COLUMN <old_column_name> TO <new_column_name>`
    RenameColumn {
        old_column_name: Ident,
        new_column_name: Ident,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterDatabase {
    pub name: Ident,
    pub options: DatabaseOptions,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterTenant {
    /// tenant name
    pub name: Ident,
    pub operation: AlterTenantOperation,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AlterTenantOperation {
    // Ident: user_name, Ident: role_name
    AddUser(Ident, Ident),
    // Ident: user_name, Ident: role_name
    SetUser(Ident, Ident),
    RemoveUser(Ident),
    Set(SqlOption),
    UnSet(Ident),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DropDatabaseObject {
    pub object_name: ObjectName,
    pub if_exist: bool,
    pub obj_type: DatabaseObjectType,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DropTenantObject {
    pub object_name: Ident,
    pub if_exist: bool,
    pub obj_type: TenantObjectType,
    pub after: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DropGlobalObject {
    pub object_name: Ident,
    pub if_exist: bool,
    pub obj_type: GlobalObjectType,
    pub after: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecoverTable {
    pub object_name: ObjectName,
    pub if_exist: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecoverDatabase {
    pub object_name: Ident,
    pub if_exist: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecoverTenant {
    pub object_name: Ident,
    pub if_exist: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeObject {
    pub object_name: ObjectName,
    pub obj_type: ObjectType,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateUser {
    pub if_not_exists: bool,
    /// User name
    pub name: Ident,
    pub with_options: Vec<SqlOption>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterUser {
    /// User name
    pub name: Ident,
    pub operation: AlterUserOperation,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AlterUserOperation {
    RenameTo(Ident),
    Set(SqlOption),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GrantRevoke {
    pub is_grant: bool,
    pub privileges: Vec<Privilege>,
    pub role_name: Ident,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Privilege {
    pub action: Action,
    pub database: Ident,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Action {
    Read,
    Write,
    All,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateRole {
    pub if_not_exists: bool,
    /// Role name
    pub name: Ident,
    pub inherit: Option<Ident>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateTenant {
    pub name: Ident,
    pub if_not_exists: bool,
    pub with_options: Vec<SqlOption>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateDatabase {
    pub name: Ident,
    pub if_not_exists: bool,
    pub options: DatabaseOptions,
    pub config: DatabaseConfig,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateTable {
    pub name: ObjectName,
    pub if_not_exists: bool,
    pub columns: Vec<ColumnOption>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnOption {
    pub name: Ident,
    pub is_tag: bool,
    pub data_type: DataType,
    pub encoding: Option<Encoding>,
}

impl ColumnOption {
    pub fn new_field(name: Ident, data_type: DataType, encoding: Option<Encoding>) -> Self {
        Self {
            name,
            is_tag: false,
            data_type,
            encoding,
        }
    }

    pub fn new_tag(name: Ident) -> Self {
        Self {
            name,
            is_tag: true,
            data_type: DataType::String(None),
            encoding: None,
        }
    }
}

#[derive(Default, Debug, Clone, PartialEq, Eq)]
pub struct DatabaseOptions {
    // data keep time
    pub ttl: Option<String>,
    pub shard_num: Option<u64>,
    // shard coverage time range
    pub vnode_duration: Option<String>,
    pub replica: Option<u64>,
}

#[derive(Default, Debug, Clone, PartialEq, Eq)]
pub struct DatabaseConfig {
    pub precision: Option<String>,
    pub max_memcache_size: Option<String>,
    pub memcache_partitions: Option<u64>,
    pub wal_max_file_size: Option<String>,
    pub wal_sync: Option<String>,
    pub strict_write: Option<String>,
    pub max_cache_readers: Option<u64>,
}

impl DatabaseConfig {
    pub fn has_some(&self) -> bool {
        self.precision.is_some()
            || self.max_memcache_size.is_some()
            || self.memcache_partitions.is_some()
            || self.wal_max_file_size.is_some()
            || self.wal_sync.is_some()
            || self.strict_write.is_some()
            || self.max_cache_readers.is_some()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeTable {
    pub table_name: ObjectName,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeDatabase {
    pub database_name: Ident,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShowTables {
    pub database_name: Ident,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ShowTagBody {
    // on db
    pub database_name: Option<Ident>,
    // from
    pub table: Ident,
    // where
    pub selection: Option<Expr>,
    // order by
    pub order_by: Vec<OrderByExpr>,
    // limit
    pub limit: Option<Expr>,
    // offset
    pub offset: Option<Offset>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ShowSeries {
    pub body: ShowTagBody,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum With {
    Equal(Ident),
    UnEqual(Ident),
    In(Vec<Ident>),
    NotIn(Vec<Ident>),
    Match(Value),
    UnMatch(Value),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ShowTagValues {
    pub body: ShowTagBody,
    pub with: With,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ObjectType {
    Table,
    Database,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Trigger {
    Once,
    Interval(String),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum OutputMode {
    Complete,
    Append,
    Update,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CreateStream {
    pub if_not_exists: bool,
    pub name: Ident,

    pub trigger: Option<Trigger>,
    pub watermark: Option<String>,
    pub output_mode: Option<OutputMode>,

    pub statement: Box<Statement>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct DropStream {
    pub if_exist: bool,
    pub name: Ident,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ShowStreams {
    pub verbose: bool,
}

impl fmt::Display for ObjectType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str(match self {
            ObjectType::Table => "TABLE",
            ObjectType::Database => "DATABASE",
        })
    }
}

pub fn parse_bool_value(value: Value) -> std::result::Result<bool, ParserError> {
    match value {
        Value::Boolean(s) => Ok(s),
        _ => Err(ParserError::ParserError(format!(
            "expected boolean value, but found : {}",
            value
        ))),
    }
}

pub fn parse_string_value(value: Value) -> std::result::Result<String, ParserError> {
    match value {
        Value::SingleQuotedString(s) => Ok(s),
        _ => Err(ParserError::ParserError(format!(
            "expected string value, but found : {}",
            value
        ))),
    }
}

pub fn parse_char_value(value: Value) -> std::result::Result<char, ParserError> {
    let token = parse_string_value(value)?;
    match token.len() {
        1 => Ok(token.chars().next().unwrap()),
        _ => Err(ParserError::ParserError(
            "Delimiter must be a single char".to_string(),
        )),
    }
}
