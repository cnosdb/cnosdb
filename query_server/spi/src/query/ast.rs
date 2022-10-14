use std::fmt;

use datafusion::sql::sqlparser::ast::{DataType, Ident};
use datafusion::sql::{parser::CreateExternalTable, sqlparser::ast::Statement};

/// Statement representations
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExtStatement {
    /// ANSI SQL AST node
    SqlStatement(Box<Statement>),

    CreateExternalTable(CreateExternalTable),
    CreateTable(CreateTable),
    CreateDatabase(CreateDatabase),
    CreateUser(CreateUser),

    Drop(DropObject),
    DropUser(DropUser),

    DescribeTable(DescribeObject),
    DescribeDatabase(DescribeObject),
    ShowDatabases,
    ShowTables,
    //todo:  insert/update/alter
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DropObject {
    pub object_name: String,
    pub if_exist: bool,
    pub obj_type: ObjectType,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeObject {
    pub object_name: String,
    pub obj_type: ObjectType,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DropUser {}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateUser {}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateDatabase {
    pub name: String,
    pub if_not_exists: bool,
    pub options: DatabaseOptions,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateTable {
    pub name: String,
    pub if_not_exists: bool,
    pub columns: Vec<ColumnOption>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnOption {
    pub name: Ident,
    pub is_tag: bool,
    pub data_type: DataType,
    pub codec: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DatabaseOptions {
    // data keep time
    pub ttl: String,

    pub shard_num: u64,
    // shard coverage time range
    pub vnode_duration: String,

    pub replica: u64,
    // timestamp percision
    pub precision: String,
}

impl Default for DatabaseOptions {
    fn default() -> Self {
        Self {
            ttl: String::default(),
            shard_num: u64::MAX,
            vnode_duration: String::default(),
            replica: u64::MAX,
            precision: String::default(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum ObjectType {
    Table,
    Database,
}

impl fmt::Display for ObjectType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str(match self {
            ObjectType::Table => "TABLE",
            ObjectType::Database => "DATABASE",
        })
    }
}
