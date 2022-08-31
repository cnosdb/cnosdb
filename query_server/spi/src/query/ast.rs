use std::fmt;

use datafusion::sql::sqlparser::ast::Statement;

/// Statement representations
#[derive(Debug, Clone, PartialEq)]
pub enum ExtStatement {
    /// ANSI SQL AST node
    SqlStatement(Box<Statement>),

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

#[derive(Debug, Clone, PartialEq)]
pub struct DropObject {
    pub object_name: String,
    pub if_exist: bool,
    pub obj_type: ObjectType,
}

#[derive(Debug, Clone, PartialEq)]
pub struct DescribeObject {
    pub object_name: String,
    pub obj_type: ObjectType,
}

#[derive(Debug, Clone, PartialEq)]
pub struct DropUser {}
#[derive(Debug, Clone, PartialEq)]
pub struct CreateUser {}
#[derive(Debug, Clone, PartialEq)]
pub struct CreateDatabase {}
#[derive(Debug, Clone, PartialEq)]
pub struct CreateTable {}

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
