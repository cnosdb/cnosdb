use crate::catalog::MetadataError;

use super::{
    ast::{ExtStatement, ObjectType},
    session::IsiphoSessionCtx,
};

use datafusion::{
    error::DataFusionError,
    logical_plan::{DFSchemaRef, LogicalPlan as DFPlan},
    prelude::{lit, Expr},
    scalar::ScalarValue,
};
use models::define_result;
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

    #[snafu(display("Metadata err: {}", source))]
    Metadata { source: MetadataError },

    #[snafu(display("This feature is not implemented: {}", err))]
    NotImplemented { err: String },
}

#[derive(Debug, Clone)]
pub enum Plan {
    /// Query plan
    Query(QueryPlan),
    /// Query plan
    DDL(DDLPlan),
}

#[derive(Debug, Clone)]
pub struct QueryPlan {
    pub df_plan: DFPlan,
}

#[derive(Debug, Clone)]
pub enum DDLPlan {
    Drop(DropPlan),
    /// Create external table. such as parquet\csv...
    CreateExternalTable(CreateExternalTable),

    CreateTable(CreateTable),
}

#[derive(Debug, Clone)]
pub struct DropPlan {
    /// Table name
    pub object_name: String,
    /// If exists
    pub if_exist: bool,
    ///ObjectType
    pub obj_type: ObjectType,
}

#[derive(Debug, Clone)]
pub struct CreateExternalTable {
    /// The table schema
    pub schema: DFSchemaRef,
    /// The table name
    pub name: String,
    /// The physical location
    pub location: String,
    /// The file type of physical file
    pub file_descriptor: FileDescriptor,
    /// Partition Columns
    pub table_partition_cols: Vec<String>,
    /// Option to not error if table already exists
    pub if_not_exists: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FileDescriptor {
    /// Newline-delimited JSON
    NdJson,
    /// Apache Parquet columnar storage
    Parquet,
    /// Comma separated values
    CSV(CSVOptions),
    /// Avro binary records
    Avro,
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
    pub schema: DFSchemaRef,
    /// The table name
    pub name: String,
    /// Option to not error if table already exists
    pub if_not_exists: bool,
    /// The table tags
    pub tags: Vec<String>,
}

pub trait LogicalPlanner {
    fn create_logical_plan(
        &self,
        statement: ExtStatement,
        session: &IsiphoSessionCtx,
    ) -> Result<Plan>;
}

/// TODO Additional output information
pub fn affected_row_expr() -> Expr {
    lit(ScalarValue::Null).alias("COUNT")
}
