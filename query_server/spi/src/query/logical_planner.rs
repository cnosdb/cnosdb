use super::{
    ast::{ExtStatement, ObjectType},
    session::IsiphoSessionCtx,
    Result,
};

use datafusion::logical_plan::{DFSchemaRef, LogicalPlan as DFPlan};

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

pub trait LogicalPlanner {
    fn create_logical_plan(
        &self,
        statement: ExtStatement,
        session: &IsiphoSessionCtx,
    ) -> Result<Plan>;
}
