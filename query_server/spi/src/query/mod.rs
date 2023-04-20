use datafusion::arrow::datatypes::DataType;

pub mod analyzer;
pub mod ast;
pub mod auth;
pub mod config;
pub mod datasource;
pub mod dispatcher;
pub mod execution;
pub mod function;
pub mod logical_planner;
pub mod optimizer;
pub mod parser;
pub mod physical_planner;
pub mod recordbatch;
pub mod scheduler;
pub mod session;
pub mod traced_stream;

pub const AFFECTED_ROWS: (&str, DataType) = ("rows", DataType::UInt64);

pub const UNEXPECTED_EXTERNAL_PLAN: &str = "Unexpected plan, maybe it's a df problem";
