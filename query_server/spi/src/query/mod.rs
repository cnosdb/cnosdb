use datafusion::{
    arrow::datatypes::DataType, error::DataFusionError, sql::sqlparser::parser::ParserError,
};
use models::define_result;
use snafu::Snafu;

use self::{execution::ExecutionError, logical_planner::LogicalPlannerError};

pub mod ast;
pub mod dispatcher;
pub mod execution;
pub mod function;
pub mod logical_planner;
pub mod optimizer;
pub mod parser;
pub mod physical_planner;
pub mod session;

pub const AFFECTED_ROWS: (&str, DataType) = ("rows", DataType::UInt64);

define_result!(QueryError);

pub const UNEXPECTED_EXTERNAL_PLAN: &str = "Unexpected plan, maybe it's a df problem";

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum QueryError {
    #[snafu(display("Failed to build QueryDispatcher. err: {}", err))]
    BuildQueryDispatcher { err: String },

    #[snafu(display("Failed to do logical plan. err: {}", source))]
    LogicalPlanner { source: LogicalPlannerError },

    #[snafu(display("Failed to do logical optimization. err: {}", source))]
    LogicalOptimize { source: DataFusionError },

    #[snafu(display("Failed to do physical plan. err: {}", source))]
    PhysicalPlaner { source: DataFusionError },

    #[snafu(display("Failed to do parse. err: {}", source))]
    Parser { source: ParserError },

    #[snafu(display("Failed to do analyze. err: {}", err))]
    Analyzer { err: String },

    #[snafu(display("Failed to do optimizer. err: {}", source))]
    Optimizer { source: DataFusionError },

    #[snafu(display("Failed to do schedule. err: {}", source))]
    Schedule { source: DataFusionError },

    #[snafu(display("Failed to do execution. err: {}", source))]
    Execution { source: ExecutionError },

    #[snafu(display("Concurrent query request limit exceeded"))]
    RequestLimit,

    #[snafu(display("Multi-statement not allow, found num:{}, sql:{}", num, sql))]
    MultiStatement { num: usize, sql: String },

    #[snafu(display(
        "Internal error: {}. This was likely caused by a bug in Cnosdb's \
    code and we would welcome that you file an bug report in our issue tracker",
        err
    ))]
    Internal { err: String },

    #[snafu(display("The query has been canceled"))]
    Cancel,

    #[snafu(display("The query server has been closed"))]
    Closed,
}
