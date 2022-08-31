use datafusion::{error::DataFusionError, sql::sqlparser::parser::ParserError};
use models::define_result;
use snafu::Snafu;

pub mod ast;
pub mod dispatcher;
pub mod execution;
pub mod function;
pub mod logical_planner;
pub mod optimizer;
pub mod parser;
pub mod physical_planner;
pub mod session;

define_result!(QueryError);

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum QueryError {
    #[snafu(display("Failed to build QueryDispatcher. err: {}", err))]
    BuildQueryDispatcher { err: String },

    #[snafu(display("Failed to do logical plan. err: {}", source))]
    LogicalPlanner { source: DataFusionError },

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
}
