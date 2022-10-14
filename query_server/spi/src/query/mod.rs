use datafusion::{error::DataFusionError, sql::sqlparser::parser::ParserError};
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
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Precision {
    MS,
    US,
    NS,
}

impl Precision {
    pub fn new(text: &str) -> Option<Self> {
        match text.to_uppercase().as_str() {
            "'MS'" => Some(Precision::MS),
            "'US'" => Some(Precision::US),
            "'NS'" => Some(Precision::NS),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DurationUnit {
    Minutes,
    Hour,
    Day,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Duration {
    pub time_num: u64,
    pub unit: DurationUnit,
}

impl Duration {
    pub fn new(text: &str) -> Option<Self> {
        let len = text.len();
        if len < 4 {
            return None;
        }
        let time = &text[1..len - 2];
        let unit = &text[len - 2..len - 1];
        let time_num = match time.parse::<u64>() {
            Ok(v) => v,
            Err(_) => {
                return None;
            }
        };
        let time_unit = match unit.to_uppercase().as_str() {
            "D" => DurationUnit::Day,
            "H" => DurationUnit::Hour,
            "M" => DurationUnit::Minutes,
            _ => return None,
        };
        Some(Duration {
            time_num,
            unit: time_unit,
        })
    }
}
