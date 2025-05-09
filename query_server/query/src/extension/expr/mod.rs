pub mod func_manager;

mod aggregate_function;
pub mod expr_fn;
pub mod expr_rewriter;
pub mod expr_utils;
mod scalar_function;
mod selector_function;
mod session_function;
mod ts_gen_func;
mod window;

use datafusion::arrow::datatypes::{DataType, IntervalUnit};
pub use scalar_function::{INTERPOLATE, LOCF, TIME_WINDOW_GAPFILL};
pub use selector_function::{BOTTOM, TOPK};
pub use session_function::register_session_udfs;
use spi::query::function::FunctionMetadataManager;
use spi::QueryResult;
pub use ts_gen_func::TimeSeriesGenFunc;
pub use window::{
    ceil_sliding_window, floor_sliding_window, time_window_signature, DEFAULT_TIME_WINDOW_START,
    TIME_WINDOW, TIME_WINDOW_UDF, WINDOW_COL_NAME, WINDOW_END, WINDOW_START,
};

pub static INTERVALS: &[DataType] = &[
    DataType::Interval(IntervalUnit::YearMonth),
    DataType::Interval(IntervalUnit::MonthDayNano),
    DataType::Interval(IntervalUnit::DayTime),
];

pub static BINARYS: &[DataType] = &[
    DataType::Binary,
    DataType::LargeBinary,
    DataType::FixedSizeBinary(i32::MAX),
];

pub static INTEGERS: &[DataType] = &[
    DataType::Int8,
    DataType::Int16,
    DataType::Int32,
    DataType::Int64,
    DataType::UInt8,
    DataType::UInt16,
    DataType::UInt32,
    DataType::UInt64,
];

/// load all cnosdb's built-in function
pub fn load_all_functions(func_manager: &mut dyn FunctionMetadataManager) -> QueryResult<()> {
    scalar_function::register_udfs(func_manager)?;
    aggregate_function::register_udafs(func_manager)?;
    selector_function::register_selector_udfs(func_manager)?;
    window::register_window_udfs(func_manager)?;
    ts_gen_func::register_all_udf(func_manager)?;
    Ok(())
}
