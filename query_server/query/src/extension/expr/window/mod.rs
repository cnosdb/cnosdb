mod time_window;

use spi::query::function::FunctionMetadataManager;
use spi::Result;

pub fn register_window_udfs(func_manager: &mut dyn FunctionMetadataManager) -> Result<()> {
    // extend function...
    // eg.
    //   example::register_udf(func_manager)?;
    time_window::register_udf(func_manager)?;
    Ok(())
}

pub const TIME_WINDOW: &str = "TIME_WINDOW";
pub const WINDOW_COL_NAME: &str = "_window";
pub const WINDOW_START: &str = "start";
pub const WINDOW_END: &str = "end";

pub use time_window::{
    ceil_sliding_window, floor_sliding_window, signature as time_window_signature,
    DEFAULT_TIME_WINDOW_START, TIME_WINDOW_UDF,
};
