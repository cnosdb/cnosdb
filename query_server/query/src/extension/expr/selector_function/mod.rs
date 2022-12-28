mod bottom;
mod topk;

use spi::query::function::FunctionMetadataManager;
use spi::Result;

pub fn register_selector_udfs(func_manager: &mut dyn FunctionMetadataManager) -> Result<()> {
    // extend function...
    // eg.
    //   example::register_udf(func_manager)?;
    bottom::register_udf(func_manager)?;
    topk::register_udf(func_manager)?;
    Ok(())
}

pub const BOTTOM: &str = "BOTTOM";
pub const TOPK: &str = "TOPK";
