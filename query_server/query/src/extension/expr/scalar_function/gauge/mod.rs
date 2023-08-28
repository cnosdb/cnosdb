use spi::query::function::FunctionMetadataManager;
use spi::Result;

mod delta;
mod rate;
mod time_delta;

pub fn register_udfs(func_manager: &mut dyn FunctionMetadataManager) -> Result<()> {
    delta::register_udf(func_manager)?;
    time_delta::register_udf(func_manager)?;
    rate::register_udf(func_manager)?;
    Ok(())
}
