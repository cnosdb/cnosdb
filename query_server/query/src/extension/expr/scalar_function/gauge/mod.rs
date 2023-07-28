use spi::query::function::FunctionMetadataManager;
use spi::Result;

mod delta;

pub fn register_udfs(func_manager: &mut dyn FunctionMetadataManager) -> Result<()> {
    delta::register_udf(func_manager)?;
    Ok(())
}
