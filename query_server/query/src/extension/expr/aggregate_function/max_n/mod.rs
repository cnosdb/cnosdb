mod max_n;

use spi::query::function::FunctionMetadataManager;
use spi::QueryError;

pub fn register_udafs(func_manager: &mut dyn FunctionMetadataManager) -> Result<(), QueryError> {
    max_n::register_udaf(func_manager)?;
    Ok(())
}
