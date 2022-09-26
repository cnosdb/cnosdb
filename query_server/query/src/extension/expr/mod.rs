pub mod func_manager;

mod aggregate_function;
pub mod expr_utils;
mod function_utils;
mod scalar_function;
pub mod selector_function;

use spi::query::function::{FunctionMetadataManager, Result};

/// load all cnosdb's built-in function
pub fn load_all_functions(func_manager: &mut dyn FunctionMetadataManager) -> Result<()> {
    scalar_function::register_udfs(func_manager)?;
    aggregate_function::register_udafs(func_manager)?;
    selector_function::register_selector_udfs(func_manager)?;
    Ok(())
}
