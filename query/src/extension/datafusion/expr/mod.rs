use crate::function::*;

pub mod func_manager;

mod aggregate_function;
mod scalar_function;

/// load all cnosdb's built-in function
pub fn load_all_functions(func_manager: &mut dyn FunctionMetadataManager) -> Result<()> {
    scalar_function::register_udfs(func_manager)?;
    aggregate_function::register_udafs(func_manager)?;
    Ok(())
}
