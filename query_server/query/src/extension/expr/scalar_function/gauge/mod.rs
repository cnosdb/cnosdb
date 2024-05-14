use spi::query::function::FunctionMetadataManager;
use spi::QueryResult;

mod delta;
mod first_time;
mod first_val;
mod idelta_left;
mod idelta_right;
mod last_time;
mod last_val;
mod rate;
mod time_delta;

pub fn register_udfs(func_manager: &mut dyn FunctionMetadataManager) -> QueryResult<()> {
    delta::register_udf(func_manager)?;
    time_delta::register_udf(func_manager)?;
    rate::register_udf(func_manager)?;
    first_time::register_udf(func_manager)?;
    first_val::register_udf(func_manager)?;
    last_time::register_udf(func_manager)?;
    last_val::register_udf(func_manager)?;
    idelta_left::register_udf(func_manager)?;
    idelta_right::register_udf(func_manager)?;
    Ok(())
}
