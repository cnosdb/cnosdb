use datafusion::logical_expr::ScalarUDF;
use spi::query::function::FunctionMetadataManager;
use spi::QueryResult;

use crate::object_accessor;

pub fn register_udf(func_manager: &mut dyn FunctionMetadataManager) -> QueryResult<()> {
    func_manager.register_udf(new())?;
    Ok(())
}

fn new() -> ScalarUDF {
    object_accessor!(GaugeData, last_val, GaugeDataLastValAccessor)
}
