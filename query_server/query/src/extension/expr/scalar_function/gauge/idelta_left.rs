use datafusion::logical_expr::ScalarUDF;
use spi::query::function::FunctionMetadataManager;
use spi::QueryResult;

use crate::object_accessor;

pub fn register_udf(func_manager: &mut dyn FunctionMetadataManager) -> QueryResult<ScalarUDF> {
    let udf = new();
    func_manager.register_udf(udf.clone())?;
    Ok(udf)
}

fn new() -> ScalarUDF {
    object_accessor!(GaugeData, idelta_left)
}
