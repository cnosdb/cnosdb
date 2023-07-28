use datafusion::logical_expr::ScalarUDF;
use spi::query::function::FunctionMetadataManager;
use spi::Result;

use crate::object_accessor;

pub fn register_udf(func_manager: &mut dyn FunctionMetadataManager) -> Result<ScalarUDF> {
    let udf = new();
    func_manager.register_udf(udf.clone())?;
    Ok(udf)
}

fn new() -> ScalarUDF {
    object_accessor!(GaugeData, delta)
}
