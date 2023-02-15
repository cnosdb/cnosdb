use std::sync::Arc;

use datafusion::arrow::datatypes::DataType;
use datafusion::logical_expr::{create_udaf, AggregateUDF, Volatility};
use datafusion::physical_plan::expressions::AvgAccumulator;
use spi::query::function::FunctionMetadataManager;
use spi::Result;

pub fn register_udaf(func_manager: &mut dyn FunctionMetadataManager) -> Result<AggregateUDF> {
    let udf = new();
    func_manager.register_udaf(udf.clone())?;
    Ok(udf)
}

fn new() -> AggregateUDF {
    create_udaf(
        "MY_AVG",
        DataType::Float64,
        Arc::new(DataType::Float64),
        Volatility::Immutable,
        Arc::new(|_| Ok(Box::new(AvgAccumulator::try_new(&DataType::Float64)?))),
        Arc::new(vec![DataType::UInt64, DataType::Float64]),
    )
}
