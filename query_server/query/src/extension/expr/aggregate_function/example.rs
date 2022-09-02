use std::sync::Arc;

use datafusion::{
    arrow::datatypes::DataType,
    logical_expr::{AggregateUDF, Volatility},
    logical_plan::create_udaf,
    physical_plan::expressions::AvgAccumulator,
};
use spi::query::function::FunctionMetadataManager;
use spi::query::function::Result;

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
        Arc::new(|| Ok(Box::new(AvgAccumulator::try_new(&DataType::Float64)?))),
        Arc::new(vec![DataType::UInt64, DataType::Float64]),
    )
}
