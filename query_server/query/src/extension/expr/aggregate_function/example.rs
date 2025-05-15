use std::sync::Arc;

use datafusion::arrow::datatypes::DataType;
use datafusion::functions_aggregate::average::AvgAccumulator;
use datafusion::logical_expr::{create_udaf, AggregateUDF, Volatility};
use spi::query::function::FunctionMetadataManager;
use spi::QueryResult;

pub fn register_udaf(func_manager: &mut dyn FunctionMetadataManager) -> QueryResult<AggregateUDF> {
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
        Arc::new(|_, _| {
            Ok(Box::new(AvgAccumulator::try_new(
                &DataType::Float64,
                &DataType::Float64,
            )?))
        }),
        Arc::new(vec![DataType::UInt64, DataType::Float64]),
    )
}
