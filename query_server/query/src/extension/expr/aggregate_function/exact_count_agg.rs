use std::sync::Arc;

use datafusion::arrow::datatypes::DataType;
use datafusion::logical_expr::{create_udaf, AggregateUDF, Volatility};
use datafusion::physical_plan::expressions::AvgAccumulator;
use spi::query::function::FunctionMetadataManager;
use spi::QueryResult;

use super::EXACT_COUNT_STAR_UDAF_NAME;

pub fn register_udaf(func_manager: &mut dyn FunctionMetadataManager) -> QueryResult<AggregateUDF> {
    let udf = new();
    func_manager.register_udaf(udf.clone())?;
    Ok(udf)
}

fn new() -> AggregateUDF {
    create_udaf(
        EXACT_COUNT_STAR_UDAF_NAME,
        DataType::Null,
        Arc::new(DataType::Int64),
        Volatility::Immutable,
        Arc::new(|_, _| {
            Ok(Box::new(AvgAccumulator::try_new(
                &DataType::Null,
                &DataType::Int64,
            )?))
        }),
        Arc::new(vec![DataType::Null, DataType::Int64]),
    )
}
