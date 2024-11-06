use std::sync::Arc;

use datafusion::arrow::datatypes::DataType;
use datafusion::logical_expr::aggregate_function::{self, signature};
use datafusion::logical_expr::{
    AccumulatorFactoryFunction, AggregateUDF, ReturnTypeFunction, StateTypeFunction,
};
use datafusion::physical_plan::expressions::AvgAccumulator;
use spi::query::function::FunctionMetadataManager;
use spi::QueryResult;

use super::EXACT_COUNT_UDAF_NAME;

pub fn register_udaf(func_manager: &mut dyn FunctionMetadataManager) -> QueryResult<AggregateUDF> {
    let udf = new();
    func_manager.register_udaf(udf.clone())?;
    Ok(udf)
}

fn new() -> AggregateUDF {
    let signature = signature(&aggregate_function::AggregateFunction::Count);
    let return_type: ReturnTypeFunction = Arc::new(move |_| Ok(Arc::new(DataType::Int64)));
    let accumulator: AccumulatorFactoryFunction = Arc::new(|_, _| {
        Ok(Box::new(AvgAccumulator::try_new(
            &DataType::Utf8,
            &DataType::Int64,
        )?))
    });
    let state_type: StateTypeFunction = Arc::new(move |_, _| Ok(Arc::new(vec![DataType::Int64])));

    AggregateUDF::new(
        EXACT_COUNT_UDAF_NAME,
        &signature,
        &return_type,
        &accumulator,
        &state_type,
    )
}
