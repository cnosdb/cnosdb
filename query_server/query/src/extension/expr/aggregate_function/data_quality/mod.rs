use std::sync::Arc;

use datafusion::logical_expr::type_coercion::aggregates::{NUMERICS, TIMESTAMPS};
use datafusion::logical_expr::{
    AccumulatorFactoryFunction, AggregateUDF, ReturnTypeFunction, Signature, StateTypeFunction,
    TypeSignature, Volatility,
};
use models::arrow::{DataType, Field};
use spi::query::function::FunctionMetadataManager;
use spi::Result;

use self::accumulator::DataQualityAccumulator;
use self::common::DataQualityFunction;

mod accumulator;
mod common;

pub fn register_udafs(func_manager: &mut dyn FunctionMetadataManager) -> Result<()> {
    func_manager.register_udaf(new(DataQualityFunction::Completeness))?;
    func_manager.register_udaf(new(DataQualityFunction::Consistency))?;
    func_manager.register_udaf(new(DataQualityFunction::Timeliness))?;
    func_manager.register_udaf(new(DataQualityFunction::Validity))?;
    Ok(())
}

fn new(func: DataQualityFunction) -> AggregateUDF {
    let return_type_func: ReturnTypeFunction = Arc::new(|_| Ok(Arc::new(DataType::Float64)));

    let type_signatures: Vec<_> = NUMERICS
        .iter()
        .flat_map(|t| {
            TIMESTAMPS
                .iter()
                .map(|s_t| TypeSignature::Exact(vec![s_t.clone(), t.clone()]))
        })
        .collect();

    let state_type_func: StateTypeFunction = Arc::new(move |_, _| {
        let times = DataType::List(Arc::new(Field::new("item", DataType::Float64, true)));
        let values = DataType::List(Arc::new(Field::new("item", DataType::Float64, true)));

        Ok(Arc::new(vec![times, values]))
    });

    let accumulator: AccumulatorFactoryFunction =
        Arc::new(move |_, _| Ok(Box::new(DataQualityAccumulator::new(func))));

    AggregateUDF::new(
        func.name(),
        &Signature::one_of(type_signatures, Volatility::Immutable),
        &return_type_func,
        &accumulator,
        &state_type_func,
    )
}
