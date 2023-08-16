mod accum_algorithm;
mod stats_agg_accumulator_2d;

use std::sync::Arc;

pub(crate) use accum_algorithm::{m3, m4};
use datafusion::arrow::datatypes::DataType;
use datafusion::logical_expr::type_coercion::aggregates::NUMERICS;
use datafusion::logical_expr::{
    AccumulatorFactoryFunction, AggregateUDF, ReturnTypeFunction, Signature, StateTypeFunction,
    TypeSignature, Volatility,
};
use spi::query::function::FunctionMetadataManager;
use spi::QueryError;
use stats_agg_accumulator_2d::StatsAggAccumulator2D;

use crate::extension::expr::aggregate_function::STATS_AGG_UDAF_NAME;

pub fn register_udafs(func_manager: &mut dyn FunctionMetadataManager) -> Result<(), QueryError> {
    func_manager.register_udaf(new())?;
    Ok(())
}

fn signature() -> Signature {
    let type_signatures: Vec<TypeSignature> = NUMERICS
        .iter()
        .flat_map(|x_type| {
            NUMERICS
                .iter()
                .map(|y_type| TypeSignature::Exact(vec![x_type.clone(), y_type.clone()]))
        })
        .collect();
    Signature::one_of(type_signatures, Volatility::Immutable)
}

fn new() -> AggregateUDF {
    let return_type: ReturnTypeFunction =
        Arc::new(|_: &[DataType]| Ok(Arc::new(StatsAggAccumulator2D::get_datatype())));
    let state_type: StateTypeFunction = Arc::new(|_: &[DataType], _: &DataType| {
        Ok(Arc::new(vec![
            DataType::UInt64,
            DataType::Float64,
            DataType::Float64,
            DataType::Float64,
            DataType::Float64,
            DataType::Float64,
            DataType::Float64,
            DataType::Float64,
            DataType::Float64,
            DataType::Float64,
        ]))
    });
    let accumulator: AccumulatorFactoryFunction =
        Arc::new(|_: &[DataType], _: &DataType| Ok(Box::<StatsAggAccumulator2D>::default()));

    AggregateUDF::new(
        STATS_AGG_UDAF_NAME,
        &signature(),
        &return_type,
        &accumulator,
        &state_type,
    )
}
