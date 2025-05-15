use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field};
use datafusion::logical_expr::type_coercion::aggregates::{STRINGS, TIMESTAMPS};
use datafusion::logical_expr::{
    AccumulatorFactoryFunction, AggregateUDF, ReturnTypeFunction, Signature, StateTypeFunction,
    TypeSignature, Volatility,
};
use spi::query::function::FunctionMetadataManager;
use spi::QueryError;
pub use state_agg_data::StateAggData;

use super::AggResult;
use crate::extension::expr::aggregate_function::state_agg::state_agg_accumulator::StateAggAccumulator;
use crate::extension::expr::aggregate_function::{
    COMPACT_STATE_AGG_UDAF_NAME, STATE_AGG_UDAF_NAME,
};
use crate::extension::expr::INTEGERS;
mod state_agg_accumulator;
mod state_agg_data;

const LIST_ELEMENT_NAME: &str = "item";

pub fn register_udafs(func_manager: &mut dyn FunctionMetadataManager) -> Result<(), QueryError> {
    func_manager.register_udaf(new_state_agg(true))?;
    func_manager.register_udaf(new_state_agg(false))?;
    Ok(())
}
pub fn new_state_agg(compact: bool) -> AggregateUDF {
    let func_name = if compact {
        COMPACT_STATE_AGG_UDAF_NAME
    } else {
        STATE_AGG_UDAF_NAME
    };

    let return_type_func: ReturnTypeFunction = Arc::new(move |input| {
        let result = StateAggData::new(input[0].clone(), input[1].clone(), compact);
        let date_type = result.to_scalar()?.data_type();

        trace::trace!("return_type: {:?}", date_type);

        Ok(Arc::new(date_type))
    });

    let state_type_func: StateTypeFunction = Arc::new(move |input, _| {
        let types = input
            .iter()
            .map(|dt| DataType::List(Arc::new(Field::new(LIST_ELEMENT_NAME, dt.clone(), true))))
            .collect::<Vec<_>>();

        trace::trace!("state_type: {:?}", types);

        Ok(Arc::new(types))
    });

    let accumulator: AccumulatorFactoryFunction = Arc::new(move |input, _| {
        Ok(Box::new(StateAggAccumulator::try_new(
            input.to_vec(),
            compact,
        )?))
    });

    // [compact_]state_agg(
    //   ts TIMESTAMPTZ,
    //   value {STRINGS | INTEGERS}
    // ) RETURNS StateAggData
    let type_signatures = STRINGS
        .iter()
        .chain(INTEGERS.iter())
        .flat_map(|t| {
            TIMESTAMPS
                .iter()
                .map(|s_t| TypeSignature::Exact(vec![s_t.clone(), t.clone()]))
        })
        .collect();

    AggregateUDF::new(
        func_name,
        &Signature::one_of(type_signatures, Volatility::Immutable),
        &return_type_func,
        &accumulator,
        &state_type_func,
    )
}
