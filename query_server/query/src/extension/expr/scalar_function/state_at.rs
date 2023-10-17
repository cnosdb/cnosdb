use std::sync::Arc;

use datafusion::arrow::array::ArrayRef;
use datafusion::arrow::datatypes::DataType;
use datafusion::error::DataFusionError;
use datafusion::logical_expr::type_coercion::aggregates::TIMESTAMPS;
use datafusion::logical_expr::{ReturnTypeFunction, ScalarUDF, Signature, Volatility};
use datafusion::physical_expr::functions::make_scalar_function;
use datafusion::scalar::ScalarValue;
use spi::query::function::FunctionMetadataManager;
use spi::{QueryError, Result};

use crate::extension::expr::aggregate_function::StateAggData;
use crate::extension::expr::scalar_function::STATE_AT;

pub fn register_udf(func_manager: &mut dyn FunctionMetadataManager) -> Result<ScalarUDF> {
    let udf = new();
    func_manager.register_udf(udf.clone())?;
    Ok(udf)
}

fn new() -> ScalarUDF {
    let return_type_fn: ReturnTypeFunction = Arc::new(|input| {
        let error = || DataFusionError::Execution("Get state_at ReturnTypeFunction error".into());
        if !TIMESTAMPS.iter().any(|d| d.eq(&input[1])) {
            return Err(error());
        }
        match &input[0] {
            DataType::Struct(f) => {
                let a = f.find("state_duration").ok_or_else(error)?.1;
                match a.data_type() {
                    DataType::List(f) => match f.data_type() {
                        DataType::Struct(f) => Ok(f
                            .find("state")
                            .ok_or_else(error)?
                            .1
                            .data_type()
                            .clone()
                            .into()),
                        _ => Err(error()),
                    },
                    _ => Err(error()),
                }
            }
            _ => Err(error()),
        }
    });

    let state_at = make_scalar_function(state_at_implement);

    ScalarUDF::new(
        STATE_AT,
        &Signature::any(2, Volatility::Immutable),
        &return_type_fn,
        &state_at,
    )
}

fn state_at_implement(input: &[ArrayRef]) -> Result<ArrayRef, DataFusionError> {
    let array_len = input[0].len();
    let mut res = Vec::with_capacity(array_len);
    for i in 0..array_len {
        let state_agg = ScalarValue::try_from_array(input[0].as_ref(), i)?;
        let ts = ScalarValue::try_from_array(input[1].as_ref(), i)?;
        let state_agg = StateAggData::try_from(state_agg)?;
        if state_agg.is_compact() {
            return Err(DataFusionError::External(Box::new(QueryError::Analyzer {err:
            "duration_in(state_agg, state, start_time, interval) doesn't support compact_agg".into()})));
        }
        let value = state_agg.state_at(&ts)?;
        res.push(value)
    }
    let array = ScalarValue::iter_to_array(res)?;
    Ok(array)
}
