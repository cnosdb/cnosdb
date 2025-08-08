use datafusion::arrow::array::Array;
use datafusion::arrow::datatypes::DataType;
use datafusion::common::ScalarValue;
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::logical_expr::type_coercion::aggregates::TIMESTAMPS;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, TypeSignature,
    Volatility,
};
use spi::query::function::FunctionMetadataManager;
use spi::{AnalyzerSnafu, QueryResult};

use crate::extension::expr::aggregate_function::StateAggData;
use crate::extension::expr::scalar_function::DURATION_IN;
use crate::extension::expr::INTERVALS;

pub fn register_udf(func_manager: &mut dyn FunctionMetadataManager) -> QueryResult<()> {
    func_manager.register_udf(ScalarUDF::new_from_impl(DurationInFunc::new()))?;
    Ok(())
}

#[derive(Debug)]
pub struct DurationInFunc {
    signature: Signature,
}

impl DurationInFunc {
    pub fn new() -> Self {
        let signature = vec![
            TypeSignature::Any(2),
            TypeSignature::Any(3),
            TypeSignature::Any(4),
        ];
        Self {
            signature: Signature::one_of(signature, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for DurationInFunc {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        DURATION_IN
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> DFResult<DataType> {
        if arg_types.len() >= 3 && !TIMESTAMPS.iter().any(|t| t.eq(&arg_types[2])) {
            return Err(DataFusionError::External(Box::new(
                AnalyzerSnafu {
                    err: format!("Expect Timestamp type, but found {} type.", &arg_types[2]),
                }
                .build(),
            )));
        }

        if arg_types.len() == 4 && !INTERVALS.iter().any(|t| t.eq(&arg_types[3])) {
            return Err(DataFusionError::External(Box::new(
                AnalyzerSnafu {
                    err: format!("Expect Interval type, but found {} type.", &arg_types[3]),
                }
                .build(),
            )));
        }

        let error =
            || DataFusionError::Execution("Get duration_in ReturnTypeFunction error".into());

        match &arg_types[0] {
            DataType::Struct(f) => {
                let (_, state_duration_field) = f.find("state_duration").ok_or_else(error)?;
                match state_duration_field.data_type() {
                    DataType::List(f) => match f.data_type() {
                        DataType::Struct(f) => {
                            let (_, duration_field) = f.find("duration").ok_or_else(error)?;
                            Ok(duration_field.data_type().clone())
                        }
                        _ => Err(error()),
                    },
                    _ => Err(error()),
                }
            }
            _ => Err(error()),
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let input = ColumnarValue::values_to_arrays(&args.args)?;
        let array_len = input[0].len();
        let mut res = Vec::with_capacity(array_len);
        match input.len() {
            // duration_in(state_agg, state)
            2 => {
                for i in 0..array_len {
                    let state_agg = ScalarValue::try_from_array(input[0].as_ref(), i)?;
                    let state = ScalarValue::try_from_array(input[1].as_ref(), i)?;
                    let state_agg = StateAggData::try_from(state_agg)?;
                    let value =
                        state_agg.duration_in(state, ScalarValue::Null, ScalarValue::Null)?;
                    res.push(value)
                }
            }
            // duration_in(state_agg, state, start_time)
            3 => {
                for i in 0..array_len {
                    let state_agg = ScalarValue::try_from_array(input[0].as_ref(), i)?;
                    let state = ScalarValue::try_from_array(input[1].as_ref(), i)?;
                    let start = ScalarValue::try_from_array(input[2].as_ref(), i)?;
                    let state_agg = StateAggData::try_from(state_agg)?;
                    if state_agg.is_compact() {
                        return Err(DataFusionError::External(Box::new(AnalyzerSnafu {
                        err:
                            "duration_in(state_agg, state, start_time) doesn't support compact_agg"
                                .to_string(),
                    }.build())));
                    }
                    let value = state_agg.duration_in(state, start, ScalarValue::Null)?;
                    res.push(value)
                }
            }
            // duration_in(state_agg, state, start_time, interval)
            4 => {
                for i in 0..array_len {
                    let state_agg = ScalarValue::try_from_array(input[0].as_ref(), i)?;
                    let state = ScalarValue::try_from_array(input[1].as_ref(), i)?;
                    let start = ScalarValue::try_from_array(input[2].as_ref(), i)?;
                    let interval = ScalarValue::try_from_array(input[3].as_ref(), i)?;
                    let state_agg = StateAggData::try_from(state_agg)?;
                    if state_agg.is_compact() {
                        return Err(DataFusionError::External(Box::new(
                            AnalyzerSnafu {
                                err: "duration_in(state_agg, state, start_time, interval) doesn't support compact_agg".to_string()
                            }.build()
                        )));
                    }
                    let value = state_agg.duration_in(state, start, interval)?;
                    res.push(value)
                }
            }
            _ => {
                return Err(DataFusionError::NotImplemented(
                    "duration in only support 2 arguments".into(),
                ));
            }
        }
        let array = ScalarValue::iter_to_array(res)?;
        Ok(ColumnarValue::Array(array))
    }
}
