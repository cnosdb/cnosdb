use datafusion::arrow::array::ArrayRef;
use datafusion::arrow::datatypes::DataType;
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::logical_expr::type_coercion::aggregates::TIMESTAMPS;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use datafusion::scalar::ScalarValue;
use spi::query::function::FunctionMetadataManager;
use spi::{AnalyzerSnafu, QueryResult};

use crate::extension::expr::aggregate_function::StateAggData;
use crate::extension::expr::scalar_function::STATE_AT;

pub fn register_udf(func_manager: &mut dyn FunctionMetadataManager) -> QueryResult<ScalarUDF> {
    let udf = StateAtFunc::new();
    func_manager.register_udf(udf.clone())?;
    Ok(udf)
}

fn state_at_implement(input: &[ArrayRef]) -> Result<ArrayRef, DataFusionError> {
    let array_len = input[0].len();
    let mut res = Vec::with_capacity(array_len);
    for i in 0..array_len {
        let state_agg = ScalarValue::try_from_array(input[0].as_ref(), i)?;
        let ts = ScalarValue::try_from_array(input[1].as_ref(), i)?;
        let state_agg = StateAggData::try_from(state_agg)?;
        if state_agg.is_compact() {
            return Err(DataFusionError::External(Box::new(AnalyzerSnafu {err:
            "duration_in(state_agg, state, start_time, interval) doesn't support compact_agg".to_string()}.build())));
        }
        let value = state_agg.state_at(&ts)?;
        res.push(value)
    }
    let array = ScalarValue::iter_to_array(res)?;
    Ok(array)
}

pub struct StateAtFunc {
    signature: Signature,
}

impl StateAtFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::any(2, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for StateAtFunc {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        STATE_AT
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> DFResult<DataType> {
        let error = || DataFusionError::Execution("Get state_at ReturnTypeFunction error".into());
        if !TIMESTAMPS.iter().any(|d| d.eq(&arg_types[1])) {
            return Err(error());
        }
        match &arg_types[0] {
            DataType::Struct(f) => {
                let a = f.find("state_duration").ok_or_else(error)?.1;
                match a.data_type() {
                    DataType::List(f) => match f.data_type() {
                        DataType::Struct(f) => {
                            let (_, state_field) = f.find("state").ok_or_else(error)?;
                            Ok(state_field.data_type().clone())
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
        let input = ColumnarValue::values_to_arrays(args)?;
        let array_len = input[0].len();
        let mut res = Vec::with_capacity(array_len);
        for i in 0..array_len {
            let state_agg = ScalarValue::try_from_array(input[0].as_ref(), i)?;
            let ts = ScalarValue::try_from_array(input[1].as_ref(), i)?;
            let state_agg = StateAggData::try_from(state_agg)?;
            if state_agg.is_compact() {
                return Err(DataFusionError::External(Box::new(
                    AnalyzerSnafu {
                        err: "duration_in(state_agg, state, start_time, interval) doesn't support compact_agg".to_string()
                    }.build()
                )));
            }
            let value = state_agg.state_at(&ts)?;
            res.push(value)
        }
        let array = ScalarValue::iter_to_array(res)?;
        Ok(array)
    }
}
