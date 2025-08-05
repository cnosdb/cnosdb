use datafusion::arrow::datatypes::DataType;
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::logical_expr::type_coercion::aggregates::{DATES, NUMERICS, STRINGS, TIMESTAMPS};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, TypeSignature,
    Volatility,
};
use spi::query::function::FunctionMetadataManager;
use spi::QueryResult;

use super::TOPK;

pub fn register_udf(func_manager: &mut dyn FunctionMetadataManager) -> QueryResult<()> {
    func_manager.register_udf(ScalarUDF::new_from_impl(TopKFunc::new()))?;
    Ok(())
}

#[derive(Debug)]
pub struct TopKFunc {
    signature: Signature,
}

impl TopKFunc {
    pub fn new() -> Self {
        // Accept any numeric value paired with a Int64 k
        let type_signatures = STRINGS
            .iter()
            .chain(NUMERICS.iter())
            .chain(TIMESTAMPS.iter())
            .chain(DATES.iter())
            // .chain(iter::once(str_dict_data_type()))
            // .chain(TIMES.iter())
            .map(|t| TypeSignature::Exact(vec![t.clone(), DataType::Int64]))
            .collect();

        Self {
            signature: Signature::one_of(type_signatures, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for TopKFunc {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        TOPK
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(arg_types[0].clone())
    }

    fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        Err(DataFusionError::Execution(format!(
            "{TOPK} has no specific implementation, should be converted to topk operator.",
        )))
    }
}
