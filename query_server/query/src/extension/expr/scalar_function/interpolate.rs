use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::logical_expr::type_coercion::aggregates::NUMERICS;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, TypeSignature,
    Volatility,
};
use models::arrow::DataType;
use spi::query::function::FunctionMetadataManager;
use spi::QueryResult;

use super::INTERPOLATE;

pub fn register_udf(func_manager: &mut dyn FunctionMetadataManager) -> QueryResult<()> {
    func_manager.register_udf(ScalarUDF::new_from_impl(InterpolateFunc::new()))?;
    Ok(())
}

#[derive(Debug)]
pub struct InterpolateFunc {
    signature: Signature,
}

impl InterpolateFunc {
    pub fn new() -> Self {
        let signatures = NUMERICS
            .iter()
            .map(|t| TypeSignature::Exact(vec![t.clone()]))
            .collect();
        Self {
            signature: Signature::one_of(signatures, Volatility::Volatile),
        }
    }
}

impl ScalarUDFImpl for InterpolateFunc {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        INTERPOLATE
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(arg_types[0].clone())
    }

    fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        Err(DataFusionError::NotImplemented(format!(
            "{INTERPOLATE} is not yet implemented"
        )))
    }
}
