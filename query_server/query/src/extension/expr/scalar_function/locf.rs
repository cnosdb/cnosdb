use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use models::arrow::DataType;
use spi::query::function::FunctionMetadataManager;
use spi::QueryResult;

use super::LOCF;

pub fn register_udf(func_manager: &mut dyn FunctionMetadataManager) -> QueryResult<()> {
    func_manager.register_udf(ScalarUDF::new_from_impl(LocfFunc::new()))?;
    Ok(())
}

#[derive(Debug)]
pub struct LocfFunc {
    signature: Signature,
}

impl LocfFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::any(1, Volatility::Volatile),
        }
    }
}

impl ScalarUDFImpl for LocfFunc {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        LOCF
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(arg_types[0].clone())
    }

    fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        Err(DataFusionError::NotImplemented(format!(
            "{LOCF} is not yet implemented"
        )))
    }
}
