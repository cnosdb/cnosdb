use datafusion::arrow::datatypes::{DataType, TimeUnit};
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use spi::query::function::FunctionMetadataManager;
use spi::QueryResult;

use super::super::time_window_signature;
use super::TIME_WINDOW_GAPFILL;

pub fn register_udf(func_manager: &mut dyn FunctionMetadataManager) -> QueryResult<()> {
    func_manager.register_udf(ScalarUDF::new_from_impl(TimeWindowGapFillFunc::default()))?;
    Ok(())
}

#[derive(Debug)]
pub struct TimeWindowGapFillFunc {
    signature: Signature,
}

impl Default for TimeWindowGapFillFunc {
    fn default() -> Self {
        // TIME_WINDOW_GAPFILL should have the same signature as DATE_BIN,
        // so that just adding _GAPFILL can turn a query into a gap-filling query.
        let mut signatures = time_window_signature();
        // We don't want this to be optimized away before we can give a helpful error message
        signatures.volatility = Volatility::Volatile;
        Self {
            signature: signatures,
        }
    }
}

impl ScalarUDFImpl for TimeWindowGapFillFunc {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        TIME_WINDOW_GAPFILL
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Timestamp(TimeUnit::Nanosecond, None))
    }

    fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        Err(DataFusionError::NotImplemented(format!(
            "{TIME_WINDOW_GAPFILL} is not yet implemented"
        )))
    }
}
