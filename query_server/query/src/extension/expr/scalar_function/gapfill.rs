use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, TimeUnit};
use datafusion::logical_expr::{ReturnTypeFunction, ScalarUDF, Volatility};
use spi::query::function::FunctionMetadataManager;
use spi::Result;

use super::super::time_window_signature;
use super::{unimplemented_scalar_impl, TIME_WINDOW_GAPFILL};

pub fn register_udf(func_manager: &mut dyn FunctionMetadataManager) -> Result<ScalarUDF> {
    let udf = new();
    func_manager.register_udf(udf.clone())?;
    Ok(udf)
}

fn new() -> ScalarUDF {
    // TIME_WINDOW_GAPFILL should have the same signature as DATE_BIN,
    // so that just adding _GAPFILL can turn a query into a gap-filling query.
    let mut signatures = time_window_signature();
    // We don't want this to be optimized away before we can give a helpful error message
    signatures.volatility = Volatility::Volatile;

    let return_type_fn: ReturnTypeFunction =
        Arc::new(|_| Ok(Arc::new(DataType::Timestamp(TimeUnit::Nanosecond, None))));
    ScalarUDF::new(
        TIME_WINDOW_GAPFILL,
        &signatures,
        &return_type_fn,
        &unimplemented_scalar_impl(TIME_WINDOW_GAPFILL),
    )
}
