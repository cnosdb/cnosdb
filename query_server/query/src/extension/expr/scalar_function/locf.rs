use std::sync::Arc;

use datafusion::logical_expr::{ReturnTypeFunction, ScalarUDF, Signature, Volatility};
use spi::query::function::FunctionMetadataManager;
use spi::Result;

use super::{unimplemented_scalar_impl, LOCF};

pub fn register_udf(func_manager: &mut dyn FunctionMetadataManager) -> Result<ScalarUDF> {
    let udf = new();
    func_manager.register_udf(udf.clone())?;
    Ok(udf)
}

fn new() -> ScalarUDF {
    let return_type_fn: ReturnTypeFunction = Arc::new(|args| Ok(Arc::new(args[0].clone())));
    ScalarUDF::new(
        LOCF,
        &Signature::any(1, Volatility::Volatile),
        &return_type_fn,
        &unimplemented_scalar_impl(LOCF),
    )
}
