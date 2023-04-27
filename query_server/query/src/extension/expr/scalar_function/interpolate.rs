use std::sync::Arc;

use datafusion::logical_expr::type_coercion::aggregates::NUMERICS;
use datafusion::logical_expr::{
    ReturnTypeFunction, ScalarUDF, Signature, TypeSignature, Volatility,
};
use spi::query::function::FunctionMetadataManager;
use spi::Result;

use super::{unimplemented_scalar_impl, INTERPOLATE};

pub fn register_udf(func_manager: &mut dyn FunctionMetadataManager) -> Result<ScalarUDF> {
    let udf = new();
    func_manager.register_udf(udf.clone())?;
    Ok(udf)
}

fn new() -> ScalarUDF {
    let return_type_fn: ReturnTypeFunction = Arc::new(|args| Ok(Arc::new(args[0].clone())));
    let signatures = NUMERICS
        .iter()
        .map(|t| TypeSignature::Exact(vec![t.clone()]))
        .collect();
    ScalarUDF::new(
        INTERPOLATE,
        &Signature::one_of(signatures, Volatility::Volatile),
        &return_type_fn,
        &unimplemented_scalar_impl(INTERPOLATE),
    )
}
