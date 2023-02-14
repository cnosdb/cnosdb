use std::sync::Arc;

use datafusion::arrow::array::ArrayRef;
use datafusion::arrow::datatypes::DataType;
use datafusion::error::DataFusionError;
use datafusion::logical_expr::type_coercion::aggregates::{DATES, NUMERICS, STRINGS, TIMESTAMPS};
use datafusion::logical_expr::{
    ReturnTypeFunction, ScalarUDF, Signature, TypeSignature, Volatility,
};
use datafusion::physical_expr::functions::make_scalar_function;
use spi::query::function::FunctionMetadataManager;
use spi::Result;

use super::BOTTOM;

pub fn register_udf(func_manager: &mut dyn FunctionMetadataManager) -> Result<ScalarUDF> {
    let udf = new();
    func_manager.register_udf(udf.clone())?;
    Ok(udf)
}

fn new() -> ScalarUDF {
    let func = |_: &[ArrayRef]| {
        Err(DataFusionError::Execution(format!(
            "{} has no specific implementation, should be converted to topk operator.",
            BOTTOM
        )))
    };
    let func = make_scalar_function(func);

    // Accept any numeric value paired with a Int64 k
    let type_signatures = STRINGS
        .iter()
        .chain(NUMERICS.iter())
        .chain(TIMESTAMPS.iter())
        .chain(DATES.iter())
        // .chain(TIMES.iter())
        .map(|t| TypeSignature::Exact(vec![t.clone(), DataType::Int64]))
        .collect();

    let signature = Signature::one_of(type_signatures, Volatility::Immutable);

    let return_type: ReturnTypeFunction =
        Arc::new(move |input_expr_types| Ok(Arc::new(input_expr_types[0].clone())));

    ScalarUDF::new(BOTTOM, &signature, &return_type, &func)
}
