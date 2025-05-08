use std::sync::Arc;

use datafusion::logical_expr::type_coercion::aggregates::{NUMERICS, TIMESTAMPS};
use datafusion::logical_expr::{
    ReturnTypeFunction, ScalarUDF, Signature, TypeSignature, Volatility,
};
use models::arrow::DataType;
use serde::Deserialize;
use spi::DFResult;

use crate::extension::expr::scalar_function::unimplemented_scalar_impl;

pub fn full_signatures() -> Signature {
    let mut signatures = Vec::new();
    for t in TIMESTAMPS {
        for v in NUMERICS {
            signatures.push(TypeSignature::Exact(vec![t.clone(), v.clone()]));
            signatures.push(TypeSignature::Exact(vec![
                t.clone(),
                v.clone(),
                DataType::Utf8,
            ]));
        }
    }
    Signature::one_of(signatures, Volatility::Immutable)
}

pub fn get_arg<T: Default + for<'a> Deserialize<'a>>(arg_str: Option<&str>) -> DFResult<T> {
    Ok(if let Some(arg_str) = arg_str {
        serde_urlencoded::from_str(arg_str).map_err(|err| {
            datafusion::error::DataFusionError::Execution(format!("Fail to parse argument: {err}"))
        })?
    } else {
        T::default()
    })
}
