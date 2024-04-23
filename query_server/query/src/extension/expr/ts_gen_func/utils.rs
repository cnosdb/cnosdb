use std::sync::Arc;

use datafusion::logical_expr::type_coercion::aggregates::{NUMERICS, TIMESTAMPS};
use datafusion::logical_expr::{
    ReturnTypeFunction, ScalarUDF, Signature, TypeSignature, Volatility,
};
use models::arrow::DataType;
use serde::Deserialize;
use spi::DFResult;

use crate::extension::expr::scalar_function::unimplemented_scalar_impl;

pub fn common_udf(name: &'static str) -> ScalarUDF {
    let return_type_func: ReturnTypeFunction = Arc::new(|args| {
        if args.len() < 2 || args.len() > 3 {
            Err(datafusion::error::DataFusionError::Plan(format!(
                "Expected 2 or 3 arguments, got {}",
                args.len()
            )))
        } else {
            Ok(Arc::new(args[1].clone()))
        }
    });

    let type_signatures = TIMESTAMPS
        .iter()
        .flat_map(|t| {
            NUMERICS.iter().flat_map(|v| {
                [
                    TypeSignature::Exact(vec![t.clone(), v.clone()]),
                    TypeSignature::Exact(vec![t.clone(), v.clone(), DataType::Utf8]),
                ]
            })
        })
        .collect();

    ScalarUDF::new(
        name,
        &Signature::one_of(type_signatures, Volatility::Immutable),
        &return_type_func,
        &unimplemented_scalar_impl(name),
    )
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
