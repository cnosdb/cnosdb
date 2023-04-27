use std::sync::Arc;

use datafusion::arrow::array::ArrayRef;
use datafusion::arrow::datatypes::{DataType, Field, TimeUnit};
use datafusion::error::DataFusionError;
use datafusion::logical_expr::type_coercion::aggregates::TIMESTAMPS;
use datafusion::logical_expr::{
    ReturnTypeFunction, ScalarUDF, Signature, TypeSignature, Volatility,
};
use datafusion::physical_expr::functions::make_scalar_function;
use spi::query::function::FunctionMetadataManager;
use spi::Result;

use super::{TIME_WINDOW, WINDOW_END, WINDOW_START};
use crate::extension::expr::INTERVALS;

pub fn register_udf(func_manager: &mut dyn FunctionMetadataManager) -> Result<ScalarUDF> {
    let udf = new();
    func_manager.register_udf(udf.clone())?;
    Ok(udf)
}

fn new() -> ScalarUDF {
    let func = |_: &[ArrayRef]| {
        Err(DataFusionError::Execution(format!(
            "{} has no specific implementation, should be converted to Expand operator.",
            TIME_WINDOW
        )))
    };
    let func = make_scalar_function(func);

    // time_window
    // - timeColumn
    // - windowDuration
    // - slideDuration
    // - startTime
    //
    // group by time_window(time, interval '10 second') => group by time_window(time, interval '10 second', interval '5 second', '1970-01-01T00:00:00.000Z')
    // group by time_window(time, interval '10 second', interval '5 second') => group by time_window(time, interval '10 second', interval '5 second', '1970-01-01T00:00:00.000Z')
    // group by time_window(time, interval '10 second', interval '5 second', '1999-12-31T00:00:00.000Z')
    let type_signatures = TIMESTAMPS
        .iter()
        .flat_map(|first| {
            INTERVALS.iter().flat_map(|second| {
                INTERVALS
                    .iter()
                    .flat_map(|third| {
                        [
                            TypeSignature::Exact(vec![
                                first.clone(),
                                second.clone(),
                                third.clone(),
                            ]),
                            TypeSignature::Exact(vec![
                                first.clone(),
                                second.clone(),
                                third.clone(),
                                DataType::Timestamp(TimeUnit::Nanosecond, None),
                            ]),
                        ]
                    })
                    .chain([TypeSignature::Exact(vec![first.clone(), second.clone()])])
            })
        })
        .collect();

    let signature = Signature::one_of(type_signatures, Volatility::Immutable);

    // Struct(_start, _end)
    let return_type: ReturnTypeFunction = Arc::new(move |input_expr_types| {
        let window = DataType::Struct(vec![
            Field::new(WINDOW_START, input_expr_types[0].clone(), false),
            Field::new(WINDOW_END, input_expr_types[0].clone(), false),
        ]);

        Ok(Arc::new(window))
    });

    ScalarUDF::new(TIME_WINDOW, &signature, &return_type, &func)
}
