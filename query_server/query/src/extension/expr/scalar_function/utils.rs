#[macro_export]
macro_rules! object_accessor {
    ($OBJECT:ident, $FUNC:ident) => {{
        use std::sync::Arc;

        use datafusion::logical_expr::{
            ReturnTypeFunction, ScalarFunctionImplementation, ScalarUDF, Signature, Volatility,
        };
        use datafusion::physical_plan::ColumnarValue;
        use datafusion::scalar::ScalarValue;
        use $crate::extension::expr::aggregate_function::$OBJECT;

        let return_type_fn: ReturnTypeFunction = Arc::new(|args| {
            let null_data = $OBJECT::try_from_scalar(ScalarValue::try_from(&args[0])?)?;
            let output = null_data.$FUNC()?.data_type();
            Ok(Arc::new(output))
        });

        let fun: ScalarFunctionImplementation = Arc::new(|input| {
            let value = input[0].clone();

            match value {
                ColumnarValue::Scalar(scalar) => {
                    let data = $OBJECT::try_from_scalar(scalar)?;
                    Ok(ColumnarValue::Scalar(data.$FUNC()?))
                }
                ColumnarValue::Array(array) => {
                    if array.is_empty() {
                        let null = ScalarValue::try_from(array.data_type())?;
                        let data = $OBJECT::try_from_scalar(null)?;
                        return Ok(ColumnarValue::Scalar(data.$FUNC()?));
                    }

                    let len = array.len();
                    let mut outputs = Vec::with_capacity(len);
                    for idx in 0..len {
                        let scalar = ScalarValue::try_from_array(array.as_ref(), idx)?;
                        let data = $OBJECT::try_from_scalar(scalar)?;
                        outputs.push(data.$FUNC()?);
                    }

                    let output_array = ScalarValue::iter_to_array(outputs)?;
                    Ok(ColumnarValue::Array(output_array))
                }
            }
        });

        ScalarUDF::new(
            stringify!($FUNC),
            &Signature::any(1, Volatility::Immutable),
            &return_type_fn,
            &fun,
        )
    }};
}
