#[macro_export]
macro_rules! object_accessor {
    ($OBJECT:ident, $FUNC:ident, $OBJECT_FUNC_IMPL:ident) => {{
        use datafusion::arrow::datatypes::DataType;
        use datafusion::logical_expr::{
            ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
        };
        use datafusion::scalar::ScalarValue;
        use $crate::extension::expr::aggregate_function::$OBJECT;

        #[derive(Debug)]
        struct $OBJECT_FUNC_IMPL {
            signature: datafusion::logical_expr::Signature,
        }

        impl $OBJECT_FUNC_IMPL {
            pub fn new() -> Self {
                Self {
                    signature: Signature::any(1, Volatility::Immutable),
                }
            }
        }

        impl ScalarUDFImpl for $OBJECT_FUNC_IMPL {
            fn as_any(&self) -> &dyn std::any::Any {
                self
            }

            fn name(&self) -> &str {
                stringify!($FUNC)
            }

            fn signature(&self) -> &Signature {
                &self.signature
            }

            fn return_type(&self, arg_types: &[DataType]) -> datafusion::error::Result<DataType> {
                let null_data =
                    $OBJECT::try_from_scalar(ScalarValue::try_new_null(&arg_types[0])?)?;
                let output = null_data.$FUNC()?.data_type();
                Ok(output)
            }

            fn invoke_with_args(
                &self,
                args: ScalarFunctionArgs,
            ) -> datafusion::error::Result<ColumnarValue> {
                let value = args.args[0].clone();
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
            }
        }

        ScalarUDF::new_from_impl($OBJECT_FUNC_IMPL::new())
    }};
}
