use std::sync::Arc;

use datafusion::arrow::array::StringArray;
use datafusion::arrow::datatypes::DataType;
use datafusion::error::Result as DFResult;
use datafusion::execution::context::SessionContext;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use datafusion::scalar::ScalarValue;
use spi::service::protocol::Context;

pub fn register_session_udf(df_session_ctx: &SessionContext, context: &Context) {
    df_session_ctx.register_udf(ScalarUDF::new_from_impl(CurrentRoleFunc::new(
        context.user().role().map(|x| x.name().to_string()),
    )));
}

#[derive(Debug)]
pub struct CurrentRoleFunc {
    signature: Signature,
    current_role: ColumnarValue,
}

impl CurrentRoleFunc {
    pub fn new(current_role: Option<String>) -> Self {
        Self {
            signature: Signature::any(0, Volatility::Immutable),
            current_role: match current_role {
                Some(role) => ColumnarValue::Array(Arc::new(StringArray::from(vec![role]))),
                None => ColumnarValue::Scalar(ScalarValue::Null),
            },
        }
    }
}

impl ScalarUDFImpl for CurrentRoleFunc {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        "current_role"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        Ok(self.current_role.clone())
    }
}
