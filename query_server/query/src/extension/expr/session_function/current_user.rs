use std::sync::Arc;

use datafusion::arrow::array::StringArray;
use datafusion::arrow::datatypes::DataType;
use datafusion::error::Result as DFResult;
use datafusion::execution::context::SessionContext;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use spi::service::protocol::Context;

pub fn register_session_udf(df_session_ctx: &SessionContext, context: &Context) {
    df_session_ctx.register_udf(ScalarUDF::new_from_impl(CurrentUserFunc::new(
        context.user().desc().name().to_string(),
    )));
}

#[derive(Debug)]
pub struct CurrentUserFunc {
    signature: Signature,
    current_user: ColumnarValue,
}

impl CurrentUserFunc {
    pub fn new(current_user: String) -> Self {
        Self {
            signature: Signature::any(0, Volatility::Immutable),
            current_user: ColumnarValue::Array(Arc::new(StringArray::from(vec![current_user]))),
        }
    }
}

impl ScalarUDFImpl for CurrentUserFunc {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        "current_user"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        Ok(self.current_user.clone())
    }
}
