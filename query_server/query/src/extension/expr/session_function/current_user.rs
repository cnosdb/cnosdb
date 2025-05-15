use std::sync::Arc;

use datafusion::arrow::array::ArrayRef;
use datafusion::arrow::datatypes::DataType;
use datafusion::error::Result as DFResult;
use datafusion::execution::context::SessionContext;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};
use datafusion::scalar::ScalarValue;
use spi::service::protocol::Context;

pub fn register_session_udf(df_session_ctx: &SessionContext, context: &Context) {
    df_session_ctx.register_udf(CurrentUserFunc::new(
        context.user().desc().name().to_string(),
    ));
}

pub struct CurrentUserFunc {
    signature: Signature,
    current_user: ArrayRef,
}

impl CurrentUserFunc {
    pub fn new(current_user: String) -> Self {
        Self {
            signature: Signature::any(0, Volatility::Immutable),
            current_user: Arc::new(ScalarValue::Utf8(Some(current_user)).to_array()),
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

    fn return_type(&self, arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        Ok(self.current_user.clone())
    }
}
