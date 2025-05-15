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
    df_session_ctx.register_udf(CurrentRoleFunc::new(
        context.user().role().map(|x| x.name().to_string()),
    ));
}

pub struct CurrentRoleFunc {
    signature: Signature,
    current_role: ArrayRef,
}

impl CurrentRoleFunc {
    pub fn new(current_role: Option<String>) -> Self {
        Self {
            signature: Signature::any(0, Volatility::Immutable),
            current_role: Arc::new(
                match current_role {
                    Some(role) => ScalarValue::Utf8(Some(role)),
                    None => ScalarValue::Null,
                }
                .to_array(),
            ),
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

    fn return_type(&self, arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        Ok(self.current_role.clone())
    }
}
