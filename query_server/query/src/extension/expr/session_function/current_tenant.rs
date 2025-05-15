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
    df_session_ctx.register_udf(CurrentTenantFunc::new(context.tenant().to_string()));
}

pub struct CurrentTenantFunc {
    signature: Signature,
    current_tenant: ArrayRef,
}

impl CurrentTenantFunc {
    pub fn new(current_tenant: String) -> Self {
        Self {
            signature: Signature::any(0, Volatility::Immutable),
            current_tenant: Arc::new(ScalarValue::Utf8(Some(current_tenant)).to_array()),
        }
    }
}

impl ScalarUDFImpl for CurrentTenantFunc {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        "current_tenant"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        Ok(self.current_tenant.clone())
    }
}
