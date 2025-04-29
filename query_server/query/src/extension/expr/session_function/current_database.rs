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
    df_session_ctx.register_udf(ScalarUDF::new_from_impl(CurrentDatabaseFunc::new(
        context.database().to_string(),
    )));
}

#[derive(Debug)]
pub struct CurrentDatabaseFunc {
    signature: Signature,
    current_database: ColumnarValue,
}

impl CurrentDatabaseFunc {
    pub fn new(current_database: String) -> Self {
        Self {
            signature: Signature::any(0, Volatility::Immutable),
            current_database: ColumnarValue::Array(Arc::new(StringArray::from(vec![
                current_database,
            ]))),
        }
    }
}

impl ScalarUDFImpl for CurrentDatabaseFunc {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        "current_database"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        Ok(self.current_database.clone())
    }
}
