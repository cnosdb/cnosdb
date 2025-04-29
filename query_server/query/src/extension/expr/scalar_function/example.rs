use std::sync::Arc;

use datafusion::arrow::datatypes::DataType;
use datafusion::logical_expr::{ColumnarValue, ScalarUDF, Volatility};
use datafusion::prelude::create_udf;
use spi::query::function::FunctionMetadataManager;
use spi::QueryResult;

pub fn register_udf(func_manager: &mut dyn FunctionMetadataManager) -> QueryResult<ScalarUDF> {
    let udf = new();
    func_manager.register_udf(udf.clone())?;
    Ok(udf)
}

fn new() -> ScalarUDF {
    let myfunc = Arc::new(|args: &[ColumnarValue]| {
        let ColumnarValue::Array(array) = &args[0] else {
            panic!("should be array")
        };
        Ok(ColumnarValue::from(Arc::clone(array)))
    });

    create_udf(
        "MY_FUNC",
        vec![DataType::Int32],
        DataType::Int32,
        Volatility::Immutable,
        myfunc,
    )
}
