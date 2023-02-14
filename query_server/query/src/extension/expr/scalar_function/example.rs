use std::sync::Arc;

use datafusion::arrow::array::ArrayRef;
use datafusion::arrow::datatypes::DataType;
use datafusion::logical_expr::{ScalarUDF, Volatility};
use datafusion::physical_expr::functions::make_scalar_function;
use datafusion::prelude::create_udf;
use spi::query::function::FunctionMetadataManager;
use spi::Result;

pub fn register_udf(func_manager: &mut dyn FunctionMetadataManager) -> Result<ScalarUDF> {
    let udf = new();
    func_manager.register_udf(udf.clone())?;
    Ok(udf)
}

fn new() -> ScalarUDF {
    let myfunc = |args: &[ArrayRef]| Ok(Arc::clone(&args[0]));
    let myfunc = make_scalar_function(myfunc);

    create_udf(
        "MY_FUNC",
        vec![DataType::Int32],
        Arc::new(DataType::Int32),
        Volatility::Immutable,
        myfunc,
    )
}
