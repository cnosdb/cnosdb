use std::sync::Arc;

use datafusion::{
    arrow::{array::ArrayRef, datatypes::DataType},
    logical_expr::{ScalarUDF, Volatility},
    physical_expr::functions::make_scalar_function,
    prelude::create_udf,
};

use crate::function::*;

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
