use std::sync::Arc;

use datafusion::arrow::array::{downcast_array, BinaryArray, StringArray};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::Result as DFResult;
use datafusion::logical_expr::{
    ColumnarValue, ReturnTypeFunction, ScalarUDF, Signature, Volatility,
};
use geozero::wkb::Wkb;
use geozero::ToWkt;
use spi::query::function::FunctionMetadataManager;
use spi::QueryResult;

pub fn register_udf(func_manager: &mut dyn FunctionMetadataManager) -> QueryResult<ScalarUDF> {
    let udf = new();
    func_manager.register_udf(udf.clone())?;
    Ok(udf)
}

fn new() -> ScalarUDF {
    let signature = Signature::exact(vec![DataType::Binary], Volatility::Immutable);
    let return_type: ReturnTypeFunction = Arc::new(move |_| Ok(Arc::new(DataType::Utf8)));

    ScalarUDF::new("st_GeomFromWKB", &signature, &return_type, Arc::new(func))
}

fn func(args: &[ColumnarValue]) -> DFResult<ColumnarValue> {
    let args = ColumnarValue::values_to_arrays(args)?;
    let wkb_arr = args[0].as_ref();
    let wkb_arr = downcast_array::<BinaryArray>(wkb_arr);

    let result: StringArray = wkb_arr
        .iter()
        .map(|opt| {
            opt.and_then(|bytes| {
                let wkb = Wkb(bytes.to_vec());
                // conversion failed to null
                wkb.to_wkt().ok()
            })
        })
        .collect();

    Ok(Arc::new(result))
}
