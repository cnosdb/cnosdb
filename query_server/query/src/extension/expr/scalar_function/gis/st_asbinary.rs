use std::sync::Arc;

use datafusion::arrow::array::{downcast_array, BinaryArray, StringArray};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::Result as DFResult;
use datafusion::logical_expr::{
    ColumnarValue, ReturnTypeFunction, ScalarUDF, Signature, Volatility,
};
use geozero::wkt::Wkt;
use geozero::{CoordDimensions, ToWkb};
use spi::query::function::FunctionMetadataManager;
use spi::QueryResult;

pub fn register_udf(func_manager: &mut dyn FunctionMetadataManager) -> QueryResult<ScalarUDF> {
    let udf = new();
    func_manager.register_udf(udf.clone())?;
    Ok(udf)
}

fn new() -> ScalarUDF {
    let signature = Signature::exact(vec![DataType::Utf8], Volatility::Immutable);
    let return_type: ReturnTypeFunction = Arc::new(move |_| Ok(Arc::new(DataType::Binary)));

    ScalarUDF::new("st_AsBinary", &signature, &return_type, Arc::new(func))
}

fn func(args: &[ColumnarValue]) -> DFResult<ColumnarValue> {
    let args = ColumnarValue::values_to_arrays(args)?;
    let wkt_arr = args[0].as_ref();
    let wkt_arr = downcast_array::<StringArray>(wkt_arr);

    let result: BinaryArray = wkt_arr
        .iter()
        .map(|opt| {
            opt.and_then(|str| {
                let wkb = Wkt(str);
                // conversion failed to null
                wkb.to_wkb(CoordDimensions::xy()).ok()
            })
        })
        .collect();

    Ok(Arc::new(result))
}
