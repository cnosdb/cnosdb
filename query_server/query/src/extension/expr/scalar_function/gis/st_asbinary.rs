use std::sync::Arc;

use datafusion::arrow::array::{downcast_array, ArrayRef, BinaryArray, StringArray};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::Result as DFResult;
use datafusion::logical_expr::{ReturnTypeFunction, ScalarUDF, Signature, Volatility};
use datafusion::physical_plan::functions::make_scalar_function;
use geozero::wkt::WktStr;
use geozero::{CoordDimensions, ToWkb};
use spi::query::function::FunctionMetadataManager;
use spi::Result;

pub fn register_udf(func_manager: &mut dyn FunctionMetadataManager) -> Result<ScalarUDF> {
    let udf = new();
    func_manager.register_udf(udf.clone())?;
    Ok(udf)
}

fn new() -> ScalarUDF {
    let fun = make_scalar_function(func);

    let signature = Signature::exact(vec![DataType::Utf8], Volatility::Immutable);
    let return_type: ReturnTypeFunction = Arc::new(move |_| Ok(Arc::new(DataType::Binary)));

    ScalarUDF::new("st_AsBinary", &signature, &return_type, &fun)
}

fn func(args: &[ArrayRef]) -> DFResult<ArrayRef> {
    let wkt_arr = args[0].as_ref();
    let wkt_arr = downcast_array::<StringArray>(wkt_arr);

    let result: BinaryArray = wkt_arr
        .iter()
        .map(|opt| {
            opt.and_then(|str| {
                let wkb = WktStr(str);
                // conversion failed to null
                wkb.to_wkb(CoordDimensions::xy()).ok()
            })
        })
        .collect();

    Ok(Arc::new(result))
}
