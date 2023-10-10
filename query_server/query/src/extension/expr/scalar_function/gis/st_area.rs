use std::sync::Arc;

use datafusion::arrow::array::{downcast_array, ArrayRef, Float64Array, StringArray};
use datafusion::arrow::datatypes::DataType;
use datafusion::logical_expr::{ReturnTypeFunction, ScalarUDF, Signature, Volatility};
use datafusion::physical_plan::functions::make_scalar_function;
use geo::Area;
use geozero::wkt::WktStr;
use geozero::ToGeo;
use spi::query::function::FunctionMetadataManager;
use spi::{DFResult, Result};

pub fn register_udf(func_manager: &mut dyn FunctionMetadataManager) -> Result<ScalarUDF> {
    let udf = new();
    func_manager.register_udf(udf.clone())?;
    Ok(udf)
}

fn new() -> ScalarUDF {
    let fun = make_scalar_function(func);

    let signature = Signature::exact(vec![DataType::Utf8], Volatility::Immutable);
    let return_type: ReturnTypeFunction = Arc::new(move |_| Ok(Arc::new(DataType::Float64)));

    ScalarUDF::new("st_Area", &signature, &return_type, &fun)
}

fn func(args: &[ArrayRef]) -> DFResult<ArrayRef> {
    let geos = args[0].as_ref();
    let geos = downcast_array::<StringArray>(geos);
    let areas = geos
        .iter()
        .map(|opt| {
            opt.and_then(|str| {
                let wkt = WktStr(str);
                wkt.to_geo().ok().map(|g| g.unsigned_area())
            })
        })
        .collect::<Float64Array>();

    Ok(Arc::new(areas))
}
