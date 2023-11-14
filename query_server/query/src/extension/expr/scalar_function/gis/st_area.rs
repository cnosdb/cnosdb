use datafusion::arrow::array::{downcast_array, ArrayRef, Float64Builder, StringArray};
use datafusion::arrow::datatypes::DataType;
use datafusion::error::DataFusionError;
use datafusion::logical_expr::ScalarUDF;
use datafusion::physical_plan::functions::make_scalar_function;
use geo::{Area, Geometry};
use spi::query::function::FunctionMetadataManager;
use spi::Result;

use crate::geometry_unary_op;

pub fn register_udf(func_manager: &mut dyn FunctionMetadataManager) -> Result<ScalarUDF> {
    let udf = geometry_unary_op!("ST_Area", area, DataType::Float64, Float64Builder);
    func_manager.register_udf(udf.clone())?;
    Ok(udf)
}

fn area(geo: &Geometry) -> Result<f64, DataFusionError> {
    Ok(geo.unsigned_area())
}
