use std::sync::Arc;

use datafusion::arrow::array::{downcast_array, BinaryArray, StringArray};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::Result as DFResult;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use geozero::wkb::Wkb;
use geozero::ToWkt;
use spi::query::function::FunctionMetadataManager;
use spi::QueryResult;

pub fn register_udf(func_manager: &mut dyn FunctionMetadataManager) -> QueryResult<()> {
    func_manager.register_udf(ScalarUDF::new_from_impl(StGeoFromWkbFunc::default()))?;
    Ok(())
}

#[derive(Debug)]
pub struct StGeoFromWkbFunc {
    signature: Signature,
}

impl Default for StGeoFromWkbFunc {
    fn default() -> Self {
        Self {
            signature: Signature::exact(vec![DataType::Binary], Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for StGeoFromWkbFunc {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        "st_GeomFromWKB"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let args = ColumnarValue::values_to_arrays(&args.args)?;
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

        Ok(ColumnarValue::Array(Arc::new(result)))
    }
}
