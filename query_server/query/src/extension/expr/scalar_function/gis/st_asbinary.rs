use std::sync::Arc;

use datafusion::arrow::array::{downcast_array, BinaryArray, StringArray};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::Result as DFResult;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use geozero::wkt::Wkt;
use geozero::{CoordDimensions, ToWkb};
use spi::query::function::FunctionMetadataManager;
use spi::QueryResult;

pub fn register_udf(func_manager: &mut dyn FunctionMetadataManager) -> QueryResult<()> {
    func_manager.register_udf(ScalarUDF::new_from_impl(StAsBinaryFunc::new()))?;
    Ok(())
}

#[derive(Debug)]
pub struct StAsBinaryFunc {
    signature: Signature,
}

impl StAsBinaryFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::exact(vec![DataType::Utf8], Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for StAsBinaryFunc {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        "st_AsBinary"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Binary)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let args = ColumnarValue::values_to_arrays(&args.args)?;
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

        Ok(ColumnarValue::Array(Arc::new(result)))
    }
}
