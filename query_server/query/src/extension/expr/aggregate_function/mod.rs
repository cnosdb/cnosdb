#[cfg(test)]
mod example;
mod gauge;
mod sample;
mod state_agg;

use datafusion::arrow::array::ArrayRef;
use datafusion::arrow::datatypes::DataType;
use datafusion::common::Result as DFResult;
use datafusion::scalar::ScalarValue;
use spi::query::function::FunctionMetadataManager;
use spi::Result;

pub const SAMPLE_UDAF_NAME: &str = "sample";
pub const COMPACT_STATE_AGG_UDAF_NAME: &str = "compact_state_agg";
pub const GAUGE_AGG_UDAF_NAME: &str = "gauge_agg";

pub fn register_udafs(func_manager: &mut dyn FunctionMetadataManager) -> Result<()> {
    // extend function...
    // eg.
    //   example::register_udaf(func_manager)?;
    sample::register_udaf(func_manager)?;
    state_agg::register_udafs(func_manager)?;
    gauge::register_udafs(func_manager)?;
    Ok(())
}

trait AggResult {
    fn to_scalar(self) -> DFResult<ScalarValue>;
}

trait AggState: Sized {
    fn try_to_state(&self) -> DFResult<Vec<ScalarValue>>;
    fn try_from_arrays(
        input_data_types: &[DataType],
        arrays: &[ArrayRef],
    ) -> DFResult<Option<Self>>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::function::simple_func_manager::SimpleFunctionMetadataManager;

    #[tokio::test]
    async fn test_example() {
        let mut func_manager = SimpleFunctionMetadataManager::default();

        let expect_udaf = example::register_udaf(&mut func_manager);

        assert!(expect_udaf.is_ok(), "register_udaf error.");

        let expect_udaf = expect_udaf.unwrap();

        let result_udaf = func_manager.udaf(&expect_udaf.name);

        assert!(result_udaf.is_ok(), "not get result from func manager.");

        assert_eq!(&expect_udaf, result_udaf.unwrap().as_ref());
    }
}
