mod data_quality;
mod exact_count_agg;
#[cfg(test)]
mod example;
mod first;
mod gauge;
mod increase;
mod last;
mod mode;
mod sample;
mod state_agg;

use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, AsArray as _, StructArray};
use datafusion::arrow::datatypes::{DataType, Field, Fields};
use datafusion::common::Result as DFResult;
use datafusion::error::DataFusionError;
use datafusion::scalar::ScalarValue;
use spi::query::function::FunctionMetadataManager;
use spi::{QueryError, QueryResult};

pub const SAMPLE_UDAF_NAME: &str = "sample";
pub const COMPACT_STATE_AGG_UDAF_NAME: &str = "compact_state_agg";
pub const STATE_AGG_UDAF_NAME: &str = "state_agg";
pub const GAUGE_AGG_UDAF_NAME: &str = "gauge_agg";
pub const FIRST_UDAF_NAME: &str = "first";
pub const LAST_UDAF_NAME: &str = "last";
pub const MODE_UDAF_NAME: &str = "mode";
pub const INCREASE_NAME: &str = "increase";
pub const COMPLETENESS_UDF_NAME: &str = "completeness";
pub const CONSISTENCY_UDF_NAME: &str = "consistency";
pub const TIMELINESS_UDF_NAME: &str = "timeliness";
pub const VALIDITY_UDF_NAME: &str = "validity";
pub const EXACT_COUNT_UDAF_NAME: &str = "exact_count";
pub use gauge::GaugeData;
pub use state_agg::StateAggData;

pub fn register_udafs(func_manager: &mut dyn FunctionMetadataManager) -> QueryResult<()> {
    // extend function...
    // eg.
    //   example::register_udaf(func_manager)?;
    sample::register_udaf(func_manager)?;
    state_agg::register_udafs(func_manager)?;
    gauge::register_udafs(func_manager)?;
    first::register_udaf(func_manager)?;
    last::register_udaf(func_manager)?;
    mode::register_udaf(func_manager)?;
    increase::register_udaf(func_manager)?;
    data_quality::register_udafs(func_manager)?;
    exact_count_agg::register_udaf(func_manager)?;
    Ok(())
}

pub trait AggResult {
    fn into_scalar(self) -> DFResult<ScalarValue>;
}

pub trait AggState: Sized {
    fn try_to_state(&self) -> DFResult<Vec<ScalarValue>>;
    fn try_from_arrays(
        input_data_types: &[DataType],
        arrays: &[ArrayRef],
    ) -> DFResult<Option<Self>>;
}

#[derive(Debug, Clone, PartialEq)]
pub struct TSPoint {
    ts: ScalarValue,
    val: ScalarValue,
}

impl TSPoint {
    pub fn try_new_null(time_data_type: DataType, value_data_type: DataType) -> DFResult<Self> {
        let ts = ScalarValue::try_from(time_data_type)?;
        let val = ScalarValue::try_from(value_data_type)?;
        Ok(Self { ts, val })
    }

    pub fn val(&self) -> &ScalarValue {
        &self.val
    }

    pub fn ts(&self) -> &ScalarValue {
        &self.ts
    }
}

impl TSPoint {
    pub fn try_into_array(self) -> DFResult<Arc<StructArray>> {
        let TSPoint { ts, val } = self;
        let ts_data_type = ts.data_type();
        let val_data_type = val.data_type();

        Ok(Arc::new(StructArray::new(
            Fields::from(vec![
                Field::new("ts", ts_data_type, true),
                Field::new("val", val_data_type, true),
            ]),
            vec![ts.to_array()?, val.to_array()?],
            None,
        )))
    }

    pub fn try_from_array(array: &ArrayRef) -> DFResult<Self> {
        match array.as_struct_opt() {
            Some(struct_array) => {
                let columns = struct_array.columns();
                debug_assert!(
                    columns.len() == 2,
                    "TSPoint: expect 2 columns, but got {columns:?}"
                );

                let ts = ScalarValue::try_from_array(&columns[0], 0)?;
                let val = ScalarValue::try_from_array(&columns[1], 0)?;
                Ok(Self { ts, val })
            }
            None => Err(DataFusionError::External(Box::new(QueryError::Internal {
                reason: format!("TSPoint: expect Struct, but got {}", array.data_type()),
            }))),
        }
    }

    pub fn data_type(&self) -> DFResult<DataType> {
        Ok(DataType::Struct(Fields::from([
            Arc::new(Field::new("ts", self.ts.data_type(), true)),
            Arc::new(Field::new("val", self.val.data_type(), true)),
        ])))
    }
}

fn scalar_to_points(value: ScalarValue) -> DFResult<Vec<TSPoint>> {
    match value {
        ScalarValue::List(list_array) => {
            let points = list_array
                .iter()
                .flatten()
                .map(|a| TSPoint::try_from_array(&a))
                .collect::<DFResult<Vec<_>>>()?;
            Ok(points)
        }
        _ => Err(DataFusionError::External(Box::new(QueryError::Internal {
            reason: format!("Expected list, got {:?}", value),
        }))),
    }
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

        let result_udaf = func_manager.udaf(&expect_udaf.name());

        assert!(result_udaf.is_ok(), "not get result from func manager.");

        assert_eq!(&expect_udaf, result_udaf.unwrap().as_ref());
    }
}
