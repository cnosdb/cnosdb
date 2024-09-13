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

use datafusion::arrow::array::ArrayRef;
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
pub const EXACT_COUNT_STAR_UDAF_NAME: &str = "exact_count_star";
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
    fn to_scalar(self) -> DFResult<ScalarValue>;
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

impl AggResult for TSPoint {
    fn to_scalar(self) -> DFResult<ScalarValue> {
        let TSPoint { ts, val } = self;
        let ts_data_type = ts.get_datatype();
        let val_data_type = val.get_datatype();

        Ok(ScalarValue::Struct(
            Some(vec![ts, val]),
            Fields::from([
                Arc::new(Field::new("ts", ts_data_type, true)),
                Arc::new(Field::new("val", val_data_type, true)),
            ]),
        ))
    }
}

impl TSPoint {
    pub fn try_from_scalar(scalar: ScalarValue) -> DFResult<Self> {
        match scalar {
            ScalarValue::Struct(None, fields) => {
                debug_assert!(fields.len() == 2, "Expected 2 fields, got {:?}", fields);

                let time_data_type = fields[0].data_type();
                let value_data_type = fields[1].data_type();
                let ts = ScalarValue::try_from(time_data_type)?;
                let val = ScalarValue::try_from(value_data_type)?;
                Ok(Self { ts, val })
            }
            ScalarValue::Struct(Some(vals), _) => {
                let ts = vals[0].clone();
                let val = vals[1].clone();
                Ok(Self { ts, val })
            }
            _ => Err(DataFusionError::External(Box::new(QueryError::Internal {
                reason: format!("Expected struct, got {:?}", scalar),
            }))),
        }
    }

    pub fn data_type(&self) -> DFResult<DataType> {
        Ok(DataType::Struct(Fields::from([
            Arc::new(Field::new("ts", self.ts.get_datatype(), true)),
            Arc::new(Field::new("val", self.val.get_datatype(), true)),
        ])))
    }
}

fn scalar_to_points(value: ScalarValue) -> DFResult<Vec<TSPoint>> {
    match value {
        ScalarValue::List(Some(vals), _) => {
            let points = vals
                .into_iter()
                .map(TSPoint::try_from_scalar)
                .collect::<DFResult<Vec<_>>>()?;
            Ok(points)
        }
        ScalarValue::List(None, _) => Ok(vec![]),
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

        let result_udaf = func_manager.udaf(&expect_udaf.name);

        assert!(result_udaf.is_ok(), "not get result from func manager.");

        assert_eq!(&expect_udaf, result_udaf.unwrap().as_ref());
    }
}
