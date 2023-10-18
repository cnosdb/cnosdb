mod gauge_agg;

use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field, Fields};
use datafusion::common::Result as DFResult;
use datafusion::scalar::ScalarValue;
use spi::query::function::FunctionMetadataManager;
use spi::QueryError;

use super::{AggResult, TSPoint};

pub fn register_udafs(func_manager: &mut dyn FunctionMetadataManager) -> Result<(), QueryError> {
    gauge_agg::register_udaf(func_manager)?;
    Ok(())
}

#[derive(Debug, PartialEq)]
pub struct GaugeData {
    first: TSPoint,
    second: TSPoint,
    penultimate: TSPoint,
    last: TSPoint,
    num_elements: u64,
}

impl GaugeData {
    fn try_new_null(time_data_type: DataType, value_data_type: DataType) -> DFResult<Self> {
        let null = TSPoint::try_new_null(time_data_type, value_data_type)?;
        Ok(Self {
            first: null.clone(),
            second: null.clone(),
            penultimate: null.clone(),
            last: null,
            num_elements: 0,
        })
    }
}

impl AggResult for GaugeData {
    fn to_scalar(self) -> DFResult<ScalarValue> {
        let Self {
            first,
            second,
            penultimate,
            last,
            num_elements,
            ..
        } = self;

        let first = first.to_scalar()?;
        let second = second.to_scalar()?;
        let penultimate = penultimate.to_scalar()?;
        let last = last.to_scalar()?;
        let num_elements = ScalarValue::from(num_elements);

        let first_data_type = first.get_datatype();
        let second_data_type = second.get_datatype();
        let penultimate_data_type = penultimate.get_datatype();
        let last_data_type = last.get_datatype();
        let num_elements_data_type = num_elements.get_datatype();

        Ok(ScalarValue::Struct(
            Some(vec![first, second, penultimate, last, num_elements]),
            Fields::from([
                Arc::new(Field::new("first", first_data_type, true)),
                Arc::new(Field::new("second", second_data_type, true)),
                Arc::new(Field::new("penultimate", penultimate_data_type, true)),
                Arc::new(Field::new("last", last_data_type, true)),
                Arc::new(Field::new("num_elements", num_elements_data_type, true)),
            ]),
        ))
    }
}
