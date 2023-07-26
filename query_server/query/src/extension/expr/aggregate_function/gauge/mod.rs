mod gauge_agg;

use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field, Fields};
use datafusion::common::Result as DFResult;
use datafusion::error::DataFusionError;
use datafusion::scalar::ScalarValue;
use spi::query::function::FunctionMetadataManager;
use spi::QueryError;

use super::AggResult;

pub fn register_udafs(func_manager: &mut dyn FunctionMetadataManager) -> Result<(), QueryError> {
    gauge_agg::register_udaf(func_manager)?;
    Ok(())
}

#[derive(Debug, PartialEq)]
struct GaugeData {
    time_data_type: DataType,
    value_data_type: DataType,

    first: TSPoint,
    second: TSPoint,
    penultimate: TSPoint,
    last: TSPoint,
    num_elements: u64,
}

impl GaugeData {
    fn new_null(time_data_type: DataType, value_data_type: DataType) -> DFResult<Self> {
        let null_ts = ScalarValue::try_from(time_data_type.clone())?;
        let null_val = ScalarValue::try_from(value_data_type.clone())?;
        let null = TSPoint {
            ts: null_ts,
            val: null_val,
        };
        Ok(Self {
            time_data_type,
            value_data_type,
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

#[derive(Debug, Clone, PartialEq)]
pub struct TSPoint {
    pub ts: ScalarValue,
    pub val: ScalarValue,
}

impl TSPoint {
    fn try_new_null(time_data_type: DataType, value_data_type: DataType) -> DFResult<Self> {
        let ts = ScalarValue::try_from(time_data_type)?;
        let val = ScalarValue::try_from(value_data_type)?;
        Ok(Self { ts, val })
    }

    fn try_from_scalar(scalar: ScalarValue) -> DFResult<Option<Self>> {
        match scalar {
            ScalarValue::Struct(Some(vals), _) => {
                let ts = vals[0].clone();
                let val = vals[1].clone();
                Ok(Some(Self { ts, val }))
            }
            ScalarValue::Struct(None, _) => Ok(None),
            _ => Err(DataFusionError::External(Box::new(QueryError::Internal {
                reason: format!("Expected struct, got {:?}", scalar),
            }))),
        }
    }
}

fn scalar_to_points(value: ScalarValue) -> DFResult<Vec<TSPoint>> {
    match value {
        ScalarValue::List(Some(vals), _) => {
            let points = vals
                .into_iter()
                .flat_map(|e| TSPoint::try_from_scalar(e).transpose())
                .collect::<DFResult<Vec<_>>>()?;
            Ok(points)
        }
        ScalarValue::List(None, _) => Ok(vec![]),
        _ => Err(DataFusionError::External(Box::new(QueryError::Internal {
            reason: format!("Expected list, got {:?}", value),
        }))),
    }
}

impl TSPoint {
    fn data_type(&self) -> DFResult<DataType> {
        Ok(DataType::Struct(Fields::from([
            Arc::new(Field::new("ts", self.ts.get_datatype(), true)),
            Arc::new(Field::new("val", self.val.get_datatype(), true)),
        ])))
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
