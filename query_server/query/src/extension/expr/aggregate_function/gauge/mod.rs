mod gauge_agg;

use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, StructArray, UInt64Array};
use datafusion::arrow::datatypes::{ArrowNativeTypeOp, DataType, Field, Fields};
use datafusion::common::Result as DFResult;
use datafusion::error::DataFusionError;
use datafusion::scalar::ScalarValue;
use spi::query::function::FunctionMetadataManager;
use spi::{AnalyzerSnafu, QueryError};

use super::TSPoint;

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

    fn is_null(&self) -> bool {
        self.num_elements == 0
    }

    pub fn delta(&self) -> DFResult<ScalarValue> {
        match self.last.val().sub_checked(self.first.val()) {
            Ok(value) => Ok(value),
            Err(_) => {
                // null if overflow
                ScalarValue::try_from(self.last.val().data_type())
            }
        }
    }

    pub fn time_delta(&self) -> DFResult<ScalarValue> {
        match self.last.ts().sub_checked(self.first.ts()) {
            Ok(value) => Ok(value),
            Err(_) => {
                // null if overflow
                let zero = ScalarValue::new_zero(&self.last.ts().data_type())?;
                let interval_datatype = zero.sub(&zero)?.data_type();
                ScalarValue::try_from(interval_datatype)
            }
        }
    }

    pub fn first_time(&self) -> DFResult<ScalarValue> {
        Ok(self.first.ts.clone())
    }

    pub fn first_val(&self) -> DFResult<ScalarValue> {
        Ok(self.first.val.clone())
    }

    pub fn last_time(&self) -> DFResult<ScalarValue> {
        Ok(self.last.ts.clone())
    }

    pub fn last_val(&self) -> DFResult<ScalarValue> {
        Ok(self.last.val.clone())
    }

    pub fn idelta_left(&self) -> DFResult<ScalarValue> {
        match self.second.val().sub_checked(self.first.val()) {
            Ok(value) => Ok(value),
            Err(_) => {
                // null if overflow
                ScalarValue::try_from(self.last.val().data_type())
            }
        }
    }

    pub fn idelta_right(&self) -> DFResult<ScalarValue> {
        match self.last.val().sub_checked(self.penultimate.val()) {
            Ok(value) => Ok(value),
            Err(_) => {
                // null if overflow
                ScalarValue::try_from(self.last.val().data_type())
            }
        }
    }

    pub fn rate(&self) -> DFResult<ScalarValue> {
        if self.is_null() {
            return ScalarValue::try_from(self.last.val().data_type());
        }

        let last_ts: i64 = self.last.ts.clone().try_into()?;
        let first_ts: i64 = self.first.ts.clone().try_into()?;

        let time_delta = last_ts.sub_checked(first_ts)?;
        if time_delta == 0 {
            // return Null
            return ScalarValue::try_from(self.last.val().data_type());
        }

        self.delta()?.div(ScalarValue::from(time_delta as f64))
    }
}

impl GaugeData {
    fn into_scalar(self) -> DFResult<ScalarValue> {
        let Self {
            first,
            second,
            penultimate,
            last,
            num_elements,
            ..
        } = self;

        let first = first.try_into_array()? as ArrayRef;
        let second = second.try_into_array()? as ArrayRef;
        let penultimate = penultimate.try_into_array()? as ArrayRef;
        let last = last.try_into_array()? as ArrayRef;
        let num_elements = Arc::new(UInt64Array::from(vec![num_elements])) as ArrayRef;

        let first_data_type = first.data_type().clone();
        let second_data_type = second.data_type().clone();
        let penultimate_data_type = penultimate.data_type().clone();
        let last_data_type = last.data_type().clone();
        let num_elements_data_type = num_elements.data_type().clone();

        Ok(ScalarValue::Struct(Arc::new(StructArray::new(
            Fields::from([
                Arc::new(Field::new("first", first_data_type, true)),
                Arc::new(Field::new("second", second_data_type, true)),
                Arc::new(Field::new("penultimate", penultimate_data_type, true)),
                Arc::new(Field::new("last", last_data_type, true)),
                Arc::new(Field::new("num_elements", num_elements_data_type, true)),
            ]),
            vec![first, second, penultimate, last, num_elements],
            None,
        ))))
    }

    pub fn try_from_scalar(scalar: ScalarValue) -> DFResult<Self> {
        let valid_func = |fields: &Fields| {
            let field_names = ["first", "second", "penultimate", "last", "num_elements"];
            let input_fields = fields.iter().map(|f| f.name().as_str()).collect::<Vec<_>>();
            if !input_fields.eq(&field_names) {
                return Err(DataFusionError::External(Box::new(
                    AnalyzerSnafu {
                        err: format!("Expected GaugeData, got {:?}", fields),
                    }
                    .build(),
                )));
            }

            Ok(())
        };

        match scalar {
            ScalarValue::Struct(struct_array) if struct_array.num_columns() > 0 => {
                valid_func(struct_array.fields())?;

                let first = TSPoint::try_from_array(struct_array.column(0))?;
                let second = TSPoint::try_from_array(struct_array.column(1))?;
                let penultimate = TSPoint::try_from_array(struct_array.column(2))?;
                let last = TSPoint::try_from_array(struct_array.column(3))?;
                let num_elements_array = struct_array.column(4);
                let num_elements = num_elements_array
                    .as_any()
                    .downcast_ref::<Arc<UInt64Array>>()
                    .map(|a| a.value(0))
                    .ok_or_else(|| {
                        DataFusionError::External(Box::new(
                            AnalyzerSnafu {
                                err: format!(
                                    "Expected UInt64Array, got {}",
                                    num_elements_array.data_type()
                                ),
                            }
                            .build(),
                        ))
                    })?;

                Ok(Self {
                    first,
                    second,
                    penultimate,
                    last,
                    num_elements,
                })
            }
            _ => Err(DataFusionError::External(Box::new(
                AnalyzerSnafu {
                    err: format!("Expected GaugeData, got {:?}", scalar),
                }
                .build(),
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use datafusion::scalar::ScalarValue;

    use super::GaugeData;
    use crate::extension::expr::aggregate_function::TSPoint;

    #[test]
    fn test_delta_of_gauge_data() {
        let point_1 = TSPoint {
            ts: ScalarValue::TimestampSecond(Some(2), None),
            val: ScalarValue::from(2.1),
        };
        let point_2 = TSPoint {
            ts: ScalarValue::TimestampSecond(Some(2), None),
            val: ScalarValue::from(4.1),
        };

        let data = GaugeData {
            first: point_1.clone(),
            second: point_2.clone(),
            penultimate: point_1,
            last: point_2,
            num_elements: 2,
        };

        let delta = data.delta().unwrap();

        let deviation = ScalarValue::from(2.0).sub(delta).unwrap();

        assert!(deviation.lt(&ScalarValue::from(0.0001)));
    }

    #[test]
    fn test_time_delta_s_of_gauge_data() {
        let point_1 = TSPoint {
            ts: ScalarValue::TimestampSecond(Some(2), None),
            val: ScalarValue::Null,
        };
        let point_2 = TSPoint {
            ts: ScalarValue::TimestampSecond(Some(3), None),
            val: ScalarValue::Null,
        };

        let data = GaugeData {
            first: point_1.clone(),
            second: point_2.clone(),
            penultimate: point_1,
            last: point_2,
            num_elements: 2,
        };

        let delta = data.time_delta().unwrap();

        assert_eq!(delta, ScalarValue::DurationNanosecond(Some(1000)))
    }

    #[test]
    fn test_time_delta_ms_of_gauge_data() {
        let point_1 = TSPoint {
            ts: ScalarValue::TimestampMillisecond(Some(2), None),
            val: ScalarValue::Null,
        };
        let point_2 = TSPoint {
            ts: ScalarValue::TimestampMillisecond(Some(3), None),
            val: ScalarValue::Null,
        };

        let data = GaugeData {
            first: point_1.clone(),
            second: point_2.clone(),
            penultimate: point_1,
            last: point_2,
            num_elements: 2,
        };

        let delta = data.time_delta().unwrap();

        assert_eq!(delta, ScalarValue::DurationNanosecond(Some(1)))
    }

    #[test]
    fn test_time_delta_us_of_gauge_data() {
        let point_1 = TSPoint {
            ts: ScalarValue::TimestampMicrosecond(Some(2), None),
            val: ScalarValue::Null,
        };
        let point_2 = TSPoint {
            ts: ScalarValue::TimestampMicrosecond(Some(3), None),
            val: ScalarValue::Null,
        };

        let data = GaugeData {
            first: point_1.clone(),
            second: point_2.clone(),
            penultimate: point_1,
            last: point_2,
            num_elements: 2,
        };

        let delta = data.time_delta().unwrap();

        assert_eq!(delta, ScalarValue::DurationNanosecond(Some(1000)))
    }

    #[test]
    fn test_time_delta_ns_of_gauge_data() {
        let point_1 = TSPoint {
            ts: ScalarValue::TimestampNanosecond(Some(2), None),
            val: ScalarValue::Null,
        };
        let point_2 = TSPoint {
            ts: ScalarValue::TimestampNanosecond(Some(3), None),
            val: ScalarValue::Null,
        };

        let data = GaugeData {
            first: point_1.clone(),
            second: point_2.clone(),
            penultimate: point_1,
            last: point_2,
            num_elements: 2,
        };

        let delta = data.time_delta().unwrap();

        assert_eq!(delta, ScalarValue::DurationNanosecond(Some(1)))
    }

    #[test]
    fn test_rate_of_gauge_data() {
        let point_1 = TSPoint {
            ts: ScalarValue::TimestampNanosecond(Some(2), None),
            val: ScalarValue::from(2.1),
        };
        let point_2 = TSPoint {
            ts: ScalarValue::TimestampNanosecond(Some(3), None),
            val: ScalarValue::from(4.1),
        };

        let data = GaugeData {
            first: point_1.clone(),
            second: point_2.clone(),
            penultimate: point_1,
            last: point_2,
            num_elements: 2,
        };

        let rate = data.rate().unwrap();

        let deviation = ScalarValue::from(2.0).sub(rate).unwrap();

        assert!(deviation.lt(&ScalarValue::from(0.0001)));
    }
}
