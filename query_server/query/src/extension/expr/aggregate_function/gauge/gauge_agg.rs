use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, UInt64Array};
use datafusion::arrow::compute::sort_to_indices;
use datafusion::arrow::datatypes::DataType;
use datafusion::common::{downcast_value, Result as DFResult};
use datafusion::logical_expr::type_coercion::aggregates::TIMESTAMPS;
use datafusion::logical_expr::{
    AccumulatorFactoryFunction, AggregateUDF, ReturnTypeFunction, Signature, StateTypeFunction,
    TypeSignature, Volatility,
};
use datafusion::physical_plan::Accumulator;
use datafusion::scalar::ScalarValue;
use spi::query::function::FunctionMetadataManager;
use spi::QueryError;

use super::{GaugeData, TSPoint};
use crate::extension::expr::aggregate_function::{
    scalar_to_points, AggResult, AggState, GAUGE_AGG_UDAF_NAME,
};

pub fn register_udaf(func_manager: &mut dyn FunctionMetadataManager) -> Result<(), QueryError> {
    func_manager.register_udaf(new())?;
    Ok(())
}

fn new() -> AggregateUDF {
    let return_type_func: ReturnTypeFunction = Arc::new(move |input| {
        let result = GaugeData::try_new_null(input[0].clone(), input[1].clone())?;
        let date_type = result.into_scalar()?.data_type();

        trace::trace!("return_type: {:?}", date_type);

        Ok(Arc::new(date_type))
    });

    let state_type_func: StateTypeFunction = Arc::new(move |input, _| {
        let null_state =
            GaugeDataBuilder::new(input[0].clone(), input[1].clone()).try_to_state()?;
        let state_data_types = null_state.iter().map(|v| v.data_type()).collect();
        Ok(Arc::new(state_data_types))
    });

    let accumulator: AccumulatorFactoryFunction = Arc::new(|input, output| {
        let ts_data_type = input[0].clone();
        let value_data_type = input[1].clone();

        Ok(Box::new(GaugeAggAccumulator::try_new(
            ts_data_type,
            value_data_type,
            output.clone(),
        )?))
    });

    // gauge_agg(
    //     ts TIMESTAMP,
    //     value DOUBLE
    // TODO    [, bounds TSTZRANGE]
    //   ) RETURNS GaugeData
    let type_signatures = TIMESTAMPS
        .iter()
        .map(|t| TypeSignature::Exact(vec![t.clone(), DataType::Float64]))
        .collect();

    AggregateUDF::new_with_preference(
        GAUGE_AGG_UDAF_NAME,
        &Signature::one_of(type_signatures, Volatility::Immutable),
        &return_type_func,
        &accumulator,
        &state_type_func,
        true,
        false,
    )
}

/// An accumulator to compute the average
#[derive(Debug)]
struct GaugeAggAccumulator {
    state: GaugeDataBuilder,

    return_date_type: DataType,
}

impl GaugeAggAccumulator {
    pub fn try_new(
        ts_data_type: DataType,
        value_data_type: DataType,
        return_date_type: DataType,
    ) -> DFResult<Self> {
        Ok(Self {
            state: GaugeDataBuilder::new(ts_data_type, value_data_type),
            return_date_type,
        })
    }
}

impl Accumulator for GaugeAggAccumulator {
    fn state(&mut self) -> DFResult<Vec<ScalarValue>> {
        self.state.try_to_state()
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> DFResult<()> {
        trace::trace!("update_batch: {:?}", values);

        if values.is_empty() {
            return Ok(());
        }

        debug_assert!(
            values.len() == 2,
            "gauge_agg can only take 2 param, but found {}",
            values.len()
        );

        let times_records = values[0].as_ref();
        let value_records = values[1].as_ref();

        // sort by time column, only get indices
        let indices = sort_to_indices(times_records, None, None)?;
        trace::trace!("indices: {:?}", indices);
        // without null value
        let non_null_indices = indices
            .iter()
            .flatten()
            .filter(|e| !value_records.is_null(*e as usize))
            .collect::<Vec<_>>();
        trace::trace!("non_null_indices: {:?}", non_null_indices);

        let len = non_null_indices.len();
        if len <= 4 {
            // save all
            for idx in non_null_indices {
                let idx = idx as usize;
                debug_assert!(
                    !value_records.is_null(idx),
                    "GaugeAggAccumulator's value column can't be null"
                );

                let ts = ScalarValue::try_from_array(times_records, idx)?;
                let val = ScalarValue::try_from_array(value_records, idx)?;
                let point = TSPoint { ts, val };
                self.state.push(point)?;
            }
        } else {
            // only save: first, second, penultimate, last
            for indices_idx in [0, 1, len - 2, len - 1] {
                let idx = non_null_indices[indices_idx] as usize;
                let ts = ScalarValue::try_from_array(times_records, idx)?;
                let val = ScalarValue::try_from_array(value_records, idx)?;
                self.state.push(TSPoint { ts, val })?;
            }
        }

        // without null value
        self.state.add_num_elements(len as u64);

        Ok(())
    }

    fn merge_batch(&mut self, arrays: &[ArrayRef]) -> DFResult<()> {
        trace::trace!("merge_batch: {:?}", arrays);

        debug_assert!(arrays.len() == 2, "MutableGaugeData requires 2 arrays.");

        let state = GaugeDataBuilder::try_from_arrays(
            &[
                self.state.time_data_type.clone(),
                self.state.value_data_type.clone(),
            ],
            arrays,
        )?;

        if let Some(state) = state {
            self.state.merge(state)?;
        }

        Ok(())
    }

    fn evaluate(&mut self) -> DFResult<ScalarValue> {
        let result = self
            .state
            .clone()
            .build()
            .map(|e| e.into_scalar())
            .unwrap_or(ScalarValue::try_from(&self.return_date_type))?;

        trace::trace!("GaugeAggAccumulator evaluate result: {:?}", result);

        Ok(result)
    }

    fn size(&self) -> usize {
        let scalar_size: usize = self
            .state
            .container
            .iter()
            .map(|e| e.ts.size() + e.val.size())
            .sum();

        std::mem::size_of_val(self) + scalar_size - std::mem::size_of_val(&self.state.container)
            + self.return_date_type.size()
            - std::mem::size_of_val(&self.return_date_type)
            + self.state.time_data_type.size()
            - std::mem::size_of_val(&self.state.time_data_type)
            + self.state.value_data_type.size()
            - std::mem::size_of_val(&self.state.value_data_type)
    }
}

#[derive(Debug, Clone)]
struct GaugeDataBuilder {
    time_data_type: DataType,
    value_data_type: DataType,

    container: Vec<TSPoint>,
    num_elements: u64,
}

impl GaugeDataBuilder {
    fn new(time_data_type: DataType, value_data_type: DataType) -> Self {
        Self {
            time_data_type,
            value_data_type,
            container: Default::default(),
            num_elements: Default::default(),
        }
    }
}

impl GaugeDataBuilder {
    pub fn add_num_elements(&mut self, num_elements: u64) {
        self.num_elements += num_elements;
    }

    pub fn push(&mut self, incoming: TSPoint) -> DFResult<()> {
        debug_assert!(!incoming.val.is_null(), "TSPoint's value can't be null");

        self.container.push(incoming);

        Ok(())
    }

    fn merge(&mut self, other: GaugeDataBuilder) -> DFResult<()> {
        let GaugeDataBuilder {
            container,
            num_elements,
            ..
        } = other;

        self.container.extend(container);
        self.num_elements += num_elements;

        Ok(())
    }

    pub fn build(mut self) -> Option<GaugeData> {
        if self.container.is_empty() {
            return None;
        }
        // max len: recordbatch num * 4
        self.container.sort_unstable_by(|a, b| {
            a.ts.partial_cmp(&b.ts)
                .expect("GaugeData's ts column can't be null")
        });

        let len = self.container.len();

        let first_idx = 0;
        let last_idx = len - 1;
        let mut second_idx = 0;
        let mut penultimate_idx = 0;

        if len > 1 {
            second_idx = 1;
            penultimate_idx = len - 2;
        }

        let first = self.container[first_idx].clone();
        let second = self.container[second_idx].clone();
        let penultimate = self.container[penultimate_idx].clone();
        let last = self.container[last_idx].clone();

        Some(GaugeData {
            first,
            second,
            penultimate,
            last,
            num_elements: self.num_elements,
        })
    }
}

impl AggState for GaugeDataBuilder {
    fn try_to_state(&self) -> DFResult<Vec<ScalarValue>> {
        let GaugeDataBuilder {
            time_data_type,
            value_data_type,
            num_elements,
            container,
            ..
        } = self.clone();

        let num_elements = ScalarValue::from(num_elements);

        let scalars = container
            .into_iter()
            .map(|e| e.into_scalar())
            .collect::<DFResult<Vec<_>>>()?;
        let child_type = TSPoint::try_new_null(time_data_type, value_data_type)?.data_type()?;
        let point_list = ScalarValue::new_list_nullable(&scalars, &child_type);

        Ok(vec![point_list, num_elements])
    }

    fn try_from_arrays(
        input_data_types: &[DataType],
        arrays: &[ArrayRef],
    ) -> DFResult<Option<Self>> {
        debug_assert!(arrays.len() == 2, "MutableGaugeData requires 2 arrays.");
        debug_assert!(
            input_data_types.len() == 2,
            "MutableGaugeData requires 2 input data types."
        );

        let point_list_array = arrays[0].as_ref();
        let num_elements_array = downcast_value!(arrays[1].as_ref(), UInt64Array);

        trace::trace!("MutableGaugeData state: {:?}", arrays);

        (0..point_list_array.len())
            .map(|idx| {
                let point_list = ScalarValue::try_from_array(point_list_array, idx)?;

                let container = scalar_to_points(point_list)?;
                let num_elements = num_elements_array.value(idx);

                Ok(GaugeDataBuilder {
                    time_data_type: input_data_types[0].clone(),
                    value_data_type: input_data_types[1].clone(),
                    container,
                    num_elements,
                })
            })
            .reduce(|l, r| {
                let mut l = l?;
                l.merge(r?)?;
                Ok(l)
            })
            .transpose()
    }
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::datatypes::{DataType, TimeUnit};
    use datafusion::scalar::ScalarValue;

    use super::GaugeDataBuilder;
    use crate::extension::expr::aggregate_function::gauge::{GaugeData, TSPoint};

    fn mutable_gauge_data() -> GaugeDataBuilder {
        let time = DataType::Timestamp(TimeUnit::Second, None);
        let value = DataType::Float64;

        GaugeDataBuilder::new(time, value)
    }

    #[test]
    fn test_mutable_gauge_data_empty() {
        let mutable_gauge_data = mutable_gauge_data();
        let result = mutable_gauge_data.build();
        assert_eq!(result, None);
    }

    #[test]
    fn test_mutable_gauge_data_one() {
        let mut mutable_gauge_data = mutable_gauge_data();

        let point = TSPoint {
            ts: ScalarValue::from(2),
            val: ScalarValue::from(2.1),
        };

        mutable_gauge_data.push(point.clone()).unwrap();

        let expected = GaugeData {
            first: point.clone(),
            second: point.clone(),
            penultimate: point.clone(),
            last: point,
            num_elements: 0,
        };

        let result = mutable_gauge_data.build();

        assert_eq!(result, Some(expected));
    }

    #[test]
    fn test_mutable_gauge_data_two() {
        let mut mutable_gauge_data = mutable_gauge_data();

        let point1 = TSPoint {
            ts: ScalarValue::from(2),
            val: ScalarValue::from(2.1),
        };
        let point2 = TSPoint {
            ts: ScalarValue::from(3),
            val: ScalarValue::from(3.1),
        };

        mutable_gauge_data.push(point1.clone()).unwrap();
        mutable_gauge_data.push(point2.clone()).unwrap();

        let expected = GaugeData {
            first: point1.clone(),
            second: point2.clone(),
            penultimate: point1,
            last: point2,
            num_elements: 0,
        };

        let result = mutable_gauge_data.build();

        assert_eq!(result, Some(expected));
    }

    #[test]
    fn test_mutable_gauge_data_num_elements() {
        let mut mutable_gauge_data = mutable_gauge_data();

        let points = vec![
            TSPoint {
                ts: ScalarValue::from(2),
                val: ScalarValue::from(2.1),
            },
            TSPoint {
                ts: ScalarValue::from(3),
                val: ScalarValue::from(3.1),
            },
            TSPoint {
                ts: ScalarValue::from(4),
                val: ScalarValue::from(3.1),
            },
            TSPoint {
                ts: ScalarValue::from(5),
                val: ScalarValue::from(4.1),
            },
            TSPoint {
                ts: ScalarValue::from(6),
                val: ScalarValue::from(2.1),
            },
        ];

        for point in &points {
            mutable_gauge_data.push(point.clone()).unwrap();
        }

        mutable_gauge_data.add_num_elements(5);

        let expected = GaugeData {
            first: points[0].clone(),
            second: points[1].clone(),
            penultimate: points[3].clone(),
            last: points[4].clone(),
            num_elements: 5,
        };

        let result = mutable_gauge_data.build();

        assert_eq!(result, Some(expected));
    }
}
