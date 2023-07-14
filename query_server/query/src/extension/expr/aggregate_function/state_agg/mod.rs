use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field, Fields};
use datafusion::common::Result as DFResult;
use datafusion::scalar::ScalarValue;
use spi::query::function::FunctionMetadataManager;
use spi::QueryError;

use super::AggResult;
mod compact_state_agg;

const LIST_ELEMENT_NAME: &str = "item";

pub fn register_udafs(func_manager: &mut dyn FunctionMetadataManager) -> Result<(), QueryError> {
    compact_state_agg::register_udaf(func_manager)?;
    Ok(())
}

/// Struct(
///     "state_duration": List[
///         Struct("state": duration),
///         Struct("state": duration),
///         ......
///     ],
///     "state_periods": List[
///         Struct(
///             "state": xxx,
///             "periods": List[
///                 Struct("start_time": time, "end_time": time),
///                 Struct("start_time": time, "end_time": time),
///                 ......
///             ]
///         ),
///         ......
///     ]
/// )
#[derive(Debug)]
struct StateAggData {
    state_duration: DurationStates,
    state_periods: StatePeriods,
    compact: bool,
}

impl StateAggData {
    fn new(time_data_type: DataType, state_data_type: DataType, compact: bool) -> Self {
        let state_duration = DurationStates::new(time_data_type.clone(), state_data_type.clone());
        let state_periods = StatePeriods::new(time_data_type, state_data_type);

        Self {
            state_duration,
            state_periods,
            compact,
        }
    }

    fn handle_record(&mut self, state: ScalarValue, time: ScalarValue) -> DFResult<()> {
        self.state_duration
            .handle_record(state.clone(), time.clone())?;

        if !self.compact {
            self.state_periods.handle_record(state, time)?;
        }

        Ok(())
    }

    fn finalize(&mut self) {
        self.state_duration.finalize();
    }
}

impl AggResult for StateAggData {
    fn to_scalar(self) -> DFResult<ScalarValue> {
        let state_duration = self.state_duration.to_scalar()?;
        let state_periods = self.state_periods.to_scalar()?;
        let state_duration_datatype = state_duration.get_datatype();
        let state_periods_datatype = state_periods.get_datatype();
        Ok(ScalarValue::Struct(
            Some(vec![state_duration, state_periods]),
            Fields::from([
                Arc::new(Field::new("state_duration", state_duration_datatype, true)),
                Arc::new(Field::new("state_periods", state_periods_datatype, true)),
            ]),
        ))
    }
}

#[derive(Debug)]
struct StatePeriods {
    time_data_type: DataType,
    state_data_type: DataType,
    states: HashMap<ScalarValue, Vec<TimePeriod>>,
}

impl StatePeriods {
    fn new(time_data_type: DataType, state_data_type: DataType) -> Self {
        Self {
            time_data_type,
            state_data_type,
            states: Default::default(),
        }
    }

    fn handle_record(&mut self, _state: ScalarValue, _time: ScalarValue) -> DFResult<()> {
        // TODO
        Ok(())
    }
}

/// List[
///     Struct(
///         "state": xxx,
///         "periods": List[
///             Struct("start_time": time, "end_time": time),
///             Struct("start_time": time, "end_time": time),
///             ......
///         ]
///     ),
///     ......
/// ]
impl AggResult for StatePeriods {
    fn to_scalar(self) -> DFResult<ScalarValue> {
        let StatePeriods {
            time_data_type,
            state_data_type,
            states,
            ..
        } = self;

        let element_fields = Fields::from([
            Arc::new(Field::new("start_time", time_data_type.clone(), true)),
            Arc::new(Field::new("end_time", time_data_type, true)),
        ]);

        let period_data_type = DataType::Struct(element_fields.clone());

        let period_list_data_type = DataType::List(Arc::new(Field::new(
            LIST_ELEMENT_NAME,
            period_data_type.clone(),
            true,
        )));

        let state_period_data_type = DataType::Struct(Fields::from([
            Arc::new(Field::new("state", state_data_type.clone(), true)),
            Arc::new(Field::new("periods", period_list_data_type.clone(), true)),
        ]));

        let mut states_with_periods = Vec::with_capacity(states.len());
        for (state, periods) in states {
            let df_periods = periods
                .into_iter()
                .map(
                    |TimePeriod {
                         start_time,
                         end_time,
                     }| {
                        ScalarValue::Struct(
                            Some(vec![start_time, end_time]),
                            element_fields.clone(),
                        )
                    },
                )
                .collect::<Vec<_>>();
            let df_periods_list = ScalarValue::new_list(Some(df_periods), period_data_type.clone());

            let state_with_periods = ScalarValue::Struct(
                Some(vec![state, df_periods_list]),
                Fields::from([
                    Arc::new(Field::new("state", state_data_type.clone(), true)),
                    Arc::new(Field::new("periods", period_list_data_type.clone(), true)),
                ]),
            );

            states_with_periods.push(state_with_periods);
        }

        let result_state = ScalarValue::new_list(Some(states_with_periods), state_period_data_type);

        Ok(result_state)
    }
}

#[derive(Debug)]
pub struct TimePeriod {
    start_time: ScalarValue,
    end_time: ScalarValue,
}

/// List[
///     Struct("state": duration),
///     Struct("state": duration),
///     ......
/// ]
#[derive(Debug)]
struct DurationStates {
    time_data_type: DataType,
    state_data_type: DataType,
    // (state_value, time)
    last_state: Option<(ScalarValue, ScalarValue)>,
    // state_value -> duration
    durations: HashMap<ScalarValue, ScalarValue>,
}

impl DurationStates {
    fn new(time_data_type: DataType, state_data_type: DataType) -> Self {
        Self {
            time_data_type,
            state_data_type,
            last_state: None,
            durations: std::collections::HashMap::new(),
        }
    }

    fn handle_record(&mut self, state: ScalarValue, time: ScalarValue) -> DFResult<()> {
        match self.last_state.take() {
            None => self.last_state = Some((state, time)),
            Some((last_state, last_time)) => {
                debug_assert!(time >= last_time);
                let this_duration = time.sub(last_time)?;
                self.last_state = Some((state, time));
                match self.durations.get_mut(&last_state) {
                    None => {
                        self.durations.insert(last_state, this_duration);
                    }
                    Some(duration) => {
                        let new_duration = duration.add(this_duration)?;
                        *duration = new_duration;
                    }
                }
            }
        }

        Ok(())
    }

    // It's possible that our last seen state was unique, in which case we'll have to
    // add a 0 duration entry so that we can handle rollup and interpolation calls
    fn finalize(&mut self) {
        if let Some((last_state, _)) = self.last_state.take() {
            self.durations
                .entry(last_state)
                .or_insert(ScalarValue::IntervalMonthDayNano(Some(0)));
        }
    }
}

impl AggResult for DurationStates {
    fn to_scalar(self) -> DFResult<ScalarValue> {
        let DurationStates {
            time_data_type,
            state_data_type,
            durations,
            ..
        } = self;

        let mut values = Vec::with_capacity(durations.len());

        let time = ScalarValue::new_zero(&time_data_type)?;
        let interval_data_type = time.sub(time.clone())?.get_datatype();

        let field = Fields::from(vec![
            Field::new("state", state_data_type, true),
            Field::new("duration", interval_data_type, true),
        ]);

        let element_data_type = DataType::Struct(field.clone());

        for (state, duration) in durations {
            values.push(ScalarValue::Struct(
                Some(vec![state, duration]),
                field.clone(),
            ));
        }

        let result_state = ScalarValue::new_list(Some(values), element_data_type);

        Ok(result_state)
    }
}

#[cfg(test)]
mod test {
    use datafusion::arrow::datatypes::{DataType, TimeUnit};
    use datafusion::scalar::ScalarValue;

    use super::DurationStates;
    use crate::extension::expr::aggregate_function::AggResult;

    type TupleList = Vec<(ScalarValue, ScalarValue)>;

    fn records_with_result() -> (TupleList, TupleList) {
        #[rustfmt::skip]
        (vec![
            (ScalarValue::TimestampNanosecond(Some(0), None), ScalarValue::Utf8(Some("111".into()))),
            (ScalarValue::TimestampNanosecond(Some(3), None), ScalarValue::Utf8(Some("222".into()))),
            (ScalarValue::TimestampNanosecond(Some(10), None), ScalarValue::Utf8(Some("222".into()))),
            (ScalarValue::TimestampNanosecond(Some(100), None), ScalarValue::Utf8(Some("333".into()))),
            (ScalarValue::TimestampNanosecond(Some(900), None), ScalarValue::Utf8(Some("111".into()))),
            (ScalarValue::TimestampNanosecond(Some(1900), None), ScalarValue::Utf8(Some("222".into()))),
        ],
        vec![
            (ScalarValue::Utf8(Some("111".into())), ScalarValue::IntervalMonthDayNano(Some(1003))),
            (ScalarValue::Utf8(Some("222".into())), ScalarValue::IntervalMonthDayNano(Some(97))),
            (ScalarValue::Utf8(Some("333".into())), ScalarValue::IntervalMonthDayNano(Some(800))),
        ])
    }

    #[test]
    fn test_duration_states() {
        let mut duration_states = DurationStates::new(
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            DataType::Utf8,
        );

        let (records, expect_result) = records_with_result();

        for (time, state) in records {
            duration_states.handle_record(state, time).unwrap();
        }

        duration_states.finalize();

        let result = duration_states.to_scalar().unwrap();

        if let ScalarValue::List(Some(values), _) = result {
            for v in values {
                if let ScalarValue::Struct(Some(state_and_duration), _) = v {
                    let state = &state_and_duration[0];
                    let duration = &state_and_duration[1];

                    let exists = expect_result
                        .iter()
                        .any(|(l, r)| state == l && duration == r);

                    assert!(
                        exists,
                        "expect_result {:?} does not contain state = {}, duration = {}",
                        expect_result, state, duration
                    );

                    assert!(exists);
                } else {
                    panic!("result is not a struct")
                }
            }
        } else {
            panic!("result is not a list")
        }
    }
}
