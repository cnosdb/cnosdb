use std::collections::HashMap;
use std::ops::Not;
use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, AsArray, StructArray};
use datafusion::arrow::datatypes::{DataType, Field, Fields, IntervalUnit};
use datafusion::common::ScalarValue;
use datafusion::error::DataFusionError;
use spi::{DFResult, QueryError};

use crate::extension::expr::aggregate_function::state_agg::LIST_ELEMENT_NAME;
use crate::extension::expr::aggregate_function::AggResult;

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
///
/// ### DataType
///
/// ```plaintext
/// Struct {
///     "state_duration": todo,
///     "state_periods": todo,
/// }
/// ```
#[derive(Debug, Clone)]
pub struct StateAggData {
    state_duration: DurationStates,
    state_periods: StatePeriods,
    compact: bool,
}

impl StateAggData {
    pub(crate) fn new(time_data_type: DataType, state_data_type: DataType, compact: bool) -> Self {
        let state_duration = DurationStates::new(time_data_type.clone(), state_data_type.clone());
        let state_periods = StatePeriods::new(time_data_type, state_data_type);

        Self {
            state_duration,
            state_periods,
            compact,
        }
    }

    pub(crate) fn handle_record(&mut self, state: ScalarValue, time: ScalarValue) -> DFResult<()> {
        self.state_duration
            .handle_record(state.clone(), time.clone())?;

        if !self.compact {
            self.state_periods.handle_record(state, time)?;
        }

        Ok(())
    }

    pub(crate) fn finalize(&mut self) {
        self.state_duration.finalize();
        self.state_periods.finalize();
    }

    pub(crate) fn interval_datatype(&self) -> DFResult<DataType> {
        let a = self.state_duration.interval_data_type()?;
        if !self.compact {
            let b = self.state_periods.interval_data_type()?;
            debug_assert_eq!(a, b);
        }
        Ok(a)
    }
    pub(crate) fn is_compact(&self) -> bool {
        self.compact
    }

    fn get_materialized(&self, state: &ScalarValue) -> DFResult<ScalarValue> {
        Ok(self
            .state_duration
            .durations
            .get(state)
            .cloned()
            .unwrap_or(ScalarValue::new_zero(&DataType::Interval(
                IntervalUnit::MonthDayNano,
            ))?))
    }

    pub fn duration_in(
        &self,
        state: ScalarValue,
        start: ScalarValue,
        interval: ScalarValue,
    ) -> DFResult<ScalarValue> {
        if start.is_null() {
            let materialized = self.get_materialized(&state)?;
            return if materialized.is_null() {
                Ok(ScalarValue::new_zero(&self.interval_datatype()?)?)
            } else {
                Ok(materialized)
            };
        }
        let interval_datatype = self.interval_datatype()?;

        if let Some(periods) = self.state_periods.states.get(&state) {
            let mut sum = ScalarValue::new_zero(&interval_datatype)?;
            if !interval.is_null() {
                let end = start.add(interval)?;
                for period in periods
                    .iter()
                    .filter(|p| p.end_time.gt(&start) && p.start_time.lt(&end))
                {
                    let interval = match (period.start_time.lt(&start), period.end_time.gt(&end)) {
                        (true, true) => end.sub(start.clone()),
                        (true, false) => period.end_time.sub(start.clone()),
                        (false, true) => end.sub(period.start_time.clone()),
                        (false, false) => period.end_time.sub(period.start_time.clone()),
                    }?;
                    sum = sum.add(interval)?
                }
                Ok(sum)
            } else {
                for period in periods.iter().filter(|p| p.end_time.gt(&start)) {
                    let interval = if period.start_time.lt(&start) {
                        period.end_time.sub(start.clone())
                    } else {
                        period.end_time.sub(period.start_time.clone())
                    }?;
                    sum = sum.add(interval)?;
                }
                Ok(sum)
            }
        } else {
            Ok(ScalarValue::new_zero(&interval_datatype)?)
        }
    }

    pub fn state_at(&self, timestamp: &ScalarValue) -> DFResult<ScalarValue> {
        for (state, time_periods) in self.state_periods.states.iter() {
            if time_periods
                .iter()
                .filter(|p| p.start_time.le(timestamp) && timestamp.lt(&p.end_time))
                .peekable()
                .peek()
                .is_some()
            {
                return Ok(state.clone());
            }
        }
        let value = self.state_periods.state_data_type.clone().try_into()?;
        Ok(value)
    }
}

impl AggResult for StateAggData {
    fn into_scalar(self) -> DFResult<ScalarValue> {
        let state_duration = self.state_duration.into_scalar()?;
        let state_periods = self.state_periods.into_scalar()?;
        Ok(ScalarValue::Struct(Arc::new(StructArray::new(
            Fields::from(vec![
                Field::new("state_duration", state_duration.data_type(), true),
                Field::new("state_periods", state_periods.data_type(), true),
            ]),
            vec![state_duration.to_array()?, state_periods.to_array()?],
            None,
        ))))
    }
}

impl TryFrom<ScalarValue> for StateAggData {
    type Error = DataFusionError;

    fn try_from(value: ScalarValue) -> Result<Self, Self::Error> {
        match value {
            ScalarValue::Struct(state_agg_struct) => {
                let columns = state_agg_struct.columns();
                let state_duration = ScalarValue::try_from_array(&columns[0], 0)?;
                let state_periods = ScalarValue::try_from_array(&columns[1], 0)?;

                let state_duration = DurationStates::try_from(state_duration)?;
                let state_periods = StatePeriods::try_from(state_periods)?;

                let compact =
                    state_periods.states.is_empty() && state_duration.durations.is_empty().not();

                Ok(StateAggData {
                    state_duration,
                    state_periods,
                    compact,
                })
            }
            _ => Err(DataFusionError::External(Box::new(QueryError::Internal {
                reason: format!("Converting ScalarValue to StateAggData failed, expected a struct, but got: {value:?}"),
            }))),
        }
    }
}

/// ### DataType
///
/// ```plaintext
/// List [
///     Struct ("state", "periods") {
///         state:   ArrayRef<self.state_data_type>,
///         periods: List (DEFAULT) [
///             Struct ("start_time", "end_time") {
///                 start_time: ArrayRef<self.time_data_type>,
///                 end_time:   ArrayRef<self.time_data_type>,
///             }
///         ],
///     }
/// ]
/// ```
#[derive(Debug, Clone)]
pub struct StatePeriods {
    time_data_type: DataType,
    state_data_type: DataType,

    /// Optional `(time, state)` pair for the last record
    last: Option<(ScalarValue, ScalarValue)>,
    /// Map `time` to `Vec<TimePeriod>`
    states: HashMap<ScalarValue, Vec<TimePeriod>>,
}

impl StatePeriods {
    fn new(time_data_type: DataType, state_data_type: DataType) -> Self {
        Self {
            time_data_type,
            state_data_type,
            last: None,
            states: Default::default(),
        }
    }

    fn handle_record(&mut self, state: ScalarValue, time: ScalarValue) -> DFResult<()> {
        match &self.last {
            None => {
                self.last = Some((state, time));
            }
            Some((last_state, last_time)) => {
                let time_period = TimePeriod {
                    start_time: last_time.clone(),
                    end_time: time.clone(),
                };
                self.states
                    .entry(last_state.clone())
                    .or_default()
                    .push(time_period);
                self.last = Some((state, time));
            }
        }
        Ok(())
    }

    fn interval_data_type(&self) -> DFResult<DataType> {
        let a = ScalarValue::new_zero(&self.time_data_type)?;
        Ok(a.sub(a.clone())?.data_type())
    }

    fn finalize(&mut self) {}
}

impl AggResult for StatePeriods {
    fn into_scalar(self) -> DFResult<ScalarValue> {
        let StatePeriods {
            time_data_type,
            state_data_type,
            states,
            ..
        } = self;

        let periods_data_type = DataType::Struct(Fields::from(vec![
            Field::new("start_time", time_data_type.clone(), true),
            Field::new("end_time", time_data_type, true),
        ]));
        let state_with_periods_fields = Fields::from(vec![
            Field::new("state", state_data_type, true),
            Field::new(
                "periods",
                DataType::new_list(periods_data_type.clone(), true),
                true,
            ),
        ]);

        let mut states_with_periods_vec = Vec::with_capacity(states.len());
        for (state, periods) in states {
            let state_array = state.to_array()?;
            let periods_vec = TimePeriod::periods_into_scalar_values(periods)?;
            let periods_array = ScalarValue::new_list_nullable(&periods_vec, &periods_data_type);

            states_with_periods_vec.push(ScalarValue::Struct(Arc::new(StructArray::new(
                state_with_periods_fields.clone(),
                vec![state_array, periods_array],
                None,
            ))));
        }

        Ok(ScalarValue::List(ScalarValue::new_list_nullable(
            &states_with_periods_vec,
            &DataType::Struct(state_with_periods_fields),
        )))
    }
}

impl TryFrom<ScalarValue> for StatePeriods {
    type Error = DataFusionError;

    fn try_from(value: ScalarValue) -> Result<Self, Self::Error> {
        match value {
            ScalarValue::List(states_with_periods_list) => {
                let mut states = HashMap::new();
                for item in states_with_periods_list.iter().flatten() {
                    if let Some(state_with_periods_struct) = item.as_struct_opt() {
                        let col_num = state_with_periods_struct.num_columns();
                        if col_num < 2 {
                            return Err(DataFusionError::Execution(
                                format!("Converting ScalarValue to StatePeriods failed, expect at least 2 columns, but got {col_num}"),
                            ));
                        }
                        let status_array = state_with_periods_struct.column(col_num - 2);
                        let periods_array = state_with_periods_struct.column(col_num - 1);
                        let s = ScalarValue::try_from_array(status_array, 0)?;
                        let v = ScalarValue::try_from_array(periods_array, 0)?;
                        let periods = TimePeriod::periods_try_from_scalar_value(v)?;
                        states.insert(s, periods);
                    } else {
                        return Err(DataFusionError::Execution(
                            format!("Converting ScalarValue to StatePeriods failed, expect a struct, but got: {}", item.data_type()),
                        ));
                    }
                }
                let (state_data_type, time_data_type) =
                    match states.iter().peekable().peekable().peek() {
                        Some(states) => (states.0.data_type(), states.1[0].time_data_type()),
                        None => (DataType::Null, DataType::Null),
                    };
                Ok(Self {
                    time_data_type,
                    state_data_type,
                    last: None,
                    states,
                })
            }
            _ => Err(DataFusionError::Execution(format!(
                "Converting ScalarValue to StatePeriods failed, expect a list, but got {:?}",
                value.data_type()
            ))),
        }
    }
}

/// ### DataType
///
/// ```plaintext
/// Struct ("start_time", "end_time") {
///     start_time: ArrayRef<self.time_data_type>,
///     end_time:   ArrayRef<self.time_data_type>,
/// }
/// ```
#[derive(Debug, Clone)]
pub struct TimePeriod {
    start_time: ScalarValue,
    end_time: ScalarValue,
}

impl TryInto<ScalarValue> for TimePeriod {
    type Error = DataFusionError;

    fn try_into(self) -> Result<ScalarValue, Self::Error> {
        let time_period = StructArray::new(
            Fields::from(vec![
                Field::new("start_time", self.start_time.data_type(), true),
                Field::new("end_time", self.end_time.data_type(), true),
            ]),
            vec![self.start_time.to_array()?, self.end_time.to_array()?],
            None,
        );
        Ok(ScalarValue::Struct(Arc::new(time_period)))
    }
}

impl TryFrom<ScalarValue> for TimePeriod {
    type Error = DataFusionError;

    fn try_from(value: ScalarValue) -> Result<Self, Self::Error> {
        match value {
            ScalarValue::Struct(time_period_struct) => {
                let col_num = time_period_struct.num_columns();
                if col_num < 2 {
                    return Err(DataFusionError::Execution(
                        format!("Converting ScalarValue to TimePeriod failed, expect at least 2 columns, but got {col_num}"),
                    ));
                }
                let start_time_array = time_period_struct.column(col_num - 2);
                let end_time_array = time_period_struct.column(col_num - 1);
                let start_time = ScalarValue::try_from_array(start_time_array, 0)?;
                let end_time = ScalarValue::try_from_array(end_time_array, 0)?;
                Ok(Self {
                    start_time,
                    end_time,
                })
            }
            _ => Err(DataFusionError::Execution(format!(
                "Converting ScalarValue to TimePeriod failed, expect a struct, but got: {value:#?}",
            ))),
        }
    }
}

impl TimePeriod {
    pub fn time_data_type(&self) -> DataType {
        self.start_time.data_type()
    }

    pub fn periods_into_scalar_values(periods: Vec<Self>) -> DFResult<Vec<ScalarValue>> {
        let mut values: Vec<ScalarValue> = Vec::with_capacity(periods.len());
        for period in periods {
            values.push(period.try_into()?);
        }
        Ok(values)
    }

    pub fn periods_try_from_scalar_value(value: ScalarValue) -> DFResult<Vec<Self>> {
        match value {
            ScalarValue::List(periods_list) if !periods_list.is_empty() => {
                let periods_array = periods_list.value(0);
                if let Some(periods_array) = periods_array.as_struct_opt() {
                    
                }
                list_array
                    .values()
                    .into_iter()
                    .map(Self::try_from)
                    .collect::<DFResult<Vec<_>>>(),
            }
            _ => Err(DataFusionError::Execution(format!(
                "TryFrom<ScalarValue> for Vec<TimePeriod> failed, value: {:#?}",
                value
            ))),
        }
    }
}

/// - List `[Struct]`
///   - Struct `(state, duration)`
///     - state: xxx
///     - duration: xxx
/// ### DataType
///
/// ```plaintext
/// List [
///     Struct ("state", "duration") {
///         state:    ArrayRef<self.time_data_type>,
///         duration: ArrayRef<self.time_data_type>,
///     }
/// ]
/// ```
#[derive(Debug, Clone)]
pub struct DurationStates {
    time_data_type: DataType,
    state_data_type: DataType,
    // (state_value, time)
    last_state: Option<(ScalarValue, ScalarValue)>,
    // state_value -> duration
    durations: HashMap<ScalarValue, ScalarValue>,
}

impl TryInto<ScalarValue> for DurationStates {
    type Error = DataFusionError;

    fn try_into(self) -> Result<ScalarValue, Self::Error> {
        let DurationStates {
            time_data_type,
            state_data_type,
            durations,
            ..
        } = self;

        let mut values = Vec::with_capacity(durations.len());

        let interval_data_type = {
            let time = ScalarValue::new_zero(&time_data_type)?;
            time.sub(time.clone())?.data_type()
        };

        let fields = Fields::from(vec![
            Field::new("state", state_data_type, true),
            Field::new("duration", interval_data_type, true),
        ]);

        let element_data_type = DataType::Struct(fields.clone());

        for (state, duration) in durations {
            values.push(ScalarValue::Struct(Arc::new(StructArray::new(
                fields.clone(),
                vec![state.to_array()?, duration.to_array()?],
                None,
            ))));
        }

        let result_state = ScalarValue::new_list_nullable(&values, &element_data_type);

        Ok(ScalarValue::List(result_state))
    }
}

impl TryFrom<ScalarValue> for DurationStates {
    type Error = DataFusionError;

    fn try_from(value: ScalarValue) -> Result<Self, Self::Error> {
        let error = || {
            DataFusionError::Execution("TryFrom<ScalarValue> for DurationStates failed".to_string())
        };
        let scalar_values = ScalarValue::convert_array_to_scalar_vec(&value)?;
        match value {
            ScalarValue::List(list_array) => {
                let (state_data_type, time_data_type) = match list_array.value_type() {
                    DataType::Struct(fields) => match (fields.first(), fields.get(1)) {
                        (Some(a), Some(b)) => (a.data_type().clone(), b.data_type().clone()),
                        (_, _) => return Err(error()),
                    },
                    _ => return Err(error()),
                };
                if list_array.is_empty() {
                    return Ok(Self::new(time_data_type, state_data_type));
                }
                let mut durations = HashMap::new();
                for e in list_array.iter().flatten() {
                    match e {
                        ScalarValue::Struct(struct_array) => {
                            let columns = struct_array.columns();
                            if !columns.is_empty() {
                                let mut cols = columns.iter().rev();
                                let duration = cols.next();
                                let state = cols.next();
                                match (state, duration) {
                                    (Some(state), Some(duration)) => {
                                        durations.insert(state, duration);
                                    }
                                    _ => return Err(error()),
                                }
                            }
                        }
                        _ => return Err(error()),
                    }
                }
                Ok(Self {
                    time_data_type,
                    state_data_type,
                    last_state: None,
                    durations,
                })
            }
            _ => Err(error()),
        }
    }
}

impl DurationStates {
    fn new(time_data_type: DataType, state_data_type: DataType) -> Self {
        Self {
            time_data_type,
            state_data_type,
            last_state: None,
            durations: HashMap::new(),
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

    fn interval_data_type(&self) -> DFResult<DataType> {
        let a = ScalarValue::new_zero(&self.time_data_type)?;
        Ok(a.sub(a.clone())?.data_type())
    }
}

impl AggResult for DurationStates {
    fn into_scalar(self) -> DFResult<ScalarValue> {
        self.try_into()
    }
}

fn array_to_scalar_value(array: &dyn Array) -> DFResult<ScalarValue> {
    // TODO(zipper): make this more efficient.
    let array_list = ScalarValue::convert_array_to_scalar_vec(array)?;
    let scalar_value_vec: Vec<ScalarValue> = array_list.into_iter().flatten().collect();
    let scalar_list = ScalarValue::new_list_nullable(&scalar_value_vec, array.data_type());
    Ok(ScalarValue::List(scalar_list))
}

#[cfg(test)]
mod test {
    use datafusion::arrow::datatypes::{DataType, TimeUnit};
    use datafusion::scalar::ScalarValue;

    use super::{AggResult, DurationStates, StateAggData, StatePeriods};
    type TupleList = Vec<(ScalarValue, ScalarValue)>;

    fn input_tuple_list() -> TupleList {
        vec![
            (
                ScalarValue::TimestampNanosecond(Some(0), None),
                ScalarValue::Utf8(Some("111".to_string())),
            ),
            (
                ScalarValue::TimestampNanosecond(Some(3), None),
                ScalarValue::Utf8(Some("222".to_string())),
            ),
            (
                ScalarValue::TimestampNanosecond(Some(10), None),
                ScalarValue::Utf8(Some("222".to_string())),
            ),
            (
                ScalarValue::TimestampNanosecond(Some(100), None),
                ScalarValue::Utf8(Some("333".to_string())),
            ),
            (
                ScalarValue::TimestampNanosecond(Some(900), None),
                ScalarValue::Utf8(Some("111".to_string())),
            ),
            (
                ScalarValue::TimestampNanosecond(Some(1900), None),
                ScalarValue::Utf8(Some("222".to_string())),
            ),
        ]
    }

    fn duration_tuple_list() -> TupleList {
        vec![
            (
                ScalarValue::Utf8(Some("111".to_string())),
                ScalarValue::IntervalMonthDayNano(Some(1003)),
            ),
            (
                ScalarValue::Utf8(Some("222".to_string())),
                ScalarValue::IntervalMonthDayNano(Some(97)),
            ),
            (
                ScalarValue::Utf8(Some("333".to_string())),
                ScalarValue::IntervalMonthDayNano(Some(800)),
            ),
        ]
    }

    fn period_tuple_list() -> Vec<(ScalarValue, Vec<(ScalarValue, ScalarValue)>)> {
        vec![
            (
                ScalarValue::Utf8(Some("111".to_string())),
                vec![
                    (
                        ScalarValue::TimestampNanosecond(Some(0), None),
                        ScalarValue::TimestampNanosecond(Some(3), None),
                    ),
                    (
                        ScalarValue::TimestampNanosecond(Some(900), None),
                        ScalarValue::TimestampNanosecond(Some(1900), None),
                    ),
                ],
            ),
            (
                ScalarValue::Utf8(Some("222".to_string())),
                vec![
                    (
                        ScalarValue::TimestampNanosecond(Some(3), None),
                        ScalarValue::TimestampNanosecond(Some(10), None),
                    ),
                    (
                        ScalarValue::TimestampNanosecond(Some(10), None),
                        ScalarValue::TimestampNanosecond(Some(100), None),
                    ),
                ],
            ),
            (
                ScalarValue::Utf8(Some("333".to_string())),
                vec![(
                    ScalarValue::TimestampNanosecond(Some(100), None),
                    ScalarValue::TimestampNanosecond(Some(900), None),
                )],
            ),
        ]
    }

    #[test]
    fn test_duration_states() {
        let mut duration_states = DurationStates::new(
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            DataType::Utf8,
        );

        for (time, state) in input_tuple_list() {
            duration_states.handle_record(state, time).unwrap();
        }

        duration_states.finalize();

        let result = duration_states.into_scalar().unwrap();

        if let ScalarValue::List(list_array) = result {
            for v in list_array.values() {
                if let ScalarValue::Struct(struct_array) = v {
                    let state_and_duration = struct_array.columns();
                    let state = &state_and_duration[0];
                    let duration = &state_and_duration[1];

                    let exists = duration_tuple_list()
                        .iter()
                        .any(|(l, r)| state == l && duration == r);

                    assert!(
                        exists,
                        "expect_result {:?} does not contain state = {}, duration = {}",
                        duration_tuple_list(),
                        state,
                        duration
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

    #[test]
    fn test_period_states() {
        let mut period_states = StatePeriods::new(
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            DataType::Utf8,
        );
        for (time, state) in input_tuple_list() {
            period_states.handle_record(state, time).unwrap();
        }
        period_states.finalize();

        let result = period_states.into_scalar().unwrap();

        if let ScalarValue::List(list_array) = result {
            for v in list_array.values() {
                if let ScalarValue::Struct(struct_array) = v {
                    let mut period_states = struct_array.columns();
                    let state = &period_states[0];
                    let periods = &period_states[1];

                    let mut period_tuple = vec![];
                    if let ScalarValue::List(periods) = periods {
                        for period in periods {
                            if let ScalarValue::Struct(struct_array) = period {
                                let period = struct_array.columns();
                                period_tuple.push((period[0].clone(), period[1].clone()))
                            }
                        }
                    }

                    let exists = period_tuple_list().iter().any(|(l, r)| {
                        state == l && period_tuple.iter().zip(r.iter()).all(|(a, b)| a == b)
                    });

                    assert!(
                        exists,
                        "expect_result {:?} does not contain state = {}, time_period = {:?}",
                        duration_tuple_list(),
                        state,
                        period_tuple
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

    #[test]
    fn test_state_agg_serialize_deserialize() {
        let time = DataType::Timestamp(TimeUnit::Second, None);
        let value = DataType::Float64;

        let state_agg_serialize_deserialize = |mut state_agg: StateAggData| {
            let v = state_agg.clone().into_scalar().unwrap();
            let cp_state_agg: StateAggData = TryFrom::try_from(v.clone()).unwrap();
            let cp_v = cp_state_agg.into_scalar().unwrap();
            assert_eq!(v.to_string(), cp_v.to_string());

            state_agg
                .handle_record(
                    ScalarValue::Float64(Some(1.0)),
                    ScalarValue::TimestampNanosecond(Some(1), None),
                )
                .unwrap();
            state_agg
                .handle_record(
                    ScalarValue::Float64(Some(1.0)),
                    ScalarValue::TimestampNanosecond(Some(1), None),
                )
                .unwrap();

            let v = state_agg.into_scalar().unwrap();
            let cp_state_agg: StateAggData = TryFrom::try_from(v.clone()).unwrap();
            let cp_v = cp_state_agg.into_scalar().unwrap();
            assert_eq!(v.to_string(), cp_v.to_string());
        };

        let state_agg = StateAggData::new(time.clone(), value.clone(), false);
        state_agg_serialize_deserialize(state_agg);

        let state_agg = StateAggData::new(time, value, true);
        state_agg_serialize_deserialize(state_agg);
    }

    #[test]
    fn test_state_agg_compact() {
        let mut state_agg = StateAggData::new(
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            DataType::Utf8,
            true,
        );
        for (time, state) in input_tuple_list() {
            state_agg.handle_record(state, time).unwrap();
        }
        state_agg.finalize();

        let a = state_agg.into_scalar().unwrap();
        let b: StateAggData = TryFrom::try_from(a).unwrap();
        assert!(b.compact);

        let mut state_agg = StateAggData::new(
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            DataType::Utf8,
            false,
        );
        for (time, state) in input_tuple_list() {
            state_agg.handle_record(state, time).unwrap();
        }
        state_agg.finalize();

        let a = state_agg.into_scalar().unwrap();
        let b: StateAggData = TryFrom::try_from(a).unwrap();
        assert!(!b.compact);
    }
}
