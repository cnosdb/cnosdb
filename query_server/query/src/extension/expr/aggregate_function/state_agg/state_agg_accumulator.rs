use datafusion::arrow::array::ArrayRef;
use datafusion::arrow::datatypes::DataType;
use datafusion::common::ScalarValue;
use datafusion::error::DataFusionError;
use datafusion::physical_plan::Accumulator;
use spi::{DFResult, QueryError};

use crate::extension::expr::aggregate_function::state_agg::StateAggData;
use crate::extension::expr::aggregate_function::AggResult;

/// An accumulator to compute the average
#[derive(Debug)]
pub struct StateAggAccumulator {
    state: IntermediateState,
    input_type: Vec<DataType>,
    compact: bool,
}

impl StateAggAccumulator {
    pub fn try_new(input_type: Vec<DataType>, compact: bool) -> DFResult<Self> {
        Ok(Self {
            state: Default::default(),
            input_type,
            compact,
        })
    }
}

impl Accumulator for StateAggAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> DFResult<()> {
        trace::trace!("update_batch: {:?}", values);

        if values.is_empty() {
            return Ok(());
        }

        debug_assert!(
            values.len() == 2,
            "compact_state_agg can only take 2 param!"
        );

        let times = ScalarValue::convert_array_to_scalar_vec(&values[0])?;
        let state_records = ScalarValue::convert_array_to_scalar_vec(&values[1])?;
        self.state.extend(times, state_records);

        Ok(())
    }

    fn evaluate(&mut self) -> DFResult<ScalarValue> {
        let indices = self.state.sort_indices();

        let mut state_agg_data = StateAggData::new(
            self.input_type[0].clone(),
            self.input_type[1].clone(),
            self.compact,
        );

        for idx in indices {
            match (
                self.state.state_value_records.get(idx),
                self.state.time_records.get(idx),
            ) {
                (Some(state_value), Some(time)) => {
                    if time.is_null() {
                        continue;
                    }
                    // All records must be processed in time order
                    state_agg_data.handle_record(state_value.clone(), time.clone())?;
                }
                _ => {
                    return Err(DataFusionError::External(Box::new(QueryError::Internal {
                        reason:
                            "The internal state data of CompactStateAggAccumulator is inconsistent."
                                .to_string(),
                    })))
                }
            }
        }
        state_agg_data.finalize();

        let result_state = state_agg_data.into_scalar()?;

        trace::trace!(
            "CompactStateAggAccumulator evaluate result: {:?}",
            result_state
        );

        Ok(result_state)
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self)
    }

    fn state(&mut self) -> DFResult<Vec<ScalarValue>> {
        if self.state.is_empty() {
            return empty_intermediate_state(&self.input_type);
        }

        let state = self.state.to_state()?;

        trace::trace!("CompactStateAggAccumulator state: {:?}", state,);

        Ok(state)
    }

    fn merge_batch(&mut self, values: &[ArrayRef]) -> DFResult<()> {
        trace::trace!("merge_batch: {:?}", values);

        if values.is_empty() {
            return Ok(());
        }

        debug_assert!(
            values.len() == 2,
            "merge_batch of compact_state_agg can only take 2 param."
        );

        let state = IntermediateState::from_state(values)?;

        self.state.append(state);

        Ok(())
    }
}

fn empty_intermediate_state(state_type: &[DataType]) -> DFResult<Vec<ScalarValue>> {
    let result = state_type
        .iter()
        .map(|dt| ScalarValue::List(ScalarValue::new_list_nullable(&[], dt)))
        .collect();

    Ok(result)
}

#[derive(Default, Debug)]
struct IntermediateState {
    time_records: Vec<ScalarValue>,
    state_value_records: Vec<ScalarValue>,
}

impl IntermediateState {
    fn extend(
        &mut self,
        time_records: Vec<Vec<ScalarValue>>,
        state_records: Vec<Vec<ScalarValue>>,
    ) {
        for records in time_records {
            self.time_records.extend(records);
        }
        for records in state_records {
            self.state_value_records.extend(records);
        }
    }

    fn append(&mut self, state: IntermediateState) {
        self.time_records.extend(state.time_records);
        self.state_value_records.extend(state.state_value_records);
    }

    fn sort_indices(&self) -> Vec<usize> {
        let mut sort = self.time_records.iter().enumerate().collect::<Vec<_>>();
        sort.sort_unstable_by(|(_, a), (_, b)| unsafe { a.partial_cmp(b).unwrap_unchecked() });
        sort.iter().map(|(i, _)| *i).collect()
    }
}

impl IntermediateState {
    fn is_empty(&self) -> bool {
        self.time_records.is_empty() || self.state_value_records.is_empty()
    }

    fn from_state(values: &[ArrayRef]) -> DFResult<Self> {
        if values.is_empty() {
            return Ok(Self::default());
        }

        debug_assert!(values.len() == 2, "compact_state_agg can only take 2 param");

        let mut time_records: Vec<ScalarValue> = vec![];
        let mut state_value_records: Vec<ScalarValue> = vec![];

        let times = ScalarValue::convert_array_to_scalar_vec(&values[0])?.into_iter();
        let states = ScalarValue::convert_array_to_scalar_vec(&values[1])?.into_iter();

        debug_assert!(
            times.len() == states.len(),
            "all columns of the input must have the same length"
        );

        for (time_values, state_values) in times.zip(states) {
            time_records.extend(time_values);
            state_value_records.extend(state_values);
        }

        Ok(Self {
            time_records,
            state_value_records,
        })
    }

    fn to_state(&self) -> DFResult<Vec<ScalarValue>> {
        if self.is_empty() {
            return Err(DataFusionError::External(Box::new(QueryError::Internal {
                reason: "IntermediateState is empty".to_string(),
            })));
        }

        let time_data_type = self.time_records[0].data_type();
        let state_data_type = self.state_value_records[0].data_type();

        let time_state = ScalarValue::new_list_nullable(&self.time_records, &time_data_type);
        let value_state =
            ScalarValue::new_list_nullable(&self.state_value_records, &state_data_type);

        trace::trace!("time_state data type: {:?}", time_state.value_type());

        Ok(vec![
            ScalarValue::List(time_state),
            ScalarValue::List(value_state),
        ])
    }
}
