use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef};
use datafusion::arrow::datatypes::{DataType, Field};
use datafusion::common::{DataFusionError, Result as DFResult};
use datafusion::logical_expr::type_coercion::aggregates::{STRINGS, TIMESTAMPS};
use datafusion::logical_expr::{
    AccumulatorFactoryFunction, AggregateUDF, ReturnTypeFunction, Signature, StateTypeFunction,
    TypeSignature, Volatility,
};
use datafusion::physical_plan::Accumulator;
use datafusion::scalar::ScalarValue;
use spi::query::function::FunctionMetadataManager;
use spi::QueryError;

use crate::extension::expr::aggregate_function::state_agg::{
    AggResult, StateAggData, LIST_ELEMENT_NAME,
};
use crate::extension::expr::aggregate_function::COMPACT_STATE_AGG_UDAF_NAME;
use crate::extension::expr::INTEGERS;

pub fn register_udaf(func_manager: &mut dyn FunctionMetadataManager) -> Result<(), QueryError> {
    func_manager.register_udaf(new())?;
    Ok(())
}

const CHECK_ARGS_FUNC: &dyn Fn(&[DataType]) -> DFResult<()> = &|input: &[DataType]| {
    if input.len() != 2 {
        return Err(DataFusionError::External(Box::new(QueryError::Analyzer {
            err: format!(
                "The function {:?} expects {} arguments, but {} were provided",
                COMPACT_STATE_AGG_UDAF_NAME,
                2,
                input.len()
            ),
        })));
    }

    Ok(())
};

fn new() -> AggregateUDF {
    let return_type_func: ReturnTypeFunction = Arc::new(move |input| {
        CHECK_ARGS_FUNC(input)?;

        let result = StateAggData::new(input[0].clone(), input[1].clone(), false);
        let date_type = result.to_scalar()?.get_datatype();

        trace::trace!("return_type: {:?}", date_type);

        Ok(Arc::new(date_type))
    });

    let state_type_func: StateTypeFunction = Arc::new(move |input, _| {
        CHECK_ARGS_FUNC(input)?;

        let types = input
            .iter()
            .map(|dt| DataType::List(Arc::new(Field::new(LIST_ELEMENT_NAME, dt.clone(), true))))
            .collect::<Vec<_>>();

        trace::trace!("state_type: {:?}", types);

        Ok(Arc::new(types))
    });

    let accumulator: AccumulatorFactoryFunction = Arc::new(|input, _| {
        CHECK_ARGS_FUNC(input)?;

        Ok(Box::new(CompactStateAggAccumulator::try_new(
            input.to_vec(),
        )?))
    });

    // compact_state_agg(
    //   ts TIMESTAMPTZ,
    //   value {STRINGS | INTEGERS}
    // ) RETURNS StateAggData
    let type_signatures = STRINGS
        .iter()
        .chain(INTEGERS.iter())
        .flat_map(|t| {
            TIMESTAMPS
                .iter()
                .map(|s_t| TypeSignature::Exact(vec![s_t.clone(), t.clone()]))
        })
        .collect();

    AggregateUDF::new(
        COMPACT_STATE_AGG_UDAF_NAME,
        &Signature::one_of(type_signatures, Volatility::Immutable),
        &return_type_func,
        &accumulator,
        &state_type_func,
    )
}

/// An accumulator to compute the average
#[derive(Debug)]
struct CompactStateAggAccumulator {
    state: IntermediateState,

    input_type: Vec<DataType>,
}

impl CompactStateAggAccumulator {
    pub fn try_new(input_type: Vec<DataType>) -> DFResult<Self> {
        Ok(Self {
            state: Default::default(),
            input_type,
        })
    }
}

impl Accumulator for CompactStateAggAccumulator {
    fn state(&self) -> DFResult<Vec<ScalarValue>> {
        if self.state.is_empty() {
            return empty_intermediate_state(&self.input_type);
        }

        let state = self.state.to_state()?;

        trace::trace!("CompactStateAggAccumulator state: {:?}", state,);

        Ok(state)
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> DFResult<()> {
        trace::trace!("update_batch: {:?}", values);

        if values.is_empty() {
            return Ok(());
        }

        debug_assert!(
            values.len() == 2,
            "compact_state_agg can only take 2 param!"
        );

        let times = &values[0];
        let state_records = &values[1];

        (0..times.len()).try_for_each(|index| {
            let time_record = ScalarValue::try_from_array(times.as_ref(), index)?;
            let state_record = ScalarValue::try_from_array(state_records.as_ref(), index)?;

            self.state.push(time_record, state_record);

            Ok(())
        })
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

    fn evaluate(&self) -> DFResult<ScalarValue> {
        let indices = self.state.sort_indices();

        let mut state_agg_data =
            StateAggData::new(self.input_type[0].clone(), self.input_type[1].clone(), true);

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

        let result_state = state_agg_data.to_scalar()?;

        trace::trace!(
            "CompactStateAggAccumulator evaluate result: {:?}",
            result_state
        );

        Ok(result_state)
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self)
    }
}

fn empty_intermediate_state(state_type: &[DataType]) -> DFResult<Vec<ScalarValue>> {
    let result = state_type
        .iter()
        .map(|e| ScalarValue::new_list(Some(vec![]), e.clone()))
        .collect();

    Ok(result)
}

#[derive(Default, Debug)]
struct IntermediateState {
    time_records: Vec<ScalarValue>,
    state_value_records: Vec<ScalarValue>,
}

impl IntermediateState {
    fn push(&mut self, time_record: ScalarValue, state_record: ScalarValue) {
        self.time_records.push(time_record);
        self.state_value_records.push(state_record);
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

        let mut time_records = vec![];
        let mut state_value_records = vec![];

        let times = &values[0];
        let state_records = &values[1];

        debug_assert!(
            times.len() == state_records.len(),
            "all columns of the input must have the same length"
        );

        (0..times.len()).try_for_each(|index| {
            let scalar = ScalarValue::try_from_array(times.as_ref(), index)?;
            if let ScalarValue::List(Some(values), _) = scalar {
                time_records.extend(values);
            } else {
                return Err(DataFusionError::Internal(format!(
                    "compact_state_agg state must be non-null list, but found: {}",
                    scalar.get_datatype()
                )));
            }

            let scalar = ScalarValue::try_from_array(state_records.as_ref(), index)?;
            if let ScalarValue::List(Some(values), _) = scalar {
                state_value_records.extend(values);
            } else {
                return Err(DataFusionError::Internal(format!(
                    "compact_state_agg state must be non-null list, but found: {}",
                    scalar.get_datatype()
                )));
            }

            Ok(())
        })?;

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

        let time_data_type = self.time_records[0].get_datatype();
        let state_data_type = self.state_value_records[0].get_datatype();

        let time_state = ScalarValue::new_list(Some(self.time_records.clone()), time_data_type);
        let value_state =
            ScalarValue::new_list(Some(self.state_value_records.clone()), state_data_type);

        trace::trace!("time_state data type: {:?}", time_state.get_datatype());

        Ok(vec![time_state, value_state])
    }
}
