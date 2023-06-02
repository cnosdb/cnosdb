use std::cmp;
use std::collections::HashSet;
use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, UInt32Array};
use datafusion::arrow::compute::{self, take};
use datafusion::arrow::datatypes::{DataType, Field, UInt32Type};
use datafusion::common::cast::{as_list_array, as_primitive_array};
use datafusion::common::{downcast_value, DataFusionError, Result as DFResult};
use datafusion::logical_expr::type_coercion::aggregates::{
    DATES, NUMERICS, STRINGS, TIMES, TIMESTAMPS,
};
use datafusion::logical_expr::{
    AccumulatorFunctionImplementation, AggregateUDF, ReturnTypeFunction, Signature,
    StateTypeFunction, TypeSignature, Volatility,
};
use datafusion::physical_plan::Accumulator;
use datafusion::scalar::ScalarValue;
use rand::Rng;
use spi::query::function::FunctionMetadataManager;
use spi::Result;

use super::SAMPLE_UDAF_NAME;
use crate::extension::expr::{BINARYS, INTEGERS};

pub fn register_udaf(func_manager: &mut dyn FunctionMetadataManager) -> Result<AggregateUDF> {
    let udf = new();
    func_manager.register_udaf(udf.clone())?;
    Ok(udf)
}

fn new() -> AggregateUDF {
    let return_type: ReturnTypeFunction = Arc::new(move |input| {
        let date_type = DataType::List(Box::new(Field::new("item", input[0].clone(), true)));
        Ok(Arc::new(date_type))
    });

    let state_type: StateTypeFunction =
        Arc::new(move |output| Ok(Arc::new(vec![output.clone(), DataType::UInt32])));

    let accumulator: AccumulatorFunctionImplementation = Arc::new(|output| match output {
        DataType::List(f) => Ok(Box::new(SampleAccumulator::try_new(
            output.clone(),
            f.data_type().clone(),
        )?)),
        _ => {
            panic!()
        }
    });

    let type_signatures = STRINGS
        .iter()
        .chain(NUMERICS.iter())
        .chain(TIMESTAMPS.iter())
        .chain(DATES.iter())
        .chain(BINARYS.iter())
        .chain(TIMES.iter())
        .flat_map(|t| {
            INTEGERS
                .iter()
                .map(|s_t| TypeSignature::Exact(vec![t.clone(), s_t.clone()]))
        })
        .collect();

    AggregateUDF::new(
        SAMPLE_UDAF_NAME,
        &Signature::one_of(type_signatures, Volatility::Volatile),
        &return_type,
        &accumulator,
        &state_type,
    )
}

/// Intermediate state data + number of samples
type IntermediateSampleState = (Vec<ScalarValue>, usize);

/// An accumulator to compute the average
#[derive(Debug)]
struct SampleAccumulator {
    states: Vec<IntermediateSampleState>,

    list_type: DataType,
    child_type: DataType,
}

impl Accumulator for SampleAccumulator {
    fn state(&self) -> DFResult<Vec<ScalarValue>> {
        if self.states.is_empty() {
            return empty_intermediate_sample_state(&self.list_type);
        }

        let (scalars, sample_n) = self.sample_state()?;

        let state = ScalarValue::new_list(Some(scalars), self.child_type.clone());
        let sample_n = ScalarValue::UInt32(Some(sample_n as u32));

        trace::trace!("SampleAccumulator state: {:?}", state);

        Ok(vec![state, sample_n])
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> DFResult<()> {
        trace::trace!("update_batch: {:?}", values);

        // Get the number of samples
        if let Some(sample_n) = extract_sample_n(values[1].as_ref())? {
            return self.update_batch_inner(values[0].clone(), sample_n);
        }

        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> DFResult<()> {
        trace::trace!("merge_batch: {:?}", states);

        let state_col = as_list_array(states[0].as_ref())?;
        let sample_n_col = downcast_value!(states[1], UInt32Array);

        if state_col.is_empty() {
            return Ok(());
        }

        state_col.iter().zip(sample_n_col).try_for_each(|e| {
            match e {
                (Some(state), Some(sample_n)) if !state.is_empty() => {
                    self.update_batch_inner(state, sample_n as usize)?;
                }
                _ => {
                    trace::info!("merge_batch, skip empty state: {:?}", e)
                }
            }

            Ok(())
        })
    }

    fn evaluate(&self) -> DFResult<ScalarValue> {
        let result = self.state()?;

        trace::trace!("SampleAccumulator evaluate result: {:?}", result);

        Ok(result[0].clone())
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self)
    }
}

impl SampleAccumulator {
    /// Creates a new `SampleAccumulator`
    pub fn try_new(list_type: DataType, child_type: DataType) -> DFResult<Self> {
        // let state = ScalarValue::new_list(None, field.data_type().clone());
        Ok(Self {
            states: vec![],
            list_type,
            child_type,
        })
    }

    /// Sample the input data
    fn sample_data(&self, arr: ArrayRef, sample_n: usize) -> DFResult<ArrayRef> {
        if arr.len() <= sample_n {
            trace::trace!("The size of data {} is less than the number of samples {}, use the original data directly", arr.len(), sample_n);
            // use arr directly
            Ok(arr)
        } else {
            trace::trace!("Take {} samples", sample_n);
            // random sampling
            let indices = UInt32Array::from(generate_unique_random_numbers(
                sample_n as u32,
                0,
                arr.len() as u32,
            ));
            Ok(take(arr.as_ref(), &indices, None)?)
        }
    }

    /// Sample the state
    fn sample_state(&self) -> DFResult<IntermediateSampleState> {
        let states = &self.states;

        let total_num = states.iter().map(|(e, _)| e.len()).sum::<usize>();

        let mut sample_n = 0;
        let mut result = vec![];

        for (s, r) in states {
            sample_n = *r;
            let num = s.len();
            let select_num = (num * sample_n + sample_n - 1) / total_num;
            let indices = generate_unique_random_numbers(select_num as u32, 0, num as u32);
            for i in indices {
                result.push(s[i as usize].clone());
            }
        }

        Ok((result, sample_n))
    }

    /// Try to merge the state
    /// If the amount of state data is greater than 10 times that of remain, merge it
    fn try_compact_state(&self, sample_n: usize) -> DFResult<Option<IntermediateSampleState>> {
        let num_rows = self.states.iter().map(|(e, _)| e.len()).sum::<usize>();
        if num_rows > sample_n * 10 {
            trace::trace!("Merge existing data: {}", num_rows);
            // compact
            Ok(Some(self.sample_state()?))
        } else {
            Ok(None)
        }
    }

    fn save_state(&mut self, state: IntermediateSampleState) {
        self.states.push(state)
    }

    fn set_state(&mut self, state: IntermediateSampleState) {
        self.states = vec![state];
    }

    fn update_batch_inner(&mut self, arr: ArrayRef, sample_n: usize) -> DFResult<()> {
        trace::trace!("update_batch_inner: {:?}, sample_n: {}", arr, sample_n);
        // sample
        let sampled_arr = self.sample_data(arr, sample_n)?;

        let df_values = arrow_array_to_df_values(sampled_arr.as_ref())?;

        // save the sampling result
        self.save_state((df_values, sample_n));
        // try to merge saved sample results
        // If merged every time, it will cause all sampling results to be traversed every time, which is inefficient
        // If not merged, it will lead to excessive memory usage
        if let Some(state) = self.try_compact_state(sample_n)? {
            self.set_state(state);
        }

        Ok(())
    }
}

fn generate_unique_random_numbers(count: u32, min: u32, max: u32) -> Vec<u32> {
    let count = cmp::min(count, max - min);

    let mut rng = rand::thread_rng();
    let mut unique_numbers = HashSet::with_capacity(count as usize);
    let mut result = Vec::new();

    while (unique_numbers.len() as u32) < count {
        let random_number = rng.gen_range(min..max);
        unique_numbers.insert(random_number);
    }

    for number in unique_numbers {
        result.push(number);
    }

    result
}

fn arrow_array_to_df_values(arr: &dyn Array) -> DFResult<Vec<ScalarValue>> {
    let size = arr.len();

    let mut result = Vec::with_capacity(size);
    for i in 0..size {
        result.push(ScalarValue::try_from_array(arr, i)?);
    }

    Ok(result)
}

/// Get the number of samples
fn extract_sample_n(arr: &dyn Array) -> DFResult<Option<usize>> {
    let sample_n = unsafe {
        if arr.is_empty() {
            return Ok(None);
        }
        let remain_arr = compute::cast(arr, &DataType::UInt32)?;
        as_primitive_array::<UInt32Type>(remain_arr.as_ref())?.value_unchecked(0)
    } as usize;

    // (0, 429496729]
    if sample_n > 0 && sample_n <= 2000 {
        return Ok(Some(sample_n));
    }

    Err(DataFusionError::Plan(format!(
        "The number of sample points for function '{SAMPLE_UDAF_NAME}' must be (0, 2000]"
    )))
}

fn empty_intermediate_sample_state(output_type: &DataType) -> DFResult<Vec<ScalarValue>> {
    let empty_value = ScalarValue::try_from(output_type)?;
    Ok(vec![empty_value, ScalarValue::UInt32(None)])
}
