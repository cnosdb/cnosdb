use std::cmp::Ordering::{Equal, Less};
use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, UInt32Array};
use datafusion::arrow::compute::{self};
use datafusion::arrow::datatypes::{DataType, Field, UInt32Type};
use datafusion::common::cast::{as_list_array, as_primitive_array};
use datafusion::common::{downcast_value, DataFusionError, Result as DFResult};
use datafusion::logical_expr::type_coercion::aggregates::{NUMERICS, TIMESTAMPS};
use datafusion::logical_expr::{
    AccumulatorFactoryFunction, AggregateUDF, ReturnTypeFunction, Signature, StateTypeFunction,
    TypeSignature, Volatility,
};
use datafusion::physical_plan::Accumulator;
use datafusion::scalar::ScalarValue;
use spi::query::function::FunctionMetadataManager;
use spi::QueryError;

use crate::extension::expr::aggregate_function::MAX_N_UDAF_NAME;
use crate::extension::expr::expr_utils::check_args;

pub fn register_udaf(func_manager: &mut dyn FunctionMetadataManager) -> Result<(), QueryError> {
    func_manager.register_udaf(new())?;
    Ok(())
}

fn new() -> AggregateUDF {
    let return_type_func: ReturnTypeFunction = Arc::new(move |input| {
        check_args(MAX_N_UDAF_NAME, 2, input)?;

        let data_type = DataType::List(Arc::new(Field::new("item", input[0].clone(), true)));

        Ok(Arc::new(data_type))
    });

    let state_type_func: StateTypeFunction =
        Arc::new(move |_, output| Ok(Arc::new(vec![output.clone(), DataType::UInt32])));

    let accumulator: AccumulatorFactoryFunction = Arc::new(|_, output| match output {
        DataType::List(f) => Ok(Box::new(MaxNDataAccumulator::try_new(
            output.clone(),
            f.data_type().clone(),
        )?)),
        _ => {
            panic!()
        }
    });

    // max_n(
    //   value {NUMERICS | TIMESTAMPS}
    // ) RETURNS MaxNData
    let type_signatures = NUMERICS
        .iter()
        .chain(TIMESTAMPS.iter())
        .map(|t| TypeSignature::Exact(vec![t.clone(), DataType::Int64]))
        .collect();

    AggregateUDF::new(
        MAX_N_UDAF_NAME,
        &Signature::one_of(type_signatures, Volatility::Immutable),
        &return_type_func,
        &accumulator,
        &state_type_func,
    )
}

#[derive(Debug)]
struct MaxNDataAccumulator {
    states: Vec<ScalarValue>,

    max_n: usize,
    list_type: DataType,
    child_type: DataType,
}

impl MaxNDataAccumulator {
    pub fn try_new(list_type: DataType, child_type: DataType) -> DFResult<Self> {
        Ok(Self {
            states: vec![],
            max_n: 0,
            list_type,
            child_type,
        })
    }

    pub fn update_batch_inner(&mut self, arr: ArrayRef, max_n: usize) -> DFResult<()> {
        trace::trace!(
            "update_batch_inner: {:?}, max_n: {}, states: {:?}",
            arr,
            max_n,
            self.states
        );

        let mut df_arr: Vec<ScalarValue> = arrow_array_to_df_values(&arr)?;

        let sort = |a: &ScalarValue, b: &ScalarValue| {
            if let Some(result) = b.partial_cmp(&a) {
                return result;
            }
            Equal
        };

        let states_len = self.states.len();

        if self.states.len() + df_arr.len() <= max_n {
            self.states.append(&mut df_arr);
            self.states.sort_unstable_by(sort);
        } else if states_len == 0 {
            df_arr.sort_unstable_by(sort);
            self.states.extend_from_slice(&df_arr[0..max_n]);
        } else {
            df_arr.sort_unstable_by(sort);

            let mut new_arr = vec![];
            let mut take_num = 0;
            
            let mut i = 0;
            loop {
                if let Some(result) = self.states[i].partial_cmp(&df_arr[0]) {
                    match result {
                        Less => {
                            new_arr.push(df_arr.remove(0));
                            take_num += 1;

                            if df_arr.is_empty() {
                                let want_n = max_n - new_arr.len();
                                new_arr.extend_from_slice(&self.states[i..i + want_n]);
                                break;
                            }
                        }
                        _ => {
                            new_arr.push(self.states[i].clone());
                            take_num += 1;
                            i += 1;
                        }
                    }
                }
                if take_num >= max_n || i >= states_len {
                    break;
                }
            }
            // 当states_len < max_n，上面的for循环并不能填充满new_arr就退出，所以这里需要再处理下
            if new_arr.len() < max_n {
                new_arr.extend_from_slice(&df_arr[0..max_n - new_arr.len()]);
            }

            self.states = new_arr;
        }
        trace::trace!("update_batch_inner, result: {:?}", self.states);
        self.max_n = max_n;
        Ok(())
    }
}

impl Accumulator for MaxNDataAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> DFResult<()> {
        trace::trace!("update_batch: {:?}", values);
        if self.max_n > 0 {
            return self.update_batch_inner(values[0].clone(), self.max_n);
        }

        if let Some(max_n) = extract_max_n(values[1].as_ref())? {
            return self.update_batch_inner(values[0].clone(), max_n);
        }

        Ok(())
    }

    fn evaluate(&self) -> DFResult<ScalarValue> {
        let result = self.state()?;

        trace::trace!("MaxNDataAccumulator evaluate result: {:?}", result);

        Ok(result[0].clone())
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self)
    }

    fn state(&self) -> DFResult<Vec<ScalarValue>> {
        if self.states.is_empty() {
            let empty_value = ScalarValue::try_from(&self.list_type)?;
            return Ok(vec![empty_value, ScalarValue::UInt32(None)]);
        }

        let state = ScalarValue::new_list(Some(self.states.clone()), self.child_type.clone());
        let max_n = ScalarValue::UInt32(Some(self.max_n as u32));

        trace::trace!("MaxNDataAccumulator state: {:?}", state);

        Ok(vec![state, max_n])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> DFResult<()> {
        trace::trace!("MaxNDataAccumulator merge_batch: {:?}", states);

        let state_col = as_list_array(states[0].as_ref())?;
        let max_n_col = downcast_value!(states[1], UInt32Array);

        if state_col.is_empty() {
            return Ok(());
        }

        state_col.iter().zip(max_n_col).try_for_each(|e| {
            match e {
                (Some(state), Some(max_n)) if !state.is_empty() => {
                    self.update_batch_inner(state, max_n as usize)?;
                }
                _ => {
                    trace::trace!("merge_batch, skip empty state: {:?}", e)
                }
            }

            Ok(())
        })
    }
}

fn arrow_array_to_df_values(arr: &dyn Array) -> DFResult<Vec<ScalarValue>> {
    let mut index = 0;
    let mut result = vec![];
    loop {
        if arr.is_valid(index) {
            result.push(ScalarValue::try_from_array(arr, index)?);
        }else {
            break;
        }
        index += 1;
    }

    Ok(result)
}

fn extract_max_n(arr: &dyn Array) -> DFResult<Option<usize>> {
    let max_n = unsafe {
        if arr.is_empty() {
            return Ok(None);
        }
        let remain_arr = compute::cast(arr, &DataType::UInt32)?;
        as_primitive_array::<UInt32Type>(remain_arr.as_ref())?.value_unchecked(0)
    } as usize;

    // (0, 429496729]
    if max_n > 0 && max_n <= 429496729 {
        return Ok(Some(max_n));
    }

    Err(DataFusionError::Plan(format!(
        "The number of sample points for function '{MAX_N_UDAF_NAME}' must be (0, 429496729]"
    )))
}
