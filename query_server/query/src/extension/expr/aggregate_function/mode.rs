use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, UInt32Array};
use datafusion::arrow::datatypes::{DataType, Field};
use datafusion::common::cast::as_list_array;
use datafusion::common::{downcast_value, Result as DFResult};
use datafusion::logical_expr::{
    AccumulatorFactoryFunction, AggregateUDF, ReturnTypeFunction, Signature, StateTypeFunction,
    Volatility,
};
use datafusion::physical_plan::Accumulator;
use datafusion::scalar::ScalarValue;
use spi::query::function::FunctionMetadataManager;
use spi::QueryResult;

use crate::extension::expr::aggregate_function::MODE_UDAF_NAME;

pub fn register_udaf(func_manager: &mut dyn FunctionMetadataManager) -> QueryResult<AggregateUDF> {
    let udf = new();
    func_manager.register_udaf(udf.clone())?;
    Ok(udf)
}

fn new() -> AggregateUDF {
    let return_type_func: ReturnTypeFunction =
        Arc::new(move |input| Ok(Arc::new(input[0].clone())));

    let state_type_func: StateTypeFunction = Arc::new(move |input, _| {
        let val_list_dt = DataType::List(Arc::new(Field::new("item", input[0].clone(), true)));
        let count_list_dt = DataType::List(Arc::new(Field::new("item", DataType::UInt32, true)));

        Ok(Arc::new(vec![val_list_dt, count_list_dt]))
    });

    let accumulator: AccumulatorFactoryFunction =
        Arc::new(|input, _| Ok(Box::new(ModeAccumulator::new(input[0].clone()))));

    AggregateUDF::new(
        MODE_UDAF_NAME,
        &Signature::any(1, Volatility::Immutable),
        &return_type_func,
        &accumulator,
        &state_type_func,
    )
}

#[derive(Debug)]
struct ModeAccumulator {
    val_dt: DataType,

    map: HashMap<ScalarValue, u32>,
}

impl ModeAccumulator {
    fn new(val_dt: DataType) -> Self {
        Self {
            val_dt,
            map: Default::default(),
        }
    }
}

impl Accumulator for ModeAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> DFResult<()> {
        trace::trace!("update_batch: {:?}", values);

        if values.is_empty() {
            return Ok(());
        }

        debug_assert!(
            values.len() == 1,
            "mode can only take 1 param, but found {}",
            values.len()
        );

        let records = values[0].as_ref();

        let iter: Box<dyn Iterator<Item = usize>> = match records.nulls() {
            Some(null_buffer) => Box::new(null_buffer.valid_indices()),
            None => Box::new(0..records.len()),
        };

        for i in iter {
            let scalar = ScalarValue::try_from_array(records, i)?;
            self.map.entry(scalar).and_modify(|e| *e += 1).or_insert(1);
        }

        Ok(())
    }

    fn evaluate(&mut self) -> DFResult<ScalarValue> {
        trace::trace!("evaluate: {:?}", &self.map);

        match self.map.iter().max_by_key(|(_, count)| **count) {
            Some((val, _)) => Ok(val.clone()),
            None => ScalarValue::try_from(&self.val_dt),
        }
    }

    fn size(&self) -> usize {
        let map_size: usize = self
            .map
            .keys()
            .map(|val| val.size() + std::mem::size_of::<u32>())
            .sum();

        std::mem::size_of_val(self) + map_size - std::mem::size_of_val(&self.map)
    }

    fn state(&mut self) -> DFResult<Vec<ScalarValue>> {
        let (values, counts): (Vec<_>, Vec<_>) = self.map.clone().into_iter().unzip();
        let counts = counts
            .into_iter()
            .map(ScalarValue::from)
            .collect::<Vec<_>>();

        let values = ScalarValue::new_list_nullable(&values, &self.val_dt);
        let counts = ScalarValue::new_list_nullable(&counts, &DataType::UInt32);

        Ok(vec![values, counts])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> DFResult<()> {
        trace::trace!("merge_batch: {:?}", states);

        let value_list_records = as_list_array(states[0].as_ref())?;
        let count_list_records = as_list_array(states[1].as_ref())?;

        for (value_list, count_list) in value_list_records
            .iter()
            .flatten()
            .zip(count_list_records.iter().flatten())
        {
            let value_list = value_list.as_ref();
            let count_list = downcast_value!(count_list.as_ref(), UInt32Array);

            for i in 0..value_list.len() {
                let scalar = ScalarValue::try_from_array(value_list, i)?;
                let count = count_list.value(i);
                self.map
                    .entry(scalar)
                    .and_modify(|e| *e += count)
                    .or_insert(count);
            }
        }

        Ok(())
    }
}
