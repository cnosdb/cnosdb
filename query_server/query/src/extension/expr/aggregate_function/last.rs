use std::cmp::Ordering;
use std::sync::Arc;

use datafusion::arrow::array::ArrayRef;
use datafusion::arrow::compute::{sort_to_indices, SortOptions};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::Result as DFResult;
use datafusion::error::DataFusionError;
use datafusion::logical_expr::type_coercion::aggregates::{
    DATES, NUMERICS, STRINGS, TIMES, TIMESTAMPS,
};
use datafusion::logical_expr::{
    AccumulatorFactoryFunction, AggregateUDF, ReturnTypeFunction, Signature, StateTypeFunction,
    TypeSignature, Volatility,
};
use datafusion::physical_plan::Accumulator;
use datafusion::scalar::ScalarValue;
use spi::query::function::FunctionMetadataManager;
use spi::{QueryError, QueryResult};

use super::TSPoint;
use crate::extension::expr::aggregate_function::LAST_UDAF_NAME;
use crate::extension::expr::BINARYS;

pub fn register_udaf(func_manager: &mut dyn FunctionMetadataManager) -> QueryResult<AggregateUDF> {
    let udf = new();
    func_manager.register_udaf(udf.clone())?;
    Ok(udf)
}

fn new() -> AggregateUDF {
    let return_type_func: ReturnTypeFunction =
        Arc::new(move |input| Ok(Arc::new(input[1].clone())));

    let state_type_func: StateTypeFunction = Arc::new(move |input, _| Ok(Arc::new(input.to_vec())));

    let accumulator: AccumulatorFactoryFunction = Arc::new(|input, _| {
        let time_data_type = input[0].clone();
        let value_data_type = input[1].clone();

        Ok(Box::new(LastAccumulator::try_new(
            time_data_type,
            value_data_type,
        )?))
    });

    // last(
    //     time TIMESTAMP,
    //     value ANY
    //   )
    let type_signatures = STRINGS
        .iter()
        .chain(NUMERICS.iter())
        .chain(TIMESTAMPS.iter())
        .chain(DATES.iter())
        .chain(BINARYS.iter())
        .chain(TIMES.iter())
        .flat_map(|t| {
            TIMESTAMPS
                .iter()
                .map(|s_t| TypeSignature::Exact(vec![s_t.clone(), t.clone()]))
        })
        .collect();

    AggregateUDF::new(
        LAST_UDAF_NAME,
        &Signature::one_of(type_signatures, Volatility::Immutable),
        &return_type_func,
        &accumulator,
        &state_type_func,
    )
}

#[derive(Debug)]
struct LastAccumulator {
    last: TSPoint,

    sort_opts: SortOptions,
}

impl LastAccumulator {
    fn try_new(time_data_type: DataType, value_data_type: DataType) -> DFResult<Self> {
        let null = TSPoint::try_new_null(time_data_type, value_data_type)?;
        Ok(Self {
            last: null,
            sort_opts: SortOptions {
                descending: true,
                nulls_first: false,
            },
        })
    }

    fn update_inner(&mut self, point: TSPoint) -> DFResult<()> {
        if point.ts().is_null() || point.val().is_null() {
            return Ok(());
        }

        if self.last.ts().is_null() {
            self.last = point;
            return Ok(());
        }

        match point.ts().partial_cmp(self.last.ts()) {
            Some(ordering) => {
                if ordering == Ordering::Greater {
                    self.last = point;
                }
            }
            None => {
                return Err(DataFusionError::External(Box::new(QueryError::Internal {
                    reason: format!("cannot compare {:?} with {:?}", point.ts(), self.last.ts()),
                })))
            }
        }

        Ok(())
    }
}

impl Accumulator for LastAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> DFResult<()> {
        trace::trace!("update_batch: {:?}", values);

        if values.is_empty() {
            return Ok(());
        }

        debug_assert!(
            values.len() == 2,
            "last can only take 2 param, but found {}",
            values.len()
        );

        let times_records = values[0].as_ref();
        let value_records = values[1].as_ref();

        let indices = sort_to_indices(times_records, Some(self.sort_opts), Some(1))?;

        if !indices.is_empty() {
            let idx = indices.value(0) as usize;
            let ts = ScalarValue::try_from_array(times_records, idx)?;
            let val = ScalarValue::try_from_array(value_records, idx)?;
            let point = TSPoint { ts, val };
            self.update_inner(point)?;
        }

        Ok(())
    }

    fn evaluate(&mut self) -> DFResult<ScalarValue> {
        Ok(self.last.val().clone())
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self) - std::mem::size_of_val(self.last.ts()) + self.last.ts().size()
            - std::mem::size_of_val(self.last.ts())
            + self.last.ts().size()
    }

    fn state(&mut self) -> DFResult<Vec<ScalarValue>> {
        Ok(vec![self.last.ts().clone(), self.last.val().clone()])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> DFResult<()> {
        trace::trace!("merge_batch: {:?}", states);

        self.update_batch(states)
    }
}
