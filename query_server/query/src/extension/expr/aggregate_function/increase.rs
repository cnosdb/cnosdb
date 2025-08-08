use datafusion::arrow::array::ArrayRef;
use datafusion::arrow::datatypes::DataType;
use datafusion::common::{DataFusionError, Result as DFResult};
use datafusion::logical_expr::function::AccumulatorArgs;
use datafusion::logical_expr::type_coercion::aggregates::{NUMERICS, TIMESTAMPS};
use datafusion::logical_expr::{
    AggregateUDF, AggregateUDFImpl, Signature, TypeSignature, Volatility,
};
use datafusion::physical_plan::Accumulator;
use datafusion::scalar::ScalarValue;
use spi::query::function::FunctionMetadataManager;
use spi::QueryResult;

use super::TSPoint;
use crate::extension::expr::aggregate_function::INCREASE_NAME;

pub fn register_udaf(func_manager: &mut dyn FunctionMetadataManager) -> QueryResult<AggregateUDF> {
    let udf = new();
    func_manager.register_udaf(udf.clone())?;
    Ok(udf)
}

#[derive(Debug)]
pub struct IncreaseFunc {
    signature: Signature,
}

impl IncreaseFunc {
    pub fn new() -> Self {
        // increase(
        //     time TIMESTAMP,
        //     value NUMERICS order by time
        //   )
        let type_signatures = NUMERICS
            .iter()
            .flat_map(|t| {
                TIMESTAMPS
                    .iter()
                    .map(|s_t| TypeSignature::Exact(vec![s_t.clone(), t.clone()]))
            })
            .collect();
        Self {
            signature: Signature::one_of(type_signatures, Volatility::Immutable),
        }
    }
}

impl AggregateUDFImpl for IncreaseFunc {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        INCREASE_NAME
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(arg_types[1].clone())
    }

    fn accumulator(&self, acc_args: AccumulatorArgs) -> DFResult<Box<dyn Accumulator>> {
        let time_data_type = acc_args.exprs[0].data_type(acc_args.schema)?;
        let value_data_type = acc_args.exprs[1].data_type(acc_args.schema)?;

        Ok(Box::new(IncreaseAccumulator::try_new(
            time_data_type,
            value_data_type,
        )?))
    }
}

fn new() -> AggregateUDF {
    AggregateUDF::new_from_impl(IncreaseFunc::new())
}

#[derive(Debug)]
struct IncreaseAccumulator {
    increase: ScalarValue,
    last: TSPoint,
}

impl IncreaseAccumulator {
    fn try_new(time_data_type: DataType, value_data_type: DataType) -> DFResult<Self> {
        let increase = ScalarValue::new_zero(&value_data_type)?;
        let null = TSPoint::try_new_null(time_data_type, value_data_type)?;
        Ok(Self {
            increase,
            last: null,
        })
    }

    fn update_inner(&mut self, point: TSPoint) -> DFResult<()> {
        if point.ts().is_null() || point.val().is_null() {
            return Ok(());
        }

        if self.last.ts().is_null() && self.last.val().is_null() {
            self.last = point;
            return Ok(());
        }

        if point.ts.lt(self.last.ts()) {
            return Err(DataFusionError::Execution(
                "INCREASE need oder by time".to_string(),
            ));
        }

        if point.val().gt(self.last.val()) {
            let delta = point.val().sub(self.last.val())?;
            self.increase = self.increase.add(delta)?;
        } else if point.val().lt(self.last.val()) {
            self.increase = self.increase.add(point.val())?;
        }
        self.last = point;

        Ok(())
    }

    fn merge_inner(&mut self, increase: &ScalarValue) -> DFResult<()> {
        if !self.increase.is_null() && !increase.is_null() {
            self.increase = self.increase.add(increase)?;
        }
        Ok(())
    }
}

impl Accumulator for IncreaseAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> DFResult<()> {
        if values.is_empty() {
            return Ok(());
        }

        debug_assert!(
            values.len() == 2,
            "first can only take 2 param, but found {}",
            values.len()
        );

        let times_records = values[0].as_ref();
        let value_records = values[1].as_ref();

        let record_len = times_records.len();

        for i in 0..record_len {
            let ts = ScalarValue::try_from_array(times_records, i)?;
            let val = ScalarValue::try_from_array(value_records, i)?;
            let point = TSPoint { ts, val };
            self.update_inner(point)?;
        }

        Ok(())
    }

    fn evaluate(&mut self) -> DFResult<ScalarValue> {
        Ok(self.increase.clone())
    }

    fn size(&self) -> usize {
        // NUMERICS scalar value only be set on stack
        std::mem::size_of_val(self)
    }

    fn state(&mut self) -> DFResult<Vec<ScalarValue>> {
        Ok(vec![self.increase.clone()])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> DFResult<()> {
        let array = states[0].as_ref();
        for i in 0..array.len() {
            let increase = ScalarValue::try_from_array(array, i)?;
            self.merge_inner(&increase)?;
        }
        Ok(())
    }
}
