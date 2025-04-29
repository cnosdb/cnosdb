use datafusion::arrow::datatypes::{DataType, Field};
use datafusion::error::Result as DFResult;
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::type_coercion::aggregates::{STRINGS, TIMESTAMPS};
use datafusion::logical_expr::{
    Accumulator, AggregateUDF, AggregateUDFImpl, Signature, TypeSignature, Volatility,
};
use spi::query::function::FunctionMetadataManager;
use spi::QueryError;
pub use state_agg_data::StateAggData;

use crate::extension::expr::aggregate_function::state_agg::state_agg_accumulator::StateAggAccumulator;
use crate::extension::expr::aggregate_function::{
    AggResult, COMPACT_STATE_AGG_UDAF_NAME, STATE_AGG_UDAF_NAME,
};
use crate::extension::expr::INTEGERS;
mod state_agg_accumulator;
mod state_agg_data;

pub fn register_udafs(func_manager: &mut dyn FunctionMetadataManager) -> Result<(), QueryError> {
    func_manager.register_udaf(AggregateUDF::new_from_impl(StageAggFunction::new(true)))?;
    func_manager.register_udaf(AggregateUDF::new_from_impl(StageAggFunction::new(false)))?;
    Ok(())
}

#[derive(Debug)]
pub struct StageAggFunction {
    signature: Signature,
    compact: bool,
}

impl StageAggFunction {
    pub fn new(compact: bool) -> Self {
        // [compact_]state_agg(
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

        Self {
            signature: Signature::one_of(type_signatures, Volatility::Immutable),
            compact,
        }
    }
}

impl AggregateUDFImpl for StageAggFunction {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        if self.compact {
            COMPACT_STATE_AGG_UDAF_NAME
        } else {
            STATE_AGG_UDAF_NAME
        }
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> DFResult<DataType> {
        let agg_result =
            StateAggData::new(arg_types[0].clone(), arg_types[1].clone(), self.compact);
        let date_type = agg_result.into_scalar()?.data_type();
        Ok(date_type)
    }

    fn accumulator(&self, acc_args: AccumulatorArgs) -> DFResult<Box<dyn Accumulator>> {
        Ok(Box::new(StateAggAccumulator::try_new(
            acc_args
                .schema
                .fields()
                .iter()
                .map(|f| f.data_type().clone())
                .collect(),
            self.compact,
        )?))
    }

    fn state_fields(&self, args: StateFieldsArgs) -> DFResult<Vec<Field>> {
        let fields = args
            .input_types
            .iter()
            .map(|t| Field::new_list_field(t.clone(), true))
            .collect::<Vec<_>>();
        Ok(fields)
    }
}
