use datafusion::arrow::datatypes::DataType;
use datafusion::error::Result as DFResult;
use datafusion::functions_aggregate::average::AvgAccumulator;
use datafusion::functions_aggregate::count::Count;
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::{Accumulator, AggregateUDF, AggregateUDFImpl, Signature};
use models::arrow::Field;
use spi::query::function::FunctionMetadataManager;
use spi::QueryResult;

use super::EXACT_COUNT_UDAF_NAME;

pub fn register_udaf(func_manager: &mut dyn FunctionMetadataManager) -> QueryResult<()> {
    func_manager.register_udaf(AggregateUDF::new_from_impl(ExactCountFunc::new()))?;
    Ok(())
}

#[derive(Debug)]
pub struct ExactCountFunc {
    signature: Signature,
}

impl ExactCountFunc {
    pub fn new() -> Self {
        Self {
            signature: Count::new().signature().clone(),
        }
    }
}

impl AggregateUDFImpl for ExactCountFunc {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        EXACT_COUNT_UDAF_NAME
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Int64)
    }

    fn accumulator(&self, _acc_args: AccumulatorArgs) -> DFResult<Box<dyn Accumulator>> {
        Ok(Box::new(AvgAccumulator::default()))
    }

    fn state_fields(&self, args: StateFieldsArgs) -> DFResult<Vec<Field>> {
        Ok(vec![Field::new(args.name, DataType::Int64, false)])
    }
}
