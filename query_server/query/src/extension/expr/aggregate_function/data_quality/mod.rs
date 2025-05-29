use datafusion::error::Result as DFResult;
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::type_coercion::aggregates::{NUMERICS, TIMESTAMPS};
use datafusion::logical_expr::{
    Accumulator, AggregateUDFImpl, Signature, TypeSignature, Volatility,
};
use models::arrow::{DataType, Field};
use spi::query::function::FunctionMetadataManager;
use spi::QueryResult;

use self::accumulator::DataQualityAccumulator;
use self::common::DataQualityFunction;

mod accumulator;
mod common;

pub fn register_udafs(func_manager: &mut dyn FunctionMetadataManager) -> QueryResult<()> {
    func_manager.register_udaf(DataQualityAggregateUDF::new(
        DataQualityFunction::Completeness,
    ))?;
    func_manager.register_udaf(DataQualityAggregateUDF::new(
        DataQualityFunction::Consistency,
    ))?;
    func_manager.register_udaf(DataQualityAggregateUDF::new(
        DataQualityFunction::Timeliness,
    ))?;
    func_manager.register_udaf(DataQualityAggregateUDF::new(DataQualityFunction::Validity))?;
    Ok(())
}

#[derive(Debug)]
pub struct DataQualityAggregateUDF {
    data_quality_func: DataQualityFunction,
    signature: Signature,
}

impl DataQualityAggregateUDF {
    pub fn new(data_quality_func: DataQualityFunction) -> Self {
        let type_signatures: Vec<_> = NUMERICS
            .iter()
            .flat_map(|t| {
                TIMESTAMPS
                    .iter()
                    .map(|s_t| TypeSignature::Exact(vec![s_t.clone(), t.clone()]))
            })
            .collect();

        Self {
            data_quality_func,
            signature: Signature::one_of(type_signatures, Volatility::Immutable),
        }
    }
}

impl AggregateUDFImpl for DataQualityAggregateUDF {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        self.data_quality_func.name()
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Float64)
    }

    fn accumulator(&self, acc_args: AccumulatorArgs) -> DFResult<Box<dyn Accumulator>> {
        Ok(Box::new(DataQualityAccumulator::new(
            self.data_quality_func,
        )))
    }

    fn state_fields(&self, args: StateFieldsArgs) -> DFResult<Vec<Field>> {
        Ok(vec![
            Field::new_list_field(DataType::Float64, true), // times
            Field::new_list_field(DataType::Float64, true), // values
        ])
    }
}
