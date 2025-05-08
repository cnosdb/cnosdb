mod data_repair;
mod utils;

use data_repair::{timestamp_repair, value_fill, value_repair};
use datafusion::logical_expr::ScalarUDF;
use spi::query::function::FunctionMetadataManager;
use spi::{DFResult, QueryResult};
use strum::IntoEnumIterator;
use strum_macros::EnumIter;

pub fn register_all_udf(func_manager: &mut dyn FunctionMetadataManager) -> QueryResult<()> {
    func_manager.register_udf(ScalarUDF::new_from_impl(
        timestamp_repair::TimestampRepairFunc::new(),
    ))?;
    func_manager.register_udf(ScalarUDF::new_from_impl(value_fill::ValueFillFunc::new()))?;
    func_manager.register_udf(ScalarUDF::new_from_impl(
        value_repair::ValueRepairFunc::new(),
    ))
}

#[derive(Debug, EnumIter, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TsGenFunc {
    TimestampRepair,
    ValueFill,
    ValueRepair,
}

impl TsGenFunc {
    pub fn try_from_str(name: &str) -> Option<Self> {
        TsGenFunc::iter().find(|func| func.name() == name)
    }

    pub const fn name(&self) -> &'static str {
        match self {
            TsGenFunc::TimestampRepair => "timestamp_repair",
            TsGenFunc::ValueFill => "value_fill",
            TsGenFunc::ValueRepair => "value_repair",
        }
    }

    pub fn compute(
        &self,
        timestamps: &mut Vec<i64>,
        fields: &mut [Vec<f64>],
        arg_str: Option<&str>,
    ) -> DFResult<(Vec<i64>, Vec<f64>)> {
        match self {
            TsGenFunc::TimestampRepair => timestamp_repair::compute(timestamps, fields, arg_str),
            TsGenFunc::ValueFill => value_fill::compute(timestamps, fields, arg_str),
            TsGenFunc::ValueRepair => value_repair::compute(timestamps, fields, arg_str),
        }
    }
}
