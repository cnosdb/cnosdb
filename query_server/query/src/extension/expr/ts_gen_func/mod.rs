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
pub enum TimeSeriesGenFunc {
    TimestampRepair,
    ValueFill,
    ValueRepair,
}

impl TimeSeriesGenFunc {
    pub fn try_from_str(name: &str) -> Option<Self> {
        TimeSeriesGenFunc::iter().find(|func| func.name() == name)
    }

    pub const fn name(&self) -> &'static str {
        match self {
            TimeSeriesGenFunc::TimestampRepair => "timestamp_repair",
            TimeSeriesGenFunc::ValueFill => "value_fill",
            TimeSeriesGenFunc::ValueRepair => "value_repair",
        }
    }

    pub fn compute(
        &self,
        timestamps: &mut Vec<i64>,
        fields: &mut [f64],
        arg_str: Option<&str>,
    ) -> DFResult<(Vec<i64>, Vec<f64>)> {
        match self {
            TimeSeriesGenFunc::TimestampRepair => {
                timestamp_repair::compute(timestamps, fields, arg_str)
            }
            TimeSeriesGenFunc::ValueFill => value_fill::compute(timestamps, fields, arg_str),
            TimeSeriesGenFunc::ValueRepair => value_repair::compute(timestamps, fields, arg_str),
        }
    }
}
