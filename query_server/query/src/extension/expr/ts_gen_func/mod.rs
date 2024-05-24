mod data_repair;
mod utils;

use data_repair::{timestamp_repair, value_fill, value_repair};
use datafusion::logical_expr::ScalarUDF;
use spi::query::function::FunctionMetadataManager;
use spi::{DFResult, QueryResult};
use strum::IntoEnumIterator;
use strum_macros::EnumIter;

#[derive(Debug, EnumIter, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TSGenFunc {
    TimestampRepair,
    ValueFill,
    ValueRepair,
}

impl TSGenFunc {
    pub const fn name(&self) -> &'static str {
        match self {
            TSGenFunc::TimestampRepair => "timestamp_repair",
            TSGenFunc::ValueFill => "value_fill",
            TSGenFunc::ValueRepair => "value_repair",
        }
    }

    pub fn from_str_opt(name: &str) -> Option<Self> {
        TSGenFunc::iter().find(|func| func.name() == name)
    }

    fn scalar_udf(&self) -> ScalarUDF {
        match self {
            TSGenFunc::TimestampRepair | TSGenFunc::ValueFill | TSGenFunc::ValueRepair => {
                utils::common_udf(self.name())
            }
        }
    }

    pub fn register_all_udf(func_manager: &mut dyn FunctionMetadataManager) -> QueryResult<()> {
        for func in TSGenFunc::iter() {
            func_manager.register_udf(func.scalar_udf())?;
        }
        Ok(())
    }

    pub fn compute(
        &self,
        timestamps: &mut Vec<i64>,
        fields: &mut [Vec<f64>],
        arg_str: Option<&str>,
    ) -> DFResult<(Vec<i64>, Vec<f64>)> {
        match self {
            TSGenFunc::TimestampRepair => timestamp_repair::compute(timestamps, fields, arg_str),
            TSGenFunc::ValueFill => value_fill::compute(timestamps, fields, arg_str),
            TSGenFunc::ValueRepair => value_repair::compute(timestamps, fields, arg_str),
        }
    }
}
