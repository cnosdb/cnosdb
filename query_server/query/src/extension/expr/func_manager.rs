use std::sync::Arc;

use datafusion::execution::FunctionRegistry;
use datafusion::logical_expr::AggregateUDF;
use datafusion::{logical_expr::ScalarUDF, prelude::SessionContext};
use snafu::ResultExt;
use spi::query::function::*;

pub struct DFSessionContextFuncAdapter<'a> {
    ctx: &'a mut SessionContext,
}

impl<'a> DFSessionContextFuncAdapter<'a> {
    pub fn new(ctx: &'a mut SessionContext) -> Self {
        Self { ctx }
    }
}

impl<'a> FunctionMetadataManager for DFSessionContextFuncAdapter<'a> {
    fn register_udf(&mut self, udf: ScalarUDF) -> Result<()> {
        if self.ctx.udf(udf.name.as_str()).is_err() {
            return Err(Error::Exists { name: udf.name });
        }

        self.ctx.register_udf(udf);
        Ok(())
    }

    fn register_udaf(&mut self, udaf: AggregateUDF) -> Result<()> {
        if self.ctx.udaf(udaf.name.as_str()).is_err() {
            return Err(Error::Exists { name: udaf.name });
        }

        self.ctx.register_udaf(udaf);
        Ok(())
    }

    fn udf(&self, name: &str) -> Result<Arc<ScalarUDF>> {
        self.ctx.udf(name).context(CausedDataFusionSnafu)
    }

    fn udaf(&self, name: &str) -> Result<Arc<AggregateUDF>> {
        self.ctx.udaf(name).context(CausedDataFusionSnafu)
    }

    fn udfs(&self) -> Vec<Arc<ScalarUDF>> {
        self.ctx
            .state()
            .scalar_functions
            .values()
            .cloned()
            .collect()
    }
}
