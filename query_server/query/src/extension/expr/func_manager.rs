use std::collections::HashSet;
use std::sync::Arc;

use datafusion::execution::FunctionRegistry;
use datafusion::logical_expr::{AggregateUDF, ScalarUDF};
use datafusion::prelude::SessionContext;
use spi::query::function::*;
use spi::{QueryError, Result};

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
            self.ctx.register_udf(udf);

            return Ok(());
        }

        Err(QueryError::FunctionExists { name: udf.name })
    }

    fn register_udaf(&mut self, udaf: AggregateUDF) -> Result<()> {
        if self.ctx.udaf(udaf.name.as_str()).is_err() {
            self.ctx.register_udaf(udaf);
            return Ok(());
        }

        Err(QueryError::FunctionExists { name: udaf.name })
    }

    fn udf(&self, name: &str) -> Result<Arc<ScalarUDF>> {
        self.ctx
            .udf(name)
            .map_err(|e| QueryError::Datafusion { source: e })
    }

    fn udaf(&self, name: &str) -> Result<Arc<AggregateUDF>> {
        self.ctx
            .udaf(name)
            .map_err(|e| QueryError::Datafusion { source: e })
    }

    fn udfs(&self) -> HashSet<String> {
        self.ctx.udfs()
    }
}
