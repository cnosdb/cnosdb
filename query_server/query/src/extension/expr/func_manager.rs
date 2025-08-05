use std::collections::HashSet;
use std::sync::Arc;

use datafusion::execution::FunctionRegistry;
use datafusion::logical_expr::{AggregateUDF, ScalarUDF};
use datafusion::prelude::SessionContext;
use snafu::ResultExt;
use spi::query::function::*;
use spi::{DatafusionSnafu, QueryError, QueryResult};

pub struct DFSessionContextFuncAdapter<'a> {
    ctx: &'a mut SessionContext,
}

impl<'a> DFSessionContextFuncAdapter<'a> {
    pub fn new(ctx: &'a mut SessionContext) -> Self {
        Self { ctx }
    }
}

impl<'a> FunctionMetadataManager for DFSessionContextFuncAdapter<'a> {
    fn register_udf(&mut self, udf: ScalarUDF) -> QueryResult<()> {
        if self.ctx.udf(udf.name()).is_err() {
            self.ctx.register_udf(Arc::new(udf));

            return Ok(());
        }

        Err(QueryError::FunctionExists {
            name: udf.name().to_string(),
        })
    }

    fn register_udaf(&mut self, udaf: AggregateUDF) -> QueryResult<()> {
        if self.ctx.udaf(udaf.name()).is_err() {
            self.ctx.register_udaf(Arc::new(udaf));
            return Ok(());
        }

        Err(QueryError::FunctionExists {
            name: udaf.name().to_string(),
        })
    }

    fn register_udwf(&mut self, udwf: datafusion::logical_expr::WindowUDF) -> QueryResult<()> {
        if self.ctx.udwf(udwf.name()).is_err() {
            self.ctx.register_udwf(Arc::new(udwf));
            return Ok(());
        }

        Err(QueryError::FunctionExists {
            name: udwf.name().to_string(),
        })
    }

    fn udf(&self, name: &str) -> QueryResult<Arc<ScalarUDF>> {
        self.ctx.udf(name).context(DatafusionSnafu)
    }

    fn udaf(&self, name: &str) -> QueryResult<Arc<AggregateUDF>> {
        self.ctx.udaf(name).context(DatafusionSnafu)
    }

    fn udwf(&self, name: &str) -> QueryResult<Arc<datafusion::logical_expr::WindowUDF>> {
        self.ctx.udwf(name).context(DatafusionSnafu)
    }

    fn udfs(&self) -> HashSet<String> {
        self.ctx.udfs()
    }
}
