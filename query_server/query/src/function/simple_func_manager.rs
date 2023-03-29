use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use datafusion::logical_expr::{AggregateUDF, ScalarUDF};
use spi::query::function::*;
use spi::{QueryError, Result};

pub type SimpleFunctionMetadataManagerRef = Arc<SimpleFunctionMetadataManager>;

#[derive(Debug, Default)]
pub struct SimpleFunctionMetadataManager {
    /// Scalar functions that are registered with the context
    pub scalar_functions: HashMap<String, Arc<ScalarUDF>>,
    /// Aggregate functions registered in the context
    pub aggregate_functions: HashMap<String, Arc<AggregateUDF>>,
}

impl FunctionMetadataManager for SimpleFunctionMetadataManager {
    fn register_udf(&mut self, f: ScalarUDF) -> Result<()> {
        self.scalar_functions
            .insert(f.name.to_uppercase(), Arc::new(f));
        Ok(())
    }

    fn register_udaf(&mut self, f: AggregateUDF) -> Result<()> {
        self.aggregate_functions
            .insert(f.name.to_uppercase(), Arc::new(f));
        Ok(())
    }

    fn udf(&self, name: &str) -> Result<Arc<ScalarUDF>> {
        let result = self.scalar_functions.get(&name.to_uppercase());

        result.cloned().ok_or_else(|| QueryError::FunctionExists {
            name: name.to_string(),
        })
    }

    fn udaf(&self, name: &str) -> Result<Arc<AggregateUDF>> {
        let result = self.aggregate_functions.get(&name.to_uppercase());

        result
            .cloned()
            .ok_or_else(|| QueryError::FunctionNotExists {
                name: name.to_string(),
            })
    }

    fn udfs(&self) -> HashSet<String> {
        self.scalar_functions.keys().cloned().collect()
    }
}
