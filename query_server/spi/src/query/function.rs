use std::collections::HashSet;
use std::sync::Arc;

use datafusion::logical_expr::{AggregateUDF, ScalarUDF};

use crate::Result;

pub type FuncMetaManagerRef = Arc<dyn FunctionMetadataManager + Send + Sync>;
pub trait FunctionMetadataManager {
    fn register_udf(&mut self, udf: ScalarUDF) -> Result<()>;

    fn register_udaf(&mut self, udaf: AggregateUDF) -> Result<()>;

    fn udf(&self, name: &str) -> Result<Arc<ScalarUDF>>;

    fn udaf(&self, name: &str) -> Result<Arc<AggregateUDF>>;

    fn udfs(&self) -> HashSet<String>;
}
