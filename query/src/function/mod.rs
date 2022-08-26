pub mod simple_func_manager;

use std::sync::Arc;

use datafusion::{
    error::DataFusionError,
    logical_expr::{AggregateUDF, ScalarUDF},
};

use snafu::Snafu;

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum Error {
    #[snafu(display("DataFusion Error, caused:{}", source))]
    CausedDataFusion { source: DataFusionError },

    #[snafu(display("Udf already exists, name:{}.", name))]
    Exists { name: String },

    #[snafu(display("Udf not exists, name:{}.", name))]
    NotExists { name: String },
}

pub trait FunctionMetadataManager {
    fn register_udf(&mut self, udf: ScalarUDF) -> Result<()>;

    fn register_udaf(&mut self, udaf: AggregateUDF) -> Result<()>;

    fn udf(&self, name: &str) -> Result<Arc<ScalarUDF>>;

    fn udaf(&self, name: &str) -> Result<Arc<AggregateUDF>>;

    fn udfs(&self) -> Vec<Arc<ScalarUDF>>;
}
