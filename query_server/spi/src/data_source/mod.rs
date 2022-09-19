use std::sync::Arc;

use async_trait::async_trait;
use datafusion::{
    datasource::TableProvider,
    error::DataFusionError,
    execution::context::SessionState,
    physical_plan::{ExecutionPlan, PhysicalExpr},
};

#[async_trait]
pub trait SupportWrite: TableProvider + Send + Sync {
    /// Create an ExecutionPlan that will write the table.
    async fn write(
        &self,
        state: &SessionState,
        input: Arc<dyn ExecutionPlan>,
        output_physical_exprs: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError>;
}
