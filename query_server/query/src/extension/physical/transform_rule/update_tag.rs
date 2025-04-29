use std::sync::Arc;

use async_trait::async_trait;
use datafusion::common::DFSchema;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::context::SessionState;
use datafusion::logical_expr::{LogicalPlan, UserDefinedLogicalNode};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_planner::{ExtensionPlanner, PhysicalPlanner};

use crate::data_source::{source_downcast_adapter, UpdateExecExt};
use crate::extension::logical::plan_node::update_tag::UpdateTagPlanNode;
use crate::extension::utils::downcast_plan_node;

pub struct UpdateTagValuePlanner {}

#[async_trait]
impl ExtensionPlanner for UpdateTagValuePlanner {
    async fn plan_extension(
        &self,
        planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        _logical_inputs: &[&LogicalPlan],
        physical_inputs: &[Arc<dyn ExecutionPlan>],
        session_state: &SessionState,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        Ok(
            if let Some(UpdateTagPlanNode {
                table_source,
                scan: _input,
                assigns,
                ..
            }) = downcast_plan_node(node)
            {
                let assigns = assigns
                    .iter()
                    .map(|(column, expr)| {
                        let e = planner.create_physical_expr(
                            expr,
                            &DFSchema::empty(),
                            session_state,
                        )?;
                        Ok((column.name.clone(), e))
                    })
                    .collect::<Result<Vec<_>>>()?;

                let table_provider = source_downcast_adapter(table_source)
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;

                let result = table_provider
                    .update(assigns.clone(), physical_inputs[0].clone())
                    .await?;

                Some(result)
            } else {
                None
            },
        )
    }
}
