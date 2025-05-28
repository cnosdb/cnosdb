use std::str::FromStr;
use std::sync::Arc;

use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::Result as DFResult;
use datafusion::config::ConfigOptions;
use datafusion::error::DataFusionError;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::ExecutionPlan;
use models::gis::data_type::GeometryType;
use models::schema::GIS_SUB_TYPE_META_KEY;
use spi::QueryError;

use crate::extension::physical::plan_node::assert::geom_write::AssertGeomType;
use crate::extension::physical::plan_node::assert::AssertExec;
use crate::extension::physical::plan_node::table_writer::TableWriterExec;
use crate::extension::utils::downcast_execution_plan;

#[derive(Debug)]
#[non_exhaustive]
pub struct AddAssertExec {}

impl AddAssertExec {
    pub fn new() -> Self {
        Self {}
    }
}

impl Default for AddAssertExec {
    fn default() -> Self {
        Self::new()
    }
}

impl PhysicalOptimizerRule for AddAssertExec {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        plan.transform_down(&|plan| {
            if let Some(exec) = downcast_execution_plan::<TableWriterExec>(plan.as_ref()) {
                if let Some(new_child) = add_table_write_asserter_if_necessary(exec)? {
                    let new_plan = plan.with_new_children(vec![new_child])?;
                    return Ok(Transformed::Yes(new_plan));
                }
            }

            Ok(Transformed::No(plan))
        })
    }

    fn name(&self) -> &str {
        "add_assert_exec"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

fn add_table_write_asserter_if_necessary(
    exec: &TableWriterExec,
) -> DFResult<Option<Arc<dyn ExecutionPlan>>> {
    let schema = exec.sink_schema();
    let child = exec.children()[0].clone();

    let geoms_with_idx = schema
        .fields()
        .iter()
        .enumerate()
        .filter_map(|(idx, field)| {
            match field
                .metadata()
                .get(GIS_SUB_TYPE_META_KEY)
                .map(|e| GeometryType::from_str(e))
            {
                Some(Ok(sub_type)) => {
                    // The target table for the write operation contains a column of type geometry
                    Ok(Some((sub_type, idx)))
                }
                Some(Err(err)) => {
                    // Contains a column of type geometry, but the type is not recognized
                    Err(DataFusionError::External(Box::new(
                        QueryError::InvalidGeometryType { reason: err },
                    )))
                }
                None => {
                    // Not contain a column of type geometry
                    Ok(None)
                }
            }
            .transpose()
        })
        .collect::<DFResult<Vec<_>>>()?;

    if geoms_with_idx.is_empty() {
        return Ok(None);
    }

    let assert_expr = Arc::new(AssertGeomType::new(geoms_with_idx));
    let new_child = Arc::new(AssertExec::new(assert_expr, child));

    Ok(Some(new_child))
}
