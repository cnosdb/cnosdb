use datafusion::common::Result as DFResult;
use datafusion::config::ConfigOptions;
use datafusion::error::DataFusionError;
use datafusion::logical_expr::{Extension, LogicalPlan};
use datafusion::optimizer::analyzer::AnalyzerRule;
use spi::QueryError;

use crate::data_source::table_source::{TableHandle, TableSourceAdapter};
use crate::extension::logical::plan_node::update::UpdateNode;
use crate::extension::utils::{downcast_plan_node, downcast_table_source};

#[derive(Default)]
#[non_exhaustive]
pub struct TransformUpdateRule {}

impl TransformUpdateRule {
    pub fn new() -> Self {
        Self::default()
    }
}

impl AnalyzerRule for TransformUpdateRule {
    fn analyze(&self, plan: LogicalPlan, _config: &ConfigOptions) -> DFResult<LogicalPlan> {
        analyze_internal(plan)
    }

    fn name(&self) -> &str {
        "transform_update"
    }
}

fn analyze_internal(plan: LogicalPlan) -> DFResult<LogicalPlan> {
    if let LogicalPlan::Extension(Extension { node }) = &plan {
        if let Some(
            update_node @ UpdateNode {
                table_source,
                assigns,
                ..
            },
        ) = downcast_plan_node::<UpdateNode>(node.as_ref())
        {
            let source_adapter = downcast_table_source::<TableSourceAdapter>(table_source.as_ref())
                .ok_or_else(|| {
                    DataFusionError::External(Box::new(QueryError::Internal {
                        reason:
                            "The implementation of TableSource in cnosdb must be TableSourceAdapter"
                                .to_string(),
                    }))
                })?;

            match source_adapter.table_handle() {
                TableHandle::Tskv(table) => {
                    let schema = table.table_schema();
                    let columns = assigns
                        .iter()
                        .map(|(col, _)| {
                            schema.column(&col.name).ok_or_else(|| {
                                DataFusionError::External(Box::new(QueryError::Internal {
                                    reason: format!("Column {} not found in table", col.name),
                                }))
                            })
                        })
                        .collect::<DFResult<Vec<_>>>()?;

                    let is_update_time = columns.iter().all(|c| c.column_type.is_time());
                    if is_update_time {
                        return update_time();
                    }

                    let is_update_tag = columns.iter().all(|c| c.column_type.is_tag());
                    if is_update_tag {
                        return update_tag();
                    }

                    let is_update_field = columns.iter().all(|c| c.column_type.is_field());
                    if is_update_field {
                        return update_field(update_node);
                    }

                    return Err(DataFusionError::External(Box::new(QueryError::Analyzer {
                        err: "Update the time/tag/field columns at the same statement is not supported".to_string(),
                    })));
                }
                _ => {
                    return Err(DataFusionError::External(Box::new(QueryError::Analyzer {
                        err: "Only support update tskv table".to_string(),
                    })))
                }
            }
        }
    }

    Ok(plan)
}

/// 不支持update time
fn update_time() -> DFResult<LogicalPlan> {
    Err(DataFusionError::External(Box::new(
        QueryError::NotImplemented {
            err: "update_time".to_string(),
        },
    )))
}

/// 生成update tag的逻辑计划
fn update_tag() -> DFResult<LogicalPlan> {
    Err(DataFusionError::External(Box::new(
        QueryError::NotImplemented {
            err: "update_tag".to_string(),
        },
    )))
}

/// 生成update field的逻辑计划
fn update_field(_update_node: &UpdateNode) -> DFResult<LogicalPlan> {
    Err(DataFusionError::External(Box::new(
        QueryError::NotImplemented {
            err: "update_field".to_string(),
        },
    )))
}
