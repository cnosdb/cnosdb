use crate::metadata::MetaDataRef;
use async_trait::async_trait;
use spi::query::ast::ObjectType;
use spi::query::execution::{Output, QueryExecution};
use spi::query::logical_planner::DropPlan;
use spi::query::QueryError;
use spi::query::Result;

pub struct DropExecution {
    plan: DropPlan,
    catalog: MetaDataRef,
}

impl DropExecution {
    pub fn new(plan: DropPlan, catalog: MetaDataRef) -> Self {
        Self { plan, catalog }
    }
}

#[async_trait]
impl QueryExecution for DropExecution {
    async fn start(&self) -> Result<Output> {
        let res = match self.plan.obj_type {
            ObjectType::Table => self.catalog.drop_table(&self.plan.object_name),
            ObjectType::Database => self.catalog.drop_database(&self.plan.object_name),
        };
        if self.plan.if_exist {
            return Ok(Output::Nil(()));
        }
        return match res {
            Ok(_) => {
                Ok(Output::Nil(()))
            }
            Err(e) => {
                Err(QueryError::Analyzer {
                    err: "drop failed".to_string(),
                })
            }
        }
    }
}
