use async_trait::async_trait;
use meta::error::MetaError;
use spi::query::execution::{Output, QueryStateMachineRef};
use spi::query::logical_planner::{DropGlobalObject, GlobalObjectType};
use spi::{QueryError, Result};
use trace::debug;

use super::DDLDefinitionTask;

pub struct DropGlobalObjectTask {
    stmt: DropGlobalObject,
}

impl DropGlobalObjectTask {
    pub fn new(stmt: DropGlobalObject) -> Self {
        Self { stmt }
    }
}

#[async_trait]
impl DDLDefinitionTask for DropGlobalObjectTask {
    async fn execute(&self, query_state_machine: QueryStateMachineRef) -> Result<Output> {
        let DropGlobalObject {
            ref name,
            ref if_exist,
            ref obj_type,
        } = self.stmt;

        let meta = query_state_machine.meta.clone();

        match obj_type {
            GlobalObjectType::User => {
                // 删除用户
                // fn drop_user(
                //     &mut self,
                //     name: &str
                // ) -> Result<bool>;
                debug!("Drop user {}", name);
                let success = meta.drop_user(name).await?;

                if let (false, false) = (if_exist, success) {
                    return Err(QueryError::Meta {
                        source: MetaError::UserNotFound {
                            user: name.to_string(),
                        },
                    });
                }

                Ok(Output::Nil(()))
            }
            GlobalObjectType::Tenant => {
                // 删除租户
                // fn drop_tenant(
                //     &self,
                //     name: &str
                // ) -> Result<bool>;
                debug!("Drop tenant {}", name);
                let success = meta.drop_tenant(name).await?;
                // .context(MetaSnafu)?;

                if let (false, false) = (if_exist, success) {
                    return Err(QueryError::Meta {
                        source: MetaError::TenantNotFound {
                            tenant: name.to_string(),
                        },
                    });
                }

                Ok(Output::Nil(()))
            }
        }
    }
}
