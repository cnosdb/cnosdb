use async_trait::async_trait;
use meta::error::MetaError;
use snafu::ResultExt;
use spi::query::execution::{Output, QueryStateMachineRef};
use spi::query::logical_planner::CreateUser;
use spi::{MetaSnafu, Result};
use trace::debug;

use crate::execution::ddl::DDLDefinitionTask;

pub struct CreateUserTask {
    stmt: CreateUser,
}

impl CreateUserTask {
    pub fn new(stmt: CreateUser) -> Self {
        Self { stmt }
    }
}

#[async_trait]
impl DDLDefinitionTask for CreateUserTask {
    async fn execute(&self, query_state_machine: QueryStateMachineRef) -> Result<Output> {
        let CreateUser {
            ref name,
            ref if_not_exists,
            ref options,
        } = self.stmt;

        // 元数据接口查询用户是否存在
        // fn user(
        //     &self,
        //     name: &str
        // ) -> Result<Option<UserDesc>>;
        let user = query_state_machine.meta.user(name).await?;

        match (if_not_exists, user) {
            // do not create if exists
            (true, Some(_)) => Ok(Output::Nil(())),
            // Report an error if it exists
            (false, Some(_)) => {
                Err(MetaError::UserAlreadyExists { user: name.clone() }).context(MetaSnafu)
            }
            // does not exist, create
            (_, None) => {
                // 创建用户
                // name: String
                // options: UserOptions
                // fn create_user(
                //     &mut self,
                //     name: String,
                //     options: UserOptions
                // ) -> Result<&UserDesc>;

                debug!("Create user {} with options [{}]", name, options);
                query_state_machine
                    .meta
                    .create_user(name.clone(), options.clone(), false)
                    .await?;

                Ok(Output::Nil(()))
            }
        }
    }
}
