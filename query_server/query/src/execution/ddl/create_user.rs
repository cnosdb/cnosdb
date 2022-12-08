use crate::execution::ddl::DDLDefinitionTask;
use async_trait::async_trait;
use meta::meta_client::MetaError;
use snafu::ResultExt;

use spi::query::execution::{self, MetadataSnafu};
use spi::query::execution::{ExecutionError, Output, QueryStateMachineRef};
use spi::query::logical_planner::CreateUser;
use trace::debug;

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
    async fn execute(
        &self,
        query_state_machine: QueryStateMachineRef,
    ) -> Result<Output, ExecutionError> {
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
        let meta = query_state_machine.meta.user_manager();
        let user = meta.user(name).context(MetadataSnafu)?;

        match (if_not_exists, user) {
            // do not create if exists
            (true, Some(_)) => Ok(Output::Nil(())),
            // Report an error if it exists
            (false, Some(_)) => Err(MetaError::UserAlreadyExists { user: name.clone() })
                .context(execution::MetadataSnafu),
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
                meta.create_user(name.clone(), options.clone(), false)
                    .context(MetadataSnafu)?;

                Ok(Output::Nil(()))
            }
        }
    }
}
