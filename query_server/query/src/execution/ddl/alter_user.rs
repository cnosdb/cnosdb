use async_trait::async_trait;
use spi::query::execution::{
    // ExecutionError, MetaSnafu,
    Output,
    QueryStateMachineRef,
};
use spi::query::logical_planner::{AlterUser, AlterUserAction};
use spi::Result;
use trace::debug;

use crate::execution::ddl::DDLDefinitionTask;

pub struct AlterUserTask {
    stmt: AlterUser,
}

impl AlterUserTask {
    pub fn new(stmt: AlterUser) -> AlterUserTask {
        Self { stmt }
    }
}

#[async_trait]
impl DDLDefinitionTask for AlterUserTask {
    async fn execute(&self, query_state_machine: QueryStateMachineRef) -> Result<Output> {
        let AlterUser {
            ref user_name,
            ref alter_user_action,
        } = self.stmt;

        match alter_user_action {
            AlterUserAction::RenameTo(new_name) => {
                // 修改用户名称
                // user_id: &Oid,
                // new_name: String,
                // fn rename_user(
                //     &mut self,
                //     user_id: &Oid,
                //     new_name: String
                // ) -> Result<()>;
                debug!("Rename user {} to {}", user_name, new_name);
                query_state_machine
                    .meta
                    .rename_user(user_name, new_name.to_string())
                    .await?;
            }
            AlterUserAction::Set(options) => {
                // TODO 修改用户的信息
                // user_id: &Oid,
                // options: UserOptions
                // fn alter_user(
                //     &self,
                //     user_id: &Oid,
                //     options: UserOptions
                // ) -> Result<()>;
                debug!("Alter user {} with options [{}]", user_name, options);
                query_state_machine
                    .meta
                    .alter_user(user_name, options.clone())
                    .await?;
                // .context(MetaSnafu)?;
            }
        }

        return Ok(Output::Nil(()));
    }
}
