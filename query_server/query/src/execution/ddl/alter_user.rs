use crate::execution::ddl::DDLDefinitionTask;
use async_trait::async_trait;
use spi::query::execution::{ExecutionError, Output, QueryStateMachineRef};
use spi::query::logical_planner::{AlterUser, AlterUserAction};
use trace::debug;

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
    async fn execute(
        &self,
        _query_state_machine: QueryStateMachineRef,
    ) -> Result<Output, ExecutionError> {
        let AlterUser {
            ref user_id,
            ref alter_user_action,
        } = self.stmt;

        match alter_user_action {
            AlterUserAction::RenameTo(new_name) => {
                // TODO 修改用户名称
                // user_id: &Oid,
                // new_name: String,
                // fn rename_user(
                //     &mut self,
                //     user_id: &Oid,
                //     new_name: String
                // ) -> Result<()>;
                debug!("Rename user {} to {}", user_id, new_name);
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
                debug!("Alter user {} with options [{}]", user_id, options);
            }
        }

        return Ok(Output::Nil(()));
    }
}
