use crate::execution::ddl::DDLDefinitionTask;
use async_trait::async_trait;
use models::schema::Tenant;
use snafu::ResultExt;
use spi::catalog::MetadataError;
use spi::query::execution;
use spi::query::execution::{ExecutionError, Output, QueryStateMachineRef};
use spi::query::logical_planner::CreateTenant;
use trace::debug;

pub struct CreateTenantTask {
    stmt: CreateTenant,
}

impl CreateTenantTask {
    pub fn new(stmt: CreateTenant) -> Self {
        Self { stmt }
    }
}

#[async_trait]
impl DDLDefinitionTask for CreateTenantTask {
    async fn execute(
        &self,
        _query_state_machine: QueryStateMachineRef,
    ) -> Result<Output, ExecutionError> {
        let CreateTenant {
            ref name,
            ref if_not_exists,
            ref options,
        } = self.stmt;

        // TODO 元数据接口查询tenant是否存在

        let tenant: Option<Tenant> = None;

        match (if_not_exists, tenant) {
            // do not create if exists
            (true, Some(_)) => Ok(Output::Nil(())),
            // Report an error if it exists
            (false, Some(_)) => Err(MetadataError::TenantAlreadyExists {
                tenant_name: name.clone(),
            })
            .context(execution::MetadataSnafu),
            // does not exist, create
            (_, None) => {
                // TODO 创建tenant
                // name: String
                // options: TenantOptions
                debug!("Create tenant {} with options [{}]", name, options);

                Ok(Output::Nil(()))
            }
        }
    }
}
