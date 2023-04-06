use meta::model::MetaRef;

use crate::errors::{CoordinatorError, CoordinatorResult};

pub struct NodeManager {
    node_id: u64,
    meta: MetaRef,
}

impl NodeManager {
    pub fn new(meta: MetaRef, node_id: u64) -> Self {
        Self { node_id, meta }
    }

    pub async fn change_node_state(
        &self,
        tenant: &str,
        node_state: String,
    ) -> CoordinatorResult<()> {
        let meta_client = self.meta.tenant_manager().tenant_meta(tenant).await.ok_or(
            CoordinatorError::TenantNotFound {
                name: tenant.to_string(),
            },
        )?;

        meta_client
            .update_node_state(self.node_id, node_state)
            .await?;

        Ok(())
    }
}
