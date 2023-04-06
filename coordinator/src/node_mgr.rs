use meta::model::MetaRef;
use models::meta_data::NodeState;

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

        let admin_meta = self.meta.admin_meta();
        let source_node_state = admin_meta.get_node_state(self.node_id).await?;

        admin_meta
            .update_node_state_cache(self.node_id, NodeState::from(node_state.clone()))
            .await?;

        if let Err(e) = meta_client
            .update_node_state(self.node_id, node_state)
            .await
        {
            admin_meta
                .update_node_state_cache(self.node_id, source_node_state)
                .await?;
            return Err(CoordinatorError::from(e));
        }

        Ok(())
    }
}
