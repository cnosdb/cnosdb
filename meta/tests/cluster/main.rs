mod test_cluster;

use meta::store::Store;
use meta::NodeId;
use openraft::testing::Suite;
use openraft::StorageError;
use std::sync::Arc;

pub async fn new_async() -> Arc<Store> {
    let res = Store::open_create(0);
    Arc::new(res)
}

#[test]
pub fn test_mem_store() -> Result<(), StorageError<NodeId>> {
    Suite::test_all(new_async)?;
    Ok(())
}
