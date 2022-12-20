// mod test_cluster;

use meta::store::Store;
use meta::ClusterNodeId;
use openraft::testing::Suite;
use openraft::{Config, LogId, StorageError};
use std::sync::Arc;

pub async fn new_async() -> Arc<Store> {
    let db_path = format!("{}/{}-{}.binlog", "./meta/journal", "test", "1");
    let db = sled::open(db_path.clone()).unwrap();
    let config = Config::default().validate().unwrap();
    let config = Arc::new(config);
    Arc::new(Store::new(db))
}

// #[test]
// pub fn test_store(){
//     // Suite::test_all(new_async)?;
//     let db = sled::open("/Users/liuyongtao/work/cnosdb/meta/journal1/meta_node-1.binlog").unwrap();
//     let tree = db.open_tree("state_machine").unwrap();
//     let res = tree.get(b"last_applied_log").unwrap().unwrap();
//     let t = serde_json::from_slice::<LogId<ClusterNodeId>>(&res).unwrap();
//     println!("{}", t);
// }
