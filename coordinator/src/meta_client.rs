pub trait MetaClient {
    fn create_user();
    fn drop_user();

    fn create_db(); //(user db_name place_rule retention_time)
    fn drop_db(); //(user db_name)

    fn set_retention_time(); //(user, db, retention_time)
    fn set_place_rule(); //(user, db, rule)

    fn create_table(); //user db table_name
    fn drop_table(); //user db table_name

    fn drop_bucket(); //drop out of data bucket
    fn create_bucket(); // (user db time_range) -> <bucket_id, vnode_list<vnode>>

    fn buckets_by_time_range(); // user dbname
    fn databases_by_user(); // user for show databases;

    fn add_node(); // (nodeid, ip, memory, cpu, disk, status)
    fn drop_node(); // drop data node
    fn nodes(); //get all nodes
    fn node(); //node_id -> nodeInfo
    fn heartbeat(); // update node status

    fn add_meta_node();
    fn del_meta_node();
    fn meta_nodes();
}
