#[cfg(test)]
pub mod test {
    use std::path::PathBuf;
    use std::sync::{Arc, Mutex};

    use http_protocol::status_code;
    use meta::model::meta_admin::AdminMeta;
    use meta::model::meta_tenant::TenantMeta;
    use metrics::metric_register::MetricsRegister;
    use rand::Rng;

    use crate::cluster_def;
    use crate::utils::{data_config_file_path, kill_process, run_cluster, run_singleton, Client};
    const SERVER_URL: &str = "http://127.0.0.1:8902/api/v1/sql?db=replica_test_db";

    fn _time_str() -> String {
        let current_time = chrono::Local::now();

        current_time.format("%Y-%m-%dT%H:%M:%S").to_string()
    }

    fn replica_test(meta: Arc<TenantMeta>) {
        let db_info = meta.get_db_info("replica_test_db").unwrap().unwrap();
        assert!(!db_info.buckets.is_empty());

        let random = rand::thread_rng().gen_range(0..100);
        let bucket = &db_info.buckets[random % db_info.buckets.len()];
        let group = &bucket.shard_group[random % bucket.shard_group.len()];
        let vnode = &group.vnodes[random % group.vnodes.len()];

        let replica_id = group.id;
        let (target_node, exist_node) = if vnode.node_id == 1 { (2, 1) } else { (1, 2) };
        let client = Client::with_auth("root".to_string(), Some(String::new()));

        // test replica add, Add a replica to a node that already has a replica
        let command = format!(
            "replica add replica_id {} node_id {}",
            replica_id, exist_node
        );
        println!(
            "-----------test replica add, Add a replica to a node that already has a replica: {}",
            command
        );
        let resp = client.post(SERVER_URL, &command).unwrap();
        assert_eq!(resp.status(), 422);
        assert!(resp.text().unwrap().contains("A Replication Already in"));
        std::thread::sleep(std::time::Duration::from_secs(1));
        let info = meta.get_replica_all_info(replica_id).unwrap();
        assert_eq!(info.replica_set.vnodes.len(), 1);
        assert_eq!(info.replica_set.leader_node_id, vnode.node_id);

        // test replica add, Add a replica to a non-existing node
        let command = format!("replica add replica_id {} node_id 9999", replica_id);
        println!(
            "-----------test replica add, Add a replica to a non-existing node: {}",
            command
        );
        let resp = client.post(SERVER_URL, &command).unwrap();
        assert_eq!(resp.status(), 422);
        assert!(resp.text().unwrap().contains("Not Found Data Node"));
        std::thread::sleep(std::time::Duration::from_secs(1));
        let info = meta.get_replica_all_info(replica_id).unwrap();
        assert_eq!(info.replica_set.vnodes.len(), 1);
        assert_eq!(info.replica_set.leader_node_id, vnode.node_id);

        // test replica promote, Promote a replica to a no replica node
        let command = format!(
            "replica promote replica_id {} node_id {}",
            replica_id, target_node
        );
        println!(
            "-----------test replica promote, Promote a replica to a no replica node: {}",
            command
        );
        let resp = client.post(SERVER_URL, &command).unwrap();
        assert_eq!(resp.status(), 422);
        assert!(resp.text().unwrap().contains("not found replica in node"));
        std::thread::sleep(std::time::Duration::from_secs(1));
        let info = meta.get_replica_all_info(replica_id).unwrap();
        assert_eq!(info.replica_set.vnodes.len(), 1);
        assert_eq!(info.replica_set.leader_node_id, exist_node);

        // test replica promote, Promote a replica to a not exist node
        let command = format!("replica promote replica_id {} node_id 999", replica_id);
        println!(
            "-----------test replica promote, Promote a replica to a not exist node: {}",
            command
        );
        let resp = client.post(SERVER_URL, &command).unwrap();
        assert_eq!(resp.status(), 422);
        assert!(resp.text().unwrap().contains("not found replica in node"));
        std::thread::sleep(std::time::Duration::from_secs(1));
        let info = meta.get_replica_all_info(replica_id).unwrap();
        assert_eq!(info.replica_set.vnodes.len(), 1);
        assert_eq!(info.replica_set.leader_node_id, exist_node);

        // test replica promote, promote a not exist replica
        let command = format!("replica promote replica_id 9999 node_id {}", exist_node);
        println!(
            "-----------test replica promote, promote a not exist replica: {}",
            command
        );
        let resp = client.post(SERVER_URL, &command).unwrap();
        assert_eq!(resp.status(), 422);
        // println!("resp.text().unwrap(): {}", resp.text().unwrap());
        assert!(resp.text().unwrap().contains("ReplicationSet not found"));
        std::thread::sleep(std::time::Duration::from_secs(1));
        let info = meta.get_replica_all_info(replica_id).unwrap();
        assert_eq!(info.replica_set.vnodes.len(), 1);
        assert_eq!(info.replica_set.leader_node_id, exist_node);

        // test replica remove, replica id does not match node id
        let command = format!(
            "replica remove replica_id {} node_id {}",
            replica_id, target_node
        );
        println!(
            "-----------test replica remove, replica id does not match node id: {}",
            command
        );
        let resp = client.post(SERVER_URL, &command).unwrap();
        assert_eq!(resp.status(), 422);
        assert!(resp.text().unwrap().contains("not found replica in node"));
        std::thread::sleep(std::time::Duration::from_secs(1));
        let info = meta.get_replica_all_info(replica_id).unwrap();
        assert_eq!(info.replica_set.vnodes.len(), 1);
        assert_eq!(info.replica_set.leader_node_id, exist_node);

        // test replica add
        let command = format!(
            "replica add replica_id {} node_id {}",
            replica_id, target_node
        );
        println!("-----------test replica add: {}", command);
        let resp = client.post(SERVER_URL, &command).unwrap();
        assert_eq!(resp.status(), status_code::OK);
        std::thread::sleep(std::time::Duration::from_secs(1));
        let info = meta.get_replica_all_info(replica_id).unwrap();
        assert_eq!(info.replica_set.vnodes.len(), 2);
        assert_eq!(info.replica_set.leader_node_id, vnode.node_id);

        // test replica promote
        let command = format!(
            "replica promote replica_id {} node_id {}",
            replica_id, target_node
        );
        println!("-----------test replica promote: {}", command);
        let resp = client.post(SERVER_URL, &command).unwrap();
        assert_eq!(resp.status(), status_code::OK);
        std::thread::sleep(std::time::Duration::from_secs(1));
        let info = meta.get_replica_all_info(replica_id).unwrap();
        assert_eq!(info.replica_set.vnodes.len(), 2);
        assert_eq!(info.replica_set.leader_node_id, target_node);

        // test replica remove
        let command = format!(
            "replica remove replica_id {} node_id {}",
            replica_id, exist_node
        );
        println!("-----------: {}", command);
        let resp = client.post(SERVER_URL, &command).unwrap();
        assert_eq!(resp.status(), status_code::OK);
        std::thread::sleep(std::time::Duration::from_secs(1));
        let info = meta.get_replica_all_info(replica_id).unwrap();
        assert_eq!(info.replica_set.vnodes.len(), 1);
        assert_eq!(info.replica_set.leader_node_id, target_node);

        // test replica remove, remove last replica
        let command = format!(
            "replica remove replica_id {} node_id {}",
            replica_id, target_node
        );
        println!(
            "-----------test replica remove, remove last replica: {}",
            command
        );
        let resp = client.post(SERVER_URL, &command).unwrap();
        assert_eq!(resp.status(), 422);
        assert!(resp
            .text()
            .unwrap()
            .contains("just only on replica can't remove"));
        std::thread::sleep(std::time::Duration::from_secs(1));
        let info = meta.get_replica_all_info(replica_id).unwrap();
        assert_eq!(info.replica_set.vnodes.len(), 1);
        assert_eq!(info.replica_set.leader_node_id, target_node);

        // test replica destory, replica not exist
        let command = "replica destory replica_id 9999".to_string();
        println!(
            "-----------test replica destory, replica not exist: {}",
            command
        );
        let resp = client.post(SERVER_URL, &command).unwrap();
        assert_eq!(resp.status(), 422);
        assert!(resp.text().unwrap().contains("ReplicationSet not found"));
        std::thread::sleep(std::time::Duration::from_secs(1));
        let info = meta.get_replica_all_info(replica_id).unwrap();
        assert_eq!(info.replica_set.vnodes.len(), 1);
        assert_eq!(info.replica_set.leader_node_id, target_node);

        // test replica destory
        let command = format!("replica destory replica_id {}", replica_id);
        println!("-----------test replica destory: {}", command);
        let resp = client.post(SERVER_URL, &command).unwrap();
        assert_eq!(resp.status(), status_code::OK);
        std::thread::sleep(std::time::Duration::from_secs(1));
        let info = meta.get_replica_all_info(replica_id);
        assert!(info.is_none());

        // test replica destory repeatly
        let command = format!("replica destory replica_id {}", replica_id);
        println!("-----------test replica destory repeatly: {}", command);
        let resp = client.post(SERVER_URL, &command).unwrap();
        assert_eq!(resp.status(), 422);
        assert!(resp.text().unwrap().contains("ReplicationSet not found"));
        std::thread::sleep(std::time::Duration::from_secs(1));
        let info = meta.get_replica_all_info(replica_id);
        assert!(info.is_none());
    }

    /// Clean test environment.
    /// 1. Kill all 'cnosdb' and 'cnosdb-meta' process,
    /// 2. Remove directory '/tmp/cnosdb'.
    fn clean_env() {
        println!("-------- Cleaning environment...");
        kill_process("cnosdb");
        kill_process("cnosdb-meta");
        println!(" - Removing directory 'replica_test'");
        let _ = std::fs::remove_dir_all("replica_test");
        println!("Clean environment completed.");
    }

    #[test]
    fn replica_test_case() {
        println!("Test begin 'replica_test_case'");
        clean_env();

        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .worker_threads(4)
            .build()
            .unwrap();
        let runtime = Arc::new(runtime);

        let (_meta_server, data_server) = run_cluster(
            "replica_test",
            runtime.clone(),
            &cluster_def::one_meta_two_data_bundled(),
            true,
            true,
        );
        std::thread::sleep(std::time::Duration::from_secs(2));
        let data_server = Arc::new(Mutex::new(data_server.unwrap()));
        let file_name = data_server.lock().unwrap().data_node_definitions[0]
            .config_file_name
            .clone();
        let config_file = data_config_file_path("replica_test", &file_name);

        let config = config::tskv::get_config(config_file).unwrap();
        let meta = runtime.block_on(AdminMeta::new(config, Arc::new(MetricsRegister::default())));
        let meta_client = runtime.block_on(meta.tenant_meta("cnosdb")).unwrap();

        let client = Client::with_auth("root".to_string(), Some(String::new()));
        let _ = client
            .post(
                "http://127.0.0.1:8902/api/v1/sql",
                "CREATE DATABASE replica_test_db WITH TTl '3560d' SHARD 1 VNOdE_DURATiON '1d' REPLICA 1 pRECISIOn 'ns';",
            )
            .unwrap();
        std::thread::sleep(std::time::Duration::from_secs(2));

        let write_count = 10000;
        for count in 0..write_count {
            let tstamp = (1711333406_u64 + count) * 1000000000;
            let random = rand::thread_rng().gen_range(0..32);
            let body = format!("ma,ta=a_{} fa={} {}", random, count, tstamp);

            let url = "http://127.0.0.1:8902/api/v1/write?db=replica_test_db";
            let resp = client.post(url, &body).unwrap();
            assert_eq!(resp.status(), status_code::OK);
        }
        std::thread::sleep(std::time::Duration::from_secs(3));

        replica_test(meta_client);

        let resp = client
            .post(SERVER_URL, "select count(1,2) from ma")
            .unwrap();
        assert_eq!(resp.status(), status_code::OK);
        let actual = resp.text().unwrap();
        let expected = format!("\"COUNT(Int64(1),Int64(2))\"\n{}\n", 0);
        println!("\nselect count(1,2): {}", actual);
        println!("expected: {}", expected);
        assert_eq!(actual, expected);

        clean_env();
        println!("#### Test complete replica_test_case ####");
    }

    #[test]
    fn replica_test_case_singleton() {
        println!("Test begin 'replica_test_case_singleton'");
        clean_env();

        let test_dir = PathBuf::from("replica_test");
        let _ = std::fs::remove_dir_all(&test_dir);
        std::fs::create_dir_all(&test_dir).unwrap();

        let data_node_def = &cluster_def::one_data(1);

        let runtime = tokio::runtime::Runtime::new().unwrap();

        let data = run_singleton(&test_dir, data_node_def, false, true);

        std::thread::sleep(std::time::Duration::from_secs(2));
        let data: Arc<Mutex<_>> = Arc::new(Mutex::new(data));

        let file_name = data.lock().unwrap().data_node_definitions[0]
            .config_file_name
            .clone();

        let config_file = data_config_file_path(&test_dir, &file_name);
        let config = config::tskv::get_config(config_file).unwrap();
        let meta = runtime.block_on(AdminMeta::new(config, Arc::new(MetricsRegister::default())));
        let meta_client = runtime.block_on(meta.tenant_meta("cnosdb")).unwrap();

        let client = Client::with_auth("root".to_string(), Some(String::new()));
        let _ = client
            .post(
                "http://127.0.0.1:8902/api/v1/sql",
                "CREATE DATABASE replica_test_db WITH TTl '3560d' SHARD 1 VNOdE_DURATiON '1d' REPLICA 1 pRECISIOn 'ns';",
            )
            .unwrap();

        let write_count = 10000;
        for count in 0..write_count {
            let tstamp = (1711333406_u64 + count) * 1000000000;
            let random = rand::thread_rng().gen_range(0..32);
            let body = format!("ma,ta=a_{} fa={} {}", random, count, tstamp);

            let url = "http://127.0.0.1:8902/api/v1/write?db=replica_test_db";
            let resp = client.post(url, &body).unwrap();
            assert_eq!(resp.status(), status_code::OK);
        }
        std::thread::sleep(std::time::Duration::from_secs(3));

        let db_info = meta_client.get_db_info("replica_test_db").unwrap().unwrap();
        assert!(!db_info.buckets.is_empty());

        let random = rand::thread_rng().gen_range(0..100);
        let bucket = &db_info.buckets[random % db_info.buckets.len()];
        let group = &bucket.shard_group[random % bucket.shard_group.len()];
        let vnode = &group.vnodes[random % group.vnodes.len()];

        let replica_id = group.id;
        let target_node = vnode.node_id;
        let client = Client::with_auth("root".to_string(), Some(String::new()));

        // test replica add
        // A Replication Already in node 1
        let command = format!(
            "replica add replica_id {} node_id {}",
            replica_id, target_node
        );
        println!("-----------: {}", command);
        let resp = client.post(SERVER_URL, &command).unwrap();
        assert_eq!(resp.status(), 422);
        assert!(resp.text().unwrap().contains("A Replication Already in"));
        std::thread::sleep(std::time::Duration::from_secs(1));
        let info = meta_client.get_replica_all_info(replica_id).unwrap();
        assert_eq!(info.replica_set.vnodes.len(), 1);
        assert_eq!(info.replica_set.leader_node_id, target_node);

        // test replica add replica not exist
        let command = format!("replica add replica_id 9999 node_id {}", target_node);
        println!(
            "-----------test replica add, replica not exist: {}",
            command
        );
        let resp = client.post(SERVER_URL, &command).unwrap();
        assert_eq!(resp.status(), 422);
        assert!(resp.text().unwrap().contains("ReplicationSet not found"));
        std::thread::sleep(std::time::Duration::from_secs(1));
        let info = meta_client.get_replica_all_info(replica_id).unwrap();
        assert_eq!(info.replica_set.vnodes.len(), 1);
        assert_eq!(info.replica_set.leader_node_id, target_node);

        // test replica add: node not exist
        let command = format!("replica add replica_id {} node_id 9999", replica_id);
        println!("-----------test replica add, node not exist: {}", command);
        let resp = client.post(SERVER_URL, &command).unwrap();
        assert_eq!(resp.status(), 422);
        assert!(resp.text().unwrap().contains("Not Found Data Node"));
        std::thread::sleep(std::time::Duration::from_secs(1));
        let info = meta_client.get_replica_all_info(replica_id).unwrap();
        assert_eq!(info.replica_set.vnodes.len(), 1);
        assert_eq!(info.replica_set.leader_node_id, target_node);

        // test replica promote
        let command = format!(
            "replica promote replica_id {} node_id {}",
            replica_id, target_node
        );
        println!("-----------test replica promote: {}", command);
        let resp = client.post(SERVER_URL, &command).unwrap();
        assert_eq!(resp.status(), status_code::OK);
        std::thread::sleep(std::time::Duration::from_secs(1));
        let info = meta_client.get_replica_all_info(replica_id).unwrap();
        assert_eq!(info.replica_set.vnodes.len(), 1);
        assert_eq!(info.replica_set.leader_node_id, target_node);

        // test replica promote, replica not exist
        let command = format!("replica promote replica_id 999 node_id {}", target_node);
        println!(
            "-----------test replica promote, replica not exist: {}",
            command
        );
        let resp = client.post(SERVER_URL, &command).unwrap();
        assert_eq!(resp.status(), 422);
        assert!(resp.text().unwrap().contains("ReplicationSet not found"));
        std::thread::sleep(std::time::Duration::from_secs(1));
        let info = meta_client.get_replica_all_info(replica_id).unwrap();
        assert_eq!(info.replica_set.vnodes.len(), 1);
        assert_eq!(info.replica_set.leader_node_id, target_node);

        // test replica promote, node not exist
        let command = format!("replica promote replica_id {} node_id 999", replica_id);
        println!(
            "-----------test replica promote, node not exist: {}",
            command
        );
        let resp = client.post(SERVER_URL, &command).unwrap();
        assert_eq!(resp.status(), 422);
        assert!(resp.text().unwrap().contains("not found replica in node"));
        std::thread::sleep(std::time::Duration::from_secs(1));
        let info = meta_client.get_replica_all_info(replica_id).unwrap();
        assert_eq!(info.replica_set.vnodes.len(), 1);
        assert_eq!(info.replica_set.leader_node_id, target_node);

        // test replica remove
        let command = format!(
            "replica remove replica_id {} node_id {}",
            replica_id, vnode.node_id
        );
        println!("-----------test replica remove: {}", command);
        let resp = client.post(SERVER_URL, &command).unwrap();
        assert_eq!(resp.status(), 422);
        assert!(resp
            .text()
            .unwrap()
            .contains("just only on replica can't remove"));
        std::thread::sleep(std::time::Duration::from_secs(1));
        let info = meta_client.get_replica_all_info(replica_id).unwrap();
        assert_eq!(info.replica_set.vnodes.len(), 1);
        assert_eq!(info.replica_set.leader_node_id, target_node);

        // test replica remove, replica not exist
        let command = format!("replica remove replica_id 9999 node_id {}", vnode.node_id);
        println!(
            "-----------test replica remove, replica not exist: {}",
            command
        );
        let resp = client.post(SERVER_URL, &command).unwrap();
        assert_eq!(resp.status(), 422);
        assert!(resp.text().unwrap().contains("ReplicationSet not found"));
        std::thread::sleep(std::time::Duration::from_secs(1));
        let info = meta_client.get_replica_all_info(replica_id).unwrap();
        assert_eq!(info.replica_set.vnodes.len(), 1);
        assert_eq!(info.replica_set.leader_node_id, target_node);

        // test replica remove, node not exist
        let command = format!("replica remove replica_id {} node_id 9999", replica_id);
        println!(
            "-----------test replica remove, node not exist: {}",
            command
        );
        let resp = client.post(SERVER_URL, &command).unwrap();
        assert_eq!(resp.status(), 422);
        assert!(resp.text().unwrap().contains("not found replica in node"));
        std::thread::sleep(std::time::Duration::from_secs(1));
        let info = meta_client.get_replica_all_info(replica_id).unwrap();
        assert_eq!(info.replica_set.vnodes.len(), 1);
        assert_eq!(info.replica_set.leader_node_id, target_node);

        // destory replica
        let command = format!("replica destory replica_id {}", replica_id);
        println!("-----------destory replica: {}", command);
        let resp = client.post(SERVER_URL, &command).unwrap();
        assert_eq!(resp.status(), status_code::OK);
        std::thread::sleep(std::time::Duration::from_secs(1));

        // destory replica  not exist
        let command = "replica destory replica_id 9999".to_string();
        println!("-----------destory replica  not exist: {}", command);
        let resp = client.post(SERVER_URL, &command).unwrap();
        assert_eq!(resp.status(), 422);
        assert!(resp.text().unwrap().contains("ReplicationSet not found"));

        // destory replica  repeatly
        let command = format!("replica destory replica_id {}", replica_id);
        println!("-----------destory replica  repeatly: {}", command);
        let resp = client.post(SERVER_URL, &command).unwrap();
        assert_eq!(resp.status(), 422);
        assert!(resp.text().unwrap().contains("ReplicationSet not found"));

        let resp = client
            .post(SERVER_URL, "select count(1,2) from ma")
            .unwrap();
        assert_eq!(resp.status(), status_code::OK);
        let actual = resp.text().unwrap();
        assert_eq!(actual, "\"COUNT(Int64(1),Int64(2))\"\n0\n");

        clean_env();
        println!("#### Test complete replica_test_case_singleton ####");
    }

    #[test]
    fn replica_test_case_1query_1tskv() {
        println!("Test begin 'replica_test_case_1query_1tskv'");
        clean_env();

        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .worker_threads(4)
            .build()
            .unwrap();
        let runtime = Arc::new(runtime);

        let (_meta_server, data_server) = run_cluster(
            "replica_test",
            runtime.clone(),
            &cluster_def::one_meta_two_data_separated(),
            true,
            true,
        );
        std::thread::sleep(std::time::Duration::from_secs(2));
        let data_server = Arc::new(Mutex::new(data_server.unwrap()));
        let file_name = data_server.lock().unwrap().data_node_definitions[0]
            .config_file_name
            .clone();
        let config_file = data_config_file_path("replica_test_case_1query_1tskv", &file_name);

        let config = config::tskv::get_config(config_file).unwrap();
        let meta = runtime.block_on(AdminMeta::new(config, Arc::new(MetricsRegister::default())));
        let meta_client = runtime.block_on(meta.tenant_meta("cnosdb")).unwrap();

        let client = Client::with_auth("root".to_string(), Some(String::new()));
        let _ = client
            .post(
                "http://127.0.0.1:8912/api/v1/sql",
                "CREATE DATABASE replica_test_db WITH TTl '3560d' SHARD 1 VNOdE_DURATiON '1d' REPLICA 1 pRECISIOn 'ns';",
            )
            .unwrap();
        std::thread::sleep(std::time::Duration::from_secs(2));

        let write_count = 10000;
        for count in 0..write_count {
            let tstamp = (1711333406_u64 + count) * 1000000000;
            let random = rand::thread_rng().gen_range(0..32);
            let body = format!("ma,ta=a_{} fa={} {}", random, count, tstamp);

            let url = "http://127.0.0.1:8912/api/v1/write?db=replica_test_db";
            let resp = client.post(url, &body).unwrap();
            assert_eq!(resp.status(), status_code::OK);
        }
        std::thread::sleep(std::time::Duration::from_secs(3));

        let db_info = meta_client.get_db_info("replica_test_db").unwrap().unwrap();
        assert!(!db_info.buckets.is_empty());

        let random = rand::thread_rng().gen_range(0..100);
        let bucket = &db_info.buckets[random % db_info.buckets.len()];
        let group = &bucket.shard_group[random % bucket.shard_group.len()];
        let vnode = &group.vnodes[random % group.vnodes.len()];

        let replica_id = group.id;
        let tskv_node = vnode.node_id;
        let query_node = 2;
        let client = Client::with_auth("root".to_string(), Some(String::new()));
        let req_url = "http://127.0.0.1:8912/api/v1/sql?db=replica_test_db";

        // test replica add, add a replica to a query node
        let command = format!(
            "replica add replica_id {} node_id {}",
            replica_id, query_node
        );
        println!(
            "-----------test replica add, add a replica to a query node: {}",
            command
        );
        let resp = client.post(req_url, &command).unwrap();
        assert_eq!(resp.status(), 422);
        assert!(resp.text().unwrap().contains("Not Found Data Node"));
        std::thread::sleep(std::time::Duration::from_secs(1));
        let info = meta_client.get_replica_all_info(replica_id).unwrap();
        assert_eq!(info.replica_set.vnodes.len(), 1);
        assert_eq!(info.replica_set.leader_node_id, tskv_node);

        // destory replica
        let command = format!("replica destory replica_id {}", replica_id);
        println!("-----------destory replica: {}", command);
        let resp = client.post(req_url, &command).unwrap();
        assert_eq!(resp.status(), status_code::OK);
        std::thread::sleep(std::time::Duration::from_secs(1));

        let resp = client.post(req_url, "select count(1,2) from ma").unwrap();
        assert_eq!(resp.status(), status_code::OK);
        let actual = resp.text().unwrap();
        assert_eq!(actual, "\"COUNT(Int64(1),Int64(2))\"\n0\n");

        clean_env();
        println!("#### Test complete replica_test_case_1query_1tskv ####");
    }
}
