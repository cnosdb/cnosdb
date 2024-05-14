#[cfg(test)]
pub mod test {
    use std::sync::{Arc, Mutex};

    use http_protocol::status_code;
    use meta::model::meta_admin::AdminMeta;
    use meta::model::meta_tenant::TenantMeta;
    use rand::Rng;

    use crate::cluster_def;
    use crate::utils::{data_config_file_path, kill_process, run_cluster, Client};
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
        let target_node = if vnode.node_id == 1 { 2 } else { 1 };
        let client = Client::with_auth("root".to_string(), Some(String::new()));

        // test replica add
        let command = format!(
            "replica add replica_id {} node_id {}",
            replica_id, target_node
        );
        println!("-----------: {}", command);
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
        println!("-----------: {}", command);
        let resp = client.post(SERVER_URL, &command).unwrap();
        assert_eq!(resp.status(), status_code::OK);
        std::thread::sleep(std::time::Duration::from_secs(1));
        let info = meta.get_replica_all_info(replica_id).unwrap();
        assert_eq!(info.replica_set.vnodes.len(), 2);
        assert_eq!(info.replica_set.leader_node_id, target_node);

        // test replica remove
        let command = format!(
            "replica remove replica_id {} node_id {}",
            replica_id, vnode.node_id
        );
        println!("-----------: {}", command);
        let resp = client.post(SERVER_URL, &command).unwrap();
        assert_eq!(resp.status(), status_code::OK);
        std::thread::sleep(std::time::Duration::from_secs(1));
        let info = meta.get_replica_all_info(replica_id).unwrap();
        assert_eq!(info.replica_set.vnodes.len(), 1);
        assert_eq!(info.replica_set.leader_node_id, target_node);
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

        let config = config::get_config(config_file).unwrap();
        let meta = runtime.block_on(AdminMeta::new(config));
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

        let resp = client.post(SERVER_URL, "select count(*) from ma").unwrap();
        assert_eq!(resp.status(), status_code::OK);
        let actual = resp.text().unwrap();
        let expected = format!("{}", write_count);
        println!("\nselect count(*): {}", actual);
        println!("expected: {}", expected);
        assert!(actual.contains(&expected));

        clean_env();
        println!("#### Test complete replica_test_case ####");
    }
}
