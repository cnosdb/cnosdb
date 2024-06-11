#[cfg(test)]
pub mod test {
    use std::sync::{Arc, Mutex};
    use std::thread;

    use http_protocol::status_code;
    use meta::model::meta_admin::AdminMeta;
    use meta::model::meta_tenant::TenantMeta;
    use metrics::metric_register::MetricsRegister;
    use rand::Rng;

    use crate::cluster_def;
    use crate::utils::{
        data_config_file_path, kill_process, run_cluster, Client, CnosdbDataTestHelper,
    };
    const SERVER_URL: &str = "http://127.0.0.1:8902/api/v1/sql?db=chaos_test_db";

    fn time_str() -> String {
        let current_time = chrono::Local::now();

        current_time.format("%Y-%m-%dT%H:%M:%S").to_string()
    }

    fn restart_datanode(data_server: Arc<Mutex<CnosdbDataTestHelper>>) {
        println!("---------------------------------- Restart node...");
        let mut data_server = data_server.lock().unwrap();
        let node_def = data_server.data_node_definitions[1].clone();
        data_server.restart_one_node(&node_def);

        std::thread::sleep(std::time::Duration::from_secs(3));
    }

    fn move_vnode(meta: Arc<TenantMeta>) {
        let db_info = meta.get_db_info("chaos_test_db").unwrap().unwrap();
        let random = rand::thread_rng().gen_range(0..100);
        if db_info.buckets.is_empty() {
            return;
        }
        let bucket = &db_info.buckets[random % db_info.buckets.len()];
        let group = &bucket.shard_group[random % bucket.shard_group.len()];
        let vnode = &group.vnodes[random % group.vnodes.len()];

        let move_vnode_id = vnode.id;
        let target_node = if vnode.node_id == 1 { 2 } else { 1 };
        let command = format!("move vnode {move_vnode_id} to node {target_node}");
        println!("{} ------- Move Vnode Command: {}", time_str(), command);
        println!("{} ------- Move Vnode Group: {:?}", time_str(), group);
        let client = Client::with_auth("root".to_string(), Some(String::new()));
        client.post(SERVER_URL, &command).unwrap();
    }

    fn alter_replica() {
        let mut rng = rand::thread_rng();
        let target_replica = rng.gen_range(1..=2);
        let command = format!("alter database chaos_test_db set replica {target_replica}");
        println!("------- Alter Replica Command: {}", command);
        let client = Client::with_auth("root".to_string(), Some(String::new()));
        client.post(SERVER_URL, &command).unwrap();
    }

    fn alter_shard() {
        let mut rng = rand::thread_rng();
        let target_shard = rng.gen_range(1..=10);
        let command = format!("alter database chaos_test_db set shard {target_shard}");
        println!("------- Alter Shard Command: {}", command);
        let client = Client::with_auth("root".to_string(), Some(String::new()));
        client.post(SERVER_URL, &command).unwrap();
    }

    fn alter_vnode_duration() {
        let mut rng = rand::thread_rng();
        let duration = rng.gen_range(1..=365);
        let command = format!("alter database chaos_test_db set vnode_duration '{duration}d'");
        println!("------- Alter Vnode Duration Command: {}", command);
        let client = Client::with_auth("root".to_string(), Some(String::new()));
        client.post(SERVER_URL, &command).unwrap();
    }

    /// Clean test environment.
    /// 1. Kill all 'cnosdb' and 'cnosdb-meta' process,
    /// 2. Remove directory '/tmp/cnosdb'.
    fn clean_env() {
        println!("-------- Cleaning environment...");
        kill_process("cnosdb");
        kill_process("cnosdb-meta");
        println!(" - Removing directory 'chaos_test'");
        let _ = std::fs::remove_dir_all("chaos_test");
        println!("Clean environment completed.");
    }

    #[test]
    fn chaos_test_case_1() {
        println!("Test begin 'chaos_test_case_1'");
        clean_env();

        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .worker_threads(4)
            .build()
            .unwrap();
        let runtime = Arc::new(runtime);

        let (_meta_server, data_server) = run_cluster(
            "chaos_test",
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
        let config_file = data_config_file_path("chaos_test", &file_name);

        let config = config::tskv::get_config(config_file).unwrap();
        let meta = runtime.block_on(AdminMeta::new(config, Arc::new(MetricsRegister::default())));
        let meta_client = runtime.block_on(meta.tenant_meta("cnosdb")).unwrap();

        let client = Client::with_auth("root".to_string(), Some(String::new()));
        let _ = client
            .post(
                "http://127.0.0.1:8902/api/v1/sql",
                "CREATE DATABASE chaos_test_db WITH TTl '3560d' SHARD 1 VNOdE_DURATiON '1d' REPLICA 1 pRECISIOn 'ns';",
            )
            .unwrap();
        std::thread::sleep(std::time::Duration::from_secs(2));

        let write_count = Arc::new(Mutex::new(0));
        let finish_flag = Arc::new(Mutex::new(false));

        let write_count_c = write_count.clone();
        let finish_flag_c = finish_flag.clone();
        let write_data_action = move || {
            let client = Client::with_auth("root".to_string(), Some(String::new()));
            while !*finish_flag_c.lock().unwrap() {
                let mut count = write_count_c.lock().unwrap();
                let tstamp = (1711333406_u64 + *count) * 1000000000;
                let random = rand::thread_rng().gen_range(0..32);
                let body = format!("ma,ta=a_{} fa={} {}", random, count, tstamp);

                loop {
                    let url = "http://127.0.0.1:8902/api/v1/write?db=chaos_test_db";
                    let resp = client.post(url, &body).unwrap();
                    if resp.status() == status_code::OK {
                        *count += 1;
                        break;
                    }
                    std::thread::sleep(std::time::Duration::from_millis(3));
                }
                std::thread::sleep(std::time::Duration::from_millis(3));
            }
        };

        let finish_flag_c = finish_flag.clone();
        let data_server_c = data_server.clone();
        let perform_actions = move || {
            while !*finish_flag_c.lock().unwrap() {
                let operation_count = 5;
                let random = rand::thread_rng().gen_range(0..100);
                if random % operation_count == 0 {
                    restart_datanode(data_server_c.clone());
                } else if random % operation_count == 1 {
                    move_vnode(meta_client.clone());
                } else if random % operation_count == 2 {
                    alter_replica();
                } else if random % operation_count == 3 {
                    alter_shard();
                } else if random % operation_count == 4 {
                    alter_vnode_duration();
                }

                std::thread::sleep(std::time::Duration::from_secs(1));
            }
        };

        let t_write_data = thread::spawn(write_data_action);
        std::thread::sleep(std::time::Duration::from_secs(3));

        let t_perform_action = thread::spawn(perform_actions);
        std::thread::sleep(std::time::Duration::from_secs(60));

        *finish_flag.lock().unwrap() = true;
        t_write_data.join().unwrap();
        t_perform_action.join().unwrap();
        std::thread::sleep(std::time::Duration::from_secs(3));

        let resp = client.post(SERVER_URL, "select count(*) from ma").unwrap();
        assert_eq!(resp.status(), status_code::OK);
        let actual = resp.text().unwrap();
        let expected = format!("{}", *write_count.lock().unwrap());
        println!("\nselect count(*): {}", actual);
        println!("expected: {}", expected);
        assert!(actual.contains(&expected));

        clean_env();
        println!("#### Test complete chaos_test_case_1 ####");
    }
}
