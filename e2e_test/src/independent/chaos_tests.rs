use std::sync::Arc;
use std::thread;

use http_protocol::status_code;
use meta::model::meta_admin::AdminMeta;
use meta::model::meta_tenant::TenantMeta;
use metrics::metric_register::MetricsRegister;
use parking_lot::Mutex;
use rand::Rng;

use crate::utils::global::E2eContext;
use crate::utils::{Client, CnosdbDataTestHelper};
use crate::{check_response, cluster_def};

fn time_str() -> String {
    let current_time = chrono::Local::now();

    current_time.format("%Y-%m-%dT%H:%M:%S").to_string()
}

fn restart_datanode(data_server: &mut CnosdbDataTestHelper) {
    println!("---------------------------------- Restart node...");
    let node_def = data_server.data_node_definitions[1].clone();
    data_server.restart_one_node(&node_def);
    std::thread::sleep(std::time::Duration::from_secs(3));
}

fn move_vnode(meta: Arc<TenantMeta>, server_url: &str) {
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
    client.post(server_url, &command).unwrap();
}

fn alter_replica(server_url: &str) {
    let mut rng = rand::thread_rng();
    let target_replica = rng.gen_range(1..=2);
    let command = format!("alter database chaos_test_db set replica {target_replica}");
    println!("------- Alter Replica Command: {}", command);
    let client = Client::with_auth("root".to_string(), Some(String::new()));
    client.post(server_url, &command).unwrap();
}

fn alter_shard(server_url: &str) {
    let mut rng = rand::thread_rng();
    let target_shard = rng.gen_range(1..=10);
    let command = format!("alter database chaos_test_db set shard {target_shard}");
    println!("------- Alter Shard Command: {}", command);
    let client = Client::with_auth("root".to_string(), Some(String::new()));
    client.post(server_url, &command).unwrap();
}

fn alter_vnode_duration(server_url: &str) {
    let mut rng = rand::thread_rng();
    let duration = rng.gen_range(1..=365);
    let command = format!("alter database chaos_test_db set vnode_duration '{duration}d'");
    println!("------- Alter Vnode Duration Command: {}", command);
    let client = Client::with_auth("root".to_string(), Some(String::new()));
    client.post(server_url, &command).unwrap();
}

#[test]
fn chaos_test_case_1() {
    println!("Test begin 'chaos_test_case_1'");
    let mut ctx = E2eContext::new("chaos_tests", "case_1");
    let mut executor = ctx.build_executor(cluster_def::one_meta_two_data_bundled());
    let host_port = executor.cluster_definition().data_cluster_def[0].http_host_port;
    let server_url = Arc::new(format!("http://{host_port}/api/v1/sql?db=chaos_test_db"));

    executor.startup();
    std::thread::sleep(std::time::Duration::from_secs(2));

    let config = executor.case_context().data().data_node_configs[0].clone();
    let meta_client = ctx.runtime().block_on(async move {
        let admin_meta = AdminMeta::new(config, Arc::new(MetricsRegister::default())).await;
        admin_meta.tenant_meta("cnosdb").await.unwrap()
    });

    let client = Client::with_auth("root".to_string(), Some(String::new()));
    check_response!(client.post(
        format!("http://{host_port}/api/v1/sql"),
        "CREATE DATABASE chaos_test_db WITH TTl '3560d' SHARD 1 VNOdE_DURATiON '1d' REPLICA 1 pRECISIOn 'ns';",
    ));
    std::thread::sleep(std::time::Duration::from_secs(2));

    let executor = Arc::new(Mutex::new(executor));

    let write_count = Arc::new(Mutex::new(0));
    let finish_flag = Arc::new(Mutex::new(false));

    let write_count_c = write_count.clone();
    let finish_flag_c = finish_flag.clone();
    let write_data_action = move || {
        let client = Client::with_auth("root".to_string(), Some(String::new()));
        let url = &format!("http://{host_port}/api/v1/write?db=chaos_test_db");
        while !*finish_flag_c.lock() {
            let mut count = write_count_c.lock();
            let tstamp = (1711333406_u64 + *count) * 1000000000;
            let random = rand::thread_rng().gen_range(0..32);
            let body = format!("ma,ta=a_{} fa={} {}", random, count, tstamp);

            loop {
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
    let executor_c = executor.clone();
    let server_url_c = server_url.clone();
    let perform_actions = move || {
        while !*finish_flag_c.lock() {
            let operation_count = 5;
            let random = rand::thread_rng().gen_range(0..100);
            if random % operation_count == 0 {
                println!("---------------------------------- Restart node...");
                let mut executor_cp = executor_c.lock();
                restart_datanode(executor_cp.case_context_mut().data_mut());
            } else if random % operation_count == 1 {
                move_vnode(meta_client.clone(), server_url_c.as_str());
            } else if random % operation_count == 2 {
                alter_replica(server_url_c.as_str());
            } else if random % operation_count == 3 {
                alter_shard(server_url_c.as_str());
            } else if random % operation_count == 4 {
                alter_vnode_duration(server_url_c.as_str());
            }

            std::thread::sleep(std::time::Duration::from_secs(1));
        }
    };

    let t_write_data = thread::spawn(write_data_action);
    std::thread::sleep(std::time::Duration::from_secs(3));

    let t_perform_action = thread::spawn(perform_actions);
    std::thread::sleep(std::time::Duration::from_secs(60));

    *finish_flag.lock() = true;
    t_write_data.join().unwrap();
    t_perform_action.join().unwrap();
    std::thread::sleep(std::time::Duration::from_secs(3));

    let resp = client
        .post(server_url.as_str(), "select count(*) from ma")
        .unwrap();
    assert_eq!(resp.status(), status_code::OK);
    let actual = resp.text().unwrap();
    let expected = format!("{}", *write_count.lock());
    println!("\nselect count(*): {}", actual);
    println!("expected: {}", expected);
    assert!(actual.contains(&expected));

    println!("#### Test complete chaos_test_case_1 ####");
}
