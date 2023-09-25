// mod test_cluster;
#![cfg(feature = "meta_e2e_test")]
use std::process::Command;
use std::sync::Arc;
use std::{env, thread, time};

// use std::time;
use meta::{client, store::command};
// use meta::store::command::*;
// use meta::client::*;
use models::{meta_data::NodeAttribute, meta_data::NodeInfo, schema::Tenant};
use openraft::Config;
use sysinfo::{ProcessExt, System, SystemExt};

#[cfg(feature = "meta_e2e_test")]
#[cfg(test)]
mod tests {
    // use std::{process::Output, thread};

    // use meta::model::TenantManager;

    use super::*;
    #[tokio::test]
    async fn test_backup() {
        kill_cnosdb_meta_process("cnosdb-meta");
        let op = star_meta_cluster();
        assert_eq!(op.status.code(), std::option::Option::Some(0));
        write_data_to_meta().await;
        let output = backup();
        assert_eq!(output.status.code(), std::option::Option::Some(0));
        let stdout = String::from_utf8(output.stdout).unwrap();
        assert!(stdout.contains("127.0.0.1:8888"));
        assert!(stdout.contains("test_add_tenant001"));
        // print!("output: {:?}", output.stdout);
        kill_cnosdb_meta_process("cnosdb-meta");
    }

    #[tokio::test]
    async fn test_restore() {
        kill_cnosdb_meta_process("cnosdb-meta");
        let op = star_meta_cluster();
        assert_eq!(op.status.code(), std::option::Option::Some(0));
        write_data_to_meta().await;
        let _output = Command::new("curl")
            .args(["http://127.0.0.1:8901/dump", "-o", "/tmp/backup.json"])
            .output()
            .expect("failed to execute process");
        drop_data_from_meta().await;
        let output = check_meta_info("8901".to_string());
        assert_eq!(output.status.code(), std::option::Option::Some(0));
        let stdout = String::from_utf8(output.stdout).unwrap();
        assert!(!stdout.contains("test_add_tenant001"));
        let output = meta_restore();
        assert_eq!(output.status.code(), std::option::Option::Some(0));
        let output = check_meta_info("8901".to_string());
        let stdout = String::from_utf8(output.stdout).unwrap();
        print!("output: {:?}", stdout);
        assert!(stdout.contains("test_add_tenant001"));
        kill_cnosdb_meta_process("cnosdb-meta");
        // let output = backup();
        // assert_eq!(output.status.code(), std::option::Option::Some(0));
        // let stdout = String::from_utf8(output.stdout).unwrap();
        // assert_eq!(stdout.contains("
    }

    #[tokio::test]
    async fn test_node_replace() {
        kill_cnosdb_meta_process("cnosdb-meta");
        let op = star_meta_cluster();
        assert_eq!(op.status.code(), std::option::Option::Some(0));
        let output = kill_from_part_str("config_8921.toml".to_string());
        // println!("pid: {:?}", pid);
        assert_eq!(output.status.code(), std::option::Option::Some(0));
        let output = check_meta_info_metrics("8901".to_string());
        assert_eq!(output.status.code(), std::option::Option::Some(0));
        let stdout = String::from_utf8(output.stdout).unwrap();
        assert!(stdout.contains("127.0.0.1:8921"));
        let res = env::current_dir().unwrap();
        let res_str: String = res.into_os_string().into_string().unwrap();
        let path = res_str + "/start_meta.sh";
        // println!("path: {}", path);
        let output = Command::new("bash")
            .arg(path)
            .output()
            .expect("failed to execute process");
        assert_eq!(output.status.code(), std::option::Option::Some(0));
        // println!("output: {:?}", output.stdout);
        let output = check_meta_info_metrics("8901".to_string());
        assert_eq!(output.status.code(), std::option::Option::Some(0));
        let stdout = String::from_utf8(output.stdout).unwrap();
        assert!(stdout.contains("127.0.0.1:8931"));
        kill_cnosdb_meta_process("cnosdb-meta");
    }

    #[tokio::test]
    async fn test_add_meta_node() {
        // clean env
        kill_cnosdb_meta_process("cnosdb-meta");
        // start meta cluster
        let op = star_meta_cluster();
        assert_eq!(op.status.code(), std::option::Option::Some(0));
        // get current dir
        let res = env::current_dir().unwrap();
        let res_str: String = res.into_os_string().into_string().unwrap();
        let path = res_str + "/start_meta.sh";
        println!("path: {}", path);
        let output = Command::new("bash")
            .args([path, std::string::String::from("add")])
            .output()
            .expect("failed to execute process");
        println!("output: {:?}", output);
        assert_eq!(output.status.code(), std::option::Option::Some(0));
        // get meta cluster info
        let output = check_meta_info_metrics("8921".to_string());
        assert_eq!(output.status.code(), std::option::Option::Some(0));
        let stdout = String::from_utf8(output.stdout).unwrap();
        println!("output: {:?}", stdout);
        assert!(stdout.contains("127.0.0.1:8931"));
        assert!(stdout.contains("127.0.0.1:8921"));
        assert!(stdout.contains("127.0.0.1:8911"));
        assert!(stdout.contains("127.0.0.1:8901"));
        // clean env
        kill_cnosdb_meta_process("cnosdb-meta");
    }

    #[tokio::test]
    async fn test_delete_meta_node() {
        // clean env
        kill_cnosdb_meta_process("cnosdb-meta");
        // start meta cluster
        let op = star_meta_cluster();
        assert_eq!(op.status.code(), std::option::Option::Some(0));
        let output = kill_from_part_str("config_8921.toml".to_string());
        assert_eq!(output.status.code(), std::option::Option::Some(0));
        // get current dir
        let res = env::current_dir().unwrap();
        let res_str: String = res.into_os_string().into_string().unwrap();
        let path = res_str + "/start_meta.sh";
        println!("path: {}", path);
        let output = Command::new("bash")
            .args([path, std::string::String::from("delete")])
            .output()
            .expect("failed to execute process");
        println!("output: {:?}", output);
        assert_eq!(output.status.code(), std::option::Option::Some(0));
        // get meta cluster info
        let output = check_meta_info_metrics("8901".to_string());
        assert_eq!(output.status.code(), std::option::Option::Some(0));
        let stdout = String::from_utf8(output.stdout).unwrap();
        println!("output: {:?}", stdout);
        assert!(!stdout.contains("127.0.0.1:8921"));
        assert!(stdout.contains("127.0.0.1:8911"));
        assert!(stdout.contains("127.0.0.1:8901"));
        // clean env
        kill_cnosdb_meta_process("cnosdb-meta");
    }

    #[tokio::test]
    async fn test_cluster_leader_election() {
        // clean env
        kill_cnosdb_meta_process("cnosdb-meta");
        // start meta cluster
        let op = star_meta_cluster();
        assert_eq!(op.status.code(), std::option::Option::Some(0));
        thread::sleep(time::Duration::from_secs(3));
        let output = check_meta_info_metrics("8901".to_string());
        assert_eq!(output.status.code(), std::option::Option::Some(0));
        let stdout = String::from_utf8(output.stdout).unwrap();
        assert!(stdout.contains("Leader"));
        assert!(stdout.contains("127.0.0.1:8901"));
        assert!(stdout.contains("127.0.0.1:8911"));
        assert!(stdout.contains("127.0.0.1:8921"));
        // clean env
        kill_cnosdb_meta_process("cnosdb-meta");
    }

    #[tokio::test]
    async fn test_verify_log_replica() {
        // clean env
        kill_cnosdb_meta_process("cnosdb-meta");
        // start meta cluster
        let op = star_meta_cluster();
        assert_eq!(op.status.code(), std::option::Option::Some(0));
        write_data_to_meta().await;
        // sleep 3 seconds
        thread::sleep(time::Duration::from_secs(3));
        // check fist meta node log
        let output = check_meta_info("8901".to_string());
        assert_eq!(output.status.code(), std::option::Option::Some(0));
        let stdout = String::from_utf8(output.stdout).unwrap();
        assert!(stdout.contains("127.0.0.1:8888"));
        assert!(stdout.contains("test_add_tenant001"));
        // check second meta node log
        let output = check_meta_info("8911".to_string());
        assert_eq!(output.status.code(), std::option::Option::Some(0));
        let stdout = String::from_utf8(output.stdout).unwrap();
        assert!(stdout.contains("127.0.0.1:8888"));
        assert!(stdout.contains("test_add_tenant001"));
        // check third meta node log
        let output = check_meta_info("8921".to_string());
        assert_eq!(output.status.code(), std::option::Option::Some(0));
        let stdout = String::from_utf8(output.stdout).unwrap();
        assert!(stdout.contains("127.0.0.1:8888"));
        assert!(stdout.contains("test_add_tenant001"));
        // clean env
        kill_cnosdb_meta_process("cnosdb-meta");
    }

    #[tokio::test]
    async fn test_reselect_new_leader() {
        // clean env
        kill_cnosdb_meta_process("cnosdb-meta");
        // start meta cluster
        let op = star_meta_cluster();
        assert_eq!(op.status.code(), std::option::Option::Some(0));
        // write data to meta
        // write_data_to_meta().await;
        let output = kill_from_part_str("config_8901.toml".to_string());
        assert_eq!(output.status.code(), std::option::Option::Some(0));
        // sleep 3 seconds
        thread::sleep(time::Duration::from_secs(10));
        // check first meta node log
        let output = check_meta_info_metrics("8901".to_string());
        assert_ne!(output.status.code(), std::option::Option::Some(0));
        let output = check_meta_info_metrics("8911".to_string());
        assert_eq!(output.status.code(), std::option::Option::Some(0));
        let stdout_node2 = String::from_utf8(output.stdout).unwrap();
        let output = check_meta_info_metrics("8921".to_string());
        assert_eq!(output.status.code(), std::option::Option::Some(0));
        let stdout_node3 = String::from_utf8(output.stdout).unwrap();
        let stdout = stdout_node2 + &stdout_node3;
        println!("stdout: {}", stdout);
        assert!(stdout.contains("Leader"));
        // clean env
        kill_cnosdb_meta_process("cnosdb-meta");
    }

    #[tokio::test]
    async fn test_cluster_scaling_log_replication() {
        // clean env
        kill_cnosdb_meta_process("cnosdb-meta");
        // start meta cluster
        let op = star_meta_cluster();
        assert_eq!(op.status.code(), std::option::Option::Some(0));
        // write data to meta
        write_data_to_meta().await;
        // get current dir
        let res = env::current_dir().unwrap();
        let res_str: String = res.into_os_string().into_string().unwrap();
        let path = res_str + "/start_meta.sh";
        println!("path: {}", path);
        let output = Command::new("bash")
            .args([path, std::string::String::from("add")])
            .output()
            .expect("failed to execute process");
        println!("output: {:?}", output);
        assert_eq!(output.status.code(), std::option::Option::Some(0));
        // sleep 3 seconds
        thread::sleep(time::Duration::from_secs(3));
        // check first meta node log
        let output = check_meta_info("8931".to_string());
        assert_eq!(output.status.code(), std::option::Option::Some(0));
        let stdout = String::from_utf8(output.stdout).unwrap();
        assert!(stdout.contains("127.0.0.1:8888"));
        assert!(stdout.contains("test_add_tenant001"));
        // clean env
        kill_cnosdb_meta_process("cnosdb-meta");
    }

    #[tokio::test]
    async fn test_repeat_request_log_replica() {
        // clean env
        kill_cnosdb_meta_process("cnosdb-meta");
        // start meta cluster
        let op = star_meta_cluster();
        assert_eq!(op.status.code(), std::option::Option::Some(0));
        // write data to meta
        write_data_to_meta().await;
        // first ckeck meta log
        let output = check_meta_info("8901".to_string());
        assert_eq!(output.status.code(), std::option::Option::Some(0));
        let stdout = String::from_utf8(output.stdout).unwrap();
        assert!(stdout.contains("127.0.0.1:8888"));
        assert!(stdout.contains("test_add_tenant001"));
        // sleep 3 seconds
        thread::sleep(time::Duration::from_secs(3));
        // second ckeck meta log
        let output = check_meta_info("8901".to_string());
        assert_eq!(output.status.code(), std::option::Option::Some(0));
        let stdout = String::from_utf8(output.stdout).unwrap();
        assert!(stdout.contains("127.0.0.1:8888"));
        assert!(stdout.contains("test_add_tenant001"));
        // sleep 3 seconds
        thread::sleep(time::Duration::from_secs(3));
        // third ckeck meta log
        let output = check_meta_info("8901".to_string());
        assert_eq!(output.status.code(), std::option::Option::Some(0));
        let stdout = String::from_utf8(output.stdout).unwrap();
        assert!(stdout.contains("127.0.0.1:8888"));
        assert!(stdout.contains("test_add_tenant001"));
        // clean env
        kill_cnosdb_meta_process("cnosdb-meta");
    }

    #[tokio::test]
    async fn test_majority_failure() {
        // clean env
        kill_cnosdb_meta_process("cnosdb-meta");
        // start meta cluster
        let op = star_meta_cluster();
        assert_eq!(op.status.code(), std::option::Option::Some(0));
        // write data to meta
        write_data_to_meta().await;
        // kill meta node 1
        let output = kill_from_part_str("config_8901.toml".to_string());
        assert_eq!(output.status.code(), std::option::Option::Some(0));
        // kill meta node 2
        let output = kill_from_part_str("config_8911.toml".to_string());
        assert_eq!(output.status.code(), std::option::Option::Some(0));
        // check meta node 3
        let output = check_meta_info_metrics("port_8921".to_string());
        assert_ne!(output.status.code(), std::option::Option::Some(0));
        // clean env
        kill_cnosdb_meta_process("cnosdb-meta");
    }
}
// fn main() {
//     println!("Hello, world!");
//     let pid = get_meta_node_pid();
//     println!("pid: {:?}", pid);
// }
#[cfg(feature = "meta_e2e_test")]
fn kill_cnosdb_meta_process(process_name: &str) {
    let system = System::new_all();
    let process_name = process_name;
    for (pid, process) in system.processes() {
        if process.name() == process_name {
            println!("{}: {}", pid, process.name());
            let output = Command::new("kill")
                .args(["-9", &(pid.to_string())])
                .output()
                .expect("failed to execute process");
            println!("status: {}", output.status);
        }
    }
}
#[cfg(feature = "meta_e2e_test")]
fn star_meta_cluster() -> std::process::Output {
    let res = env::current_dir().unwrap();
    let res_str: String = res.into_os_string().into_string().unwrap();
    let path = res_str + "/cluster.sh";
    // println!("path: {}", path);
    let output = Command::new("bash")
        .arg(path)
        .output()
        .expect("failed to execute process");
    output
}

#[cfg(feature = "meta_e2e_test")]
async fn write_data_to_meta() {
    use models::oid::UuidGenerator;

    let node = NodeInfo {
        id: 111,
        grpc_addr: "".to_string(),
        http_addr: "127.0.0.1:8888".to_string(),
        attribute: NodeAttribute::Hot,
    };
    let req = command::WriteCommand::AddDataNode("cluster_xxx".to_string(), node);
    let cli = client::MetaHttpClient::new("127.0.0.1:8901");
    cli.write::<()>(&req).await.unwrap();
    // let req = command::WriteCommand::CreateTenant(“cluster_xxx”.to_string(), (), ())
    let oid = UuidGenerator::default().next_id();
    let tenant = Tenant::new(
        oid,
        "test_add_tenant001".to_string(),
        models::schema::TenantOptions::default(),
    );
    let req = command::WriteCommand::CreateTenant("cluster_xxx".to_string(), tenant);
    let cli = client::MetaHttpClient::new("127.0.0.1:8901");
    cli.write::<()>(&req).await.unwrap();
}
#[cfg(feature = "meta_e2e_test")]
async fn drop_data_from_meta() {
    let req = command::WriteCommand::DropTenant(
        "cluster_xxx".to_string(),
        "test_add_tenant001".to_string(),
    );
    let cli = client::MetaHttpClient::new("127.0.0.1:8901");
    cli.write::<()>(&req).await.unwrap();
}
#[cfg(feature = "meta_e2e_test")]
fn backup() -> std::process::Output {
    let output = Command::new("curl")
        .args(["http://127.0.0.1:8901/dump"])
        .output()
        .expect("failed to execute process");
    println!("status: {}", output.status);
    output
}
#[cfg(feature = "meta_e2e_test")]
fn meta_restore() -> std::process::Output {
    let output = Command::new("curl")
        .args([
            "-XPOST",
            "http://127.0.0.1:8901/restore",
            "-d",
            "@/tmp/backup.json",
        ])
        .output()
        .expect("failed to execute process");
    println!("status: {}", output.status);
    output
}
#[cfg(feature = "meta_e2e_test")]
fn check_meta_info(port: String) -> std::process::Output {
    let url = format!("http://127.0.0.1:{}/debug", port);
    let output = Command::new("curl")
        .args([url.as_str()])
        .output()
        .expect("failed to execute process");
    println!("status: {}", output.status);
    output
}
#[cfg(feature = "meta_e2e_test")]
fn check_meta_info_metrics(port: String) -> std::process::Output {
    let url = format!("http://127.0.0.1:{}/metrics", port);
    let output = Command::new("curl")
        .args([url.as_str()])
        .output()
        .expect("failed to execute process");
    println!("status: {}", output.status);
    output
}
#[cfg(feature = "meta_e2e_test")]
fn kill_from_part_str(part_str: String) -> std::process::Output {
    // let full_str = format!("ps -ef | grep {} | grep -v grep | awk '{print $2}'", part_str);
    let full_str: String =
        "ps -ef | grep ".to_string() + &part_str + "| grep -v grep | awk '{print $2}'";
    let pro = Command::new("bash")
        .args(["-c", full_str.as_str()])
        .output()
        .expect("failed to execute process");
    println!("status: {:?}", pro);
    let pid = String::from_utf8(pro.stdout).unwrap();
    // let kill = "kill -9 ".to_string() + pid;
    let kill = "kill -9 ".to_string() + &pid;
    let pro = Command::new("bash")
        .args(["-c", &kill])
        .output()
        .expect("failed to execute process");
    println!("status: {:?}", pro);
    pro
}
