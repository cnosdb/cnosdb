#![cfg(feature = "meta_e2e_test")]

use std::path::PathBuf;
use std::process::{Command, Output};
use std::sync::Arc;
use std::{env, thread, time};

use meta::client;
use meta::store::command;
use metrics::metric_register::MetricsRegister;
use models::meta_data::NodeInfo;
use models::schema::tenant::{Tenant, TenantOptions};
use sysinfo::System;

const TENANT_NAME: &str = "test_add_tenant001";

#[cfg(feature = "meta_e2e_test")]
fn kill_cnosdb_meta_process(process_name: &str) {
    let system = System::new_all();
    for (pid, process) in system.processes() {
        if process.name() == process_name {
            println!("killing {pid}: {}", process.name().to_string_lossy());
            let output = Command::new("kill")
                .args(["-9", &(pid.to_string())])
                .output()
                .expect("failed to execute process");
            println!("kill -9 {pid}: {}", output.status);
        }
    }
}

#[cfg(feature = "meta_e2e_test")]
fn star_meta_cluster() {
    let crate_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let path = crate_dir.join("scripts").join("cluster.sh");
    let output = Command::new("bash")
        .arg(path)
        .output()
        .expect("failed to execute process");
    if !output.status.success() {
        panic!(
            "start meta cluster failed.\n## stdout\n{}\n## stderr\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }
}

#[cfg(feature = "meta_e2e_test")]
async fn write_data_to_meta() {
    use models::oid::UuidGenerator;

    let node = NodeInfo {
        id: 111,
        grpc_addr: "".to_string(),
    };
    let req = command::WriteCommand::AddDataNode("cluster_xxx".to_string(), node);
    let cli = client::MetaHttpClient::new("127.0.0.1:8901", Arc::new(MetricsRegister::default()));
    if let Err(e) = cli.write::<()>(&req).await {
        panic!("Failed to write cmd AddDataNode: {e}");
    }

    let oid = UuidGenerator::default().next_id();
    let tenant = Tenant::new(oid, TENANT_NAME.to_string(), TenantOptions::default());
    let req = command::WriteCommand::CreateTenant("cluster_xxx".to_string(), tenant);
    let cli = client::MetaHttpClient::new("127.0.0.1:8901", Arc::new(MetricsRegister::default()));
    if let Err(e) = cli.write::<()>(&req).await {
        panic!("Failed to write cmd CreateTenant: {e}");
    }
}

#[cfg(feature = "meta_e2e_test")]
async fn drop_data_from_meta() {
    let req = command::WriteCommand::DropTenant("cluster_xxx".to_string(), TENANT_NAME.to_string());
    let cli = client::MetaHttpClient::new("127.0.0.1:8901", Arc::new(MetricsRegister::default()));
    if let Err(e) = cli.write::<()>(&req).await {
        panic!("Failed to write DropTenant: {e}");
    }
}

#[cfg(feature = "meta_e2e_test")]
fn handle_curl_process(api: &str, output: Output) -> String {
    let stdout = String::from_utf8_lossy(&output.stdout);
    if !output.status.success() {
        panic!(
            "call api {api} failed.\n## stdout\n{stdout}\n## stderr\n{}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
    stdout.into_owned()
}

#[cfg(feature = "meta_e2e_test")]
fn backup(output: Option<String>) -> String {
    const URL: &str = "http://127.0.0.1:8901/dump";
    let output = match output {
        Some(path) => Command::new("curl")
            .args([URL, "-o", path.as_str()])
            .output(),
        None => Command::new("curl").arg(URL).output(),
    }
    .expect("failed to execute process");
    handle_curl_process(URL, output)
}

#[cfg(feature = "meta_e2e_test")]
fn meta_restore() -> String {
    const URL: &str = "http://127.0.0.1:8901/restore";
    let output = Command::new("curl")
        .args(["-XPOST", URL, "-d", "@/tmp/backup.json"])
        .output()
        .expect("failed to execute process");
    handle_curl_process(URL, output)
}

#[cfg(feature = "meta_e2e_test")]
fn check_meta_info(port: u16) -> String {
    let url = format!("http://127.0.0.1:{port}/debug");
    let output = Command::new("curl")
        .args([url.as_str()])
        .output()
        .expect("failed to execute process");
    handle_curl_process(&url, output)
}

#[cfg(feature = "meta_e2e_test")]
fn check_meta_info_metrics(port: u16) -> String {
    let url = format!("http://127.0.0.1:{port}/metrics");
    let output = Command::new("curl")
        .args([url.as_str()])
        .output()
        .expect("failed to execute process");
    handle_curl_process(&url, output)
}

#[cfg(feature = "meta_e2e_test")]
fn kill_from_cmd_part(cmd_part: String) {
    let list_cmd_str = format!("ps -ef | grep {cmd_part} | grep -v grep | awk '{{print $2}}'");
    let pro = Command::new("bash")
        .args(["-c", list_cmd_str.as_str()])
        .output()
        .expect("failed to execute process");
    println!("List processes: {:?}", pro.status.code());
    let pid = String::from_utf8_lossy(&pro.stdout);
    if pid.is_empty() {
        println!("No process found for {cmd_part}");
        return;
    }
    let kill_cmd_str = format!("kill -s KILL {pid}");
    let pro = Command::new("bash")
        .args(["-c", &kill_cmd_str])
        .output()
        .expect("failed to execute process");
    println!("Kill process: {:?}", pro.status.code());
    if !pro.status.success() {
        println!(
            "kill process {pid} failed.\n## stdout\n{}\n## stderr\n{}",
            String::from_utf8_lossy(&pro.stdout),
            String::from_utf8_lossy(&pro.stderr)
        );
        panic!("failed to start meta cluster");
    }
}
#[cfg(feature = "meta_e2e_test")]
#[cfg(test)]
mod tests {
    use super::*;

    fn script_file_path() -> PathBuf {
        let crate_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        crate_dir.join("tests").join("test_meta.sh")
    }

    #[tokio::test]
    async fn test_backup() {
        kill_cnosdb_meta_process("cnosdb-meta");
        star_meta_cluster();
        write_data_to_meta().await;
        let stdout = backup(None);
        assert!(stdout.contains(TENANT_NAME), "stdout: {stdout}");
        kill_cnosdb_meta_process("cnosdb-meta");
    }

    #[tokio::test]
    async fn test_restore() {
        kill_cnosdb_meta_process("cnosdb-meta");
        star_meta_cluster();
        write_data_to_meta().await;
        let _output = Command::new("curl")
            .args(["http://127.0.0.1:8901/dump", "-o", "/tmp/backup.json"])
            .output()
            .expect("failed to execute process");
        drop_data_from_meta().await;
        let stdout = check_meta_info(8901);
        assert!(!stdout.contains(TENANT_NAME));
        let _stdout = meta_restore();
        let stdout = check_meta_info(8901);
        assert!(stdout.contains(TENANT_NAME), "stdout: {stdout}");
        kill_cnosdb_meta_process("cnosdb-meta");
        // let stdout = backup(None);
        // assert_eq!(stdout.contains("
    }

    #[tokio::test]
    async fn test_node_replace() {
        kill_cnosdb_meta_process("cnosdb-meta");
        star_meta_cluster();
        // Kill node-3
        kill_from_cmd_part("cnosdb/meta/3".to_string());
        let stdout = check_meta_info_metrics(8901);
        assert!(stdout.contains("127.0.0.1:8921"));
        // Startup node-4, change cluster membership to [1,2,4]
        let output = Command::new("bash")
            .arg(script_file_path().as_os_str())
            .args(["add", "--node-id", "4", "--change-membership", "[1, 2, 4]"])
            .output()
            .expect("failed to execute process");
        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(
            output.status.success(),
            "stdout: {stdout}, stderr: {stderr}"
        );
        // get meta cluster info
        let stdout = check_meta_info_metrics(8901);
        assert!(
            stdout.contains("127.0.0.1:8931"),
            "stdout: {stdout}, stderr: {stderr}"
        );
        kill_cnosdb_meta_process("cnosdb-meta");
    }

    #[tokio::test]
    async fn test_add_meta_node() {
        // clean env
        kill_cnosdb_meta_process("cnosdb-meta");
        // start meta cluster
        star_meta_cluster();
        // Startup node-4, change cluster membership to [1,2,3,4]
        let output = Command::new("bash")
            .arg(script_file_path().as_os_str())
            .args([
                "add",
                "--node-id",
                "4",
                "--change-membership",
                "[1, 2, 3, 4]",
            ])
            .output()
            .expect("failed to execute process");
        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(
            output.status.success(),
            "stdout: {stdout}, stderr: {stderr}"
        );
        // get meta cluster info
        let stdout = check_meta_info_metrics(8921);
        assert!(
            stdout.contains("127.0.0.1:8931"),
            "stdout: {stdout}, stderr: {stderr}"
        );
        assert!(
            stdout.contains("127.0.0.1:8921"),
            "stdout: {stdout}, stderr: {stderr}"
        );
        assert!(
            stdout.contains("127.0.0.1:8911"),
            "stdout: {stdout}, stderr: {stderr}"
        );
        assert!(
            stdout.contains("127.0.0.1:8901"),
            "stdout: {stdout}, stderr: {stderr}"
        );
        // clean env
        kill_cnosdb_meta_process("cnosdb-meta");
    }

    #[tokio::test]
    async fn test_delete_meta_node() {
        // clean env
        kill_cnosdb_meta_process("cnosdb-meta");
        // start meta cluster
        star_meta_cluster();
        kill_from_cmd_part("cnosdb/meta/3".to_string());
        // Remove node-3, change cluster membership to [1,2]
        let output = Command::new("bash")
            .arg(script_file_path().as_os_str())
            .args(["change", "--change-membership", "[1, 2]"])
            .output()
            .expect("failed to execute process");
        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(
            output.status.success(),
            "stdout: {stdout}, stderr: {stderr}"
        );
        // get meta cluster info
        let stdout = check_meta_info_metrics(8901);
        assert!(
            !stdout.contains("127.0.0.1:8921"),
            "stdout: {stdout}, stderr: {stderr}"
        );
        assert!(
            stdout.contains("127.0.0.1:8911"),
            "stdout: {stdout}, stderr: {stderr}"
        );
        assert!(
            stdout.contains("127.0.0.1:8901"),
            "stdout: {stdout}, stderr: {stderr}"
        );
        // clean env
        kill_cnosdb_meta_process("cnosdb-meta");
    }

    #[tokio::test]
    async fn test_cluster_leader_election() {
        // clean env
        kill_cnosdb_meta_process("cnosdb-meta");
        // start meta cluster
        star_meta_cluster();
        thread::sleep(time::Duration::from_secs(3));
        let stdout = check_meta_info_metrics(8901);
        assert!(stdout.contains("Leader"), "stdout: {stdout}");
        assert!(stdout.contains("127.0.0.1:8901"), "stdout: {stdout}");
        assert!(stdout.contains("127.0.0.1:8911"), "stdout: {stdout}");
        assert!(stdout.contains("127.0.0.1:8921"), "stdout: {stdout}");
        // clean env
        kill_cnosdb_meta_process("cnosdb-meta");
    }

    #[tokio::test]
    async fn test_verify_log_replica() {
        // clean env
        kill_cnosdb_meta_process("cnosdb-meta");
        // start meta cluster
        star_meta_cluster();
        write_data_to_meta().await;
        // sleep 3 seconds
        thread::sleep(time::Duration::from_secs(3));
        // check fist meta node log
        let stdout = check_meta_info(8901);
        assert!(stdout.contains(TENANT_NAME), "stdout: {stdout}");
        // check second meta node log
        let stdout = check_meta_info(8911);
        assert!(stdout.contains(TENANT_NAME), "stdout: {stdout}");
        // check third meta node log
        let stdout = check_meta_info(8921);
        assert!(stdout.contains(TENANT_NAME), "stdout: {stdout}");
        // clean env
        kill_cnosdb_meta_process("cnosdb-meta");
    }

    #[tokio::test]
    async fn test_reselect_new_leader() {
        // clean env
        kill_cnosdb_meta_process("cnosdb-meta");
        // start meta cluster
        star_meta_cluster();
        // write data to meta
        // write_data_to_meta().await;
        kill_from_cmd_part("cnosdb/meta/1".to_string());
        thread::sleep(time::Duration::from_secs(30));
        // check first meta node log
        let stdout_node2 = check_meta_info_metrics(8911);
        let stdout_node3 = check_meta_info_metrics(8921);
        let stdout = stdout_node2 + &stdout_node3;
        assert!(stdout.contains("Leader"), "stdout: {stdout}");
        // clean env
        kill_cnosdb_meta_process("cnosdb-meta");
    }

    #[tokio::test]
    async fn test_cluster_scaling_log_replication() {
        // clean env
        kill_cnosdb_meta_process("cnosdb-meta");
        // start meta cluster
        star_meta_cluster();
        // write data to meta
        write_data_to_meta().await;
        // Startup node-4, change cluster membership to [1,2,4]
        let output = Command::new("bash")
            .arg(script_file_path().as_os_str())
            .args(["add", "--node-id", "4", "--change-membership", "[1, 2, 4]"])
            .output()
            .expect("failed to execute process");
        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(
            output.status.success(),
            "stdout: {stdout}, stderr: {stderr}"
        );
        // sleep 3 seconds
        thread::sleep(time::Duration::from_secs(3));
        // check first meta node log
        let stdout = check_meta_info(8931);
        assert!(stdout.contains(TENANT_NAME), "stdout: {stdout}");
        // clean env
        kill_cnosdb_meta_process("cnosdb-meta");
    }

    #[tokio::test]
    async fn test_repeat_request_log_replica() {
        // clean env
        kill_cnosdb_meta_process("cnosdb-meta");
        // start meta cluster
        star_meta_cluster();
        // write data to meta
        write_data_to_meta().await;
        // first check meta log
        let stdout = check_meta_info(8901);
        assert!(stdout.contains(TENANT_NAME), "stdout: {stdout}");
        // sleep 3 seconds
        thread::sleep(time::Duration::from_secs(3));
        // second check meta log
        let stdout = check_meta_info(8901);
        assert!(stdout.contains(TENANT_NAME), "stdout: {stdout}");
        // sleep 3 seconds
        thread::sleep(time::Duration::from_secs(3));
        // third check meta log
        let stdout = check_meta_info(8901);
        assert!(stdout.contains(TENANT_NAME), "stdout: {stdout}");
        // clean env
        kill_cnosdb_meta_process("cnosdb-meta");
    }

    #[tokio::test]
    async fn test_majority_failure() {
        // clean env
        kill_cnosdb_meta_process("cnosdb-meta");
        // start meta cluster
        star_meta_cluster();
        // write data to meta
        write_data_to_meta().await;
        // kill meta node 1
        kill_from_cmd_part("cnosdb/meta/1".to_string());
        // kill meta node 2
        kill_from_cmd_part("cnosdb/meta/2".to_string());
        // check meta node 3
        let _stdout = check_meta_info_metrics(8921);
        // clean env
        kill_cnosdb_meta_process("cnosdb-meta");
    }
}
