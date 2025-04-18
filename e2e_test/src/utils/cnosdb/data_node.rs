use std::collections::HashMap;
use std::ffi::{OsStr, OsString};
use std::net::SocketAddrV4;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use config::tskv::Config as CnosdbConfig;

use super::FnUpdateCnosdbConfig;
use crate::cluster_def::{DataNodeDefinition, DeploymentMode};
use crate::utils::{kill_child_process, Client, PROFILE};

pub struct CnosdbDataTestHelper {
    pub workspace_dir: PathBuf,
    /// The data test dir, usually /e2e_test/$mod/$test/data
    pub test_dir: PathBuf,
    pub data_node_definitions: Vec<DataNodeDefinition>,
    pub data_node_configs: Vec<CnosdbConfig>,
    pub exe_path: PathBuf,

    pub data_node_clients: Vec<Arc<Client>>,
    /// Maps from server node-id to the (data_node_definitions index, (process, deployment mode)).
    pub sub_processes: HashMap<u8, (usize, (Child, DeploymentMode))>,
}

impl CnosdbDataTestHelper {
    pub fn new(
        workspace_dir: impl AsRef<Path>,
        test_dir: impl AsRef<Path>,
        data_node_definitions: Vec<DataNodeDefinition>,
        data_node_configs: Vec<CnosdbConfig>,
    ) -> Self {
        let workspace_dir = workspace_dir.as_ref().to_path_buf();
        let mut data_node_clients = Vec::with_capacity(data_node_configs.len());
        for config in data_node_configs.iter() {
            if config.security.tls_config.is_some() {
                // TODO(zipper): the client certificate should be customizable.
                let ca_crt_path = workspace_dir.join("config/resource/tls/ca.crt");
                let client: Arc<Client> = Arc::new(Client::with_auth_and_tls(
                    "root".to_string(),
                    Some(String::new()),
                    &ca_crt_path,
                ));
                data_node_clients.push(client);
            } else {
                // TODO(zipper): the user should be customizable.
                let client = Arc::new(Client::with_auth("root".to_string(), Some(String::new())));
                data_node_clients.push(client);
            }
        }
        Self {
            workspace_dir: workspace_dir.clone(),
            test_dir: test_dir.as_ref().join("data"),
            data_node_definitions,
            data_node_configs,
            exe_path: workspace_dir.join("target").join(PROFILE).join("cnosdb"),
            data_node_clients,
            sub_processes: HashMap::with_capacity(2),
        }
    }

    pub fn run(&mut self) {
        println!("Running cnosdb at '{}'", self.workspace_dir.display());
        let node_definitions = self.data_node_definitions.clone();

        #[cfg(feature = "startup_in_serial")]
        {
            // Start data nodes in serial
            for (i, (data_node_def, data_node_client)) in node_definitions
                .into_iter()
                .zip(self.data_node_clients.iter())
                .enumerate()
            {
                println!(" - cnosdb '{}' starting", &data_node_def.http_host_port);
                let proc = self.execute(&data_node_def);
                self.sub_processes
                    .insert(data_node_def.id, (i, (proc, data_node_def.mode)));

                let jh = self.wait_startup(
                    data_node_def.http_host_port,
                    data_node_def.enable_tls,
                    data_node_client.clone(),
                );
                jh.join().unwrap();
                println!(" - cnosdb '{}' started", data_node_def.http_host_port);
                thread::sleep(Duration::from_secs(1));
            }
        }
        #[cfg(not(feature = "startup_in_serial"))]
        {
            // Start data nodes in parallel
            let mut wait_startup_threads = Vec::with_capacity(self.data_node_definitions.len());
            for (i, (data_node_def, data_node_client)) in node_definitions
                .into_iter()
                .zip(self.data_node_clients.iter())
                .enumerate()
            {
                println!(" - cnosdb '{}' starting", &data_node_def.http_host_port);
                let proc = self.execute(&data_node_def);
                self.sub_processes
                    .insert(data_node_def.id, (i, (proc, data_node_def.mode)));

                let jh = self.wait_startup(
                    data_node_def.http_host_port,
                    data_node_def.enable_tls,
                    data_node_client.clone(),
                );
                wait_startup_threads.push((jh, data_node_def.http_host_port));
            }
            thread::sleep(Duration::from_secs(5));
            for (jh, addr) in wait_startup_threads {
                jh.join().unwrap();
                println!(" - cnosdb '{addr}' started");
            }
        }

        thread::sleep(Duration::from_secs(1));
    }

    /// Wait cnosdb startup by checking ping api in loop
    pub fn wait_startup(
        &self,
        addr: SocketAddrV4,
        enabled_tls: bool,
        client: Arc<Client>,
    ) -> thread::JoinHandle<()> {
        let ping_api = format!(
            "{}://{addr}/api/v1/ping",
            if enabled_tls { "https" } else { "http" }
        );
        let startup_time = std::time::Instant::now();
        thread::spawn(move || {
            let mut counter = 0;
            loop {
                thread::sleep(Duration::from_secs(3));
                if let Err(e) = client.get(&ping_api, "") {
                    println!(
                        "HTTP get '{ping_api}' failed after {} seconds: {}",
                        startup_time.elapsed().as_secs(),
                        e
                    );
                } else {
                    break;
                }
                counter += 1;
                if counter == 30 {
                    panic!("Test case failed, waiting too long for {addr} to startup");
                }
            }
        })
    }

    pub fn restart_one_node(&mut self, node_def: &DataNodeDefinition) {
        let (i, (proc, _)) = self
            .sub_processes
            .remove(&node_def.id)
            .unwrap_or_else(|| panic!("No data node created with id: {}", node_def.id));
        kill_child_process(proc, false);

        let new_proc = self.execute(node_def);
        self.sub_processes
            .insert(node_def.id, (i, (new_proc, node_def.mode)));

        let jh = self.wait_startup(
            node_def.http_host_port,
            node_def.enable_tls,
            self.data_node_clients[i].clone(),
        );
        jh.join().unwrap();
    }

    pub fn stop_one_node(&mut self, id: u8, force: bool) {
        let (_, (proc, _)) = self
            .sub_processes
            .remove(&id)
            .unwrap_or_else(|| panic!("No data node created with id: {id}"));
        kill_child_process(proc, force);
    }

    pub fn start_one_node(&mut self, data_node_index: usize) {
        let node_def = &self.data_node_definitions[data_node_index];
        let config_path = node_def.to_config_path(&self.test_dir);
        let new_proc = Command::new(&self.exe_path)
            .args([
                OsStr::new("run"),
                OsStr::new("--config"),
                config_path.as_os_str(),
                OsStr::new("-M"),
                &OsString::from(node_def.mode.to_string()),
            ])
            .stderr(Stdio::inherit())
            .stdout(Stdio::inherit())
            .spawn()
            .expect("failed to execute cnosdb");
        self.sub_processes
            .insert(node_def.id, (data_node_index, (new_proc, node_def.mode)));
        let jh = self.wait_startup(
            node_def.http_host_port,
            node_def.enable_tls,
            self.data_node_clients[data_node_index].clone(),
        );
        jh.join().unwrap();
    }

    fn execute(&self, node_def: &DataNodeDefinition) -> Child {
        let config_path = node_def.to_config_path(&self.test_dir);
        println!(
            "Executing {} run --config {} -M {}",
            self.exe_path.display(),
            config_path.display(),
            node_def.mode,
        );
        Command::new(&self.exe_path)
            .args([
                OsStr::new("run"),
                OsStr::new("--config"),
                config_path.as_os_str(),
                OsStr::new("-M"),
                OsString::from(node_def.mode.to_string()).as_os_str(),
            ])
            .stderr(Stdio::inherit())
            .stdout(Stdio::inherit())
            .spawn()
            .expect("failed to execute cnosdb")
    }
}

impl Drop for CnosdbDataTestHelper {
    fn drop(&mut self) {
        for (k, (i, (p, _))) in self.sub_processes.drain() {
            println!("Killing cnosdb ({k}, index={i}) sub_processes: {}", p.id());
            kill_child_process(p, true);
        }
    }
}

/// Build cnosdb config with paths:
///
/// - deployment_mode: singleton
/// - hh file: $test_dir/data/$id/hh
/// - log.level: INFO
/// - log.path: $test_dir/data/$id/log
/// - storage.path: $test_dir/data/$id/storage
/// - wal.path: $test_dir/data/$id/wal
pub fn build_data_node_config(test_dir: impl AsRef<Path>, id: u8) -> CnosdbConfig {
    let mut config = CnosdbConfig::default();
    config.deployment.mode = "singleton".to_string();
    let test_dir = test_dir.as_ref().display();
    let data_path = format!("{test_dir}/data/{id}");
    config.log.level = "INFO".to_string();
    config.log.path = format!("{data_path}/log");
    config.storage.path = format!("{data_path}/storage");
    config.wal.path = format!("{data_path}/wal");
    config.service.http_listen_port = Some(8902);
    config.service.grpc_listen_port = Some(8903);
    config.service.flight_rpc_listen_port = Some(8904);
    config.service.tcp_listen_port = Some(8905);

    config
}

/// Build cnosdb config with paths and write to test_dir.
/// Will be write to $test_dir/data/$id/config.toml.
pub fn write_data_node_config_files(
    test_dir: impl AsRef<Path>,
    data_node_definitions: &[DataNodeDefinition],
    regenerate: bool,
    regenerate_update_config: &[Option<FnUpdateCnosdbConfig>],
) -> Vec<CnosdbConfig> {
    let mut data_configs = Vec::with_capacity(data_node_definitions.len());
    let base_dir = test_dir.as_ref().join("data");
    for (i, data_node_def) in data_node_definitions.iter().enumerate() {
        let cnosdb_config_path = data_node_def.to_config_path(&base_dir);
        let cnosdb_dir = cnosdb_config_path.parent().unwrap();
        std::fs::create_dir_all(cnosdb_dir).unwrap();

        let mut cnosdb_config;
        if regenerate {
            // Generate config file.
            cnosdb_config = build_data_node_config(&test_dir, data_node_def.id);
            data_node_def.update_config(&mut cnosdb_config);
            if let Some(Some(f)) = regenerate_update_config.get(i) {
                f(&mut cnosdb_config);
            }
            std::fs::write(&cnosdb_config_path, cnosdb_config.to_string_pretty()).unwrap();
        } else {
            cnosdb_config = CnosdbConfig::new(Some(&cnosdb_config_path)).unwrap()
        }

        // If we do not make directory $storage.path, the data node seems to be sick by the meta node.
        // TODO(zipper): I think it's the data node who should do this job.
        if let Err(e) = std::fs::create_dir_all(&cnosdb_config.storage.path) {
            println!("Failed to pre-create $storage.path for data node: {e}");
        }
        data_configs.push(cnosdb_config);
    }
    data_configs
}
