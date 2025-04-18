mod data_node;
mod meta_node;

use std::path::Path;
use std::process::Command;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use config::meta::Opt as MetaStoreConfig;
use config::tskv::Config as CnosdbConfig;
pub use data_node::*;
pub use meta_node::*;
use tokio::runtime::Runtime;

use crate::cluster_def::CnosdbClusterDefinition;
use crate::utils::{execute_command, get_workspace_dir, kill_process};

pub type FnUpdateMetaStoreConfig = Box<dyn Fn(&mut MetaStoreConfig) + Send>;
pub type FnUpdateCnosdbConfig = Box<dyn Fn(&mut CnosdbConfig) + Send>;

pub fn cargo_build_cnosdb_meta(workspace_dir: impl AsRef<Path>) {
    let workspace_dir = workspace_dir.as_ref();
    println!("- Building 'meta' at '{}'", workspace_dir.display());
    let mut cargo_build = Command::new("cargo");

    let mut build_args = vec!["build", "--package", "meta", "--bin", "cnosdb-meta"];
    #[cfg(not(feature = "debug_mode"))]
    build_args.push("--release");

    cargo_build.current_dir(workspace_dir).args(build_args);
    execute_command(cargo_build).expect("Failed to build cnosdb-meta");
    println!("- Build 'meta' at '{}' completed", workspace_dir.display());
}

pub fn cargo_build_cnosdb_data(workspace_dir: impl AsRef<Path>) {
    let workspace_dir = workspace_dir.as_ref();
    println!("Building 'main' at '{}'", workspace_dir.display());
    let mut cargo_build = Command::new("cargo");

    let mut build_args = vec!["build", "--package", "main", "--bin", "cnosdb"];
    #[cfg(not(feature = "debug_mode"))]
    build_args.push("--release");

    cargo_build.current_dir(workspace_dir).args(build_args);
    execute_command(cargo_build).expect("Failed to build cnosdb");
    println!("Build 'main' at '{}' completed", workspace_dir.display());
}

/// Run CnosDB cluster.
///
/// - Meta server directory: $test_dir/meta
/// - Data server directory: $test_dir/data
///
/// # Arguments
/// - generate_meta_config: If true, regenerate meta node config files.
/// - generate_data_config: If true, regenerate data node config files.
pub fn run_cluster(
    test_dir: impl AsRef<Path>,
    runtime: Arc<Runtime>,
    cluster_def: &CnosdbClusterDefinition,
    generate_meta_config: bool,
    generate_data_config: bool,
) -> (Option<CnosdbMetaTestHelper>, Option<CnosdbDataTestHelper>) {
    run_cluster_with_customized_configs(
        test_dir,
        runtime,
        cluster_def,
        generate_meta_config,
        generate_data_config,
        &[],
        &[],
    )
}

/// Run CnosDB cluster with customized configs.
///
/// # Arguments
/// - runtime: If None and need meta nodes in cluster, build a new runtime.
/// - generate_meta_config: If true, regenerate meta node config files.
/// - generate_data_config: If true, regenerate data node config files.
/// - regenerate_update_meta_config: If generate_meta_config is true, and the `ith` optional closure is Some(Fn),
///   alter the default config of the `ith` meta node by the Fn. (Do not change service ports.)
/// - regenerate_update_meta_config: If generate_data_config is true, and the `ith` optional closure is Some(Fn),
///   alter the default config of the `ith` data node by the Fn. (Do not change service ports.)
pub fn run_cluster_with_customized_configs(
    test_dir: impl AsRef<Path>,
    runtime: Arc<Runtime>,
    cluster_def: &CnosdbClusterDefinition,
    generate_meta_config: bool,
    generate_data_config: bool,
    regenerate_update_meta_config: &[Option<FnUpdateMetaStoreConfig>],
    regenerate_update_data_config: &[Option<FnUpdateCnosdbConfig>],
) -> (Option<CnosdbMetaTestHelper>, Option<CnosdbDataTestHelper>) {
    let workspace_dir = get_workspace_dir();
    if !cluster_def.meta_cluster_def.is_empty() {
        cargo_build_cnosdb_meta(&workspace_dir);
    }
    if !cluster_def.data_cluster_def.is_empty() {
        cargo_build_cnosdb_data(&workspace_dir);
    }

    let (mut meta_test_helper, mut data_test_helper) = (
        Option::<CnosdbMetaTestHelper>::None,
        Option::<CnosdbDataTestHelper>::None,
    );

    if !cluster_def.meta_cluster_def.is_empty() {
        // If need to run `cnosdb-meta`
        let configs = write_meta_node_config_files(
            &test_dir,
            &cluster_def.meta_cluster_def,
            generate_meta_config,
            regenerate_update_meta_config,
        );
        let mut meta = CnosdbMetaTestHelper::new(
            runtime,
            &workspace_dir,
            &test_dir,
            cluster_def.meta_cluster_def.clone(),
            configs,
        );
        if cluster_def.meta_cluster_def.len() == 1 {
            meta.run_single_meta();
        } else {
            meta.run_cluster();
        }
        meta_test_helper = Some(meta);
    }

    thread::sleep(Duration::from_secs(1));

    if !cluster_def.data_cluster_def.is_empty() {
        // If need to run `cnosdb run`
        let configs = write_data_node_config_files(
            &test_dir,
            &cluster_def.data_cluster_def,
            generate_data_config,
            regenerate_update_data_config,
        );
        let mut data = CnosdbDataTestHelper::new(
            workspace_dir,
            &test_dir,
            cluster_def.data_cluster_def.clone(),
            configs,
        );
        data.run();
        data_test_helper = Some(data);
    }

    (meta_test_helper, data_test_helper)
}

/// Kill all 'cnosdb' and 'cnosdb-meta' process with signal 'KILL(9)'.
#[allow(unused)]
pub fn kill_all() {
    println!("Killing all test processes...");
    kill_process("cnosdb");
    kill_process("cnosdb-meta");
    println!("Killed all test processes.");
}
