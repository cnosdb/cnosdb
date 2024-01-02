#![cfg(test)]

use std::ffi::OsStr;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::thread::sleep;
use std::time::Duration;

use serial_test::serial;

use crate::utils::{build_data_node_config, get_workspace_dir, kill_all, run_singleton};
use crate::{cluster_def, defer};

fn exec_dump_cmd(tenant: Option<&str>, out: impl Into<PathBuf>) -> Child {
    let mut dump_cmd = Command::new("cargo");
    dump_cmd.args(["run", "--package", "client", "--", "dump-ddl"]);
    if let Some(t) = tenant {
        dump_cmd.args(["--tenant", t]);
    }
    let out = out.into();
    let file = File::create(out).unwrap();
    dump_cmd.stdout(file).spawn().unwrap()
}

fn exec_restore_cmd(file: impl AsRef<OsStr>) -> Child {
    let mut restore_cmd = Command::new("cargo");
    restore_cmd.args([
        "run",
        "--package",
        "client",
        "--",
        "--error-stop",
        "restore-dump-ddl",
    ]);
    restore_cmd.arg(file);
    restore_cmd.stdout(Stdio::null()).spawn().unwrap()
}

fn test_dump_impl(test_dir: &str, tenant: Option<&str>, dump_file: &str) {
    let case_name = match tenant {
        Some(t) => "dump ".to_string() + t,
        None => "dump".to_string(),
    };
    println!("[{case_name}]: begin ...");

    kill_all();

    let test_dir = Path::new(test_dir);
    let _ = std::fs::remove_dir_all(test_dir);
    std::fs::create_dir_all(test_dir).unwrap();
    let cnosdb_dir = test_dir.join("data");

    let data_node_def = &cluster_def::one_data(1);

    let mut config = build_data_node_config(test_dir, &data_node_def.config_file_name);
    data_node_def.update_config(&mut config);
    config.global.store_metrics = false;

    let config_dir = cnosdb_dir.join("config");
    std::fs::create_dir_all(&config_dir).unwrap();
    let config_file_path = config_dir.join(&data_node_def.config_file_name);
    println!(
        "[{case_name}]: saving new config_file to '{}'.",
        config_file_path.display()
    );
    std::fs::write(&config_file_path, config.to_string_pretty()).unwrap();
    println!(
        "[{case_name}]: saved new config_file to '{}'.",
        config_file_path.display()
    );

    let old_path = std::env::current_dir().unwrap();
    std::env::set_current_dir(get_workspace_dir()).unwrap();
    defer! {
        std::env::set_current_dir(old_path).unwrap();
    };

    println!("[{case_name}]: start cnosdb singleton");
    let data = run_singleton(test_dir, data_node_def, false, false);
    sleep(Duration::from_secs(1));

    let dump_sql_path = data
        .workspace_dir
        .join("e2e_test")
        .join("test_data")
        .join(format!("{dump_file}.sql"));
    let dump_out_dir = test_dir.join("test_data");
    std::fs::create_dir_all(&dump_out_dir).unwrap();
    let dump_out_path = dump_out_dir.join(format!("{dump_file}.out"));

    println!("[{case_name}]: restore");
    exec_restore_cmd(&dump_sql_path).wait().unwrap();

    println!("[{case_name}]: dump");
    exec_dump_cmd(tenant, &dump_out_path).wait().unwrap();

    println!("[{case_name}]: diff");
    let dump1_str = std::io::read_to_string(File::open(&dump_sql_path).unwrap()).unwrap();
    let dump2_str = std::io::read_to_string(File::open(&dump_out_path).unwrap()).unwrap();
    assert_eq!(dump1_str, dump2_str);
}

#[test]
#[serial]
fn test_dump() {
    test_dump_impl("/tmp/e2e_test/dump/test_dump", None, "dump")
}

#[test]
#[serial]
fn test_dump_tenant() {
    test_dump_impl(
        "/tmp/e2e_test/dump/test_dump_tenant",
        Some("cnosdb"),
        "dump_tenant",
    )
}
