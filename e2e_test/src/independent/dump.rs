use std::ffi::OsStr;
use std::fs::File;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::thread::sleep;
use std::time::Duration;

use crate::utils::get_workspace_dir;
use crate::utils::global::E2eContext;
use crate::{cluster_def, defer};

fn exec_dump_cmd(port: u16, tenants: Option<Vec<&str>>, out: impl Into<PathBuf>) -> Child {
    let mut dump_cmd = Command::new("cargo");
    dump_cmd.args([
        "run",
        "--package",
        "client",
        "--",
        "--port",
        port.to_string().as_str(),
        "dump-ddl",
    ]);
    if let Some(tenants) = tenants {
        for tenant in tenants {
            dump_cmd.arg("-t").arg(tenant);
        }
    }
    let out = out.into();
    let file = File::create(out).unwrap();
    dump_cmd.stdout(file).spawn().unwrap()
}

fn exec_restore_cmd(port: u16, file: impl AsRef<OsStr>) -> Child {
    let mut restore_cmd = Command::new("cargo");
    restore_cmd.args([
        "run",
        "--package",
        "client",
        "--",
        "--error-stop",
        "--port",
        port.to_string().as_str(),
        "restore-dump-ddl",
    ]);
    restore_cmd.arg(file);
    restore_cmd.stdout(Stdio::null()).spawn().unwrap()
}

fn test_dump_impl(mut e2e_context: E2eContext, tenants: Option<Vec<&str>>, dump_file: &str) {
    let case_name = match tenants.clone() {
        Some(t) => "dump ".to_string() + t.join(" ").as_str(),
        None => "dump".to_string(),
    };
    println!("[{case_name}]: begin ...");

    // Test file `dump.sql` contains relative path, so we need to change current dir to workspace.
    let old_current_dir = std::env::current_dir().unwrap();
    std::env::set_current_dir(get_workspace_dir()).unwrap();
    defer! {
        std::env::set_current_dir(old_current_dir).unwrap();
    };

    println!("[{case_name}]: start cnosdb singleton");
    let mut executor = e2e_context.build_executor(cluster_def::one_data(1));
    let port = executor.cluster_definition().data_cluster_def[0]
        .http_host_port
        .port();
    executor.set_update_data_config_fn_vec(vec![Some(Box::new(|config| {
        config.global.store_metrics = false;
    }))]);

    executor.startup();

    sleep(Duration::from_secs(1));

    let test_dir = executor.case_context().test_dir();
    let data = executor.case_context().data();

    let dump_sql_path = data
        .workspace_dir
        .join("e2e_test")
        .join("test_data")
        .join(format!("{dump_file}.sql"));
    println!("dump_sql_path: {}", dump_sql_path.display());
    let dump_out_dir = test_dir.join("test_data");
    println!("dump_out_dir: {}", dump_out_dir.display());
    let dump_out_path = dump_out_dir.join(format!("{dump_file}.out"));
    println!("dump_out_path: {}", dump_out_path.display());

    std::fs::create_dir_all(&dump_out_dir).unwrap();

    println!("[{case_name}]: restore from '{}'", dump_sql_path.display());
    exec_restore_cmd(port, &dump_sql_path).wait().unwrap();

    println!("[{case_name}]: dump to '{}'", dump_out_path.display());
    exec_dump_cmd(port, tenants, &dump_out_path).wait().unwrap();

    println!("[{case_name}]: diff");
    let dump1_str = std::io::read_to_string(File::open(&dump_sql_path).unwrap()).unwrap();
    let dump2_str = std::io::read_to_string(File::open(&dump_out_path).unwrap()).unwrap();
    assert_eq!(dump1_str, dump2_str);
}

#[test]
fn test_dump() {
    let ctx = E2eContext::new("dump", "test_dump");
    test_dump_impl(ctx, None, "dump")
}

#[test]
fn test_dump_tenant() {
    let ctx = E2eContext::new("dump", "test_dump_tenant");
    test_dump_impl(ctx, Some(vec!["cnosdb"]), "dump_tenant")
}

#[test]
fn test_multi_dump_tenant() {
    let ctx = E2eContext::new("dump", "test_multi_dump_tenant");
    test_dump_impl(ctx, Some(vec!["test1", "test2"]), "dump_multi")
}
