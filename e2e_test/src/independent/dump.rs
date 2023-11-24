#[cfg(test)]
mod test {
    use std::ffi::OsStr;
    use std::fs;
    use std::fs::File;
    use std::path::PathBuf;
    use std::process::{Child, Command, Stdio};
    use std::thread::sleep;
    use std::time::Duration;

    use serial_test::serial;

    use crate::utils::{change_config_file, clean_env, start_singleton, workspace_dir};

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

    fn test_dump_impl(tenant: Option<&str>, dump_file: &str) {
        let case_name = match tenant {
            Some(t) => "dump ".to_string() + t,
            None => "dump".to_string(),
        };

        println!("Test {case_name}, begin ...");
        clean_env();

        println!("Test {case_name}, change_config_file");
        let dir = workspace_dir();
        let config_path = dir.join("config").join("config_8902.toml");
        let old_config = change_config_file(
            &config_path,
            vec![("store_metrics = true", "store_metrics = false")],
        );

        println!("Test {case_name}, start cnosdb");
        let old_path = std::env::current_dir().unwrap();
        std::env::set_current_dir(workspace_dir()).unwrap();
        let mut data = start_singleton(None::<PathBuf>, "config_8902.toml", "127.0.0.1:8902");
        sleep(Duration::from_secs(1));
        println!("Test {case_name}, restore");
        let dump_sql = data
            .workspace
            .join(format!("e2e_test/test_data/{dump_file}.sql"));
        let dump_out = data
            .workspace
            .join(format!("e2e_test/test_data/{dump_file}.out"));

        exec_restore_cmd(&dump_sql).wait().unwrap();
        println!("Test {case_name}, dump");
        exec_dump_cmd(tenant, &dump_out).wait().unwrap();
        println!("Test {case_name}, diff");
        let dump1_str = std::io::read_to_string(File::open(&dump_sql).unwrap()).unwrap();
        let dump2_str = std::io::read_to_string(File::open(&dump_out).unwrap()).unwrap();
        assert_eq!(dump1_str, dump2_str);
        fs::remove_file(dump_out).unwrap();
        data.kill_process("config_8902.toml");
        fs::write(&config_path, old_config).unwrap();
        clean_env();
        std::env::set_current_dir(old_path).unwrap();
    }

    #[test]
    #[serial]
    fn test_dump() {
        test_dump_impl(None, "dump")
    }
    #[test]
    #[serial]
    fn test_dump_tenant() {
        test_dump_impl(Some("cnosdb"), "dump_tenant")
    }
}
