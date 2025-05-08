#[cfg(test)]
mod tests {
    use std::path::{Path, PathBuf};
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use config::tskv::{Config, MetaConfig};
    use memory_pool::GreedyMemoryPool;
    use meta::model::meta_admin::AdminMeta;
    use metrics::metric_register::MetricsRegister;
    use models::meta_data::VnodeId;
    use models::schema::database_schema::make_owner;
    use models::schema::tenant::TenantOptions;
    use protos::kv_service::{raft_write_command, WriteDataRequest};
    use protos::models_helper;
    use serial_test::serial;
    use sysinfo::{ProcessRefreshKind, RefreshKind, System};
    use tokio::runtime;
    use tokio::runtime::Runtime;
    use trace::global_logging::init_default_global_tracing;
    use trace::{debug, error, info, warn};
    use tskv::file_system::async_filesystem::LocalFileSystem;
    use tskv::file_system::FileSystem;
    use tskv::{file_utils, kv_option, Engine, TsKv};
    use utils::precision::Precision;

    /// Initializes a TsKv instance in specified directory, with an optional runtime,
    /// returns the TsKv and runtime.
    ///
    /// If the given runtime is none, get_tskv will create a new runtime and
    /// put into the return value, or else the given runtime will be returned.

    fn get_config(dir: impl AsRef<Path>) -> Config {
        let dir = dir.as_ref();
        let mut global_config = Config::for_test();
        global_config.wal.path = dir.join("wal").to_str().unwrap().to_string();
        global_config.storage.path = dir.to_str().unwrap().to_string();

        global_config
    }

    fn get_tskv(dir: impl AsRef<Path>, runtime: Option<Arc<Runtime>>) -> (Arc<Runtime>, TsKv) {
        let global_config = get_config(dir);
        let opt = kv_option::Options::from(&global_config);
        let rt = runtime.unwrap_or_else(|| {
            let mut builder = runtime::Builder::new_multi_thread();
            builder.enable_all().max_blocking_threads(2);
            let runtime = builder.build().unwrap();
            Arc::new(runtime)
        });
        let memory = Arc::new(GreedyMemoryPool::default());
        let tskv = rt.block_on(async {
            let meta_manager =
                AdminMeta::new(global_config, Arc::new(MetricsRegister::default())).await;
            meta_manager.add_data_node().await.unwrap();
            let _ = meta_manager
                .create_tenant("cnosdb".to_string(), TenantOptions::default())
                .await;
            TsKv::open(
                meta_manager,
                opt,
                rt.clone(),
                memory,
                Arc::new(MetricsRegister::default()),
            )
            .await
            .unwrap()
        });
        (rt, tskv)
    }

    fn tskv_write(
        rt: Arc<Runtime>,
        tskv: &TsKv,
        tenant: &str,
        db: &str,
        id: VnodeId,
        index: u64,
        request: WriteDataRequest,
    ) {
        let apply_ctx = replication::ApplyContext {
            index,
            raft_id: id.into(),
            apply_type: replication::APPLY_TYPE_WRITE,
        };
        let vnode_store = Arc::new(rt.block_on(tskv.open_tsfamily(tenant, db, id)).unwrap());

        let command = raft_write_command::Command::WriteData(request);
        rt.block_on(vnode_store.apply(&apply_ctx, command)).unwrap();
    }
    pub fn kill_process(process_name: &str) {
        println!("- Killing processes {process_name}...");
        let system = System::new_with_specifics(
            RefreshKind::nothing().with_processes(ProcessRefreshKind::everything()),
        );
        for (pid, process) in system.processes() {
            if process.name() == process_name {
                match process.kill_with(sysinfo::Signal::Kill) {
                    Some(true) => println!(
                        "- Killed process {pid} ('{}')",
                        process.name().to_string_lossy()
                    ),
                    Some(false) => {
                        println!(
                            "- Failed killing process {pid} ('{}')",
                            process.name().to_string_lossy()
                        )
                    }
                    None => println!("- Kill with signal 'Kill' isn't supported on this platform"),
                }
            }
        }
    }

    #[tokio::test]
    #[serial]
    async fn test_kvcore_single_meta_all() {
        let temp_dir = tempfile::Builder::new().prefix("meta").tempdir().unwrap();
        let path = temp_dir.path().to_string_lossy().to_string();
        let cluster_name = "cluster_001".to_string();
        let size = 1024 * 1024 * 1024;
        meta::service::single::start_singe_meta_server(
            path,
            cluster_name,
            &MetaConfig::default(),
            size,
        )
        .await;
        let join_handle = tokio::spawn(async {
            let _ = tokio::task::spawn_blocking(|| {
                test_kvcore_init();
                test_kvcore_write();
                test_kvcore_flush();
                test_kvcore_flush_delta();
                test_kvcore_build_row_data();
                test_kvcore_snapshot_create_apply_delete();
            })
            .await;
        });
        join_handle.await.unwrap();
        kill_process("cnosdb-meta");
        temp_dir.close().unwrap();
    }

    fn test_kvcore_init() {
        println!("Enter serial test: test_kvcore_init");
        init_default_global_tracing("tskv_log", "tskv.log", "debug");
        let dir = "/tmp/test/kvcore/kvcore_init";
        let _ = std::fs::remove_dir_all(dir);
        std::fs::create_dir_all(dir).unwrap();

        get_tskv(dir, None);
        dbg!("Ok");
        println!("Leave serial test: test_kvcore_init");
    }

    fn test_kvcore_write() {
        println!("Enter serial test: test_kvcore_write");
        init_default_global_tracing("tskv_log", "tskv.log", "debug");
        let dir = "/tmp/test/kvcore/kvcore_write";
        let _ = std::fs::remove_dir_all(dir);
        std::fs::create_dir_all(dir).unwrap();
        let (rt, tskv) = get_tskv(dir, None);

        let mut fbb = flatbuffers::FlatBufferBuilder::new();
        let points = models_helper::create_random_points_with_delta(&mut fbb, 1);
        fbb.finish(points, None);
        let points = fbb.finished_data().to_vec();
        let request = WriteDataRequest {
            data: points,
            precision: Precision::NS as u32,
        };

        tskv_write(rt.clone(), &tskv, "cnosdb", "public", 0, 1, request);
        rt.block_on(async move {
            tskv.flush_tsfamily("cnosdb", "public", 0, true)
                .await
                .unwrap();
            tokio::time::sleep(Duration::from_secs(1)).await;
            let ts_family = tskv
                .open_tsfamily("cnosdb", "public", 0)
                .await
                .unwrap()
                .ts_family();

            let last_seq = ts_family.read().await.version().last_seq();
            assert_eq!(last_seq, 1)
        });

        println!("Leave serial test: test_kvcore_write");
    }

    // tips : to test all read method, we can use a small MAX_MEMCACHE_SIZE
    fn test_kvcore_flush() {
        println!("Enter serial test: test_kvcore_flush");
        init_default_global_tracing("tskv_log", "tskv.log", "info");
        let dir = "/tmp/test/kvcore/kvcore_flush";
        let _ = std::fs::remove_dir_all(dir);
        std::fs::create_dir_all(dir).unwrap();
        let (rt, tskv) = get_tskv(dir, None);

        let mut fbb = flatbuffers::FlatBufferBuilder::new();
        let points = models_helper::create_random_points_with_delta(&mut fbb, 2000);
        fbb.finish(points, None);
        let points = fbb.finished_data().to_vec();
        let request = WriteDataRequest {
            data: points,
            precision: Precision::NS as u32,
        };

        tskv_write(rt.clone(), &tskv, "cnosdb", "db", 0, 1, request.clone());
        tskv_write(rt.clone(), &tskv, "cnosdb", "db", 0, 2, request.clone());
        tskv_write(rt.clone(), &tskv, "cnosdb", "db", 0, 3, request.clone());
        tskv_write(rt.clone(), &tskv, "cnosdb", "db", 0, 4, request.clone());

        rt.block_on(async move {
            tokio::time::sleep(Duration::from_secs(1)).await;

            tskv.flush_tsfamily("cnosdb", "db", 0, true).await.unwrap();
            tokio::time::sleep(Duration::from_secs(1)).await;
            let ts_family = tskv
                .open_tsfamily("cnosdb", "db", 0)
                .await
                .unwrap()
                .ts_family();

            let last_seq = ts_family.read().await.version().last_seq();
            assert_eq!(last_seq, 4)
        });

        assert!(LocalFileSystem::try_exists(
            "/tmp/test/kvcore/kvcore_flush/data/cnosdb.db/0/tsm"
        ));
        println!("Leave serial test: test_kvcore_flush");
    }

    #[test]
    #[ignore]
    fn test_kvcore_big_write() {
        println!("Enter serial test: test_kvcore_big_write");
        init_default_global_tracing("tskv_log", "tskv.log", "debug");
        let dir = "/tmp/test/kvcore/kvcore_big_write";
        let _ = std::fs::remove_dir_all(dir);
        std::fs::create_dir_all(dir).unwrap();
        let (rt, tskv) = get_tskv(dir, None);

        for i in 0..100 {
            let mut fbb = flatbuffers::FlatBufferBuilder::new();
            let points = models_helper::create_big_random_points(&mut fbb, "kvcore_big_write", 10);
            fbb.finish(points, None);
            let points = fbb.finished_data().to_vec();
            let request = WriteDataRequest {
                data: points,
                precision: Precision::NS as u32,
            };

            tskv_write(rt.clone(), &tskv, "cnosdb", "public", 0, i, request.clone());
        }

        println!("Leave serial test: test_kvcore_big_write");
    }

    fn test_kvcore_flush_delta() {
        println!("Enter serial test: test_kvcore_flush_delta");
        init_default_global_tracing("tskv_log", "tskv.log", "debug");
        let dir = "/tmp/test/kvcore/kvcore_flush_delta";
        let _ = std::fs::remove_dir_all(dir);
        std::fs::create_dir_all(dir).unwrap();
        let (rt, tskv) = get_tskv(dir, None);
        let mut fbb = flatbuffers::FlatBufferBuilder::new();
        let database = "db_flush_delta";
        let table = "kvcore_flush_delta";
        let points =
            models_helper::create_random_points_include_delta(&mut fbb, database, table, 20);
        fbb.finish(points, None);
        let points = fbb.finished_data().to_vec();
        let request = WriteDataRequest {
            data: points,
            precision: Precision::NS as u32,
        };

        tskv_write(rt.clone(), &tskv, "cnosdb", database, 0, 1, request.clone());
        tskv_write(rt.clone(), &tskv, "cnosdb", database, 0, 2, request.clone());
        tskv_write(rt.clone(), &tskv, "cnosdb", database, 0, 3, request.clone());
        tskv_write(rt.clone(), &tskv, "cnosdb", database, 0, 4, request.clone());

        let db_cloned = database;
        rt.block_on(async {
            tokio::time::sleep(Duration::from_secs(2)).await;

            tskv.flush_tsfamily("cnosdb", db_cloned, 0, true)
                .await
                .unwrap();
            tokio::time::sleep(Duration::from_secs(1)).await;
            let ts_family = tskv
                .open_tsfamily("cnosdb", db_cloned, 0)
                .await
                .unwrap()
                .ts_family();

            let last_seq = ts_family.read().await.version().last_seq();
            assert_eq!(last_seq, 4)
        });

        assert!(LocalFileSystem::try_exists(format!(
            "/tmp/test/kvcore/kvcore_flush_delta/data/cnosdb.{database}/0/tsm"
        )));
        assert!(LocalFileSystem::try_exists(format!(
            "/tmp/test/kvcore/kvcore_flush_delta/data/cnosdb.{database}/0/delta"
        )));
        println!("Leave serial test: test_kvcore_flush_delta");
    }

    #[tokio::test]
    #[serial]
    async fn test_kvcore_log() {
        println!("Enter serial test: nc fn test_kvcore_log");
        init_default_global_tracing("tskv_log", "tskv.log", "debug");
        info!("hello");
        warn!("hello");
        debug!("hello");
        error!("hello"); //maybe we can use panic directly
        println!("Leave serial test: nc fn test_kvcore_log");
    }

    fn test_kvcore_build_row_data() {
        println!("Enter serial test: test_kvcore_build_row_data");
        init_default_global_tracing("tskv_log", "tskv.log", "debug");
        let dir = "/tmp/test/kvcore/kvcore_build_row_data";
        let _ = std::fs::remove_dir_all(dir);
        std::fs::create_dir_all(dir).unwrap();
        let (rt, tskv) = get_tskv(dir, None);
        let mut fbb = flatbuffers::FlatBufferBuilder::new();
        let points = models_helper::create_random_points_include_delta(
            &mut fbb,
            "db_build_row_data",
            "kvcore_build_row_data",
            20,
        );
        fbb.finish(points, None);
        let points = fbb.finished_data().to_vec();

        let request = WriteDataRequest {
            data: points,
            precision: Precision::NS as u32,
        };

        tskv_write(rt.clone(), &tskv, "cnosdb", "public", 0, 1, request.clone());

        println!("{:?}", tskv);
        println!("Leave serial test: test_kvcore_build_row_data");
    }

    fn test_kvcore_snapshot_create_apply_delete() {
        println!("Enter serial test: test_kvcore_snapshot_create_apply_delete");
        let dir = PathBuf::from("/tmp/test/kvcore/kvcore_snapshot_create_apply_delete");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        init_default_global_tracing(dir.join("log"), "tskv.log", "debug");
        let tenant = "cnosdb";
        let database = "db_test_snapshot";
        let table = "tab_test_snapshot";
        let vnode_id = 11;

        let (runtime, tskv) = get_tskv(&dir, None);

        let owner = make_owner(tenant, database);
        let storage_opt = tskv.get_storage_options();
        let vnode_tsm_dir = storage_opt.tsm_dir(&owner, vnode_id);
        let vnode_delta_dir = storage_opt.delta_dir(&owner, vnode_id);

        {
            // Write test data
            let mut fbb = flatbuffers::FlatBufferBuilder::new();
            let points =
                models_helper::create_random_points_include_delta(&mut fbb, database, table, 20);
            fbb.finish(points, None);
            let request = WriteDataRequest {
                data: fbb.finished_data().to_vec(),
                precision: Precision::NS as u32,
            };

            tskv_write(
                runtime.clone(),
                &tskv,
                tenant,
                database,
                vnode_id,
                1,
                request.clone(),
            );
        }

        let mut vnode = runtime
            .block_on(tskv.open_tsfamily(tenant, database, vnode_id))
            .unwrap();
        runtime
            .block_on(tskv.flush_tsfamily(tenant, database, vnode_id, true))
            .unwrap();
        let vnode_snapshot = {
            // Test create snapshot.
            sleep_in_runtime(runtime.clone(), Duration::from_secs(3));

            let vnode_snap = runtime.block_on(vnode.create_snapshot()).unwrap();
            for f in vnode_snap.version_edit.add_files.iter() {
                let path = if f.is_delta {
                    file_utils::make_delta_file(&vnode_delta_dir, f.file_id)
                } else {
                    file_utils::make_tsm_file(&vnode_tsm_dir, f.file_id)
                };

                assert!(
                    LocalFileSystem::try_exists(&path),
                    "{} not exists",
                    path.display(),
                );
            }

            vnode_snap
        };

        let vnode_backup_dir = dir.join("backup_for_test");
        let vnode_data_dir = storage_opt.ts_family_dir(&owner, vnode_id);
        dircpy::copy_dir(vnode_data_dir, &vnode_backup_dir).unwrap();

        let new_vnode_id = 12;
        let vnode_tsm_dir = storage_opt.tsm_dir(&owner, new_vnode_id);
        let vnode_delta_dir = storage_opt.delta_dir(&owner, new_vnode_id);

        {
            let mut vnode = runtime
                .block_on(tskv.open_tsfamily(tenant, database, new_vnode_id))
                .unwrap();

            runtime
                .block_on(vnode.apply_snapshot(vnode_snapshot, vnode_backup_dir.as_path()))
                .unwrap();
            sleep_in_runtime(runtime.clone(), Duration::from_secs(3));

            let version_edit = runtime
                .block_on(async move { vnode.ts_family().read().await.build_version_edit() });

            assert_eq!(version_edit.tsf_id, new_vnode_id);
            assert_eq!(version_edit.add_files.len(), 2);
            for f in version_edit.add_files.iter() {
                let path = if f.is_delta {
                    file_utils::make_delta_file(&vnode_delta_dir, f.file_id)
                } else {
                    file_utils::make_tsm_file(&vnode_tsm_dir, f.file_id)
                };

                assert!(
                    LocalFileSystem::try_exists(&path),
                    "{} not exists",
                    path.display(),
                );
            }
        }

        runtime.block_on(tskv.close());
        println!("Leave serial test: test_kvcore_snapshot_create_apply_delete");
    }

    fn sleep_in_runtime(runtime: Arc<Runtime>, duration: Duration) {
        let rt = runtime.clone();
        runtime.block_on(async move {
            rt.spawn(async move { tokio::time::sleep(duration).await })
                .await
                .unwrap();
        });
    }

    async fn async_func1() {
        // println!("run async func1");
        async_func3().await;
    }

    async fn async_func2() {
        // println!("run async func2");
    }

    async fn async_func3() {
        // println!("run async func3");
    }

    // #[tokio::test]

    fn sync_func1() {
        // println!("run sync func1");
        sync_func3();
    }

    fn sync_func2() {
        // println!("run sync func2");
    }

    fn sync_func3() {
        // println!("run sync func3");
    }

    // #[test]
    fn test_sync() {
        for _ in 0..10000 {
            sync_func1();
            sync_func2();
        }
    }

    async fn test_async() {
        for _ in 0..10000 {
            async_func1().await;
            async_func2().await;
        }
    }

    #[tokio::test]
    async fn compare() {
        let start = Instant::now();
        test_async().await;
        let duration = start.elapsed();

        let start1 = Instant::now();
        test_sync();
        let duration1 = start1.elapsed();

        println!("ASync Time elapsed  is: {:?}", duration);
        println!("Sync Time elapsed  is: {:?}", duration1);
    }
}
