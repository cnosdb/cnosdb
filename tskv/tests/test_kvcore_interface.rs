#[cfg(test)]
mod tests {
    use std::path::{Path, PathBuf};
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use memory_pool::GreedyMemoryPool;
    use meta::model::meta_admin::AdminMeta;
    use meta::model::MetaRef;
    use metrics::metric_register::MetricsRegister;
    use models::schema::{make_owner, Precision, TenantOptions};
    use protos::kv_service::Meta;
    use protos::{kv_service, models_helper};
    use serial_test::serial;
    use tokio::runtime;
    use tokio::runtime::Runtime;
    use trace::{debug, error, info, init_default_global_tracing, warn};
    use tskv::file_system::file_manager;
    use tskv::{file_utils, kv_option, Engine, SnapshotFileMeta, TsKv};

    /// Initializes a TsKv instance in specified directory, with an optional runtime,
    /// returns the TsKv and runtime.
    ///
    /// If the given runtime is none, get_tskv will create a new runtime and
    /// put into the return value, or else the given runtime will be returned.
    fn get_tskv(dir: impl AsRef<Path>, runtime: Option<Arc<Runtime>>) -> (Arc<Runtime>, TsKv) {
        let dir = dir.as_ref();
        let mut global_config = config::get_config_for_test();
        global_config.wal.path = dir.join("wal").to_str().unwrap().to_string();
        global_config.storage.path = dir.to_str().unwrap().to_string();
        global_config.cache.max_buffer_size = 128;
        let opt = kv_option::Options::from(&global_config);
        let rt = match runtime {
            Some(rt) => rt,
            None => {
                let mut builder = runtime::Builder::new_multi_thread();
                builder.enable_all().max_blocking_threads(2);
                let runtime = builder.build().unwrap();
                Arc::new(runtime)
            }
        };
        let memory = Arc::new(GreedyMemoryPool::default());
        let meta_manager: MetaRef = rt.block_on(AdminMeta::new(global_config));

        rt.block_on(meta_manager.add_data_node()).unwrap();
        let _ =
            rt.block_on(meta_manager.create_tenant("cnosdb".to_string(), TenantOptions::default()));
        rt.block_on(async {
            (
                rt.clone(),
                TsKv::open(
                    meta_manager,
                    opt,
                    rt.clone(),
                    memory,
                    Arc::new(MetricsRegister::default()),
                )
                .await
                .unwrap(),
            )
        })
    }

    #[test]
    #[serial]
    fn test_kvcore_init() {
        init_default_global_tracing("tskv_log", "tskv.log", "debug");
        get_tskv("/tmp/test/kvcore/kvcore_init", None);
        dbg!("Ok");
    }

    #[test]
    #[serial]
    fn test_kvcore_write() {
        init_default_global_tracing("tskv_log", "tskv.log", "debug");

        let (rt, tskv) = get_tskv("/tmp/test/kvcore/kvcore_write", None);

        let mut fbb = flatbuffers::FlatBufferBuilder::new();
        let points = models_helper::create_random_points_with_delta(&mut fbb, 1);
        fbb.finish(points, None);
        let points = fbb.finished_data().to_vec();
        let request = kv_service::WritePointsRequest {
            version: 1,
            meta: Some(Meta {
                tenant: "cnosdb".to_string(),
                user: None,
                password: None,
            }),
            points,
        };

        rt.spawn(async move {
            tskv.write(None, 0, Precision::NS, request).await.unwrap();
        });
    }

    // tips : to test all read method, we can use a small MAX_MEMCACHE_SIZE
    #[test]
    #[serial]
    fn test_kvcore_flush() {
        init_default_global_tracing("tskv_log", "tskv.log", "debug");
        let dir = "/tmp/test/kvcore/kvcore_flush";
        let _ = std::fs::remove_dir_all(dir);
        std::fs::create_dir_all(dir).unwrap();
        let (rt, tskv) = get_tskv(dir, None);

        let mut fbb = flatbuffers::FlatBufferBuilder::new();
        let points = models_helper::create_random_points_with_delta(&mut fbb, 2000);
        fbb.finish(points, None);
        let points = fbb.finished_data().to_vec();
        let request = kv_service::WritePointsRequest {
            version: 1,
            meta: Some(Meta {
                tenant: "cnosdb".to_string(),
                user: None,
                password: None,
            }),
            points,
        };
        rt.block_on(async {
            tskv.write(None, 0, Precision::NS, request.clone())
                .await
                .unwrap();
            tokio::time::sleep(Duration::from_secs(1)).await;
        });
        rt.block_on(async {
            tskv.write(None, 0, Precision::NS, request.clone())
                .await
                .unwrap();
            tokio::time::sleep(Duration::from_secs(1)).await;
        });
        rt.block_on(async {
            tskv.write(None, 0, Precision::NS, request.clone())
                .await
                .unwrap();
            tokio::time::sleep(Duration::from_secs(1)).await;
        });
        rt.block_on(async {
            tskv.write(None, 0, Precision::NS, request.clone())
                .await
                .unwrap();
            tokio::time::sleep(Duration::from_secs(3)).await;
        });

        assert!(file_manager::try_exists(
            "/tmp/test/kvcore/kvcore_flush/data/cnosdb.db/0/tsm"
        ))
    }

    #[test]
    #[ignore]
    fn test_kvcore_big_write() {
        init_default_global_tracing("tskv_log", "tskv.log", "debug");
        let (rt, tskv) = get_tskv("/tmp/test/kvcore/kvcore_big_write", None);

        for _ in 0..100 {
            let mut fbb = flatbuffers::FlatBufferBuilder::new();
            let points = models_helper::create_big_random_points(&mut fbb, "kvcore_big_write", 10);
            fbb.finish(points, None);
            let points = fbb.finished_data().to_vec();

            let request = kv_service::WritePointsRequest {
                version: 1,
                meta: Some(Meta {
                    tenant: "cnosdb".to_string(),
                    user: None,
                    password: None,
                }),
                points,
            };

            rt.block_on(async {
                tskv.write(None, 0, Precision::NS, request.clone())
                    .await
                    .unwrap();
            });
        }
    }

    #[test]
    #[serial]
    fn test_kvcore_flush_delta() {
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
        let request = kv_service::WritePointsRequest {
            version: 1,
            meta: Some(Meta {
                tenant: "cnosdb".to_string(),
                user: None,
                password: None,
            }),
            points,
        };

        rt.block_on(async {
            tskv.write(None, 0, Precision::NS, request.clone())
                .await
                .unwrap();
            tokio::time::sleep(Duration::from_secs(3)).await;
        });
        rt.block_on(async {
            tskv.write(None, 0, Precision::NS, request.clone())
                .await
                .unwrap();
            tokio::time::sleep(Duration::from_secs(3)).await;
        });
        rt.block_on(async {
            tskv.write(None, 0, Precision::NS, request.clone())
                .await
                .unwrap();
            tokio::time::sleep(Duration::from_secs(3)).await;
        });
        rt.block_on(async {
            tskv.write(None, 0, Precision::NS, request.clone())
                .await
                .unwrap();
            tokio::time::sleep(Duration::from_secs(3)).await;
        });

        assert!(file_manager::try_exists(format!(
            "/tmp/test/kvcore/kvcore_flush_delta/data/cnosdb.{database}/0/tsm"
        )));
        assert!(file_manager::try_exists(format!(
            "/tmp/test/kvcore/kvcore_flush_delta/data/cnosdb.{database}/0/delta"
        )));
    }

    #[tokio::test]
    #[serial]
    async fn test_kvcore_log() {
        init_default_global_tracing("tskv_log", "tskv.log", "debug");
        info!("hello");
        warn!("hello");
        debug!("hello");
        error!("hello"); //maybe we can use panic directly
    }

    #[test]
    #[serial]
    fn test_kvcore_build_row_data() {
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
        let request = kv_service::WritePointsRequest {
            version: 1,
            meta: Some(Meta {
                tenant: "cnosdb".to_string(),
                user: None,
                password: None,
            }),
            points,
        };

        rt.block_on(async {
            tskv.write(None, 0, Precision::NS, request.clone())
                .await
                .unwrap();
        });
        println!("{:?}", tskv)
    }

    #[test]
    fn test_kvcore_recover() {
        let dir = PathBuf::from("/tmp/test/kvcore/kvcore_recover");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        init_default_global_tracing(dir.join("log"), "tskv.log", "debug");
        let tenant = "cnosdb";
        let database = "db_recover";
        let table = "kvcore_recover";
        let vnode_id = 10;

        let runtime = {
            let (runtime, tskv) = get_tskv(&dir, None);

            let mut fbb = flatbuffers::FlatBufferBuilder::new();
            let points =
                models_helper::create_random_points_include_delta(&mut fbb, database, table, 20);
            fbb.finish(points, None);
            let request = kv_service::WritePointsRequest {
                version: 1,
                meta: Some(Meta {
                    tenant: tenant.to_string(),
                    user: None,
                    password: None,
                }),
                points: fbb.finished_data().to_vec(),
            };
            runtime
                .block_on(tskv.write(None, vnode_id, Precision::NS, request))
                .unwrap();
            runtime.block_on(tskv.close());

            runtime
        };

        let (runtime, tskv) = get_tskv(&dir, Some(runtime));
        let version = runtime
            .block_on(tskv.get_db_version(tenant, database, vnode_id))
            .unwrap()
            .unwrap();
        let cached_data = tskv::test::get_one_series_cache_data(version.caches.mut_cache.clone());
        // TODO: compare cached_data and the wrote_data
        assert!(!cached_data.is_empty());
    }

    #[test]
    fn test_kvcore_snapshot_create_apply_delete() {
        let dir = PathBuf::from("/tmp/test/kvcore/test_kvcore_snapshot_create_apply_delete");
        let _ = std::fs::remove_dir_all(&dir);

        init_default_global_tracing(dir.join("log"), "tskv.log", "debug");
        let tenant = "cnosdb";
        let database = "test_db";
        let table = "test_tab";
        let vnode_id = 11;

        let (runtime, tskv) = get_tskv(&dir, None);

        let tenant_database = make_owner(tenant, database);
        let storage_opt = tskv.get_storage_options();
        let vnode_tsm_dir = storage_opt.tsm_dir(&tenant_database, vnode_id);
        let vnode_delta_dir = storage_opt.delta_dir(&tenant_database, vnode_id);
        let vnode_snapshot_dir = storage_opt.snapshot_dir(&tenant_database, vnode_id);
        let vnode_backup_dir = dir.join("backup_for_test");

        {
            // Write test data
            let mut fbb = flatbuffers::FlatBufferBuilder::new();
            let points =
                models_helper::create_random_points_include_delta(&mut fbb, database, table, 20);
            fbb.finish(points, None);
            let request = kv_service::WritePointsRequest {
                version: 1,
                meta: Some(Meta {
                    tenant: tenant.to_string(),
                    user: None,
                    password: None,
                }),
                points: fbb.finished_data().to_vec(),
            };

            runtime
                .block_on(tskv.write(None, vnode_id, Precision::NS, request))
                .unwrap();
        }

        let (vnode_snapshot_sub_dir, mut vnode_snapshot) = {
            // Test create snapshot.
            sleep_in_runtime(runtime.clone(), Duration::from_secs(3));
            let vnode_snap = runtime.block_on(tskv.create_snapshot(vnode_id)).unwrap();

            assert_eq!(vnode_snap.tenant, tenant);
            assert_eq!(vnode_snap.database, database);
            assert_eq!(vnode_snap.vnode_id, 11);
            let files = vnode_snap.files.clone();
            let tsm_files: Vec<SnapshotFileMeta> =
                files.clone().into_iter().filter(|f| f.level != 0).collect();
            let delta_files: Vec<SnapshotFileMeta> =
                files.into_iter().filter(|f| f.level == 0).collect();
            assert_eq!(tsm_files.len(), 1);
            assert_eq!(delta_files.len(), 1);

            let tsm_file_path =
                file_utils::make_tsm_file(vnode_tsm_dir, tsm_files.first().unwrap().file_id);
            assert!(
                file_manager::try_exists(&tsm_file_path),
                "{} not exists",
                tsm_file_path.display(),
            );

            let delta_file_path =
                file_utils::make_delta_file(vnode_delta_dir, delta_files.first().unwrap().file_id);
            assert!(
                file_manager::try_exists(&delta_file_path),
                "{} not exists",
                delta_file_path.display(),
            );

            (vnode_snapshot_dir.join(&vnode_snap.snapshot_id), vnode_snap)
        };

        {
            // Test delete snapshot.
            // 1. Backup files.
            dircpy::copy_dir(vnode_snapshot_sub_dir, &vnode_backup_dir).unwrap();

            // 2. Do test delete snapshot.
            runtime.block_on(tskv.delete_snapshot(vnode_id)).unwrap();
            sleep_in_runtime(runtime.clone(), Duration::from_secs(3));
            assert!(
                !file_manager::try_exists(&vnode_snapshot_dir),
                "{} still exists unexpectedly",
                vnode_snapshot_dir.display()
            );
        }

        let new_vnode_id = 12;
        let vnode_tsm_dir = storage_opt.tsm_dir(&tenant_database, new_vnode_id);
        let vnode_delta_dir = storage_opt.delta_dir(&tenant_database, new_vnode_id);
        let vnode_move_dir = storage_opt.move_dir(&tenant_database, new_vnode_id);
        vnode_snapshot.vnode_id = new_vnode_id;

        {
            // Test apply snapshot
            // 1. Restore files to migration directory.
            std::fs::create_dir_all(&vnode_move_dir).unwrap();
            dircpy::copy_dir(vnode_backup_dir, vnode_move_dir).unwrap();

            // 2. Do test apply snapshot.
            runtime
                .block_on(tskv.apply_snapshot(vnode_snapshot))
                .unwrap();
            sleep_in_runtime(runtime.clone(), Duration::from_secs(3));
            let vnode = runtime
                .block_on(tskv.get_vnode_summary(tenant, database, new_vnode_id))
                .unwrap()
                .unwrap();
            assert_eq!(vnode.tsf_id, new_vnode_id);
            assert_eq!(vnode.add_files.len(), 2);
            let tsm_files: Vec<SnapshotFileMeta> = vnode
                .add_files
                .iter()
                .filter(|f| !f.is_delta)
                .map(From::from)
                .collect();
            let delta_files: Vec<SnapshotFileMeta> = vnode
                .add_files
                .iter()
                .filter(|f| f.is_delta)
                .map(From::from)
                .collect();
            assert_eq!(tsm_files.len(), 1);
            assert_eq!(delta_files.len(), 1);

            let tsm_file_path =
                file_utils::make_tsm_file(vnode_tsm_dir, tsm_files.first().unwrap().file_id);
            assert!(
                file_manager::try_exists(&tsm_file_path),
                "{} not exists",
                tsm_file_path.display(),
            );

            let delta_file_path =
                file_utils::make_delta_file(vnode_delta_dir, delta_files.first().unwrap().file_id);
            assert!(
                file_manager::try_exists(&delta_file_path),
                "{} not exists",
                delta_file_path.display(),
            );
        }

        runtime.block_on(tskv.close());
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
