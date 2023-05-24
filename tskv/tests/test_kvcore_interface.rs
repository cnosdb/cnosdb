#[cfg(test)]
mod tests {
    use std::path::Path;
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use memory_pool::GreedyMemoryPool;
    use meta::model::meta_manager::RemoteMetaManager;
    use meta::model::MetaRef;
    use metrics::metric_register::MetricsRegister;
    use models::schema::{Precision, TenantOptions};
    use protos::kv_service::Meta;
    use protos::{kv_service, models_helper};
    use serial_test::serial;
    use tokio::runtime;
    use tokio::runtime::Runtime;
    use trace::{debug, error, info, init_default_global_tracing, warn};
    use tskv::file_system::file_manager;
    use tskv::{kv_option, Engine, TsKv};

    fn get_tskv(dir: impl AsRef<Path>) -> (Arc<Runtime>, TsKv) {
        let dir = dir.as_ref();
        let mut global_config = config::get_config_for_test();
        global_config.wal.path = dir.join("wal").to_str().unwrap().to_string();
        global_config.storage.path = dir.to_str().unwrap().to_string();
        global_config.cache.max_buffer_size = 128;
        let opt = kv_option::Options::from(&global_config);
        let rt = Arc::new(runtime::Runtime::new().unwrap());
        let memory = Arc::new(GreedyMemoryPool::new(1024 * 1024 * 1024));
        let meta_manager: MetaRef = rt.block_on(RemoteMetaManager::new(
            global_config.clone(),
            global_config.storage.path,
        ));

        rt.block_on(meta_manager.admin_meta().add_data_node())
            .unwrap();
        let _ = rt.block_on(
            meta_manager
                .tenant_manager()
                .create_tenant("cnosdb".to_string(), TenantOptions::default()),
        );
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
        get_tskv("/tmp/test/kvcore/kvcore_init");
        dbg!("Ok");
    }

    #[test]
    #[serial]
    fn test_kvcore_write() {
        init_default_global_tracing("tskv_log", "tskv.log", "debug");

        let (rt, tskv) = get_tskv("/tmp/test/kvcore/kvcore_write");

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
            tskv.write(0, Precision::NS, request).await.unwrap();
        });
    }

    // tips : to test all read method, we can use a small MAX_MEMCACHE_SIZE
    #[test]
    #[serial]
    fn test_kvcore_flush() {
        init_default_global_tracing("tskv_log", "tskv.log", "debug");
        let (rt, tskv) = get_tskv("/tmp/test/kvcore/kvcore_flush");

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
            tskv.write(0, Precision::NS, request.clone()).await.unwrap();
            tokio::time::sleep(Duration::from_secs(1)).await;
        });
        rt.block_on(async {
            tskv.write(0, Precision::NS, request.clone()).await.unwrap();
            tokio::time::sleep(Duration::from_secs(1)).await;
        });
        rt.block_on(async {
            tskv.write(0, Precision::NS, request.clone()).await.unwrap();
            tokio::time::sleep(Duration::from_secs(1)).await;
        });
        rt.block_on(async {
            tskv.write(0, Precision::NS, request.clone()).await.unwrap();
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
        let (rt, tskv) = get_tskv("/tmp/test/kvcore/kvcore_big_write");

        for _ in 0..100 {
            let mut fbb = flatbuffers::FlatBufferBuilder::new();
            let points = models_helper::create_big_random_points(&mut fbb, 10);
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
                tskv.write(0, Precision::NS, request.clone()).await.unwrap();
            });
        }
    }

    #[test]
    #[serial]
    fn test_kvcore_flush_delta() {
        init_default_global_tracing("tskv_log", "tskv.log", "debug");
        let (rt, tskv) = get_tskv("/tmp/test/kvcore/kvcore_flush_delta");
        let mut fbb = flatbuffers::FlatBufferBuilder::new();
        let points = models_helper::create_random_points_include_delta(&mut fbb, 20);
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
            tskv.write(0, Precision::NS, request.clone()).await.unwrap();
            tokio::time::sleep(Duration::from_secs(3)).await;
        });
        rt.block_on(async {
            tskv.write(0, Precision::NS, request.clone()).await.unwrap();
            tokio::time::sleep(Duration::from_secs(3)).await;
        });
        rt.block_on(async {
            tskv.write(0, Precision::NS, request.clone()).await.unwrap();
            tokio::time::sleep(Duration::from_secs(3)).await;
        });
        rt.block_on(async {
            tskv.write(0, Precision::NS, request.clone()).await.unwrap();
            tokio::time::sleep(Duration::from_secs(3)).await;
        });

        assert!(file_manager::try_exists(
            "/tmp/test/kvcore/kvcore_flush_delta/data/cnosdb.db/0/tsm"
        ));
        assert!(file_manager::try_exists(
            "/tmp/test/kvcore/kvcore_flush_delta/data/cnosdb.db/0/delta"
        ));
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
        let (rt, tskv) = get_tskv("/tmp/test/kvcore/kvcore_build_row_data");
        let mut fbb = flatbuffers::FlatBufferBuilder::new();
        let points = models_helper::create_random_points_include_delta(&mut fbb, 20);
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
            tskv.write(0, Precision::NS, request.clone()).await.unwrap();
        });
        println!("{:?}", tskv)
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
