#[cfg(test)]
mod tests {
    use serial_test::serial;
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::runtime;
    use tokio::runtime::Runtime;

    use config::get_config;
    use protos::{kv_service, models_helper};
    use trace::{debug, error, info, init_default_global_tracing, warn};
    use tskv::engine::Engine;
    use tskv::file_manager;
    use tskv::{kv_option, TsKv};

    fn get_tskv() -> (Arc<Runtime>, TsKv) {
        let mut global_config = get_config("../config/config.toml");
        global_config.wal.path = "/tmp/test/wal".to_string();
        global_config.cache.max_buffer_size = 128;
        let opt = kv_option::Options::from(&global_config);
        let rt = Arc::new(runtime::Runtime::new().unwrap());
        rt.block_on(async { (rt.clone(), TsKv::open(opt, rt.clone()).await.unwrap()) })
    }

    #[test]
    #[serial]
    fn test_kvcore_init() {
        init_default_global_tracing("tskv_log", "tskv.log", "debug");
        get_tskv();
        dbg!("Ok");
    }

    #[test]
    #[serial]
    fn test_kvcore_write() {
        init_default_global_tracing("tskv_log", "tskv.log", "debug");

        let (rt, tskv) = get_tskv();

        let mut fbb = flatbuffers::FlatBufferBuilder::new();
        let points = models_helper::create_random_points_with_delta(&mut fbb, 1);
        fbb.finish(points, None);
        let points = fbb.finished_data().to_vec();
        let request = kv_service::WritePointsRpcRequest { version: 1, points };

        rt.spawn(async move {
            tskv.write(request).await.unwrap();
        });
    }

    // tips : to test all read method, we can use a small MAX_MEMCACHE_SIZE
    #[test]
    #[serial]
    fn test_kvcore_flush() {
        init_default_global_tracing("tskv_log", "tskv.log", "debug");
        let (rt, tskv) = get_tskv();

        let mut fbb = flatbuffers::FlatBufferBuilder::new();
        let points = models_helper::create_random_points_with_delta(&mut fbb, 2000);
        fbb.finish(points, None);
        let points = fbb.finished_data().to_vec();
        let request = kv_service::WritePointsRpcRequest { version: 1, points };
        rt.block_on(async {
            tskv.write(request.clone()).await.unwrap();
            tokio::time::sleep(Duration::from_secs(1)).await;
        });
        rt.block_on(async {
            tskv.write(request.clone()).await.unwrap();
            tokio::time::sleep(Duration::from_secs(1)).await;
        });
        rt.block_on(async {
            tskv.write(request.clone()).await.unwrap();
            tokio::time::sleep(Duration::from_secs(1)).await;
        });
        rt.block_on(async {
            tskv.write(request.clone()).await.unwrap();
            tokio::time::sleep(Duration::from_secs(3)).await;
        });

        assert!(file_manager::try_exists("dev/db/data/db/tsm/0"))
    }

    #[test]
    #[ignore]
    fn test_kvcore_big_write() {
        init_default_global_tracing("tskv_log", "tskv.log", "debug");
        let (rt, tskv) = get_tskv();

        for _ in 0..100 {
            let mut fbb = flatbuffers::FlatBufferBuilder::new();
            let points = models_helper::create_big_random_points(&mut fbb, 10);
            fbb.finish(points, None);
            let points = fbb.finished_data().to_vec();

            let request = kv_service::WritePointsRpcRequest { version: 1, points };

            rt.block_on(async {
                tskv.write(request).await.unwrap();
            });
        }
    }

    #[test]
    #[serial]
    fn test_kvcore_flush_delta() {
        init_default_global_tracing("tskv_log", "tskv.log", "debug");
        let (rt, tskv) = get_tskv();
        let mut fbb = flatbuffers::FlatBufferBuilder::new();
        let points = models_helper::create_random_points_include_delta(&mut fbb, 20);
        fbb.finish(points, None);
        let points = fbb.finished_data().to_vec();
        let request = kv_service::WritePointsRpcRequest { version: 1, points };

        rt.block_on(async {
            tskv.write(request.clone()).await.unwrap();
            tokio::time::sleep(Duration::from_secs(3)).await;
        });
        rt.block_on(async {
            tskv.write(request.clone()).await.unwrap();
            tokio::time::sleep(Duration::from_secs(3)).await;
        });
        rt.block_on(async {
            tskv.write(request.clone()).await.unwrap();
            tokio::time::sleep(Duration::from_secs(3)).await;
        });
        rt.block_on(async {
            tskv.write(request).await.unwrap();
            tokio::time::sleep(Duration::from_secs(3)).await;
        });

        assert!(file_manager::try_exists("dev/db/data/db/tsm/0"));
        assert!(file_manager::try_exists("dev/db/data/db/delta/0"));
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
        let (rt, tskv) = get_tskv();
        let mut fbb = flatbuffers::FlatBufferBuilder::new();
        let points = models_helper::create_random_points_include_delta(&mut fbb, 20);
        fbb.finish(points, None);
        let points = fbb.finished_data().to_vec();
        let request = kv_service::WritePointsRpcRequest { version: 1, points };

        rt.block_on(async {
            tskv.write(request).await.unwrap();
        });
        println!("{:?}", tskv)
    }
}
