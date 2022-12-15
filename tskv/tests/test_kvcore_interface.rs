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
    use tskv::file_system::file_manager;
    use tskv::{kv_option, TsKv};

    fn get_tskv() -> (Arc<Runtime>, TsKv) {
        let mut global_config = get_config("../config/config.toml");
        global_config.wal.path = "/tmp/test/wal".to_string();
        global_config.cache.max_buffer_size = 128;
        let opt = kv_option::Options::from(&global_config);
        let rt = Arc::new(runtime::Runtime::new().unwrap());
        rt.block_on(async {
            (
                rt.clone(),
                TsKv::open(global_config.cluster.clone(), opt, rt.clone())
                    .await
                    .unwrap(),
            )
        })
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
            tskv.write(0, "cnosdb", request).await.unwrap();
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
            tskv.write(0, "cnosdb", request.clone()).await.unwrap();
            tokio::time::sleep(Duration::from_secs(1)).await;
        });
        rt.block_on(async {
            tskv.write(0, "cnosdb", request.clone()).await.unwrap();
            tokio::time::sleep(Duration::from_secs(1)).await;
        });
        rt.block_on(async {
            tskv.write(0, "cnosdb", request.clone()).await.unwrap();
            tokio::time::sleep(Duration::from_secs(1)).await;
        });
        rt.block_on(async {
            tskv.write(0, "cnosdb", request.clone()).await.unwrap();
            tokio::time::sleep(Duration::from_secs(3)).await;
        });

        assert!(file_manager::try_exists("data/db/data/db/tsm/0"))
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
                tskv.write(0, "cnosdb", request.clone()).await.unwrap();
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
            tskv.write(0, "cnosdb", request.clone()).await.unwrap();
            tokio::time::sleep(Duration::from_secs(3)).await;
        });
        rt.block_on(async {
            tskv.write(0, "cnosdb", request.clone()).await.unwrap();
            tokio::time::sleep(Duration::from_secs(3)).await;
        });
        rt.block_on(async {
            tskv.write(0, "cnosdb", request.clone()).await.unwrap();
            tokio::time::sleep(Duration::from_secs(3)).await;
        });
        rt.block_on(async {
            tskv.write(0, "cnosdb", request.clone()).await.unwrap();
            tokio::time::sleep(Duration::from_secs(3)).await;
        });

        assert!(file_manager::try_exists("data/db/data/db/tsm/0"));
        assert!(file_manager::try_exists("data/db/data/db/delta/0"));
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
            tskv.write(0, "cnosdb", request.clone()).await.unwrap();
        });
        println!("{:?}", tskv)
    }

    // #[test]
    // #[serial]
    // fn test_kvcore_create_table() {
    //     init_default_global_tracing("tskv_log", "tskv.log", "debug");
    //     let (_rt, tskv) = get_tskv();
    //     tskv.create_database(&DatabaseSchema::new("cnosdb", "public"))
    //         .unwrap();
    //     tskv.create_database(&DatabaseSchema::new("cnosdb", "test"))
    //         .unwrap();
    //     let expected = TskvTableSchema::new(
    //         "cnosdb".to_string(),
    //         "test".to_string(),
    //         "test0".to_string(),
    //         vec![
    //             TableColumn::new(0, "time".to_string(), ColumnType::Time, Encoding::Default),
    //             TableColumn::new(1, "ta".to_string(), ColumnType::Tag, Encoding::Default),
    //             TableColumn::new(2, "tb".to_string(), ColumnType::Tag, Encoding::Default),
    //             TableColumn::new(
    //                 3,
    //                 "fa".to_string(),
    //                 ColumnType::Field(ValueType::Integer),
    //                 Encoding::Default,
    //             ),
    //             TableColumn::new(
    //                 4,
    //                 "fb".to_string(),
    //                 ColumnType::Field(ValueType::Float),
    //                 Encoding::Default,
    //             ),
    //         ],
    //     );
    //     tskv.create_table(&expected).unwrap();
    //     let table_schema = tskv
    //         .get_table_schema("cnosdb", "test", "test0")
    //         .unwrap()
    //         .unwrap();
    //     assert_eq!(expected, table_schema);
    // }
}
