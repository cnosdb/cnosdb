mod test {
    use std::sync::Arc;
    use std::time::Duration;

    use datafusion::arrow::util::pretty::pretty_format_batches;

    use config::get_config;
    use protos::{kv_service, models_helper};
    use tokio::runtime::Runtime;
    use trace::init_default_global_tracing;
    use tskv::engine::Engine;
    use tskv::{kv_option, TsKv};

    #[tokio::test]
    #[ignore]
    async fn test_query() {
        init_default_global_tracing("tskv_log", "tskv.log", "debug");
        let mut global_config = get_config("../config/config.toml");
        global_config.wal.path = "/tmp/test/wal".to_string();
        let opt = kv_option::Options::from(&global_config);

        let tskv = TsKv::open(opt, Arc::new(Runtime::new().unwrap()))
            .await
            .unwrap();

        let mut fbb = flatbuffers::FlatBufferBuilder::new();
        let points = models_helper::create_random_points_with_delta(&mut fbb, 200);
        fbb.finish(points, None);
        let points = fbb.finished_data().to_vec();
        let request = kv_service::WritePointsRpcRequest { version: 1, points };

        tskv.write(request.clone()).await.unwrap();
        tokio::time::sleep(Duration::from_secs(3)).await;

        let query = query::db::Db::new(Arc::new(tskv));
        let res = query.run_query("select * from cnosdb.db.table").await;
        if let Some(res) = res {
            let scheduled = pretty_format_batches(&*res).unwrap().to_string();
            println!("{}", scheduled);
            return;
        }
        println!("find none row")
    }
}
