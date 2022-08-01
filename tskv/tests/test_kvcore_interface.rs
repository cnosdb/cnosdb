mod tests {

    use std::sync::Arc;

    use chrono::Local;
    use config::GLOBAL_CONFIG;
    use lazy_static::lazy_static;
    use models::SeriesInfo;
    use protos::{kv_service, models as fb_models, models_helper};
    use serial_test::serial;
    use snafu::ResultExt;
    use tokio::sync::{mpsc, oneshot::channel, OnceCell};
    use trace::{debug, error, info, init_default_global_tracing, warn};
    use tskv::{
        error, kv_option,
        kv_option::{TseriesFamOpt, WalConfig},
        Summary, Task, TimeRange, TsKv,
    };

    async fn get_tskv() -> TsKv {
        let opt = kv_option::Options {
            wal: Arc::new(WalConfig {
                dir: String::from("/tmp/test/wal"),
                ..Default::default()
            }),
            ..Default::default()
        };

        TsKv::open(opt).await.unwrap()
    }

    #[tokio::test]
    #[serial]
    async fn test_kvcore_init() {
        init_default_global_tracing("tskv_log", "tskv.log", "debug");
        let tskv = get_tskv().await;

        dbg!("Ok");
    }

    #[tokio::test]
    #[serial]
    async fn test_kvcore_write() {
        init_default_global_tracing("tskv_log", "tskv.log", "debug");
        let tskv = get_tskv().await;

        let database = "db".to_string();
        let mut fbb = flatbuffers::FlatBufferBuilder::new();
        let points = models_helper::create_random_points_with_delta(&mut fbb, 1);
        fbb.finish(points, None);
        let points = fbb.finished_data().to_vec();
        let request = kv_service::WritePointsRpcRequest {
            version: 1,
            database,
            points,
        };

        tskv.write(request).await.unwrap();
    }

    // remove repeat sid and fields_id
    pub fn remove_duplicates(nums: &mut [u64]) -> usize {
        init_default_global_tracing("tskv_log", "tskv.log", "debug");
        if nums.len() <= 1 {
            return nums.len() as usize;
        }
        let mut l = 1;
        for r in 1..nums.len() {
            if nums[r] != nums[l - 1] {
                nums[l] = nums[r];
                l += 1;
            }
        }
        l as usize
    }

    // tips : to test all read method, we can use a small MAX_MEMCACHE_SIZE
    #[tokio::test]
    #[serial]
    async fn test_kvcore_read() {
        init_default_global_tracing("tskv_log", "tskv.log", "debug");
        let tskv = get_tskv().await;

        let database = "db".to_string();
        let mut fbb = flatbuffers::FlatBufferBuilder::new();
        let points = models_helper::create_random_points_with_delta(&mut fbb, 20);
        fbb.finish(points, None);
        let points = fbb.finished_data().to_vec();
        let request = kv_service::WritePointsRpcRequest {
            version: 1,
            database,
            points,
        };

        tskv.write(request.clone()).await.unwrap();

        let shared_write_batch = Arc::new(request.points);
        let fb_points = flatbuffers::root::<fb_models::Points>(&shared_write_batch)
            .context(error::InvalidFlatbufferSnafu)
            .unwrap();
        let mut sids = vec![];
        let mut fields_id = vec![];
        for point in fb_points.points().unwrap() {
            let mut info = SeriesInfo::from_flatbuffers(&point)
                .context(error::InvalidModelSnafu)
                .unwrap();
            info.finish();
            sids.push(info.series_id());
            for field in info.field_infos().iter() {
                fields_id.push(field.field_id());
            }
        }

        sids.sort_unstable();
        fields_id.sort_unstable();
        let l = remove_duplicates(&mut sids);
        sids = sids[0..l].to_owned();
        let l = remove_duplicates(&mut fields_id);
        fields_id = fields_id[0..l].to_owned();
        tskv.read(
            sids,
            &TimeRange::new(Local::now().timestamp_millis() + 100, 0),
            fields_id,
        )
        .await;
    }

    #[tokio::test]
    #[ignore]
    async fn test_kvcore_delete_cache() {
        init_default_global_tracing("tskv_log", "tskv.log", "debug");
        let tskv = get_tskv().await;
        let database = "db".to_string();
        let mut fbb = flatbuffers::FlatBufferBuilder::new();
        let points = models_helper::create_random_points_with_delta(&mut fbb, 5);
        fbb.finish(points, None);
        let points = fbb.finished_data().to_vec();
        let request = kv_service::WritePointsRpcRequest {
            version: 1,
            database,
            points,
        };

        tskv.write(request.clone()).await.unwrap();

        let shared_write_batch = Arc::new(request.points);
        let fb_points = flatbuffers::root::<fb_models::Points>(&shared_write_batch)
            .context(error::InvalidFlatbufferSnafu)
            .unwrap();
        let mut sids = vec![];
        let mut fields_id = vec![];
        for point in fb_points.points().unwrap() {
            let mut info = SeriesInfo::from_flatbuffers(&point)
                .context(error::InvalidModelSnafu)
                .unwrap();
            info.finish();
            sids.push(info.series_id());
            for field in info.field_infos().iter() {
                fields_id.push(field.field_id());
            }
        }

        sids.sort_unstable();
        fields_id.sort_unstable();
        let l = remove_duplicates(&mut sids);
        sids = sids[0..l].to_owned();
        let l = remove_duplicates(&mut fields_id);
        fields_id = fields_id[0..l].to_owned();
        tskv.read(
            sids.clone(),
            &TimeRange::new(Local::now().timestamp_millis() + 100, 0),
            fields_id.clone(),
        )
        .await;
        info!("delete delta data");
        tskv.delete_series(sids.clone(), 1, 1).await.unwrap();
        tskv.read(
            sids.clone(),
            &TimeRange::new(Local::now().timestamp_millis() + 100, 0),
            fields_id.clone(),
        )
        .await;
    }

    #[tokio::test]
    #[ignore]
    async fn test_kvcore_big_write() {
        init_default_global_tracing("tskv_log", "tskv.log", "debug");
        let tskv = get_tskv().await;

        for _ in 0..100 {
            let database = "db".to_string();
            let mut fbb = flatbuffers::FlatBufferBuilder::new();
            let points = models_helper::create_big_random_points(&mut fbb, 10);
            fbb.finish(points, None);
            let points = fbb.finished_data().to_vec();

            let request = kv_service::WritePointsRpcRequest {
                version: 1,
                database,
                points,
            };

            tskv.write(request).await.unwrap();
        }
    }

    #[tokio::test]
    #[serial]
    async fn test_kvcore_insert_cache() {
        init_default_global_tracing("tskv_log", "tskv.log", "debug");
        let tskv = get_tskv().await;

        let mut fbb = flatbuffers::FlatBufferBuilder::new();
        let points = models_helper::create_random_points_with_delta(&mut fbb, 1);
        fbb.finish(points, None);
        let points = fbb.finished_data();

        tskv.insert_cache(1, points).await.unwrap();
    }

    #[tokio::test]
    #[ignore]
    async fn test_kvcore_start() {
        init_default_global_tracing("tskv_log", "tskv.log", "debug");
        let tskv = get_tskv().await;

        let (wal_sender, wal_receiver) = mpsc::unbounded_channel();
        let (tx, rx) = channel();

        let database = "db".to_string();
        let mut fbb = flatbuffers::FlatBufferBuilder::new();
        let points = models_helper::create_random_points_with_delta(&mut fbb, 1);
        fbb.finish(points, None);
        let points = fbb.finished_data().to_vec();
        let req = kv_service::WritePointsRpcRequest {
            version: 1,
            database,
            points,
        };

        wal_sender.send(Task::WritePoints { req, tx }).unwrap();

        TsKv::start(tskv, wal_receiver);

        match rx.await {
            Ok(Ok(_)) => info!("successful"),
            _ => panic!("wrong"),
        };
    }

    #[tokio::test]
    #[serial]
    async fn test_kvcore_flush_delta() {
        init_default_global_tracing("tskv_log", "tskv.log", "debug");
        let tskv = get_tskv().await;
        let database = "db".to_string();
        let mut fbb = flatbuffers::FlatBufferBuilder::new();
        let points = models_helper::create_random_points_include_delta(&mut fbb, 20);
        fbb.finish(points, None);
        let points = fbb.finished_data().to_vec();
        let request = kv_service::WritePointsRpcRequest {
            version: 1,
            database,
            points,
        };

        tskv.write(request).await.unwrap();
    }

    #[tokio::test]
    #[serial]
    async fn test_kvcore_log() {
        init_default_global_tracing("tskv_log", "tskv.log", "debug");
        info!("hello");
        warn!("hello");
        debug!("hello");
        error!("hello"); //maybe we can use panic directly
                         // panic!("hello");
    }
}
