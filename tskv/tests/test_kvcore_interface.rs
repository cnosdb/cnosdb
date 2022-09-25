#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use async_channel as channel;
    use chrono::Local;
    use serial_test::serial;
    use snafu::ResultExt;
    use tokio::runtime;
    use tokio::runtime::Runtime;
    use tokio::sync::oneshot::channel;

    use config::get_config;
    use models::SeriesInfo;
    use protos::{kv_service, models as fb_models, models_helper};
    use trace::{debug, error, info, init_default_global_tracing, warn};
    use tskv::engine::Engine;
    use tskv::{error, kv_option, Task, TimeRange, TsKv};

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
    #[test]
    #[serial]
    fn test_kvcore_read() {
        init_default_global_tracing("tskv_log", "tskv.log", "debug");
        let (rt, tskv) = get_tskv();

        let database = "db".to_string();
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
            tokio::time::sleep(Duration::from_secs(1)).await;
        });

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

        let fields_id = fields_id.iter().map(|id| *id as u32).collect();

        let output = tskv.read(
            &database,
            sids,
            &TimeRange::new(0, Local::now().timestamp_millis() + 100),
            fields_id,
        );

        info!("{:#?}", output);
    }

    #[test]
    #[serial]
    fn test_kvcore_delete_cache() {
        init_default_global_tracing("tskv_log", "tskv.log", "debug");
        let (rt, tskv) = get_tskv();
        let database = "db".to_string();
        let mut fbb = flatbuffers::FlatBufferBuilder::new();
        let points = models_helper::create_random_points_with_delta(&mut fbb, 5);
        fbb.finish(points, None);
        let points = fbb.finished_data().to_vec();
        let request = kv_service::WritePointsRpcRequest { version: 1, points };

        rt.block_on(async {
            tskv.write(request.clone()).await.unwrap();
            tokio::time::sleep(Duration::from_secs(3)).await;
        });

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

        let fields_id: Vec<u32> = fields_id.iter().map(|id| *id as u32).collect();

        tskv.read(
            &database,
            sids.clone(),
            &TimeRange::new(0, Local::now().timestamp_millis() + 100),
            fields_id.clone(),
        );

        info!("delete delta data");
        tskv.delete_series(&database, &sids, &[1], &TimeRange::new(1, 1))
            .unwrap();
        tskv.read(
            &database,
            sids.clone(),
            &TimeRange::new(0, Local::now().timestamp_millis() + 100),
            fields_id,
        );
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
    fn test_kvcore_start() {
        init_default_global_tracing("tskv_log", "tskv.log", "debug");
        let (rt, tskv) = get_tskv();

        let (wal_sender, wal_receiver) = channel::unbounded();
        let (tx, rx) = channel();

        let mut fbb = flatbuffers::FlatBufferBuilder::new();
        let points = models_helper::create_random_points_with_delta(&mut fbb, 1);
        fbb.finish(points, None);
        let points = fbb.finished_data().to_vec();
        let req = kv_service::WritePointsRpcRequest { version: 1, points };

        rt.block_on(async {
            wal_sender
                .send(Task::WritePoints { req, tx })
                .await
                .unwrap();
            TsKv::start(Arc::new(tskv), wal_receiver);
            match rx.await {
                Ok(Ok(_)) => info!("successful"),
                _ => panic!("wrong"),
            };
        });
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
            tskv.write(request).await.unwrap();
            tokio::time::sleep(Duration::from_secs(3)).await;
        })
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
}
