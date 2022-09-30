use std::sync::Arc;

use criterion::{criterion_group, criterion_main, Criterion};
use parking_lot::Mutex;

use tokio::runtime::{self, Runtime};

use protos::{kv_service::WritePointsRpcRequest, models_helper};
use tskv::{engine::Engine, TsKv};

async fn get_tskv() -> TsKv {
    let mut global_config = config::get_config("../config/config.toml");
    global_config.wal.path = "/tmp/test_bench/wal".to_string();
    let opt = tskv::kv_option::Options::from(&global_config);

    let runtime = Arc::new(
        runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap(),
    );

    TsKv::open(opt, runtime).await.unwrap()
}

fn test_write(tskv: Arc<Mutex<TsKv>>, request: WritePointsRpcRequest) {
    let rt = Runtime::new().unwrap();
    rt.block_on(tskv.lock().write(request)).unwrap();
}

// fn test_insert_cache(tskv: Arc<Mutex<TsKv>>, buf: &[u8]) {
//     let rt = Runtime::new().unwrap();
//     let ps = flatbuffers::root::<fb_models::Points>(buf)
//         .context(error::InvalidFlatbufferSnafu)
//         .unwrap();
//     if let Some(inmem_points) = ps.points().map(|points| {
//         points
//             .iter()
//             .map(|p| InMemPoint::from_flatbuffers(&p).unwrap())
//             .collect()
//     }) {
//         rt.block_on(tskv.lock().insert_cache(&1.to_string(), 0, &inmem_points));
//     }
// }

fn big_write(c: &mut Criterion) {
    c.bench_function("big_write", |b| {
        b.iter(|| {
            let rt = Runtime::new().unwrap();
            let tskv = rt.block_on(get_tskv());
            for _i in 0..50 {
                let _database = "db".to_string();
                let mut fbb = flatbuffers::FlatBufferBuilder::new();
                let points = models_helper::create_big_random_points(&mut fbb, 10);
                fbb.finish(points, None);
                let points = fbb.finished_data().to_vec();

                let request = WritePointsRpcRequest { version: 1, points };
                rt.block_on(tskv.write(request)).unwrap();
            }
        })
    });
}

#[allow(dead_code)]
fn run(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let tskv = Arc::new(Mutex::new(rt.block_on(get_tskv())));
    let _database = "db".to_string();
    let mut fbb = flatbuffers::FlatBufferBuilder::new();
    let points = models_helper::create_random_points_with_delta(&mut fbb, 1);
    fbb.finish(points, None);
    let points_str = fbb.finished_data();
    let points = points_str.to_vec();
    let request = WritePointsRpcRequest { version: 1, points };

    // maybe 500 us
    c.bench_function("write", |b| {
        b.iter(|| test_write(tskv.clone(), request.clone()))
    });
    // maybe 200 us
    // c.bench_function("insert_cache", |b| {
    //     b.iter(|| test_insert_cache(tskv.clone(), &(*points_str)))
    // });
}

criterion_group!(benches, big_write);
criterion_main!(benches);
