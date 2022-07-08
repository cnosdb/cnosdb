use std::borrow::Borrow;
use std::ops::Deref;
use std::sync::Arc;

use criterion::{black_box, criterion_group, criterion_main, Criterion};

use tokio::runtime::Runtime;
use tokio::sync::mpsc::unbounded_channel;
use tokio::sync::oneshot::channel;
use protos::{kv_service::WritePointsRpcRequest, models_helper};
use tskv::{Task, TsKv, kv_option::WalConfig};
use parking_lot::Mutex;

async fn get_tskv() -> TsKv {
    let opt = tskv::kv_option::Options {
        wal: WalConfig {
            dir: String::from("/tmp/test/wal"),
            ..Default::default()
        },
        ..Default::default()
    };

    TsKv::open(opt).await.unwrap()
}

fn test_write(tskv: Arc<Mutex<TsKv>>, request: WritePointsRpcRequest) {
    let rt = Runtime::new().unwrap();
    rt.block_on(tskv.lock().write(request)).unwrap();
}

fn test_insert_cache(tskv: Arc<Mutex<TsKv>>, points: &[u8]) {
    let rt = Runtime::new().unwrap();
    rt.block_on(tskv.lock().insert_cache(1, points)).unwrap();
}

fn big_write(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let tskv = rt.block_on(get_tskv());

    let database = "db".to_string();
    let mut fbb = flatbuffers::FlatBufferBuilder::new();
    let points = models_helper::create_big_random_points(&mut fbb, 1);
    fbb.finish(points, None);
    let points = fbb.finished_data().to_vec();
    let request = WritePointsRpcRequest { version: 1, database, points };

    //maybe 250 ms
    c.bench_function("big_write", |b| b.iter(|| {
        let rt = Runtime::new().unwrap();
        rt.block_on(tskv.write(request.clone())).unwrap()
    }));
}

fn run(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let tskv = Arc::new(Mutex::new(rt.block_on(get_tskv())));
    let database = "db".to_string();
    let mut fbb = flatbuffers::FlatBufferBuilder::new();
    let points = models_helper::create_random_points(&mut fbb, 1);
    fbb.finish(points, None);
    let points_str = fbb.finished_data();
    let points = points_str.to_vec();
    let request = WritePointsRpcRequest { version: 1, database, points };

    //maybe 500 us
    c.bench_function("write", |b| b.iter(||
        test_write(tskv.clone(), request.clone(),
        )));
    //maybe 200 us
    c.bench_function("insert_cache", |b| b.iter(||
        test_insert_cache(tskv.clone(), points_str.clone())
    ));
}

criterion_group!(benches, run, big_write);
criterion_main!(benches);
