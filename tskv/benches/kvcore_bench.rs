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

fn test_concurrent_write(tskv: Arc<Mutex<TsKv>>, request: WritePointsRpcRequest) {
    let rt = Runtime::new().unwrap();
    for _ in 0..2 {
        let req = request.clone();
        rt.block_on(tskv.lock().write(req)).unwrap();
    }
}

fn test_insert_cache(tskv: Arc<Mutex<TsKv>>, points: &[u8]) {
    let rt = Runtime::new().unwrap();
    rt.block_on(tskv.lock().insert_cache(1, points)).unwrap();
}

fn test_concurrent_insert_cache(tskv: Arc<Mutex<TsKv>>, points: &[u8]) {
    let rt = Runtime::new().unwrap();
    for _ in 0..2 {
        rt.block_on(tskv.lock().insert_cache(1, points)).unwrap();
    }
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

    c.bench_function("write", |b| b.iter(||
        test_write(tskv.clone(), request.clone(),
        )));
    c.bench_function("insert_cache", |b| b.iter(||
        test_insert_cache(tskv.clone(), points_str.clone())
    ));
}

criterion_group!(benches,run);
criterion_main!(benches);
