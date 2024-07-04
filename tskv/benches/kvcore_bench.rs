use std::sync::Arc;

use criterion::{criterion_group, criterion_main, Criterion};
use memory_pool::GreedyMemoryPool;
use meta::model::meta_admin::AdminMeta;
use meta::model::MetaRef;
use metrics::metric_register::MetricsRegister;
use models::meta_data::VnodeId;
use protos::kv_service::{raft_write_command, WriteDataRequest};
use protos::models_helper;
use tokio::runtime::{self, Runtime};
use tskv::{Engine, TsKv};
use utils::precision::Precision;

async fn get_tskv() -> TsKv {
    let mut global_config = config::tskv::get_config_for_test();
    global_config.wal.path = "/tmp/test_bench/wal".to_string();
    let opt = tskv::kv_option::Options::from(&global_config);

    let runtime = Arc::new(
        runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap(),
    );

    let meta_manager: MetaRef =
        AdminMeta::new(global_config.clone(), Arc::new(MetricsRegister::default())).await;

    meta_manager.add_data_node().await.unwrap();

    TsKv::open(
        meta_manager,
        opt,
        runtime,
        Arc::new(GreedyMemoryPool::default()),
        Arc::new(MetricsRegister::default()),
    )
    .await
    .unwrap()
}

fn tskv_write(
    rt: Arc<Runtime>,
    tskv: &TsKv,
    tenant: &str,
    db: &str,
    id: VnodeId,
    index: u64,
    request: WriteDataRequest,
) {
    let apply_ctx = replication::ApplyContext {
        index,
        raft_id: id.into(),
        apply_type: replication::APPLY_TYPE_WRITE,
    };
    let vnode_store = Arc::new(rt.block_on(tskv.open_tsfamily(tenant, db, id)).unwrap());

    let command = raft_write_command::Command::WriteData(request);
    rt.block_on(vnode_store.apply(&apply_ctx, command)).unwrap();
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
            let rt = Arc::new(Runtime::new().unwrap());
            let tskv = rt.block_on(get_tskv());

            for index in 0..50 {
                let mut fbb = flatbuffers::FlatBufferBuilder::new();
                let points = models_helper::create_big_random_points(&mut fbb, "big_write", 10);
                fbb.finish(points, None);
                let points = fbb.finished_data().to_vec();
                let request = WriteDataRequest {
                    data: points,
                    precision: Precision::NS as u32,
                };

                tskv_write(
                    rt.clone(),
                    &tskv,
                    "cnosdb",
                    "public",
                    0,
                    index,
                    request.clone(),
                );
            }
        })
    });
}

#[allow(dead_code)]
fn run(c: &mut Criterion) {
    let rt = Arc::new(Runtime::new().unwrap());
    let tskv = rt.block_on(get_tskv());
    let _database = "db".to_string();
    let mut fbb = flatbuffers::FlatBufferBuilder::new();
    let points = models_helper::create_random_points_with_delta(&mut fbb, 1);
    fbb.finish(points, None);
    let points_str = fbb.finished_data();
    let points = points_str.to_vec();
    let request = WriteDataRequest {
        data: points,
        precision: Precision::NS as u32,
    };

    // maybe 500 us
    let mut index = 0;
    c.bench_function("write", |b| {
        b.iter(|| {
            index += 1;
            tskv_write(
                rt.clone(),
                &tskv,
                "cnosdb",
                "public",
                0,
                index,
                request.clone(),
            );
        })
    });
    // maybe 200 us
    // c.bench_function("insert_cache", |b| {
    //     b.iter(|| test_insert_cache(tskv.clone(), &(*points_str)))
    // });
}

criterion_group!(benches, big_write);
criterion_main!(benches);
