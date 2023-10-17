#[macro_use]
extern crate criterion;

use std::sync::Arc;

use cache::{AsyncCache, ShardedAsyncCache, ShardedSyncCache, SyncCache};
use rand::random;

use crate::criterion::Criterion;

fn criterion_benchmark(c: &mut Criterion) {
    let lru = Arc::new(ShardedSyncCache::<i8, Arc<i8>>::create_lru_sharded_cache(
        128,
    ));
    let mut group = c.benchmark_group("bench cache");
    group.bench_function("bench random insert", |b| {
        b.iter(|| {
            lru.insert(random(), Arc::new(random()));
        })
    });
    group.bench_function("bench random get", |b| {
        b.iter(|| {
            lru.get(&random());
        })
    });
    group.bench_function("bench get / insert", |b| {
        b.iter(|| {
            let k = random();
            let _ = match lru.get(&k) {
                Some(v) => v,
                None => {
                    let v = Arc::new(k);
                    lru.insert(k, v.clone());
                    v
                }
            };
        })
    });
    group.finish();

    let rt = Arc::new(tokio::runtime::Runtime::new().unwrap());
    let lru = Arc::new(ShardedAsyncCache::<i8, Arc<i8>>::create_lru_sharded_cache(
        128,
    ));
    let mut group = c.benchmark_group("bench async cache");
    group.bench_function("bench random insert", |b| {
        b.to_async(rt.as_ref()).iter(|| async {
            lru.insert(random(), Arc::new(random())).await;
        })
    });
    group.bench_function("bench random get", |b| {
        b.to_async(rt.as_ref()).iter(|| async {
            lru.get(&random()).await;
        })
    });
    group.bench_function("bench get / insert", |b| {
        b.to_async(rt.as_ref()).iter(|| async {
            let k = random();
            let _ = match lru.get(&k).await {
                Some(v) => v,
                None => {
                    let v = Arc::new(k);
                    lru.insert(k, v.clone()).await;
                    v.clone()
                }
            };
        })
    });
    group.finish();
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
