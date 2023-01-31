use std::sync::Arc;

use criterion::{criterion_group, criterion_main, Criterion};
use lru_cache::ShardedCache;

fn random() -> i8 {
    rand::random::<i8>()
}

fn bench_sharded_cache(c: &mut Criterion) {
    let lru = Arc::new(ShardedCache::<i8, Arc<i8>>::default());
    c.bench_function("bench random insert", |b| {
        b.iter(|| {
            lru.insert(random(), Arc::new(random()));
        })
    });
    c.bench_function("bench random get", |b| {
        b.iter(|| {
            lru.get(&random());
        })
    });
    c.bench_function("bench get / insert", |b| {
        b.iter(|| {
            let k = random();
            let _ = match lru.get(&k) {
                Some(v) => v.clone(),
                None => {
                    let v = Arc::new(k);
                    let v_ref = lru.insert(k, v).unwrap();
                    v_ref.clone()
                }
            };
        })
    });
}

criterion_group!(benches, bench_sharded_cache);
criterion_main!(benches);
