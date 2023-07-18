#[macro_use]
extern crate criterion;
extern crate datafusion;

mod data_utils;
use crate::criterion::Criterion;

fn criterion_benchmark(c: &mut Criterion) {
    let partitions_len = 8;
    let array_len = 32768000 * 2;
    let batch_size = 2048; // 2^11
    let ctx = data_utils::create_context(partitions_len, array_len, batch_size).unwrap();

    c.bench_function("aggregate_query_no_group_by_min", |b| {
        b.iter(|| {
            data_utils::query(
                ctx.clone(),
                "SELECT MIN(f64) \
                 FROM t",
            )
        })
    });

    for i in [10, 100, 1000] {
        c.bench_function(&format!("aggregate_query_no_group_by_sample_{i}"), |b| {
            b.iter(|| {
                data_utils::query(
                    ctx.clone(),
                    &format!(
                        "SELECT sample(f64, {i}) \
                     FROM t"
                    ),
                )
            })
        });
    }
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
