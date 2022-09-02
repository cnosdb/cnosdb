#[macro_use]
extern crate criterion;
extern crate datafusion;

mod data_utils;
use crate::criterion::Criterion;

fn criterion_benchmark(c: &mut Criterion) {
    let partitions_len = 8;
    let array_len = 32768 * 2; // 2^16
    let batch_size = 2048; // 2^11
    let ctx = data_utils::create_context(partitions_len, array_len, batch_size).unwrap();

    c.bench_function("scalar_query_sqrt", |b| {
        b.iter(|| {
            data_utils::query(
                ctx.clone(),
                "SELECT sqrt(f64) \
                 FROM t",
            )
        })
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
