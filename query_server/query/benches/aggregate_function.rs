#[macro_use]
extern crate criterion;
extern crate datafusion;

mod data_utils;
use crate::criterion::Criterion;

fn criterion_benchmark(c: &mut Criterion) {
    let partitions_len = 8;
    let array_len = 3276800 * 2;
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

    let mut gauge_agg_group = c.benchmark_group("gauge_agg");
    gauge_agg_group
        .sample_size(10)
        .bench_function("aggregate_query_gauge_agg", |b| {
            b.iter(|| data_utils::query(ctx.clone(), "select gauge_agg(ts, f64) FROM t"))
        });
    gauge_agg_group.finish();

    let mut state_agg_group = c.benchmark_group("state_agg");
    state_agg_group
        .sample_size(10)
        .bench_function("aggregate_query_state_agg", |b| {
            b.iter(|| data_utils::query(ctx.clone(), "select state_agg(ts, f64) from t"))
        });
    state_agg_group.finish();

    c.bench_function("aggregate_query_no_group_by_first", |b| {
        b.iter(|| {
            data_utils::query(
                ctx.clone(),
                "SELECT first(ts, f64) \
                 FROM t",
            )
        })
    });

    c.bench_function("aggregate_query_no_group_by_mode", |b| {
        b.iter(|| {
            data_utils::query(
                ctx.clone(),
                "SELECT mode(f64) \
                 FROM t",
            )
        })
    });

    c.bench_function("aggregate_query_no_group_by_increase", |b| {
        b.iter(|| data_utils::query(ctx.clone(), "SELECT increase(ts, f64 order by ts) FROM t"))
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
