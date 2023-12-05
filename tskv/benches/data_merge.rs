#[macro_use]
extern crate criterion;
extern crate datafusion;

use tskv::reader::test_util::{
    collect_stream, create_sort_merge_stream, random_record_batches, test_schema,
};

use crate::criterion::Criterion;

fn data_merge(c: &mut Criterion) {
    let schema = test_schema();
    let batch_size = 4096;
    let batch_num = 100;
    let column_name = "time";
    let batches = random_record_batches(
        schema.clone(),
        column_name,
        batch_size,
        batch_num,
        8,
        Some(100000),
    );
    let stream = create_sort_merge_stream(schema.clone(), batches.clone(), column_name, batch_size);
    let ans = collect_stream(stream)
        .iter()
        .map(|b| b.num_rows())
        .sum::<usize>();

    c.bench_function("sort_merge", |b| {
        b.iter(|| {
            let stream =
                create_sort_merge_stream(schema.clone(), batches.clone(), column_name, batch_size);
            let res = collect_stream(stream)
                .iter()
                .map(|b| b.num_rows())
                .sum::<usize>();
            assert_eq!(ans, res)
        })
    });

    // c.bench_function("dedup_merge", |b| {
    //     b.iter(|| {
    //         let stream =
    //             create_dedup_stream(schema.clone(), batches.clone(), batch_size, column_name);
    //         let res = collect_df_stream(stream)
    //             .iter()
    //             .map(|b| b.num_rows())
    //             .sum::<usize>();
    //         assert_eq!(ans, res)
    //     })
    // });
}

criterion_group!(benches, data_merge);
criterion_main!(benches);
