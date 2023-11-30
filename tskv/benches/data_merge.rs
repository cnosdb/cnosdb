#[macro_use]
extern crate criterion;
extern crate datafusion;

use tskv::reader::test_util::{
    collect_stream, create_cut_merge_stream, create_sort_batches, create_sort_merge_stream,
    random_record_batches, test_schema,
};

use crate::criterion::Criterion;

fn data_merge(c: &mut Criterion) {
    let schema = test_schema();
    let batch_size = 4096;
    let batch_num = 100;
    let column_name = "time";
    let batches = random_record_batches(schema.clone(), batch_size, batch_num, column_name);
    c.bench_function("sort_merge", |b| {
        b.iter(|| {
            let stream = create_sort_merge_stream(
                schema.clone(),
                batch_size,
                column_name,
                batches.clone(),
                batch_num / 4,
            );
            collect_stream(stream)
        })
    });

    let batches = create_sort_batches(schema.clone(), batch_size, 4, batch_num, column_name);
    c.bench_function("cut_merge", |b| {
        b.iter(|| {
            let stream =
                create_cut_merge_stream(schema.clone(), batches.clone(), batch_size, column_name);
            collect_stream(stream)
        })
    });
}

criterion_group!(benches, data_merge);
criterion_main!(benches);
