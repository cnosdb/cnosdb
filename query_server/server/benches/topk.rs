#[macro_use]
extern crate criterion;
extern crate datafusion;

use criterion::async_executor::FuturesExecutor;
use futures::StreamExt;
use std::sync::Arc;

use crate::criterion::Criterion;
use datafusion::arrow::record_batch::RecordBatch;
use server::instance::{make_cnosdbms, Cnosdbms};
use spi::server::dbms::DatabaseManagerSystem;
use spi::service::protocol::Query;
use tskv::engine::MockEngine;

fn generate_data(n: usize) -> String {
    let rows: Vec<String> = (0..n)
        .into_iter()
        .map(|i| {
            format!(
                "({}, '{}----xxxxxx=====3333444hhhhhhxx324r9cc')",
                i % 1000,
                i % 100
            )
        })
        .collect();
    // .reduce(|l, r| {
    //     format!("{}, {}", l, r)
    // }).unwrap();

    let result = rows.join(",");

    result
}

async fn exec_sql(db: &Cnosdbms, sql: &str) -> Vec<RecordBatch> {
    let query = Query::new(Default::default(), sql.to_string());

    // let db = make_cnosdbms(Arc::new(MockEngine::default())).unwrap();
    let mut actual = vec![];

    let mut result = db.execute(&query).await.unwrap();

    for ele in result.result().iter_mut() {
        while let Some(next) = ele.next().await {
            actual.push(next.unwrap());
        }
    }
    actual
}

fn criterion_benchmark(c: &mut Criterion) {
    let db = make_cnosdbms(Arc::new(MockEngine::default())).unwrap();

    let sql = format!(
        "SELECT * FROM 
    (VALUES  {}) AS t (num,letter) 
    order by num limit 20",
        generate_data(1_000_000)
    );

    c.bench_function("top_100", |b| {
        b.to_async(FuturesExecutor {}).iter(|| exec_sql(&db, &sql))
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
