use std::io;

use criterion::{criterion_group, criterion_main, Criterion};
use snafu::{ResultExt, Snafu};

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("This an error with it's base: {source}"))]
    WithSource { source: std::io::Error },

    #[snafu(display("This an error with external'{external}': {source}"))]
    WithExternal {
        external: String,
        source: std::io::Error,
    },
}

fn foo(i: usize) -> Result<(), io::Error> {
    if i == 0 {
        Ok(())
    } else {
        Err(match i {
            1 => io::ErrorKind::Other.into(),
            2 => io::ErrorKind::Other.into(),
            _ => io::ErrorKind::Other.into(),
        })
    }
}

fn bench_with_source(c: &mut Criterion) {
    let len = 10;
    let mut numbers = Vec::with_capacity(len);
    for _ in 0..numbers.len() {
        numbers.push(rand::random::<usize>() % 3);
    }

    let mut idx = 0_usize;
    c.bench_function("bench_snafu_with_source", |b| {
        b.iter(|| {
            let i = unsafe { numbers.get_unchecked(idx % len) };
            idx += 1;

            let r = foo(*i).context(WithSourceSnafu);
            drop(r);
        });
    });

    idx = 0;
    c.bench_function("bench_map_err_with_source", |b| {
        b.iter(|| {
            let i = unsafe { numbers.get_unchecked(idx % len) };
            idx += 1;

            let r = foo(*i).map_err(|e| Error::WithSource { source: e });
            drop(r);
        });
    });
}

fn bench_with_external(c: &mut Criterion) {
    let len = 100;
    let mut numbers = Vec::with_capacity(len);
    for _ in 0..len {
        numbers.push(rand::random::<usize>() % 3);
    }
    let base_err_message = "Failed to get file_metadata".to_string();

    let mut idx = 0_usize;
    c.bench_function("bench_snafu_with_external", |b| {
        b.iter(|| {
            let i = unsafe { numbers.get_unchecked(idx % len) };
            idx += 1;

            let r = foo(*i).context(WithExternalSnafu {
                external: format!("Failed to run foo: {}", &base_err_message),
            });
            drop(r);
        });
    });

    idx = 0;
    c.bench_function("bench_map_err_with_external", |b| {
        b.iter(|| {
            let i = unsafe { numbers.get_unchecked(idx % len) };
            idx += 1;

            let r = foo(*i).map_err(|e| Error::WithExternal {
                external: format!("Failed to run foo: {}", &base_err_message),
                source: e,
            });
            drop(r);
        });
    });
}

criterion_group!(benches, bench_with_source, bench_with_external);
criterion_main!(benches);
