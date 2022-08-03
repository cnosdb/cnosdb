use criterion::{criterion_group, criterion_main, Criterion};
use std::fs::OpenOptions;
use tskv::direct_io::*;

fn test_direct_io(c: &mut Criterion) {
    let fs = FileSystem::new(&Options::default());
    let mut test_file = fs
        .open_with(
            "/tmp/direct_io/test_file",
            OpenOptions::new().read(true).write(true).create(true),
        )
        .unwrap()
        .into_cursor();

    let mut buf: Vec<u8> = vec![];
    while buf.len() < 1024 * 512 {
        buf.push(rand::random::<u8>());
    }

    c.bench_function("direct_io", |b| {
        b.iter(|| {
            for _ in 0..100 {
                test_file.write(&buf).unwrap();
            }
        })
    });
    fs.sync_all(FileSync::Hard).unwrap();
}

criterion_group!(benches, test_direct_io);
criterion_main!(benches);
