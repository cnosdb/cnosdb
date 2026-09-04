use arrow::buffer::NullBuffer;
use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use models::codec::Encoding;
use tskv::tsm::codec::get_f64_codec;

fn generate_sensor_data(n: usize) -> Vec<f64> {
    (0..n)
        .map(|i| {
            let t = i as f64 * 0.1;
            let temp = 20.0 + 10.0 * (t * 0.05).sin() + ((i % 7) as f64 * 0.01);
            (temp * 100.0).round() / 100.0
        })
        .collect()
}

fn bench_float_codecs(c: &mut Criterion) {
    let mut group = c.benchmark_group("float_codecs_sensor_data");
    let n = 10_000;
    let data = generate_sensor_data(n);
    let null_bitset = NullBuffer::new_valid(n);

    let gorilla = get_f64_codec(Encoding::Gorilla);
    let pco = get_f64_codec(Encoding::Quantile);
    let alp = get_f64_codec(Encoding::Alp);

    group.throughput(Throughput::Elements(n as u64));

    // Encode benchmarks
    group.bench_function(BenchmarkId::new("encode", "gorilla"), |b| {
        let mut dst = Vec::with_capacity(n * 8);
        b.iter(|| {
            dst.clear();
            gorilla
                .encode(black_box(&data), black_box(&mut dst))
                .unwrap();
        });
    });

    group.bench_function(BenchmarkId::new("encode", "pco"), |b| {
        let mut dst = Vec::with_capacity(n * 8);
        b.iter(|| {
            dst.clear();
            pco.encode(black_box(&data), black_box(&mut dst)).unwrap();
        });
    });

    group.bench_function(BenchmarkId::new("encode", "fastalp"), |b| {
        let mut dst = Vec::with_capacity(n * 8);
        b.iter(|| {
            dst.clear();
            alp.encode(black_box(&data), black_box(&mut dst)).unwrap();
        });
    });

    // Prepare compressed buffers for decode benchmarks
    let mut gorilla_buf = Vec::new();
    gorilla.encode(&data, &mut gorilla_buf).unwrap();

    let mut pco_buf = Vec::new();
    pco.encode(&data, &mut pco_buf).unwrap();

    let mut alp_buf = Vec::new();
    alp.encode(&data, &mut alp_buf).unwrap();

    // Print compression ratio summary
    let raw_bytes = n * 8;
    println!("\nCompression ratios on {} sensor float points:", n);
    println!(
        "Gorilla: {} bytes ({:.2}x, {:.3} B/pt)",
        gorilla_buf.len(),
        raw_bytes as f64 / gorilla_buf.len() as f64,
        gorilla_buf.len() as f64 / n as f64
    );
    println!(
        "Pco:     {} bytes ({:.2}x, {:.3} B/pt)",
        pco_buf.len(),
        raw_bytes as f64 / pco_buf.len() as f64,
        pco_buf.len() as f64 / n as f64
    );
    println!(
        "fastalp: {} bytes ({:.2}x, {:.3} B/pt)",
        alp_buf.len(),
        raw_bytes as f64 / alp_buf.len() as f64,
        alp_buf.len() as f64 / n as f64
    );

    // Decode benchmarks
    group.bench_function(BenchmarkId::new("decode", "gorilla"), |b| {
        b.iter(|| {
            black_box(
                gorilla
                    .decode_to_array(black_box(&gorilla_buf), black_box(&null_bitset))
                    .unwrap(),
            );
        });
    });

    group.bench_function(BenchmarkId::new("decode", "pco"), |b| {
        b.iter(|| {
            black_box(
                pco.decode_to_array(black_box(&pco_buf), black_box(&null_bitset))
                    .unwrap(),
            );
        });
    });

    group.bench_function(BenchmarkId::new("decode", "fastalp"), |b| {
        b.iter(|| {
            black_box(
                alp.decode_to_array(black_box(&alp_buf), black_box(&null_bitset))
                    .unwrap(),
            );
        });
    });

    group.finish();
}

criterion_group!(benches, bench_float_codecs);
criterion_main!(benches);
