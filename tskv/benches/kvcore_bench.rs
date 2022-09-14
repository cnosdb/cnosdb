use chrono::Local;
use std::io::Write;
use std::sync::Arc;
use std::thread::sleep;
use std::time::Duration;

use bzip2::write::{BzDecoder, BzEncoder};
use bzip2::Compression as CompressionBzip;
use criterion::{criterion_group, criterion_main, Criterion};
use flate2::write::{GzDecoder, GzEncoder, ZlibDecoder, ZlibEncoder};
use flate2::Compression as CompressionGzip;
use lzzzz::lz4;
use parking_lot::Mutex;
use q_compress::{auto_compress, auto_decompress, DEFAULT_COMPRESSION_LEVEL};
use rand::distributions::Alphanumeric;
use rand::{thread_rng, Rng};

use models::ValueType;
use tokio::runtime::Runtime;

use protos::{kv_service::WritePointsRpcRequest, models_helper};
use tskv::memcache::{DataType, I64Cell};
use tskv::tsm::{coders, DataBlock};
use tskv::{engine::Engine, TsKv};

fn encode_time(c: &mut Criterion) {
    fn gen_str() -> String {
        let s: String = rand::thread_rng()
            .sample_iter(&Alphanumeric)
            .take(20)
            .map(char::from)
            .collect();
        s
    }

    let size = 1000;

    // zigzag simple8b
    let mut data = vec![];
    let mut buf = vec![];
    for i in 0..size {
        data.push(rand::random::<i64>());
    }

    c.bench_function("i64 zigzag simple8b encode_time", |b| {
        b.iter(|| {
            coders::integer::encode(&data[0..size], &mut buf).unwrap();
        })
    });

    //i64 gzip
    let mut data = vec![];
    let mut buf = vec![];
    for i in 0..size {
        data.push(rand::random::<i64>());
    }

    c.bench_function("i64 gzip encode_time", |b| {
        b.iter(|| {
            let mut encoder = GzEncoder::new(vec![], CompressionGzip::default());
            let mut data_u8 = vec![];
            for i in data.iter() {
                data_u8.extend_from_slice(i.to_be_bytes().as_slice());
            }
            encoder.write_all(&data_u8).unwrap();
            buf.append(&mut encoder.finish().unwrap());
        })
    });

    //i64 bzip
    let mut data = vec![];
    let mut buf = vec![];
    for i in 0..size {
        data.push(rand::random::<i64>());
    }
    c.bench_function("i64 bzip encode_time", |b| {
        b.iter(|| {
            let mut encoder = BzEncoder::new(vec![], CompressionBzip::default());
            let mut data_u8 = vec![];
            for i in data.iter() {
                data_u8.extend_from_slice(i.to_be_bytes().as_slice());
            }
            encoder.write_all(&data_u8).unwrap();
            buf.append(&mut encoder.finish().unwrap());
        })
    });

    //i64 q_compress
    let mut data = vec![];
    for i in 0..size {
        data.push(rand::random::<i64>());
    }
    c.bench_function("i64 q_compress encode_time", |b| {
        b.iter(|| {
            let buf = auto_compress(&data, DEFAULT_COMPRESSION_LEVEL);
        })
    });

    //f64 Gorilla
    let uniform = rand::distributions::Uniform::new(0.0, 10000000.0);
    let mut data = vec![];
    let mut buf = vec![];

    let mut rng = thread_rng();
    for i in 0..size {
        data.push(rng.sample(uniform))
    }
    c.bench_function("f64 Gorilla encode_time", |b| {
        b.iter(|| {
            coders::float::encode(&data[0..size], &mut buf).unwrap();
        })
    });

    // f64 q_compress
    let uniform = rand::distributions::Uniform::new(0.0, 10000000.0);
    let mut data = vec![];
    let mut rng = thread_rng();
    for i in 0..size {
        data.push(rng.sample(uniform))
    }

    c.bench_function("f64 q_compress encode_time", |b| {
        b.iter(|| {
            let buf = auto_compress(&data, DEFAULT_COMPRESSION_LEVEL);
        })
    });

    // f64 zstd
    let uniform = rand::distributions::Uniform::new(0.0, 10000000.0);
    let mut data = vec![];
    let mut data_u8 = vec![];
    let mut buf = vec![];
    let mut rng = thread_rng();
    for i in 0..size {
        data.push(rng.sample(uniform))
    }

    for i in data.iter() {
        data_u8.extend_from_slice(((*i) as f64).to_be_bytes().as_slice());
    }

    c.bench_function("f64 zstd encode_time", |b| {
        b.iter(|| {
            for i in data.iter() {
                data_u8.extend_from_slice(((*i) as f64).to_be_bytes().as_slice());
            }
            zstd::stream::copy_encode(data_u8.as_slice(), &mut buf, 3).unwrap();
        })
    });

    // f64 gzip
    let uniform = rand::distributions::Uniform::new(0.0, 10000000.0);

    let mut str = vec![];
    let mut data = vec![];
    let mut rng = thread_rng();

    for i in 0..size {
        data.push(rng.sample(uniform));
    }

    c.bench_function("f64 gzip encode_time", |b| {
        b.iter(|| {
            for i in data.iter() {
                str.extend_from_slice(((*i) as f64).to_be_bytes().as_slice());
            }
            let mut encoder = GzEncoder::new(vec![], CompressionGzip::default());
            encoder.write_all(&str).unwrap();
        })
    });

    // str snappy
    let mut str = vec![];
    let mut data = vec![];
    let mut buf = vec![];
    for i in 0..size {
        data.push(gen_str());
    }

    for i in data.iter() {
        str.push(i.as_bytes());
    }

    c.bench_function("str snappy encode_time", |b| {
        b.iter(|| {
            coders::string::encode(&str[0..size], &mut buf).unwrap();
        })
    });

    // str gzip
    let mut str = vec![];
    let mut data = vec![];
    for i in 0..size {
        data.push(gen_str());
    }

    c.bench_function("str gzip encode_time", |b| {
        b.iter(|| {
            for i in data.iter() {
                str.extend_from_slice(i.as_bytes());
            }

            let mut encoder = GzEncoder::new(vec![], CompressionGzip::default());
            encoder.write_all(&str).unwrap();
            encoder.finish().unwrap();

            str.clear();
        })
    });

    // str bzip
    let mut str = vec![];
    let mut data = vec![];
    for i in 0..size {
        data.push(gen_str());
    }

    c.bench_function("str bzip encode_time", |b| {
        b.iter(|| {
            for i in data.iter() {
                str.extend_from_slice(i.as_bytes());
            }

            let mut encoder = BzEncoder::new(vec![], CompressionBzip::default());
            encoder.write_all(&str).unwrap();
            encoder.finish().unwrap();

            str.clear();
        })
    });

    // str zstd
    let mut str = vec![];
    let mut data = vec![];
    let mut buf = vec![];
    for i in 0..size {
        data.push(gen_str());
    }

    c.bench_function("str zstd encode_time", |b| {
        b.iter(|| {
            for i in data.iter() {
                str.extend_from_slice(i.as_bytes());
            }
            zstd::stream::copy_encode(str.as_slice(), &mut buf, 3).unwrap();
            str.clear();
        })
    });

    // str zlib
    let mut str = vec![];
    let mut data = vec![];
    for i in 0..size {
        data.push(gen_str());
    }

    c.bench_function("str zlib encode_time", |b| {
        b.iter(|| {
            for i in data.iter() {
                str.extend_from_slice(i.as_bytes());
            }

            let mut encoder = ZlibEncoder::new(vec![], CompressionGzip::default());
            encoder.write_all(&str).unwrap();

            str.clear();
        })
    });

    // str lzzzz
    let mut str = vec![];
    let mut data = vec![];
    let mut buf = vec![];
    for i in 0..size {
        data.push(gen_str());
    }

    c.bench_function("str lzzzz encode_time", |b| {
        b.iter(|| {
            for i in data.iter() {
                str.extend_from_slice(i.as_bytes());
            }

            lz4::compress_to_vec(&str, &mut buf, lz4::ACC_LEVEL_DEFAULT).unwrap();

            str.clear();
            buf.clear();
        })
    });
}

fn decode_time(c: &mut Criterion) {
    fn gen_str() -> String {
        let s: String = rand::thread_rng()
            .sample_iter(&Alphanumeric)
            .take(20)
            .map(char::from)
            .collect();
        s
    }

    let size = 1000;

    //i64 zigzag simple8b
    let mut data = vec![];
    let mut buf = vec![];
    for i in 0..size {
        data.push(rand::random::<i64>());
    }
    coders::integer::encode(&data[0..size], &mut buf).unwrap();

    c.bench_function("i64 zigzag simple8b decode_time", |b| {
        b.iter(|| {
            coders::integer::decode(&buf, &mut vec![]).unwrap();
        })
    });

    // i64 gzip
    let mut data = vec![];
    let mut buf = vec![];
    for i in 0..size {
        data.push(rand::random::<i64>());
    }
    let mut encoder = GzEncoder::new(vec![], CompressionGzip::default());
    let mut data_u8 = vec![];
    for i in data.iter() {
        data_u8.extend_from_slice(i.to_be_bytes().as_slice());
    }
    encoder.write_all(&data_u8).unwrap();
    buf.append(&mut encoder.finish().unwrap());

    c.bench_function("i64 gzip decode_time", |b| {
        b.iter(|| {
            let mut decoder = GzDecoder::new(vec![]);
            decoder.write_all(&buf).unwrap();
            decoder.finish().unwrap();
        })
    });

    // i64 bzip
    let mut data = vec![];
    let mut buf = vec![];
    for i in 0..size {
        data.push(rand::random::<i64>());
    }
    let mut encoder = BzEncoder::new(vec![], CompressionBzip::default());
    let mut data_u8 = vec![];
    for i in data.iter() {
        data_u8.extend_from_slice(i.to_be_bytes().as_slice());
    }
    encoder.write_all(&data_u8).unwrap();
    buf.append(&mut encoder.finish().unwrap());

    c.bench_function("i64 bzip decode_time", |b| {
        b.iter(|| {
            let mut decoder = BzDecoder::new(vec![]);
            decoder.write_all(&buf).unwrap();
            decoder.finish().unwrap();
        })
    });

    // i64 q_compress
    let mut data = vec![];
    for i in 0..size {
        data.push(rand::random::<i64>());
    }
    let buf = auto_compress(&data, DEFAULT_COMPRESSION_LEVEL);

    c.bench_function("i64 q compress decode_time", |b| {
        b.iter(|| {
            let de_data: Vec<i64> = auto_decompress(&buf).unwrap();
        })
    });

    // f64 Gorilla
    let uniform = rand::distributions::Uniform::new(0.0, 10000000.0);
    let mut data = vec![];
    let mut buf = vec![];

    let mut rng = thread_rng();
    for i in 0..size {
        data.push(rng.sample(uniform))
    }

    coders::float::encode(&data[0..size], &mut buf).unwrap();

    c.bench_function("f64 Gorilla decode_time", |b| {
        b.iter(|| {
            coders::float::decode(&buf, &mut vec![]).unwrap();
        })
    });

    // f64 q_compress
    let uniform = rand::distributions::Uniform::new(0.0, 10000000.0);
    let mut data = vec![];
    let mut rng = thread_rng();
    for i in 0..size {
        data.push(rng.sample(uniform))
    }

    let buf = auto_compress(&data, DEFAULT_COMPRESSION_LEVEL);

    c.bench_function("f64 q compress decode_time", |b| {
        b.iter(|| {
            let de_data: Vec<f64> = auto_decompress(&buf).unwrap();
        })
    });

    // f64 zstd
    let uniform = rand::distributions::Uniform::new(0.0, 10000000.0);
    let mut data = vec![];
    let mut data_u8 = vec![];
    let mut buf = vec![];
    let mut rng = thread_rng();
    for i in 0..size {
        data.push(rng.sample(uniform))
    }

    for i in data.iter() {
        data_u8.extend_from_slice(((*i) as f64).to_be_bytes().as_slice());
    }

    zstd::stream::copy_encode(data_u8.as_slice(), &mut buf, 3).unwrap();

    c.bench_function("f64 zstd decode_time", |b| {
        b.iter(|| {
            zstd::stream::copy_decode(buf.as_slice(), vec![]).unwrap();
        })
    });

    // f64 gzip
    let mut data = vec![];
    let mut buf = vec![];
    for i in 0..size {
        data.push(rand::random::<f64>());
    }
    let mut encoder = GzEncoder::new(vec![], CompressionGzip::default());
    let mut data_u8 = vec![];
    for i in data.iter() {
        data_u8.extend_from_slice(i.to_be_bytes().as_slice());
    }
    encoder.write_all(&data_u8).unwrap();
    buf.append(&mut encoder.finish().unwrap());

    c.bench_function("f64 gzip decode_time", |b| {
        b.iter(|| {
            let mut decoder = GzDecoder::new(vec![]);
            decoder.write_all(&buf).unwrap();
            decoder.finish().unwrap();
        })
    });

    // str snappy
    let mut str = vec![];
    let mut data = vec![];
    let mut buf = vec![];
    for i in 0..size {
        data.push(gen_str());
    }

    for i in data.iter() {
        str.push(i.as_bytes());
    }
    coders::string::encode(&str[0..size], &mut buf).unwrap();

    c.bench_function("str snappy decode_time", |b| {
        b.iter(|| coders::string::decode(&buf, &mut vec![]))
    });

    // str gzip
    let mut str = vec![];
    let mut data = vec![];
    for i in 0..size {
        data.push(gen_str());
    }

    for i in data.iter() {
        str.extend_from_slice(i.as_bytes());
    }

    let mut encoder = GzEncoder::new(vec![], CompressionGzip::default());
    encoder.write_all(&str).unwrap();
    let buf = encoder.finish().unwrap();

    c.bench_function("str gzip decode_time", |b| {
        b.iter(|| {
            let mut decoder = GzDecoder::new(vec![]);
            decoder.write_all(&buf).unwrap();
            decoder.finish().unwrap();
        })
    });

    // str bzip
    let mut str = vec![];
    let mut data = vec![];
    for i in 0..size {
        data.push(gen_str());
    }

    for i in data.iter() {
        str.extend_from_slice(i.as_bytes());
    }

    let mut encoder = BzEncoder::new(vec![], CompressionBzip::default());
    encoder.write_all(&str).unwrap();
    let buf = encoder.finish().unwrap();

    c.bench_function("str bzip decode_time", |b| {
        b.iter(|| {
            let mut decoder = BzDecoder::new(vec![]);
            decoder.write_all(&buf).unwrap();
            decoder.finish().unwrap();
        })
    });

    // str zstd
    let mut str = vec![];
    let mut data = vec![];
    let mut buf = vec![];
    for i in 0..size {
        data.push(gen_str());
    }

    for i in data.iter() {
        str.extend_from_slice(i.as_bytes());
    }

    zstd::stream::copy_encode(str.as_slice(), &mut buf, 3).unwrap();

    c.bench_function("str zstd decode_time", |b| {
        b.iter(|| {
            zstd::stream::copy_decode(buf.as_slice(), vec![]).unwrap();
        })
    });

    // str zlib
    let mut str = vec![];
    let mut data = vec![];
    for i in 0..size {
        data.push(gen_str());
    }

    for i in data.iter() {
        str.extend_from_slice(i.as_bytes());
    }

    let mut encoder = ZlibEncoder::new(vec![], CompressionGzip::default());
    encoder.write_all(&str).unwrap();
    let buf = encoder.finish().unwrap();

    c.bench_function("str zlib decode_time", |b| {
        b.iter(|| {
            let mut decoder = ZlibDecoder::new(vec![]);
            decoder.write_all(&buf).unwrap();
            decoder.finish().unwrap();
        })
    });

    // str lzzzz
    let mut str = vec![];
    let mut data = vec![];
    let mut buf = vec![];
    for i in 0..size {
        data.push(gen_str());
    }

    for i in data.iter() {
        str.extend_from_slice(i.as_bytes());
    }

    lz4::compress_to_vec(&str, &mut buf, lz4::ACC_LEVEL_DEFAULT).unwrap();

    c.bench_function("str lzzzz decode_time", |b| {
        b.iter(|| {
            lz4::decompress(&buf, &mut [0; 20000]).unwrap();
        })
    });
}

criterion_group!(benches, decode_time);
criterion_main!(benches);
