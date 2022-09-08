#[cfg(test)]
mod test {
    use crate::memcache::{DataType, I64Cell, StrCell};
    use crate::tsm::{coders, DataBlock};
    use bzip2::write::BzEncoder;
    use bzip2::Compression as CompressionBzip;
    use chrono::Local;
    use compression::prelude::{Action, EncodeExt, LzssCode, LzssEncoder};
    use flate2::write::ZlibEncoder;
    use flate2::write::{GzDecoder, GzEncoder};
    use flate2::Compression as CompressionGzip;
    use lzzzz::lz4;
    use models::ValueType;
    use q_compress::{auto_compress, auto_decompress, DEFAULT_COMPRESSION_LEVEL};
    use rand::distributions::Alphanumeric;
    use rand::{thread_rng, Rng};
    use std::cmp::Ordering;
    use std::io;
    use std::io::Write;
    use std::mem::size_of_val;
    use std::ops::Deref;
    use std::thread::sleep;
    use std::time::Duration;
    use zstd::stream::Encoder as ZstdEncoder;
    use zstd::zstd_safe::WriteBuf;

    static SIZES: [usize; 4] = [10, 100, 1000, 10000];
    static ALLSTR: [&str; 10] = [
        "beijing",
        "shanghai",
        "guangzhou",
        "shenzhen",
        "wuhan",
        "qingdao",
        "beihai",
        "nanjing",
        "chengdu",
        "shijiazhuang",
    ];

    fn gen_str() -> String {
        let s: String = rand::thread_rng()
            .sample_iter(&Alphanumeric)
            .take(20)
            .map(char::from)
            .collect();
        s
    }

    #[test]
    fn test_block_encode_i64_without_order() {
        for size in SIZES {
            let mut data = vec![];
            let mut buf = vec![];
            for i in 0..size {
                data.push(rand::random::<i64>());
            }

            println!("data before encode {}", size_of_val(&*data));

            coders::integer::encode(&data[0..size], &mut buf).unwrap();

            println!("data after encode {}", size_of_val(&*buf));
        }
    }

    #[test]
    fn test_block_encode_i64_small_without_order() {
        for size in SIZES {
            let mut data = vec![];
            let mut buf = vec![];
            for i in 0..size {
                data.push(rand::random::<i64>() % 1000);
            }

            println!("data before encode {}", size_of_val(&*data));

            coders::integer::encode(&data[0..size], &mut buf).unwrap();

            println!("data after encode {}", size_of_val(&*buf));
        }
    }

    #[test]
    fn test_block_encode_i64_with_order() {
        for size in SIZES {
            let mut data = vec![];
            let mut buf = vec![];
            for i in 0..size {
                data.push(Local::now().timestamp_millis());
                sleep(Duration::from_micros(1000));
            }

            println!("data before encode {}", size_of_val(&*data));

            coders::integer::encode(&data[0..size], &mut buf).unwrap();

            println!("data after encode {}", size_of_val(&*buf));
        }
    }

    #[test]
    fn test_block_encode_bool() {
        for size in SIZES {
            let mut data = vec![];
            let mut buf = vec![];
            for i in 0..size {
                data.push(rand::random::<bool>());
            }

            println!("data before encode {}", size_of_val(&*data));

            coders::boolean::encode(&data[0..size], &mut buf).unwrap();

            println!("data after encode {}", size_of_val(&*buf));
        }
    }

    #[test]
    fn test_block_encode_str() {
        for size in SIZES {
            let mut str = vec![];
            let mut data = vec![];
            let mut buf = vec![];
            for i in 0..size {
                data.push(gen_str());
            }

            for i in data.iter() {
                str.push(i.as_bytes());
            }

            println!("data before encode {}", size_of_val(&*data));

            coders::string::encode(&str[0..size], &mut buf).unwrap();

            println!("data after encode {}", size_of_val(&*buf));
        }

        println!("-------------------");

        for size in SIZES {
            let mut str = vec![];
            let mut data = vec![];
            let mut buf = vec![];
            for i in 0..size {
                data.push(String::from_utf8(b"hello,world".to_vec()).unwrap());
            }

            for i in data.iter() {
                str.push(i.as_bytes());
            }

            println!("data before encode {}", size_of_val(&*data));

            coders::string::encode(&str[0..size], &mut buf).unwrap();

            println!("data after encode {}", size_of_val(&*buf));
        }

        println!("------------------");

        for size in SIZES {
            let mut str = vec![];
            let mut buf = vec![];
            let mut size_str = 0;
            for i in 0..size {
                str.push(ALLSTR[(rand::random::<u64>() % 10) as usize].as_bytes());
                size_str += size_of_val(&*str[i]);
            }
            println!("data before encode {}", size_str);

            coders::string::encode(&str[0..size], &mut buf).unwrap();

            println!("data after encode {}", size_of_val(&*buf));
        }
    }

    #[test]
    fn test_block_encode_gzip_i64() {
        for size in SIZES {
            let mut data = vec![];
            let mut buf = vec![];
            for i in 0..size {
                data.push(rand::random::<i64>());
            }

            println!("data before encode {}", size_of_val(&*data));

            let mut encoder = GzEncoder::new(vec![], CompressionGzip::default());
            let mut data_u8 = vec![];
            for i in data.iter() {
                data_u8.extend_from_slice(i.to_be_bytes().as_slice());
            }
            encoder.write_all(&data_u8).unwrap();
            buf.append(&mut encoder.finish().unwrap());

            println!("data after encode {}", size_of_val(&*buf));
        }
    }

    #[test]
    fn test_block_encode_bzip_i64() {
        for size in SIZES {
            let mut data = vec![];
            let mut buf = vec![];
            for i in 0..size {
                data.push(rand::random::<i64>());
            }

            println!("data before encode {}", size_of_val(&*data));

            let mut encoder = BzEncoder::new(vec![], CompressionBzip::default());
            let mut data_u8 = vec![];
            for i in data.iter() {
                data_u8.extend_from_slice(i.to_be_bytes().as_slice());
            }
            encoder.write_all(&data_u8).unwrap();
            buf.append(&mut encoder.finish().unwrap());

            println!("data after encode {}", size_of_val(&*buf));
        }
    }

    #[test]
    fn test_block_encode_gzip_i64_with_order() {
        for size in SIZES {
            let mut data = vec![];
            let mut buf = vec![];
            for i in 0..size {
                data.push(Local::now().timestamp_millis());
                sleep(Duration::from_micros(1000));
            }

            println!("data before encode {}", size_of_val(&*data));

            let mut encoder = GzEncoder::new(vec![], CompressionGzip::default());
            let mut data_u8 = vec![];
            for i in data.iter() {
                data_u8.extend_from_slice(i.to_be_bytes().as_slice());
            }
            encoder.write_all(&data_u8).unwrap();
            buf.append(&mut encoder.finish().unwrap());

            println!("data after encode {}", size_of_val(&*buf));
        }
    }

    #[test]
    fn test_block_encode_bzip_i64_with_order() {
        for size in SIZES {
            let mut data = vec![];
            let mut buf = vec![];
            for i in 0..size {
                data.push(Local::now().timestamp_millis());
                sleep(Duration::from_micros(1000));
            }

            println!("data before encode {}", size_of_val(&*data));

            let mut encoder = BzEncoder::new(vec![], CompressionBzip::default());
            let mut data_u8 = vec![];
            for i in data.iter() {
                data_u8.extend_from_slice(i.to_be_bytes().as_slice());
            }
            encoder.write_all(&data_u8).unwrap();
            buf.append(&mut encoder.finish().unwrap());

            println!("data after encode {}", size_of_val(&*buf));
        }
    }

    #[test]
    fn test_block_encode_q_compress() {
        for size in SIZES {
            let mut data = vec![];
            for i in 0..size {
                data.push(rand::random::<i64>());
            }

            println!("data before encode {}", size_of_val(&*data));

            let buf = auto_compress(&data, DEFAULT_COMPRESSION_LEVEL);

            println!("data after encode {}", size_of_val(&*buf));
        }
    }

    #[test]
    fn test_block_encode_f64() {
        for size in SIZES {
            let uniform = rand::distributions::Uniform::new(0.0, 10000000.0);
            let mut data = vec![];
            let mut buf = vec![];

            let mut rng = thread_rng();
            for i in 0..size {
                data.push(rng.sample(uniform))
            }

            println!("data before encode {}", size_of_val(&*data));

            coders::float::encode(&data[0..size], &mut buf).unwrap();

            println!("data after encode {}", size_of_val(&*buf))
        }

        println!("-------------------");

        for size in SIZES {
            let uniform = rand::distributions::Uniform::new(0.0, 10000000.0);
            let mut data = vec![];
            let mut rng = thread_rng();
            for i in 0..size {
                data.push(rng.sample(uniform))
            }

            println!("data before encode {}", size_of_val(&*data));

            let buf = auto_compress(&data, DEFAULT_COMPRESSION_LEVEL);

            println!("data after encode {}", size_of_val(&*buf))
        }

        println!("-------------------");

        for size in SIZES {
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

            println!("data before encode {}", size_of_val(&*data));

            zstd::stream::copy_encode(data_u8.as_slice(), &mut buf, 3).unwrap();

            println!("data after encode {}", size_of_val(&*buf))
        }
    }

    #[test]
    fn test_block_encode_f64_with_order() {
        for size in SIZES {
            let uniform = rand::distributions::Uniform::new(0.0, 10000000.0);
            let mut data = vec![];
            let mut buf = vec![];

            let mut rng = thread_rng();
            for i in 0..size {
                data.push(rng.sample(uniform))
            }

            data.sort_by(|a, b| b.partial_cmp(a).unwrap());

            println!("data before encode {}", size_of_val(&*data));

            coders::float::encode(&data[0..size], &mut buf).unwrap();

            println!("data after encode {}", size_of_val(&*buf))
        }

        println!("-------------------");

        for size in SIZES {
            let uniform = rand::distributions::Uniform::new(0.0, 10000000.0);
            let mut data = vec![];
            let mut rng = thread_rng();
            for i in 0..size {
                data.push(rng.sample(uniform))
            }

            data.sort_by(|a, b| b.partial_cmp(a).unwrap());

            println!("data before encode {}", size_of_val(&*data));

            let buf = auto_compress(&data, DEFAULT_COMPRESSION_LEVEL);

            println!("data after encode {}", size_of_val(&*buf))
        }

        println!("--------------");

        for size in SIZES {
            let uniform = rand::distributions::Uniform::new(0.0, 10000000.0);
            let mut data = vec![];
            let mut data_u8 = vec![];
            let mut buf = vec![];
            let mut rng = thread_rng();
            for i in 0..size {
                data.push(rng.sample(uniform))
            }

            data.sort_by(|a, b| b.partial_cmp(a).unwrap());

            for i in data.iter() {
                data_u8.extend_from_slice(((*i) as f64).to_be_bytes().as_slice());
            }

            println!("data before encode {}", size_of_val(&*data));

            zstd::stream::copy_encode(data_u8.as_slice(), &mut buf, 3).unwrap();

            println!("data after encode {}", size_of_val(&*buf))
        }

        println!("--------------");

        for size in SIZES {
            let uniform = rand::distributions::Uniform::new(0.0, 10000000.0);

            let mut str = vec![];
            let mut data = vec![];
            let mut rng = thread_rng();

            for i in 0..size {
                data.push(rng.sample(uniform));
            }

            data.sort_by(|a, b| b.partial_cmp(a).unwrap());

            for i in data.iter() {
                str.extend_from_slice(((*i) as f64).to_be_bytes().as_slice());
            }

            println!("data before encode {}", size_of_val(&*data));

            let mut encoder = GzEncoder::new(vec![], CompressionGzip::default());
            encoder.write_all(&str).unwrap();

            println!(
                "data after encode {}",
                size_of_val(&*encoder.finish().unwrap())
            );
        }
    }

    #[test]
    fn test_block_encode_small_f64() {
        for size in SIZES {
            let uniform = rand::distributions::Uniform::new(0.0, 1000.0);
            let mut data = vec![];
            let mut buf = vec![];

            let mut rng = thread_rng();
            for i in 0..size {
                data.push(rng.sample(uniform));
            }

            println!("data before encode {}", size_of_val(&*data));

            coders::float::encode(&data[0..size], &mut buf).unwrap();

            println!("data after encode {}", size_of_val(&*buf))
        }

        println!("-------------------");

        for size in SIZES {
            let uniform = rand::distributions::Uniform::new(0.0, 1000.0);
            let mut data = vec![];
            let mut rng = thread_rng();
            for i in 0..size {
                data.push(rng.sample(uniform))
            }

            println!("data before encode {}", size_of_val(&*data));

            let buf = auto_compress(&data, DEFAULT_COMPRESSION_LEVEL);

            println!("data after encode {}", size_of_val(&*buf))
        }

        println!("--------------");

        for size in SIZES {
            let uniform = rand::distributions::Uniform::new(0.0, 1000.0);
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

            println!("data before encode {}", size_of_val(&*data));

            zstd::stream::copy_encode(data_u8.as_slice(), &mut buf, 3).unwrap();

            println!("data after encode {}", size_of_val(&*buf))
        }

        println!("-----------------");

        for size in SIZES {
            let uniform = rand::distributions::Uniform::new(0.0, 1000.0);

            let mut str = vec![];
            let mut data = vec![];
            let mut rng = thread_rng();

            for i in 0..size {
                data.push(rng.sample(uniform));
            }

            for i in data.iter() {
                str.extend_from_slice(((*i) as f64).to_be_bytes().as_slice());
            }

            println!("data before encode {}", size_of_val(&*data));

            let mut encoder = GzEncoder::new(vec![], CompressionGzip::default());
            encoder.write_all(&str).unwrap();

            println!(
                "data after encode {}",
                size_of_val(&*encoder.finish().unwrap())
            );
        }
    }

    #[test]
    fn test_block_encode_normal_f64() {
        for size in SIZES {
            let normal = rand_distr::Normal::new(1500.0, 2.0).unwrap();
            let mut data = vec![];
            let mut buf = vec![];

            let mut rng = thread_rng();
            for i in 0..size {
                data.push(rng.sample(normal));
            }

            println!("data before encode {}", size_of_val(&*data));

            coders::float::encode(&data[0..size], &mut buf).unwrap();

            println!("data after encode {}", size_of_val(&*buf))
        }

        println!("-------------------");

        for size in SIZES {
            let normal = rand_distr::Normal::new(1500.0, 2.0).unwrap();
            let mut data = vec![];
            let mut rng = thread_rng();
            for i in 0..size {
                data.push(rng.sample(normal));
            }

            println!("data before encode {}", size_of_val(&*data));

            let buf = auto_compress(&data, DEFAULT_COMPRESSION_LEVEL);

            println!("data after encode {}", size_of_val(&*buf))
        }

        println!("--------------");

        for size in SIZES {
            let normal = rand_distr::Normal::new(1500.0, 2.0).unwrap();
            let mut data = vec![];
            let mut data_u8 = vec![];
            let mut buf = vec![];
            let mut rng = thread_rng();
            for i in 0..size {
                data.push(rng.sample(normal));
            }

            for i in data.iter() {
                data_u8.extend_from_slice(((*i) as f64).to_be_bytes().as_slice());
            }

            println!("data before encode {}", size_of_val(&*data));

            zstd::stream::copy_encode(data_u8.as_slice(), &mut buf, 3).unwrap();

            println!("data after encode {}", size_of_val(&*buf))
        }

        println!("-----------------");

        for size in SIZES {
            let normal = rand_distr::Normal::new(1500.0, 2.0).unwrap();

            let mut str = vec![];
            let mut data = vec![];
            let mut rng = thread_rng();

            for i in 0..size {
                data.push(rng.sample(normal));
            }

            for i in data.iter() {
                str.extend_from_slice(((*i) as f64).to_be_bytes().as_slice());
            }

            println!("data before encode {}", size_of_val(&*data));

            let mut encoder = GzEncoder::new(vec![], CompressionGzip::default());
            encoder.write_all(&str).unwrap();

            println!(
                "data after encode {}",
                size_of_val(&*encoder.finish().unwrap())
            );
        }
    }

    #[test]
    fn test_block_encode_str_gzip() {
        for size in SIZES {
            let mut str = vec![];
            let mut data = vec![];
            for i in 0..size {
                data.push(gen_str());
            }

            for i in data.iter() {
                str.extend_from_slice(i.as_bytes());
            }

            println!("data before encode {}", size_of_val(&*data));

            let mut encoder = GzEncoder::new(vec![], CompressionGzip::default());
            encoder.write_all(&str).unwrap();

            println!(
                "data after encode {}",
                size_of_val(&*encoder.finish().unwrap())
            );
        }

        println!("-------------------");

        for size in SIZES {
            let mut str = vec![];
            let mut data = vec![];
            for i in 0..size {
                data.push(String::from_utf8(b"hello,world".to_vec()).unwrap());
            }

            for i in data.iter() {
                str.extend_from_slice(i.as_bytes());
            }

            println!("data before encode {}", size_of_val(&*data));
            let mut encoder = GzEncoder::new(vec![], CompressionGzip::default());
            encoder.write_all(&str).unwrap();

            println!(
                "data after encode {}",
                size_of_val(&*encoder.finish().unwrap())
            );
        }

        println!("------------------");

        for size in SIZES {
            let mut str = vec![];
            for i in 0..size {
                str.extend_from_slice(ALLSTR[(rand::random::<u64>() % 10) as usize].as_bytes());
            }
            println!("data before encode {}", size_of_val(&*str));

            let mut encoder = GzEncoder::new(vec![], CompressionGzip::default());
            encoder.write_all(&str).unwrap();

            println!(
                "data after encode {}",
                size_of_val(&*encoder.finish().unwrap())
            );
        }
    }

    #[test]
    fn test_block_encode_str_bzip() {
        for size in SIZES {
            let mut str = vec![];
            let mut data = vec![];
            for i in 0..size {
                data.push(gen_str());
            }

            for i in data.iter() {
                str.extend_from_slice(i.as_bytes());
            }

            println!("data before encode {}", size_of_val(&*data));

            let mut encoder = BzEncoder::new(vec![], CompressionBzip::default());
            encoder.write_all(&str).unwrap();

            println!(
                "data after encode {}",
                size_of_val(&*encoder.finish().unwrap())
            );
        }

        println!("-------------------");

        for size in SIZES {
            let mut str = vec![];
            let mut data = vec![];
            for i in 0..size {
                data.push(String::from_utf8(b"hello,world".to_vec()).unwrap());
            }

            for i in data.iter() {
                str.extend_from_slice(i.as_bytes());
            }

            println!("data before encode {}", size_of_val(&*data));
            let mut encoder = BzEncoder::new(vec![], CompressionBzip::default());
            encoder.write_all(&str).unwrap();

            println!(
                "data after encode {}",
                size_of_val(&*encoder.finish().unwrap())
            );
        }

        println!("------------------");

        for size in SIZES {
            let mut str = vec![];
            for i in 0..size {
                str.extend_from_slice(ALLSTR[(rand::random::<u64>() % 10) as usize].as_bytes());
            }
            println!("data before encode {}", size_of_val(&*str));

            let mut encoder = BzEncoder::new(vec![], CompressionBzip::default());
            encoder.write_all(&str).unwrap();

            println!(
                "data after encode {}",
                size_of_val(&*encoder.finish().unwrap())
            );
        }
    }

    #[test]
    fn test_block_encode_str_zstd() {
        for size in SIZES {
            let mut str = vec![];
            let mut data = vec![];
            let mut buf = vec![];
            for i in 0..size {
                data.push(gen_str());
            }

            for i in data.iter() {
                str.extend_from_slice(i.as_bytes());
            }

            println!("data before encode {}", size_of_val(&*data));

            zstd::stream::copy_encode(str.as_slice(), &mut buf, 3).unwrap();

            println!("data after encode {}", size_of_val(&*buf));
        }

        println!("-------------------");

        for size in SIZES {
            let mut str = vec![];
            let mut data = vec![];
            let mut buf = vec![];
            for i in 0..size {
                data.push(String::from_utf8(b"hello,world".to_vec()).unwrap());
            }

            for i in data.iter() {
                str.extend_from_slice(i.as_bytes());
            }

            println!("data before encode {}", size_of_val(&*data));
            zstd::stream::copy_encode(str.as_slice(), &mut buf, 3).unwrap();

            println!("data after encode {}", size_of_val(&*buf));
        }

        println!("------------------");

        for size in SIZES {
            let mut str = vec![];
            let mut buf = vec![];
            for i in 0..size {
                str.extend_from_slice(ALLSTR[(rand::random::<u64>() % 10) as usize].as_bytes());
            }
            println!("data before encode {}", size_of_val(&*str));

            zstd::stream::copy_encode(str.as_slice(), &mut buf, 3).unwrap();

            println!("data after encode {}", size_of_val(&*buf));
        }
    }

    #[test]
    fn test_block_encode_str_zlib() {
        for size in SIZES {
            let mut str = vec![];
            let mut data = vec![];
            for i in 0..size {
                data.push(gen_str());
            }

            for i in data.iter() {
                str.extend_from_slice(i.as_bytes());
            }

            println!("data before encode {}", size_of_val(&*data));

            let mut encoder = ZlibEncoder::new(vec![], CompressionGzip::default());
            encoder.write_all(&str).unwrap();

            println!(
                "data after encode {}",
                size_of_val(&*encoder.finish().unwrap())
            );
        }

        println!("-------------------");

        for size in SIZES {
            let mut str = vec![];
            let mut data = vec![];
            for i in 0..size {
                data.push(String::from_utf8(b"hello,world".to_vec()).unwrap());
            }

            for i in data.iter() {
                str.extend_from_slice(i.as_bytes());
            }

            println!("data before encode {}", size_of_val(&*data));
            let mut encoder = ZlibEncoder::new(vec![], CompressionGzip::default());
            encoder.write_all(&str).unwrap();

            println!(
                "data after encode {}",
                size_of_val(&*encoder.finish().unwrap())
            );
        }

        println!("------------------");

        for size in SIZES {
            let mut str = vec![];
            for i in 0..size {
                str.extend_from_slice(ALLSTR[(rand::random::<u64>() % 10) as usize].as_bytes());
            }
            println!("data before encode {}", size_of_val(&*str));

            let mut encoder = ZlibEncoder::new(vec![], CompressionGzip::default());
            encoder.write_all(&str).unwrap();

            println!(
                "data after encode {}",
                size_of_val(&*encoder.finish().unwrap())
            );
        }
    }

    #[test]
    fn test_block_encode_str_lzzzz() {
        for size in SIZES {
            let mut str = vec![];
            let mut data = vec![];
            let mut buf = vec![];
            let mut de_data = vec![];
            for i in 0..size {
                data.push(gen_str());
            }

            for i in data.iter() {
                str.extend_from_slice(i.as_bytes());
            }

            de_data.resize(str.len(), 0);

            println!("data before encode {}", size_of_val(&*data));

            lz4::compress_to_vec(&str, &mut buf, lz4::ACC_LEVEL_DEFAULT).unwrap();
            println!("data after encode {}", size_of_val(&*buf));
            lz4::decompress(&buf, &mut de_data).unwrap();
            assert_eq!(de_data, str);
        }

        println!("-------------------");

        for size in SIZES {
            let mut str = vec![];
            let mut data = vec![];
            let mut buf = vec![];
            for i in 0..size {
                data.push(String::from_utf8(b"hello,world".to_vec()).unwrap());
            }

            for i in data.iter() {
                str.extend_from_slice(i.as_bytes());
            }

            println!("data before encode {}", size_of_val(&*data));
            lz4::compress_to_vec(&str, &mut buf, lz4::ACC_LEVEL_DEFAULT).unwrap();
            println!("data after encode {}", size_of_val(&*buf));
        }

        println!("------------------");

        for size in SIZES {
            let mut str = vec![];
            let mut buf = vec![];
            for i in 0..size {
                str.extend_from_slice(ALLSTR[(rand::random::<u64>() % 10) as usize].as_bytes());
            }
            println!("data before encode {}", size_of_val(&*str));

            lz4::compress_to_vec(&str, &mut buf, lz4::ACC_LEVEL_DEFAULT).unwrap();
            println!("data after encode {}", size_of_val(&*buf));
        }
    }

    #[test]
    fn test_block_encode_str_lzss() {
        pub fn comparison(lhs: LzssCode, rhs: LzssCode) -> Ordering {
            match (lhs, rhs) {
                (
                    LzssCode::Reference {
                        len: llen,
                        pos: lpos,
                    },
                    LzssCode::Reference {
                        len: rlen,
                        pos: rpos,
                    },
                ) => ((llen << 3) + rpos).cmp(&((rlen << 3) + lpos)).reverse(),
                (LzssCode::Symbol(_), LzssCode::Symbol(_)) => Ordering::Equal,
                (_, LzssCode::Symbol(_)) => Ordering::Greater,
                (LzssCode::Symbol(_), _) => Ordering::Less,
            }
        }

        for size in SIZES {
            let mut str = vec![];
            let mut data = vec![];
            for i in 0..size {
                data.push(gen_str());
            }

            for i in data.iter() {
                str.extend_from_slice(i.as_bytes());
            }

            println!("data before encode {}", size_of_val(&*data));

            let buf = str
                .into_iter()
                .clone()
                .encode(
                    &mut LzssEncoder::new(comparison, 0x1_0000, 256, 3, 3),
                    Action::Finish,
                )
                .collect::<Result<Vec<_>, _>>()
                .unwrap();
            println!("data after encode {}", size_of_val(&*buf));
        }

        println!("-------------------");

        for size in SIZES {
            let mut str = vec![];
            let mut data = vec![];
            for i in 0..size {
                data.push(String::from_utf8(b"hello,world".to_vec()).unwrap());
            }

            for i in data.iter() {
                str.extend_from_slice(i.as_bytes());
            }

            println!("data before encode {}", size_of_val(&*data));
            let buf = str
                .into_iter()
                .clone()
                .encode(
                    &mut LzssEncoder::new(comparison, 0x1_0000, 256, 3, 3),
                    Action::Finish,
                )
                .collect::<Result<Vec<_>, _>>()
                .unwrap();
            println!("data after encode {}", size_of_val(&*buf));
        }

        println!("------------------");

        for size in SIZES {
            let mut str = vec![];
            for i in 0..size {
                str.extend_from_slice(ALLSTR[(rand::random::<u64>() % 10) as usize].as_bytes());
            }
            println!("data before encode {}", size_of_val(&*str));

            let buf = str
                .into_iter()
                .clone()
                .encode(
                    &mut LzssEncoder::new(comparison, 100, 256, 3, 3),
                    Action::Finish,
                )
                .collect::<Result<Vec<_>, _>>()
                .unwrap();

            println!("data after encode {}", size_of_val(&*buf));
        }
    }

    #[test]
    fn test_block_encode_i32() {
        for size in SIZES {
            let mut data = vec![];
            for i in 0..size {
                data.push(rand::random::<i32>());
            }

            println!("data before encode {}", size_of_val(&*data));

            let buf = auto_compress(&data, DEFAULT_COMPRESSION_LEVEL);

            println!("data after encode {}", size_of_val(&*buf));
        }

        println!("---------------");

        for size in SIZES {
            let mut data = vec![];
            for i in 0..size {
                data.push(rand::random::<i32>() % 1000);
            }

            data.sort();

            println!("data before encode {}", size_of_val(&*data));

            let buf = auto_compress(&data, DEFAULT_COMPRESSION_LEVEL);

            println!("data after encode {}", size_of_val(&*buf));
        }

        println!("--------------");

        for size in SIZES {
            let mut data = vec![];
            let mut buf = vec![];
            for i in 0..size {
                data.push(rand::random::<i32>());
            }

            println!("data before encode {}", size_of_val(&*data));

            let mut encoder = GzEncoder::new(vec![], CompressionGzip::default());
            let mut data_u8 = vec![];
            for i in data.iter() {
                data_u8.extend_from_slice(i.to_be_bytes().as_slice());
            }
            encoder.write_all(&data_u8).unwrap();
            buf.append(&mut encoder.finish().unwrap());

            println!("data after encode {}", size_of_val(&*buf));
        }

        println!("----------------");
        for size in SIZES {
            let mut data = vec![];
            let mut buf = vec![];
            for i in 0..size {
                data.push(rand::random::<i32>());
            }

            println!("data before encode {}", size_of_val(&*data));

            let mut encoder = BzEncoder::new(vec![], CompressionBzip::default());
            let mut data_u8 = vec![];
            for i in data.iter() {
                data_u8.extend_from_slice(i.to_be_bytes().as_slice());
            }
            encoder.write_all(&data_u8).unwrap();
            buf.append(&mut encoder.finish().unwrap());

            println!("data after encode {}", size_of_val(&*buf));
        }
    }
}
