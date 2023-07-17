use std::error::Error;

// note: encode/decode adapted from influxdb_iox
// https://github.com/influxdata/influxdb_iox/tree/main/influxdb_tsm/src/encoders

/// Encodes a slice of unsigned 64-bit integers into `dst`.
///
/// Deltas between the integers in the input are first calculated, then the
/// deltas are further compressed if possible, either via bit-packing using
/// simple8b or by run-length encoding the deltas if they're all the same.
pub fn u64_zigzag_simple8b_encode(
    src: &[u64],
    dst: &mut Vec<u8>,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let signed = u64_to_i64_vector(src);
    super::integer::i64_zigzag_simple8b_encode(&signed, dst)
}

pub fn u64_q_compress_encode(
    src: &[u64],
    dst: &mut Vec<u8>,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let signed = u64_to_i64_vector(src);
    super::integer::i64_q_compress_encode(&signed, dst)
}

pub fn u64_without_compress_encode(
    src: &[u64],
    dst: &mut Vec<u8>,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let signed = u64_to_i64_vector(src);
    super::integer::i64_without_compress_encode(&signed, dst)
}

pub fn u64_sdt_encode(
    src: &[u64],
    dst: &mut Vec<u8>,
    deviation: f64,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    dst.clear(); // reset buffer.

    if src.is_empty() {
        return Ok(());
    }
    let mut prev_upper_slope = 0.0;
    let mut prev_lower_slope = 0.0;
    let mut home = 0; //store the index of the number which opens the door
    let first_u64 = src[0]; //the number which opens the door
    dst.extend_from_slice(&first_u64.to_be_bytes()); //add the first one
    let mut first = first_u64 as f64;
    for i in 1..src.len() {
        let value = src[i] as f64;
        let check = (i - home) as u32;
        if check == 1 {
            prev_upper_slope = value + deviation - first;
            prev_lower_slope = value - deviation - first;
        }

        let offset = (i - home) as f64;

        let slope = value / offset;

        if slope <= prev_upper_slope && slope >= prev_lower_slope {
            let upper_slope = (value + deviation - first) / offset;
            let lower_slope = (value - deviation - first) / offset;
            if upper_slope < prev_upper_slope {
                prev_upper_slope = upper_slope;
            }
            if lower_slope > prev_lower_slope {
                prev_lower_slope = lower_slope;
            }
            continue;
        }

        home = i - 1;

        let adfirst = src[home];
        first = adfirst as f64;
        let prev_value = adfirst;
        dst.extend_from_slice(&prev_value.to_be_bytes());
        let home_u16 = home as u16;
        dst.extend_from_slice(&home_u16.to_be_bytes());
        prev_upper_slope = value + deviation - first;
        prev_lower_slope = value - deviation - first;
    }

    let adfirst = src[src.len() - 1];
    dst.extend_from_slice(&adfirst.to_be_bytes());
    let len = (src.len() - 1) as u16;
    dst.extend_from_slice(&len.to_be_bytes());
    Ok(())
}

/// Decodes a slice of bytes into a destination vector of unsigned integers.
pub fn u64_zigzag_simple8b_decode(
    src: &[u8],
    dst: &mut Vec<u64>,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    if src.is_empty() {
        return Ok(());
    }
    let mut signed_results = vec![];
    super::integer::i64_zigzag_simple8b_decode(src, &mut signed_results)?;
    dst.clear();
    dst.reserve_exact(signed_results.len() - dst.capacity());
    for s in signed_results {
        dst.push(s as u64);
    }
    Ok(())
}

pub fn u64_q_compress_decode(
    src: &[u8],
    dst: &mut Vec<u64>,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    if src.is_empty() {
        return Ok(());
    }
    let mut signed_results = vec![];
    super::integer::i64_q_compress_decode(src, &mut signed_results)?;
    dst.clear();
    dst.reserve_exact(signed_results.len() - dst.capacity());
    for s in signed_results {
        dst.push(s as u64);
    }
    Ok(())
}

pub fn u64_without_compress_decode(
    src: &[u8],
    dst: &mut Vec<u64>,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    if src.is_empty() {
        return Ok(());
    }
    let mut signed_results = vec![];
    super::integer::i64_without_compress_decode(src, &mut signed_results)?;
    dst.clear();
    dst.reserve_exact(signed_results.len() - dst.capacity());
    for s in signed_results {
        dst.push(s as u64);
    }
    Ok(())
}

pub fn u64_sdt_decode(src: &[u8], dst: &mut Vec<u64>) -> Result<(), Box<dyn Error + Send + Sync>> {
    if src.len() < 8 {
        return Ok(());
    }

    let mut i = 0; //
    let mut buf: [u8; 8] = [0; 8];
    let mut interval: [u8; 2] = [0; 2];
    let mut val_first: u64;
    let mut val: u64;
    let mut home_first: u16;
    let mut home: u16;
    let mut val_first_f64: f64;
    let mut val_f64: f64;
    let mut differ: f64;
    let mut offset: u16;
    let mut slope: f64;
    let mut dec: f64;
    let mut addec;
    //incase the src is short
    if (src.len() - i) <= 10 {
        buf.copy_from_slice(&src[i..i + 8]);
        val_first = u64::from_be_bytes(buf); //as f64
                                             //val_first_f64 = f64::from_bits(val_first);
        dst.push(val_first);
        return Ok(());
    }
    //the first one

    buf.copy_from_slice(&src[i..i + 8]);
    val_first = u64::from_be_bytes(buf);
    val_first_f64 = val_first as f64;
    i += 8;
    home_first = 0;
    loop {
        //the last one
        //let mut cnt = 0;
        if src.len() - i <= 10 {
            dst.push(val_first);
            //println!("{}",i);
            buf.copy_from_slice(&src[i..i + 8]);
            interval.copy_from_slice(&src[i + 8..i + 10]);
            val = u64::from_be_bytes(buf);
            val_f64 = val as f64;

            home = u16::from_be_bytes(interval);
            offset = home - home_first + 1;
            if offset > 2 {
                dec = val_first as f64;
                differ = val_f64 - dec;
                for _ in 0..(offset - 2) {
                    slope = differ / offset as f64;
                    dec += slope;
                    addec = dec as u64;
                    dst.push(addec);
                }
            }
            dst.push(val);
            break;
        }
        dst.push(val_first);

        buf.copy_from_slice(&src[i..i + 8]);
        val = u64::from_be_bytes(buf);
        val_f64 = val as f64;

        interval.copy_from_slice(&src[i + 8..i + 10]);
        home = u16::from_be_bytes(interval);
        offset = home - home_first + 1;
        differ = val_f64 - val_first_f64;
        dec = val_first_f64;
        if offset > 2 {
            slope = differ / offset as f64;
            for _ in 0..(offset - 2) {
                //cnt += 1;
                dec += slope; //as u64  6.3 -> 6
                addec = dec as u64;
                dst.push(addec);
            }
        }

        val_first_f64 = val_f64;
        val_first = val;
        home_first = home;
        i += 10;
    }

    Ok(())
}

// Converts a slice of `u64` values to a `Vec<i64>`.
// TODO(edd): this is expensive as it copies. There are cheap
// but unsafe alternatives to look into such as std::mem::transmute
fn u64_to_i64_vector(src: &[u64]) -> Vec<i64> {
    src.iter().map(|&x| x as i64).collect()
}

#[cfg(test)]
#[allow(clippy::unreadable_literal)]
mod tests {
    use super::super::integer::DeltaEncoding;
    use super::super::simple8b;
    use super::*;
    use crate::tsm::codec::{get_encoding, Encoding};

    #[test]
    fn encode_no_values() {
        let src: Vec<u64> = vec![];
        let mut dst = vec![];

        // check for error
        u64_zigzag_simple8b_encode(&src, &mut dst).expect("failed to encode src");

        // verify encoded no values.
        assert_eq!(dst.len(), 0);
    }

    #[test]
    fn test_sdt_encode_decode() {
        let input: [u64; 22] = [
            3, //0
            9, //1
            1, //2
            9, //3
            2, //4
            1, //5
            1, //6
            8, //7
            4, //8
            2, //9
            4, //10
            1, //11
            1, //12
            1, //13
            1, //14
            1, //15
            3, //16
            1, //17
            1, //18
            1, //19
            1, //20
            3, //21
        ];
        let expected_got: [u64; 22] = [
            3, 9, 1, 9, 2, 1, 1, 2, 4, 3, 3, 2, 2, 2, 1, 1, 1, 1, 1, 2, 2, 3,
        ];
        let threshold = 5.0;
        let mut dst: Vec<u8> = vec![];
        let mut got: Vec<u64> = vec![];
        u64_sdt_encode(&input, &mut dst, threshold).expect("encode wrong");
        u64_sdt_decode(&dst, &mut got).expect("decode wrong");
        assert_eq!(expected_got.to_vec(), got)
    }

    #[test]
    fn encode_uncompressed() {
        let src: Vec<u64> = vec![1000, 0, simple8b::MAX_VALUE, 213123421];
        let mut dst = vec![];

        let exp = src.clone();
        u64_zigzag_simple8b_encode(&src, &mut dst).expect("failed to encode");

        // verify uncompressed encoding used
        assert_eq!(&dst[0] >> 4, DeltaEncoding::Uncompressed as u8);
        let mut got = vec![];
        u64_zigzag_simple8b_decode(&dst, &mut got).expect("failed to decode");

        // verify got same values back
        assert_eq!(got, exp);
    }

    #[test]
    fn encode_q_compress_and_uncompress() {
        let src: Vec<u64> = vec![1000, 0, simple8b::MAX_VALUE, 213123421];
        let mut dst = vec![];
        let mut got = vec![];
        let exp = src.clone();

        u64_q_compress_encode(&src, &mut dst).unwrap();
        let exp_code_type = Encoding::Quantile;
        let got_code_type = get_encoding(&dst);
        assert_eq!(exp_code_type, got_code_type);

        u64_q_compress_decode(&dst, &mut got).unwrap();
        assert_eq!(exp, got);

        dst.clear();
        got.clear();

        u64_without_compress_encode(&src, &mut dst).unwrap();
        let exp_code_type = Encoding::Null;
        let got_code_type = get_encoding(&dst);
        assert_eq!(exp_code_type, got_code_type);

        u64_without_compress_decode(&dst, &mut got).unwrap();
        assert_eq!(exp, got);
    }

    struct Test {
        name: String,
        input: Vec<u64>,
    }

    #[test]
    fn encode_rle() {
        let tests = vec![
            Test {
                name: String::from("no delta"),
                input: vec![123; 8],
            },
            Test {
                name: String::from("delta increasing"),
                input: vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12],
            },
            Test {
                name: String::from("delta decreasing"),
                input: vec![350, 200, 50],
            },
        ];

        for test in tests {
            let mut dst = vec![];
            let src = test.input.clone();
            let exp = test.input;
            u64_zigzag_simple8b_encode(&src, &mut dst).expect("failed to encode");

            // verify RLE encoding used
            assert_eq!(
                &dst[1] >> 4,
                DeltaEncoding::Rle as u8,
                "didn't use rle on {:?}",
                src
            );
            let mut got = vec![];
            u64_zigzag_simple8b_decode(&dst, &mut got).expect("failed to decode");
            // verify got same values back
            assert_eq!(got, exp, "{}", test.name);
        }
    }

    #[test]
    fn encode_rle_byte_for_byte_with_go() {
        let mut dst = vec![];
        let src = vec![1232342341234u64; 1000];
        u64_zigzag_simple8b_encode(&src, &mut dst).expect("failed to encode");

        let expected_encoded = vec![32, 0, 0, 2, 61, 218, 167, 172, 228, 0, 231, 7];
        assert_eq!(dst[1..], expected_encoded);

        assert_eq!(&dst[1] >> 4, DeltaEncoding::Rle as u8);
        let mut got = vec![];
        u64_zigzag_simple8b_decode(&dst, &mut got).expect("failed to decode");
        assert_eq!(got, src);
    }

    #[test]
    fn encode_simple8b() {
        let tests = vec![Test {
            name: String::from("positive"),
            input: vec![1, 11, 3124, 123543256, 2398567984273478],
        }];

        for test in tests {
            let mut dst = vec![];
            let src = test.input.clone();
            let exp = test.input;
            u64_zigzag_simple8b_encode(&src, &mut dst).expect("failed to encode");
            // verify Simple8b encoding used
            assert_eq!(&dst[1] >> 4, DeltaEncoding::Simple8b as u8);

            let mut got = vec![];
            u64_zigzag_simple8b_decode(&dst, &mut got).expect("failed to decode");
            // verify got same values back
            assert_eq!(got, exp, "{}", test.name);
        }
    }

    #[test]
    // This tests against a defect found when decoding a TSM block from InfluxDB.
    fn rle_regression() {
        let values = vec![809201799168u64; 509];
        let mut enc = vec![];
        u64_zigzag_simple8b_encode(&values, &mut enc).expect("encoding failed");

        // this is a compressed rle integer block representing 509 identical
        // 809201799168 values.
        let enc_influx = [32, 0, 0, 1, 120, 208, 95, 32, 0, 0, 252, 3];

        // ensure that encoder produces same bytes as InfluxDB encoder.
        assert_eq!(enc[1..], enc_influx);

        let mut dec = vec![];
        u64_zigzag_simple8b_decode(&enc, &mut dec).expect("failed to decode");

        assert_eq!(dec.len(), values.len());
        assert_eq!(dec, values);
    }
}
