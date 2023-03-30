use std::error::Error;

use serde::{Deserialize, Deserializer, Serializer};

// The signature of a serialize_with function must follow the pattern:
//
//    fn serialize<S>(&T, S) -> Result<S::Ok, S::Error>
//    where
//        S: Serializer
//
// although it may also be generic over the input types T.
pub fn serialize<S>(num: &u64, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    let s = format_bytes_number(*num);
    serializer.serialize_str(&s)
}

// The signature of a deserialize_with function must follow the pattern:
//
//    fn deserialize<'de, D>(D) -> Result<T, D::Error>
//    where
//        D: Deserializer<'de>
//
// although it may also be generic over the output types T.
pub fn deserialize<'de, D>(deserializer: D) -> Result<u64, D::Error>
where
    D: Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;
    parse_bytes_number(&s).map_err(serde::de::Error::custom)
}

const UNITS_LIST: [&[char]; 10] = [
    &['b'],
    &['k', 'b'],
    &['k', 'i', 'b'],
    &['k'],
    &['m', 'b'],
    &['m', 'i', 'b'],
    &['m'],
    &['g', 'b'],
    &['g', 'i', 'b'],
    &['g'],
];
// terabytes
// tebibytes
// petabytes
// pebibytes
// exabytes
// exbibytes
// zettabyte
// zebibyte

const UNITS_STR_LIST: [&str; 10] = ["B", "KB", "KiB", "K", "MB", "MiB", "M", "GB", "GiB", "G"];

const SCALES_LIST: [u64; 10] = [
    1,
    1_000,
    1024,
    1024,
    1_000_000,
    1024 * 1024,
    1024 * 1024,
    1_000_000_000,
    1024 * 1024 * 1024,
    1024 * 1024 * 1024,
];

pub(crate) fn format_bytes_number(num: u64) -> String {
    if num == 0 {
        return "0".to_string();
    }

    let mut i = 0_usize;
    for s in SCALES_LIST.iter().skip(1) {
        if num < *s {
            break;
        }
        i += 1;
    }
    for s in SCALES_LIST[..=i].iter().rev() {
        if num % *s == 0 {
            break;
        }
        i -= 1;
    }

    format!("{}{}", num / SCALES_LIST[i], UNITS_STR_LIST[i])
}

/// Parse ([0-9]+[a-z]+) to u64 bytes.
pub(crate) fn parse_bytes_number(num_str: &str) -> Result<u64, Box<dyn Error>> {
    if num_str == "0" {
        return Ok(0);
    }
    if num_str.is_empty() {
        return Err(From::from(format!("Invalid bytes number '{}'", num_str)));
    }

    let chars: Vec<char> = num_str.to_lowercase().chars().collect();
    let mut s = chars.as_slice();
    let mut v;

    // Consume integers
    (v, s) = match consume_int(s) {
        Ok((val, sli)) => (val, sli),
        Err(e) => {
            return Err(From::from(format!(
                "Invalid bytes number ({}): '{}'",
                e, num_str
            )))
        }
    };

    // Consume unit.
    let mut i = 0_usize;
    for c in s {
        if *c == '.' || '0' <= *c && *c <= '9' {
            break;
        }
        i += 1;
    }
    let mut unit = 0;
    if i > 0 {
        let u = &s[..i];
        for (ui, cu) in UNITS_LIST.into_iter().enumerate() {
            if cu == u {
                unit = SCALES_LIST[ui];
            }
        }
        if unit == 0 {
            return Err(From::from(format!(
                "Unknown unit '{:?}' in bytes number '{}'",
                u, num_str
            )));
        }
    } else {
        // No unit means unit is byte.
        unit = 1;
    }

    if v > u64::MAX / unit {
        return Err(From::from(format!(
            "Invalid bytes number (u64 overflow) '{}'",
            num_str
        )));
    }
    v *= unit;

    Ok(v)
}

fn consume_int(s: &[char]) -> Result<(u64, &[char]), Box<dyn Error>> {
    let mut i = 0_usize;
    let mut x = 0_u64;
    for c in s {
        if *c == '_' {
            i += 1;
            continue;
        }
        if *c < '0' || *c > '9' {
            break;
        }
        if x > u64::MAX / 10 {
            return Err(From::from("u64 overflow"));
        }
        x = x * 10 + *c as u64 - '0' as u64;
        i += 1;
    }
    Ok((x, &s[i..]))
}

#[cfg(test)]
mod test {
    use std::any::Any;

    use serde::{Deserialize, Serialize};

    use crate::codec::bytes_num::{self, format_bytes_number, parse_bytes_number};

    #[test]
    fn test_stringfy() {
        assert_eq!(format_bytes_number(1024 + 1).as_str(), "1025B");
        assert_eq!(format_bytes_number(1024).as_str(), "1K");
        assert_eq!(format_bytes_number(1_000).as_str(), "1KB");
        assert_eq!(format_bytes_number(102400).as_str(), "100K");
        assert_eq!(format_bytes_number(100_000).as_str(), "100KB");
        assert_eq!(format_bytes_number(1024 * (1024 + 1)).as_str(), "1025K");
        assert_eq!(
            format_bytes_number(4 * 1024 * 1024 + 1024).as_str(),
            "4097K"
        );
        assert_eq!(format_bytes_number(1024 * 1024).as_str(), "1M");
        assert_eq!(format_bytes_number(1_000_000).as_str(), "1MB");
        assert_eq!(
            format_bytes_number(1024 * 1024 * (1024 + 1)).as_str(),
            "1025M"
        );
        assert_eq!(
            format_bytes_number(4 * 1024 * 1024 * 1024 + 1024 * 1024).as_str(),
            "4097M"
        );
        assert_eq!(format_bytes_number(1024 * 1024 * 1024).as_str(), "1G");
        assert_eq!(format_bytes_number(1_000_000_000).as_str(), "1GB");
    }

    #[test]
    fn test_parse() {
        assert_eq!(parse_bytes_number("1024").unwrap(), 1024);
        assert_eq!(parse_bytes_number("1Kib").unwrap(), 1024);
        assert_eq!(parse_bytes_number("1k").unwrap(), 1024);
        assert_eq!(parse_bytes_number("1K").unwrap(), 1024);
        assert_eq!(parse_bytes_number("1kb").unwrap(), 1_000);
        assert_eq!(parse_bytes_number("1mib").unwrap(), 1024 * 1024);
        assert_eq!(parse_bytes_number("1Mib").unwrap(), 1024 * 1024);
        assert_eq!(parse_bytes_number("1m").unwrap(), 1024 * 1024);
        assert_eq!(parse_bytes_number("1M").unwrap(), 1024 * 1024);
        assert_eq!(parse_bytes_number("1mb").unwrap(), 1_000_000);
        assert_eq!(parse_bytes_number("1mB").unwrap(), 1_000_000);
        assert_eq!(parse_bytes_number("1gib").unwrap(), 1024 * 1024 * 1024);
        assert_eq!(parse_bytes_number("1Gib").unwrap(), 1024 * 1024 * 1024);
        assert_eq!(parse_bytes_number("1g").unwrap(), 1024 * 1024 * 1024);
        assert_eq!(parse_bytes_number("1G").unwrap(), 1024 * 1024 * 1024);
        assert_eq!(parse_bytes_number("1gb").unwrap(), 1_000_000_000);
        assert_eq!(parse_bytes_number("1Gb").unwrap(), 1_000_000_000);
    }

    #[derive(Serialize, Deserialize, Debug)]
    pub struct Foo {
        pub name: String,
        #[serde(with = "bytes_num")]
        pub number: u64,
    }

    #[rustfmt::skip]
    fn check_foo(foo: Foo, name: &str, number: u64) -> Result<(), Box<dyn Any + Send + 'static>> {
        std::panic::catch_unwind(|| {
            assert_eq!(foo.number, number, "Checking foo {:?} with '{}' and {}", foo, name, number);
            assert_eq!(foo.name.as_str(), name,
                "Checking {:?} with name: '{}' and number: {}", foo, name, number);
        })
    }

    #[test]
    fn test_ok() {
        let config_str = r#"
            number = "1024_000"
            name = "Bar0"
        "#;
        check_foo(toml::from_str(config_str).unwrap(), "Bar0", 1024000).unwrap();

        let config_str = r#"
            number = "1kB"
            name = "Bar1_1"
        "#;
        check_foo(toml::from_str(config_str).unwrap(), "Bar1_1", 1_000).unwrap();

        let config_str = r#"
            number = "2kIb"
            name = "Bar1_2"
        "#;
        check_foo(toml::from_str(config_str).unwrap(), "Bar1_2", 2 * 1024).unwrap();

        let config_str = r#"
            number = "3MB"
            name = "Bar2_1"
        "#;
        check_foo(toml::from_str(config_str).unwrap(), "Bar2_1", 3_000_000).unwrap();

        let config_str = r#"
            number = "4miB"
            name = "Bar2_2"
        "#;
        check_foo(
            toml::from_str(config_str).unwrap(),
            "Bar2_2",
            4 * 1024 * 1024,
        )
        .unwrap();

        let config_str = r#"
            number = "5Gb"
            name = "Bar3_1"
        "#;
        check_foo(toml::from_str(config_str).unwrap(), "Bar3_1", 5_000_000_000).unwrap();

        let config_str = r#"
            number = "6GiB"
            name = "Bar3_2"
        "#;
        check_foo(
            toml::from_str(config_str).unwrap(),
            "Bar3_2",
            6 * 1024 * 1024 * 1024,
        )
        .unwrap();
    }

    #[test]
    fn test_error() {
        let config_str = r#"
            number = "a1s"
            name = "Bar1"
        "#;
        let err = toml::from_str::<Foo>(config_str).unwrap_err();
        let err_msg = format!("{}", err);
        assert_eq!(
            &err_msg,
            "Unknown unit '['a']' in bytes number 'a1s' for key `number` at line 1 column 1"
        );

        let config_str = r#"
            number = "10_000_000_000_000Gib"
            name = "Bar2"
        "#;
        let err = toml::from_str::<Foo>(config_str).unwrap_err();
        let err_msg = format!("{}", err);
        assert_eq!(
            &err_msg,
            "Invalid bytes number (u64 overflow) '10_000_000_000_000Gib' for key `number` at line 1 column 1"
        );
    }
}
