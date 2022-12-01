use std::{error::Error, time::Duration};

use serde::{Deserialize, Deserializer, Serializer};

// The signature of a serialize_with function must follow the pattern:
//
//    fn serialize<S>(&T, S) -> Result<S::Ok, S::Error>
//    where
//        S: Serializer
//
// although it may also be generic over the input types T.
pub fn serialize<S>(date: &Duration, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    let s = format!("{:?}", date);
    serializer.serialize_str(&s)
}

// The signature of a deserialize_with function must follow the pattern:
//
//    fn deserialize<'de, D>(D) -> Result<T, D::Error>
//    where
//        D: Deserializer<'de>
//
// although it may also be generic over the output types T.
pub fn deserialize<'de, D>(deserializer: D) -> Result<Duration, D::Error>
where
    D: Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;
    parse_duration(&s).map_err(serde::de::Error::custom)
}

const NANOSECOND_CHARS: &[char] = &['n', 's'];
const MICROSECOND_CHARS: &[char] = &['u', 's'];
const MICROSECOND_GREEK_CHARS: &[char] = &['μ', 's'];
const MILLISECOND_CHARS: &[char] = &['m', 's'];
const SECOND_CHARS: &[char] = &['s'];
const MINUTE_CHARS: &[char] = &['m'];
const HOUR_CHARS: &[char] = &['h'];

const NANOSECOND: u64 = 1;
const MICROSECOND: u64 = NANOSECOND * 1000;
const MILLISECOND: u64 = MICROSECOND * 1000;
const SECOND: u64 = MILLISECOND * 1000;
const MINUTE: u64 = SECOND * 60;
const HOUR: u64 = MINUTE * 60;

/// Parse ([0-9]+[a-z]+) to Duration.
pub(crate) fn parse_duration(duration_str: &str) -> Result<Duration, Box<dyn Error>> {
    if duration_str == "0" {
        return Ok(Duration::from_nanos(0));
    }
    if duration_str.is_empty() {
        return Err(From::from(format!("invalid duration '{}'", duration_str)));
    }

    let chars: Vec<char> = duration_str.chars().collect();
    let mut s = chars.as_slice();
    let mut v;

    // Consume integers
    (v, s) = match consume_int(s) {
        Ok((val, sli)) => (val, sli),
        Err(e) => {
            return Err(From::from(format!(
                "Invalid duration ({}): '{}'",
                e, duration_str
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
    if i == 0 {
        return Err(From::from(format!(
            "Missing unit in duration '{}'",
            duration_str
        )));
    }

    let u = &s[..i];
    let unit = match u {
        NANOSECOND_CHARS => NANOSECOND,
        MICROSECOND_CHARS => MICROSECOND,
        MICROSECOND_GREEK_CHARS => MICROSECOND,
        MILLISECOND_CHARS => MILLISECOND,
        SECOND_CHARS => SECOND,
        MINUTE_CHARS => MINUTE,
        HOUR_CHARS => HOUR,
        _ => {
            return Err(From::from(format!(
                "Unknown unit '{:?}' in duration '{}'",
                u, duration_str
            )))
        }
    };

    if v > (1 << 63) / unit {
        return Err(From::from(format!(
            "Invalid duration (overflow) '{}'",
            duration_str
        )));
    }
    v *= unit;
    if v > (1 << 63) - 1 {
        return Err(From::from(format!(
            "Invalid duration (overflow) '{}'",
            duration_str
        )));
    }

    Ok(Duration::from_nanos(v))
}

fn consume_int(s: &[char]) -> Result<(u64, &[char]), Box<dyn Error>> {
    let mut i = 0_usize;
    let mut x = 0_u64;
    for c in s {
        if *c < '0' || *c > '9' {
            break;
        }
        if x > (1 << 63) / 10 {
            return Err(From::from("overflow"));
        }
        x = x * 10 + *c as u64 - '0' as u64;
        if x > 1 << 63 {
            return Err(From::from("overflow"));
        }
        i += 1;
    }
    Ok((x, &s[i..]))
}

#[cfg(test)]
mod test {
    use std::time::Duration;

    use serde::{Deserialize, Serialize};

    use crate::duration;

    #[derive(Serialize, Deserialize, Debug)]
    pub struct Foo {
        #[serde(with = "duration")]
        pub duration: Duration,
        pub name: String,
    }

    #[test]
    fn test_ok() {
        let config_str = r#"
            duration = "6h"
            name = "Bar0"
        "#;
        let foo1: Foo = toml::from_str(config_str).unwrap();
        assert_eq!(foo1.duration, Duration::from_secs(6 * 3600));
        assert_eq!(foo1.name, "Bar0".to_string());

        let config_str = r#"
            duration = "1m"
            name = "Bar1"
        "#;
        let foo1: Foo = toml::from_str(config_str).unwrap();
        assert_eq!(foo1.duration, Duration::from_secs(60));
        assert_eq!(foo1.name, "Bar1".to_string());

        let config_str = r#"
            duration = "1s"
            name = "Bar2"
        "#;
        let foo2: Foo = toml::from_str(config_str).unwrap();
        assert_eq!(foo2.duration, Duration::from_secs(1));
        assert_eq!(foo2.name, "Bar2".to_string());

        let config_str = r#"
            duration = "150ms"
            name = "Bar3"
        "#;
        let foo3: Foo = toml::from_str(config_str).unwrap();
        assert_eq!(foo3.duration, Duration::from_millis(150));
        assert_eq!(foo3.name, "Bar3".to_string());

        let config_str = r#"
            duration = "1500ns"
            name = "Bar4"
        "#;
        let foo4: Foo = toml::from_str(config_str).unwrap();
        assert_eq!(foo4.duration, Duration::from_nanos(1500));
        assert_eq!(foo4.name, "Bar4".to_string());
    }

    #[test]
    fn test_error() {
        let config_str = r#"
            duration = "a1s"
            name = "Bar1"
        "#;
        let err = toml::from_str::<Foo>(config_str).unwrap_err();
        let err_msg = format!("{}", err);
        assert_eq!(
            &err_msg,
            "Unknown unit '['a']' in duration 'a1s' for key `duration` at line 1 column 1"
        );
    }
}
