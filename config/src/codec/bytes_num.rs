use serde::{Deserialize, Deserializer, Serializer};
use utils::byte_nums::CnosByteNumber;

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
    let s = CnosByteNumber::format_bytes(*num);

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
    CnosByteNumber::parse_bytes(&s).map_err(serde::de::Error::custom)
}

#[cfg(test)]
mod test {
    use std::any::Any;

    use serde::{Deserialize, Serialize};
    use utils::byte_nums::CnosByteNumber;

    use crate::codec::bytes_num;

    #[test]
    fn test_stringfy() {
        assert_eq!(
            CnosByteNumber::format_bytes(1024 + 1).as_str(),
            "1.0009765625 KiB"
        );
        assert_eq!(CnosByteNumber::format_bytes(1024).as_str(), "1 KiB");
        assert_eq!(CnosByteNumber::format_bytes(1_000).as_str(), "1 KB");
        assert_eq!(CnosByteNumber::format_bytes(102400).as_str(), "100 KiB");
        assert_eq!(
            CnosByteNumber::format_bytes(100_000).as_str(),
            "97.65625 KiB"
        );
        assert_eq!(
            CnosByteNumber::format_bytes(1024 * (1024 + 1)).as_str(),
            "1.0009765625 MiB"
        );
        assert_eq!(
            CnosByteNumber::format_bytes(4 * 1024 * 1024 + 1024).as_str(),
            "4.0009765625 MiB"
        );
        assert_eq!(CnosByteNumber::format_bytes(1024 * 1024).as_str(), "1 MiB");
        assert_eq!(CnosByteNumber::format_bytes(1_000_000).as_str(), "1 MB");
        assert_eq!(
            CnosByteNumber::format_bytes(1024 * 1024 * (1024 + 1)).as_str(),
            "1.0009765625 GiB"
        );
        assert_eq!(
            CnosByteNumber::format_bytes(4 * 1024 * 1024 * 1024 + 1024 * 1024).as_str(),
            "4.0009765625 GiB"
        );
        assert_eq!(
            CnosByteNumber::format_bytes(1024 * 1024 * 1024).as_str(),
            "1 GiB"
        );
        assert_eq!(CnosByteNumber::format_bytes(1_000_000_000).as_str(), "1 GB");
    }

    #[test]
    fn test_parse() {
        assert_eq!(CnosByteNumber::parse_bytes("1024").unwrap(), 1024);
        assert_eq!(CnosByteNumber::parse_bytes("1Kib").unwrap(), 1024);
        assert_eq!(CnosByteNumber::parse_bytes("1k").unwrap(), 1000);
        assert_eq!(CnosByteNumber::parse_bytes("1K").unwrap(), 1000);
        assert_eq!(CnosByteNumber::parse_bytes("1kb").unwrap(), 1_000);
        assert_eq!(CnosByteNumber::parse_bytes("1mib").unwrap(), 1024 * 1024);
        assert_eq!(CnosByteNumber::parse_bytes("1Mib").unwrap(), 1024 * 1024);
        assert_eq!(CnosByteNumber::parse_bytes("1m").unwrap(), 1000 * 1000);
        assert_eq!(CnosByteNumber::parse_bytes("1M").unwrap(), 1000 * 1000);
        assert_eq!(CnosByteNumber::parse_bytes("1mb").unwrap(), 1_000_000);
        assert_eq!(CnosByteNumber::parse_bytes("1mB").unwrap(), 1_000_000);
        assert_eq!(
            CnosByteNumber::parse_bytes("1gib").unwrap(),
            1024 * 1024 * 1024
        );
        assert_eq!(
            CnosByteNumber::parse_bytes("1Gib").unwrap(),
            1024 * 1024 * 1024
        );
        assert_eq!(
            CnosByteNumber::parse_bytes("1g").unwrap(),
            1000 * 1000 * 1000
        );
        assert_eq!(
            CnosByteNumber::parse_bytes("1G").unwrap(),
            1000 * 1000 * 1000
        );
        assert_eq!(CnosByteNumber::parse_bytes("1gb").unwrap(), 1_000_000_000);
        assert_eq!(CnosByteNumber::parse_bytes("1Gb").unwrap(), 1_000_000_000);
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
            number = "1_024_000"
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
        let exp_err_msg = r#"TOML parse error at line 2, column 22
  |
2 |             number = "a1s"
  |                      ^^^^^
the character 'a' is not a number
"#;
        assert_eq!(&err_msg, exp_err_msg);

        let config_str = r#"
            number = "10_000_000_000_000Gib"
            name = "Bar2"
        "#;
        let err = toml::from_str::<Foo>(config_str).unwrap_err();
        let err_msg = format!("{}", err);
        let exp_err_msg = r#"TOML parse error at line 2, column 22
  |
2 |             number = "10_000_000_000_000Gib"
  |                      ^^^^^^^^^^^^^^^^^^^^^^^
the value 10000000000000 exceeds the valid range
"#;
        assert_eq!(&err_msg, exp_err_msg);
    }
}
