use std::time::Duration;

use serde::{Deserialize, Deserializer, Serializer};
use utils::duration::parse_duration;

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
