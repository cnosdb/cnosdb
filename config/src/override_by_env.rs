use std::str::FromStr;
use std::time::Duration;

use crate::codec::duration;

pub trait OverrideByEnv {
    fn override_by_env(&mut self);
}

pub fn entry_override<T: FromStr>(value: &mut T, env_key: &str) -> bool {
    if let Ok(env_val) = std::env::var(env_key) {
        if let Ok(val) = env_val.parse::<T>() {
            *value = val;
            true
        } else {
            println!("failed to parse environment variable: {env_key}");
            false
        }
    } else {
        false
    }
}

pub fn entry_override_option<T: FromStr>(value: &mut Option<T>, env_key: &str) -> bool {
    if let Ok(env_val) = std::env::var(env_key) {
        if let Ok(val) = env_val.parse::<T>() {
            *value = Some(val);
            true
        } else {
            println!("failed to parse environment variable: {env_key}");
            false
        }
    } else {
        false
    }
}

pub fn entry_override_to_vec_string(value: &mut Vec<String>, env_key: &str) -> bool {
    if let Ok(env_val) = std::env::var(env_key) {
        if let Some(val) = parse_str_to_vec_string(&env_val) {
            *value = val;
            true
        } else {
            println!("failed to parse environment variable: {env_key}");
            false
        }
    } else {
        false
    }
}

pub fn entry_override_to_duration(value: &mut Duration, env_key: &str) -> bool {
    if let Ok(env_val) = std::env::var(env_key) {
        if let Ok(val) = duration::parse_duration(&env_val) {
            *value = val;
            true
        } else {
            println!("failed to parse environment variable: {env_key}");
            false
        }
    } else {
        false
    }
}

fn parse_str_to_vec_string(s: &str) -> Option<Vec<String>> {
    let toml = "tmp = ".to_owned() + s;
    let map: toml::map::Map<String, toml::Value> = toml::from_str(&toml).unwrap();
    let val = map.get("tmp").unwrap();
    if let Some(arr) = val.as_array() {
        let mut res = vec![];
        for v in arr {
            if let Some(s) = v.as_str() {
                res.push(s.to_owned());
            } else {
                return None;
            }
        }
        Some(res)
    } else {
        None
    }
}

#[test]
fn test() {
    parse_str_to_vec_string("[\"127.0.0.1:8901\"]");
    parse_str_to_vec_string("[\"127.0.0.1:8901\", \"127.0.0.1:8911\"]");
}
