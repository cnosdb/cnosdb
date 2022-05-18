use std::path::PathBuf;

use lazy_static::lazy_static;
use regex::Regex;
use snafu::ResultExt;

use crate::{error, Error, Result};

lazy_static! {
    static ref SUMMARY_FILE_NAME_PATTERN: Regex = Regex::new("summary-\\d{6}").unwrap();
    static ref WAL_FILE_NAME_PATTERN: Regex = Regex::new("_.\\d{5}\\.wal").unwrap();
}

pub fn make_summary_file(path: &str, number: u64) -> PathBuf {
    let p = format!("{}/summary-{:06}", path, number);
    PathBuf::from(p)
}

pub fn check_summary_file_name(file_name: &str) -> bool {
    SUMMARY_FILE_NAME_PATTERN.is_match(file_name)
}

pub fn get_summary_file_id(file_name: &str) -> Result<u64> {
    if !check_summary_file_name(file_name) {
        return Err(Error::InvalidFileName {
            file_name: file_name.to_string(),
            message: "summary file name does not contain an id".to_string()
        });
    }
    let (_, file_number) = file_name.split_at(8);
    file_number.parse::<u64>().map_err(|_| {
                                  Error::InvalidFileName {
            file_name: file_name.to_string(),
            message: "sumary file name contains an invalid id".to_string()
        }
                              })
}

pub fn make_wal_file(path: &str, sequence: u64) -> PathBuf {
    let p = format!("{}/_{:06}.wal", path, sequence);
    PathBuf::from(p)
}

pub fn check_wal_file_name(file_name: &str) -> bool {
    WAL_FILE_NAME_PATTERN.is_match(file_name)
}

pub fn get_wal_file_id(file_name: &str) -> Result<u64> {
    if !check_wal_file_name(file_name) {
        return Err(Error::InvalidFileName { file_name: file_name.to_string(),
                                            message:
                                                "wal file name does not contain an id".to_string() });
    }
    let file_number = &file_name[1..7];
    file_number.parse::<u64>().map_err(|_| {
                                  Error::InvalidFileName {
            file_name: file_name.to_string(),
            message: "wal file name contains an invalid id".to_string()
        }
                              })
}

pub fn make_tsm_file_name(path: &str, sequence: u64) -> PathBuf {
    let p = format!("{}/_{:06}.tsm", path, sequence);
    PathBuf::from(p)
}

#[cfg(test)]
mod test {
    use crate::file_utils;

    #[test]
    fn test_get_file_id() {
        let summary_file_name = "summary-000123";
        let summary_file_id = file_utils::get_summary_file_id(summary_file_name).unwrap();
        dbg!(summary_file_id);
        assert_eq!(summary_file_id, 123);

        let wal_file_name = "_000123.wal";
        let wal_file_id = file_utils::get_wal_file_id(wal_file_name).unwrap();
        dbg!(wal_file_id);
        assert_eq!(wal_file_id, 123);
    }
}
