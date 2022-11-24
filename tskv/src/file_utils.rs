use std::path::{Path, PathBuf};

use lazy_static::lazy_static;
use regex::Regex;
use snafu::ResultExt;

use crate::file_system::file_manager;
use crate::{error, Error, Result};

lazy_static! {
    static ref SUMMARY_FILE_NAME_PATTERN: Regex = Regex::new(r"summary-\d{6}").unwrap();
    static ref WAL_FILE_NAME_PATTERN: Regex = Regex::new(r"_\d{6}\.wal").unwrap();
    static ref TSM_FILE_NAME_PATTERN: Regex = Regex::new(r"_\d{6}\.tsm").unwrap();
    static ref SCHEMA_FILE_NAME_PATTERN: Regex = Regex::new(r"_\d{6}\.schema").unwrap();
}

// Summary file.

pub fn make_summary_file(path: impl AsRef<Path>, number: u64) -> PathBuf {
    let p = format!("summary-{:06}", number);
    path.as_ref().join(p)
}

pub fn make_summary_file_tmp(path: impl AsRef<Path>) -> PathBuf {
    let p = "summary.tmp".to_string();
    path.as_ref().join(p)
}

pub fn check_summary_file_name(file_name: &str) -> bool {
    SUMMARY_FILE_NAME_PATTERN.is_match(file_name)
}

pub fn get_summary_file_id(file_name: &str) -> Result<u64> {
    if !check_summary_file_name(file_name) {
        return Err(Error::InvalidFileName {
            file_name: file_name.to_string(),
            message: "summary file name does not contain an id".to_string(),
        });
    }
    let (_, file_number) = file_name.split_at(8);
    file_number
        .parse::<u64>()
        .map_err(|_| Error::InvalidFileName {
            file_name: file_name.to_string(),
            message: "summary file name contains an invalid id".to_string(),
        })
}

// WAL (write ahead log) file.

pub fn make_wal_file(path: impl AsRef<Path>, sequence: u64) -> PathBuf {
    let p = format!("_{:06}.wal", sequence);
    path.as_ref().join(p)
}

pub fn check_wal_file_name(file_name: &str) -> bool {
    WAL_FILE_NAME_PATTERN.is_match(file_name)
}

pub fn get_wal_file_id(file_name: &str) -> Result<u64> {
    if !check_wal_file_name(file_name) {
        return Err(Error::InvalidFileName {
            file_name: file_name.to_string(),
            message: "wal file name does not contain an id".to_string(),
        });
    }
    let file_number = &file_name[1..7];
    file_number
        .parse::<u64>()
        .map_err(|_| Error::InvalidFileName {
            file_name: file_name.to_string(),
            message: "wal file name contains an invalid id".to_string(),
        })
}

// TSM file

pub fn make_tsm_file_name(path: impl AsRef<Path>, sequence: u64) -> PathBuf {
    let p = format!("_{:06}.tsm", sequence);
    path.as_ref().join(p)
}

pub fn get_tsm_file_id_by_path(tsm_path: impl AsRef<Path>) -> Result<u64> {
    let path = tsm_path.as_ref();
    let file_name = path
        .file_name()
        .expect("path must not be '..'")
        .to_str()
        .expect("file name must be UTF-8 string");
    if file_name.len() == 1 {
        return Err(Error::InvalidFileName {
            file_name: file_name.to_string(),
            message: "tsm file name contains an invalid id".to_string(),
        });
    }
    let start = file_name.find('_').unwrap_or(0_usize) + 1;
    let end = file_name.find('.').unwrap_or(file_name.len());
    let file_number = &file_name[start..end];
    file_number
        .parse::<u64>()
        .map_err(|_| Error::InvalidFileName {
            file_name: file_name.to_string(),
            message: "tsm file name contains an invalid id".to_string(),
        })
}

// TSM tombstone file

pub fn make_tsm_tombstone_file_name(path: impl AsRef<Path>, sequence: u64) -> PathBuf {
    let p = format!("_{:06}.tombstone", sequence);
    path.as_ref().join(p)
}

// delta file

pub fn make_delta_file_name(path: impl AsRef<Path>, sequence: u64) -> PathBuf {
    let p = format!("_{:06}.delta", sequence);
    path.as_ref().join(p)
}

// Schema file

pub fn make_schema_file(path: impl AsRef<Path>, sequence: u64) -> PathBuf {
    let p = format!("_{:06}.schema", sequence);
    path.as_ref().join(p)
}

pub fn check_schema_file(file_name: &str) -> bool {
    SCHEMA_FILE_NAME_PATTERN.is_match(file_name)
}

pub fn get_schema_file_id(file_name: &str) -> Result<u64> {
    if !check_schema_file(file_name) {
        return Err(Error::InvalidFileName {
            file_name: file_name.to_string(),
            message: "schema file name does not contain an id".to_string(),
        });
    }
    let file_number = &file_name[1..7];
    file_number
        .parse::<u64>()
        .map_err(|_| Error::InvalidFileName {
            file_name: file_name.to_string(),
            message: "schema file name contains an invalid id".to_string(),
        })
}

// Common

pub fn get_max_sequence_file_name<F>(
    dir: impl AsRef<Path>,
    get_sequence: F,
) -> Option<(PathBuf, u64)>
where
    F: Fn(&str) -> Result<u64>,
{
    let segments = file_manager::list_file_names(dir);
    if segments.is_empty() {
        return None;
    }
    let mut max_id = 1;
    let mut max_index = 0;
    for (i, file_name) in segments.iter().enumerate() {
        match get_sequence(file_name) {
            Ok(id) => {
                if max_id < id {
                    max_id = id;
                    max_index = i;
                }
            }
            Err(_) => continue,
        }
    }
    let max_file_name = segments.get(max_index).unwrap();
    Some((PathBuf::from(max_file_name), max_id))
}

#[cfg(test)]
mod test {
    use std::path::PathBuf;

    use super::{check_summary_file_name, make_summary_file};
    use crate::file_utils::{
        self, check_schema_file, check_wal_file_name, get_schema_file_id, get_summary_file_id,
        get_wal_file_id, make_schema_file, make_wal_file,
    };

    #[test]
    fn test_get_file_id() {
        let summary_file_name = "summary-000123";
        let summary_file_id = get_summary_file_id(summary_file_name).unwrap();
        dbg!(summary_file_id);
        assert_eq!(summary_file_id, 123);

        let wal_file_name = "_000123.wal";
        let wal_file_id = get_wal_file_id(wal_file_name).unwrap();
        dbg!(wal_file_id);
        assert_eq!(wal_file_id, 123);
    }

    #[test]
    fn test_make_file() {
        let path = PathBuf::from("/tmp/test".to_string());
        {
            let summary_file_path = make_summary_file(&path, 0);
            let summary_file_name = summary_file_path.file_name().unwrap().to_str().unwrap();
            assert!(check_summary_file_name(summary_file_name));
            let summary_file_id = get_summary_file_id(summary_file_name).unwrap();
            assert_eq!(summary_file_id, 0);
        }
        {
            let wal_file_path = make_wal_file(&path, 0);
            let wal_file_name = wal_file_path.file_name().unwrap().to_str().unwrap();
            assert!(check_wal_file_name(wal_file_name));
            let wal_file_id = get_wal_file_id(wal_file_name).unwrap();
            assert_eq!(wal_file_id, 0);
        }
        {
            let schema_file_path = make_schema_file(&path, 0);
            let schema_file_name = schema_file_path.file_name().unwrap().to_str().unwrap();
            assert!(check_schema_file(schema_file_name));
            let schema_file_id = get_schema_file_id(schema_file_name).unwrap();
            assert_eq!(schema_file_id, 0);
        }
    }
}
