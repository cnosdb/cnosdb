use std::path::{Path, PathBuf};

use lazy_static::lazy_static;
use regex::Regex;
use snafu::ResultExt;
use tokio::fs;

use crate::error::{IOSnafu, InvalidFileNameSnafu, ReadFileSnafu};
use crate::file_system::async_filesystem::LocalFileSystem;
use crate::file_system::FileSystem;
use crate::TskvResult;

lazy_static! {
    static ref SUMMARY_FILE_NAME_PATTERN: Regex = Regex::new(r"summary-\d{6}").unwrap();
    static ref WAL_FILE_NAME_PATTERN: Regex = Regex::new(r"_\d{6}\.wal").unwrap();
    static ref TSM_FILE_NAME_PATTERN: Regex = Regex::new(r"_\d{6}\.tsm").unwrap();
    static ref HINTEDOFF_FILE_NAME_PATTERN: Regex = Regex::new(r"_\d{6}\.hh").unwrap();
    static ref INDEX_BINLOG_FILE_NAME_PATTERN: Regex = Regex::new(r"_\d{6}\.binlog").unwrap();
}

/// Make a path for summary file by it's directory and id.
pub fn make_summary_file(dir: impl AsRef<Path>, number: u64) -> PathBuf {
    let p = format!("summary-{:06}", number);
    dir.as_ref().join(p)
}

/// Make a path for summary temporary file by it's directory.
pub fn make_summary_file_tmp(dir: impl AsRef<Path>) -> PathBuf {
    let p = "summary.tmp".to_string();
    dir.as_ref().join(p)
}

/// Make a path for summary file by it's directory and id.
pub fn make_tsfamily_summary_file(dir: impl AsRef<Path>) -> PathBuf {
    dir.as_ref().join("summary")
}

/// Check a summary file's name.
pub fn check_summary_file_name(file_name: &str) -> bool {
    SUMMARY_FILE_NAME_PATTERN.is_match(file_name)
}

/// Rename a file, from old path to new path.
pub async fn rename(old_name: impl AsRef<Path>, new_name: impl AsRef<Path>) -> TskvResult<()> {
    fs::create_dir_all(new_name.as_ref().parent().unwrap())
        .await
        .context(IOSnafu)?;
    fs::rename(old_name, new_name).await.context(IOSnafu)
}

/// Get id from a summary file's name.
pub fn get_summary_file_id(file_name: &str) -> TskvResult<u64> {
    if !check_summary_file_name(file_name) {
        return Err(InvalidFileNameSnafu {
            file_name: file_name.to_string(),
            message: "summary file name does not contain an id".to_string(),
        }
        .build());
    }
    let (_, file_number) = file_name.split_at(8);
    file_number.parse::<u64>().map_err(|_| {
        InvalidFileNameSnafu {
            file_name: file_name.to_string(),
            message: "summary file name contains an invalid id".to_string(),
        }
        .build()
    })
}

/// Make a path for index binlog file by it's directory and id.
pub fn make_index_binlog_file(dir: impl AsRef<Path>, sequence: u64) -> PathBuf {
    let p = format!("_{:06}.binlog", sequence);
    dir.as_ref().join(p)
}

/// Check a index binlog file's name.
pub fn check_index_binlog_file_name(file_name: &str) -> bool {
    INDEX_BINLOG_FILE_NAME_PATTERN.is_match(file_name)
}

/// Get id from a index binlog file's name.
pub fn get_index_binlog_file_id(file_name: &str) -> TskvResult<u64> {
    if !check_index_binlog_file_name(file_name) {
        return Err(InvalidFileNameSnafu {
            file_name: file_name.to_string(),
            message: "index binlog file name does not contain an id".to_string(),
        }
        .build());
    }
    let file_number = &file_name[1..7];
    file_number.parse::<u64>().map_err(|_| {
        InvalidFileNameSnafu {
            file_name: file_name.to_string(),
            message: "index binlog file name contains an invalid id".to_string(),
        }
        .build()
    })
}

/// Make a path for WAL (write ahead log) file by it's directory and id.
pub fn make_wal_file(dir: impl AsRef<Path>, sequence: u64) -> PathBuf {
    let p = format!("_{:06}.wal", sequence);
    dir.as_ref().join(p)
}

/// Check a WAL file's name.
pub fn check_wal_file_name(file_name: &str) -> bool {
    WAL_FILE_NAME_PATTERN.is_match(file_name)
}

/// Get id from a WAL file's name.
pub fn get_wal_file_id(file_name: &str) -> TskvResult<u64> {
    if !check_wal_file_name(file_name) {
        return Err(InvalidFileNameSnafu {
            file_name: file_name.to_string(),
            message: "wal file name does not contain an id".to_string(),
        }
        .build());
    }
    let file_number = &file_name[1..7];
    file_number.parse::<u64>().map_err(|_| {
        InvalidFileNameSnafu {
            file_name: file_name.to_string(),
            message: "wal file name contains an invalid id".to_string(),
        }
        .build()
    })
}

pub fn make_tsm_file_name(sequence: u64) -> String {
    format!("_{:06}.tsm", sequence)
}

/// Make a path for TSM file by it's directory and id.
pub fn make_tsm_file(dir: impl AsRef<Path>, sequence: u64) -> PathBuf {
    dir.as_ref().join(make_tsm_file_name(sequence))
}

/// Get id from a TSM file's name.
pub fn get_tsm_file_id_by_path(tsm_path: impl AsRef<Path>) -> TskvResult<u64> {
    let path = tsm_path.as_ref();
    let file_name = path
        .file_name()
        .expect("path must not be '..'")
        .to_str()
        .expect("file name must be UTF-8 string");
    if file_name.len() == 1 {
        return Err(InvalidFileNameSnafu {
            file_name: file_name.to_string(),
            message: "tsm file name contains an invalid id".to_string(),
        }
        .build());
    }
    let start = file_name.find('_').unwrap_or(0_usize) + 1;
    let end = file_name.find('.').unwrap_or(file_name.len());
    let file_number = &file_name[start..end];
    file_number.parse::<u64>().map_err(|_| {
        InvalidFileNameSnafu {
            file_name: file_name.to_string(),
            message: "tsm file name contains an invalid id".to_string(),
        }
        .build()
    })
}

pub fn make_tsm_tombstone_file_name(sequence: u64) -> String {
    format!("_{:06}.tombstone", sequence)
}

/// Make a path for TSM tombstone file by it's directory and id.
pub fn make_tsm_tombstone_file(dir: impl AsRef<Path>, sequence: u64) -> PathBuf {
    dir.as_ref().join(make_tsm_tombstone_file_name(sequence))
}

pub fn make_delta_file_name(sequence: u64) -> String {
    format!("_{:06}.delta", sequence)
}

/// Make a path for TSM delta file by it's directory and id.
pub fn make_delta_file(dir: impl AsRef<Path>, sequence: u64) -> PathBuf {
    dir.as_ref().join(make_delta_file_name(sequence))
}

/// Get the file's path that has the maximum id of files in a directory.
pub fn get_max_sequence_file_name<F>(
    dir: impl AsRef<Path>,
    get_sequence: F,
) -> Option<(PathBuf, u64)>
where
    F: Fn(&str) -> TskvResult<u64>,
{
    let segments = LocalFileSystem::list_file_names(dir);
    if segments.is_empty() {
        return None;
    }

    let mut max_id = 0;
    let mut max_index = 0;
    let mut is_found = false;
    for (i, file_name) in segments.iter().enumerate() {
        match get_sequence(file_name) {
            Ok(id) => {
                is_found = true;
                if max_id < id {
                    max_id = id;
                    max_index = i;
                }
            }
            Err(_) => continue,
        }
    }

    if !is_found {
        return None;
    }

    let max_file_name = segments.get(max_index).unwrap();
    Some((PathBuf::from(max_file_name), max_id))
}

/// Iterate all files in a directory and call `f` for each file that matches `filter`.
fn for_each_file_in_dir<P, F>(dir: impl AsRef<Path>, filter: P, mut f: F) -> TskvResult<usize>
where
    P: Fn(std::ffi::OsString) -> bool,
    F: FnMut(PathBuf, std::fs::Metadata),
{
    let read_dir_result = std::fs::read_dir(&dir).with_context(|_| ReadFileSnafu {
        path: dir.as_ref().to_path_buf(),
    })?;
    let mut dir_entries = Vec::new();
    for ret in read_dir_result {
        let entry = ret.with_context(|_| ReadFileSnafu {
            path: dir.as_ref().to_path_buf(),
        })?;
        if entry
            .file_type()
            .is_ok_and(|t| t.is_file() && filter(entry.file_name()))
        {
            dir_entries.push(entry);
        }
    }
    dir_entries.sort_by_cached_key(|a| a.file_name().to_string_lossy().to_string());

    let mut total = 0;
    for entry in dir_entries {
        let metadata = entry
            .metadata()
            .with_context(|_| ReadFileSnafu { path: entry.path() })?;
        f(entry.path(), metadata);
        total += 1;
    }
    Ok(total)
}

/// Iterate all WAL files in a directory and call `f` for each file.
pub fn for_each_wal_file_in_dir<F>(dir: impl AsRef<Path>, f: F) -> TskvResult<usize>
where
    F: FnMut(PathBuf, std::fs::Metadata),
{
    for_each_file_in_dir(
        dir,
        |file_name| check_wal_file_name(file_name.to_string_lossy().as_ref()),
        f,
    )
}

#[cfg(test)]
mod test {
    use std::path::PathBuf;

    use super::{check_summary_file_name, make_summary_file};
    use crate::file_utils::{
        check_wal_file_name, get_summary_file_id, get_wal_file_id, make_wal_file,
    };
    use crate::TskvResult;

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
    #[allow(clippy::type_complexity)]
    fn test_make_file() {
        let test_dir = PathBuf::from("/tmp/test");

        let seq_numbers = [0, 1, 123, 123456 /*1234567*/];
        let functions_to_test: [(
            &str,
            Box<dyn Fn(&PathBuf, u64) -> PathBuf>,
            Box<dyn Fn(&str) -> bool>,
            Box<dyn Fn(&str) -> TskvResult<u64>>,
        ); 2] = [
            (
                "summary",
                Box::new(|p, s| make_summary_file(p, s)),
                Box::new(check_summary_file_name),
                Box::new(get_summary_file_id),
            ),
            (
                "wal",
                Box::new(|p, s| make_wal_file(p, s)),
                Box::new(check_wal_file_name),
                Box::new(get_wal_file_id),
            ),
        ];
        for (file_type, make_file, check_file_name, get_file_id) in functions_to_test {
            let dir = test_dir.join(file_type);
            for seq_no in seq_numbers {
                let file_path = make_file(&dir, seq_no);
                let file_name = file_path.file_name().unwrap().to_str().unwrap();
                assert!(check_file_name(file_name), "path: {file_path:?}");
                let file_id = get_file_id(file_name).unwrap();
                assert_eq!(file_id, seq_no, "path: {file_path:?}");
            }
        }
    }
}
