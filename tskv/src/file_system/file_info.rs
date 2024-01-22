use std::path::PathBuf;

use md5::{Digest, Md5};
use serde::{Deserialize, Serialize};
use tokio::fs::File;
use tokio::io::AsyncReadExt;

use crate::Result;

#[derive(Serialize, Deserialize, Default, Debug, Clone)]
pub struct FileInfo {
    pub md5: String,
    pub name: String,
    pub size: u64,
}

pub async fn get_files_info(dir: &PathBuf) -> Result<Vec<FileInfo>> {
    let mut infos = vec![];
    for name in recursive_list_files(dir).iter() {
        let info = get_file_info(name).await?;
        infos.push(info);
    }

    Ok(infos)
}

pub async fn get_file_info(name: &str) -> Result<FileInfo> {
    let mut file = File::open(name).await?;
    let file_meta = file.metadata().await?;

    let mut md5 = Md5::new();
    let mut buffer = Vec::with_capacity(8 * 1024);
    loop {
        let len = file.read_buf(&mut buffer).await?;
        if len == 0 {
            break;
        }

        md5.update(&buffer[0..len]);
        buffer.clear();
    }

    let result = md5.finalize();

    Ok(FileInfo {
        md5: hex::encode(result),
        name: name.to_string(),
        size: file_meta.len(),
    })
}

fn recursive_list_files(dir: impl AsRef<std::path::Path>) -> Vec<String> {
    let mut list = Vec::new();
    for file_name in walkdir::WalkDir::new(dir)
        .min_depth(1)
        .max_depth(5)
        .sort_by_file_name()
        .into_iter()
        .filter_map(|e| {
            let entry = match e {
                Ok(e) if e.file_type().is_file() => e,
                _ => {
                    return None;
                }
            };

            Some(entry.path().to_string_lossy().to_string())
        })
    {
        list.push(file_name);
    }

    list
}

mod test {
    #[tokio::test]
    async fn test_list_filenames() {
        use crate::file_system::file_info::{get_files_info, recursive_list_files};

        let list = recursive_list_files(std::path::PathBuf::from("../common/".to_string()));
        print!("list_all_filenames: {:#?}", list);

        let path = std::path::PathBuf::from("../common/");
        let files_meta = get_files_info(&path).await.unwrap();
        print!("get_files_info: {:#?}", files_meta);
    }
}
