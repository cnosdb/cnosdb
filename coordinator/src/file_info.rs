use md5::{Digest, Md5};
use serde::{Deserialize, Serialize};
use tokio::fs::File;
use tokio::io::AsyncReadExt;

use crate::errors::CoordinatorResult;

#[derive(Serialize, Deserialize, Default, Debug, Clone)]
pub struct FileInfo {
    pub md5: String,
    pub name: String,
    pub size: u64,
}

#[derive(Serialize, Deserialize, Default, Debug, Clone)]
pub struct PathFilesMeta {
    pub path: String,
    pub meta: Vec<FileInfo>,
}

impl From<PathFilesMeta> for protos::kv_service::GetFilesMetaResponse {
    fn from(src: PathFilesMeta) -> Self {
        let mut pb_file_infos = vec![];
        for it in src.meta.iter() {
            let info = protos::kv_service::FileInfo {
                md5: it.md5.clone(),
                name: it.name.clone(),
                size: it.size,
            };

            pb_file_infos.push(info);
        }

        protos::kv_service::GetFilesMetaResponse {
            path: src.path,
            infos: pb_file_infos,
        }
    }
}

pub async fn get_files_meta(dir: &str) -> CoordinatorResult<PathFilesMeta> {
    let mut files_meta = vec![];
    for name in list_all_filenames(std::path::PathBuf::from(dir)).iter() {
        let meta = get_file_info(name).await?;
        files_meta.push(meta);
    }

    Ok(PathFilesMeta {
        meta: files_meta,
        path: dir.to_string(),
    })
}

pub async fn get_file_info(name: &str) -> CoordinatorResult<FileInfo> {
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

    Ok(FileInfo {
        md5: format!("{:x?}", md5.finalize()),
        name: name.to_string(),
        size: file_meta.len(),
    })
}

fn list_all_filenames(dir: impl AsRef<std::path::Path>) -> Vec<String> {
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
        use crate::file_info::{get_files_meta, list_all_filenames};

        let list = list_all_filenames(std::path::PathBuf::from("../common/".to_string()));
        print!("list_all_filenames: {:#?}", list);

        let files_meta = get_files_meta("../common/").await.unwrap();
        print!("get_files_meta: {:#?}", files_meta);

        let path = "/tmp/cnosdb/test/1/2/3.txt";
        let path = std::path::PathBuf::from(path);

        tokio::fs::create_dir_all(path.parent().unwrap())
            .await
            .unwrap();

        let _file = tokio::fs::OpenOptions::new()
            .create(true)
            .truncate(true)
            .read(true)
            .write(true)
            .open(path)
            .await
            .unwrap();
    }
}
