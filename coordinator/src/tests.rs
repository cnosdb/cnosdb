mod test {
    use std::net::ToSocketAddrs;

    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpStream,
    };

    use crate::{
        command::*,
        file_info::{get_file_info, PathFilesMeta},
    };

    #[tokio::test]
    async fn test_list_dir_names() {
        let vnode_id = 3;
        let tenant = "cnosdb".to_string();
        let test_db = "my_db".to_string();

        // get file meta
        let req = AdminStatementRequest {
            tenant: tenant.clone(),
            stmt: AdminStatementType::GetVnodeFilesMeta(test_db.clone(), vnode_id),
        };
        let cmd = CoordinatorTcpCmd::AdminStatementCmd(req);

        let mut files_meta = PathFilesMeta::default();
        let mut conn = TcpStream::connect("127.0.0.1:31003").await.unwrap();
        send_command(&mut conn, &cmd).await.unwrap();
        let rsp_cmd = recv_command(&mut conn).await.unwrap();
        if let CoordinatorTcpCmd::StatusResponseCmd(msg) = rsp_cmd {
            files_meta = serde_json::from_str::<PathFilesMeta>(&msg.data).unwrap();
            println!("========= files meta: {:?}", files_meta);
        } else {
            println!("========= response error: {:?}", rsp_cmd);
        }

        // download file
        for info in files_meta.meta.iter() {
            let filename = info.name.clone();

            let filename = filename
                .strip_prefix(&(files_meta.path.clone() + "/"))
                .unwrap();
            let req = AdminStatementRequest {
                tenant: tenant.clone(),
                stmt: AdminStatementType::DownloadFile(
                    test_db.clone(),
                    vnode_id,
                    filename.to_string(),
                ),
            };
            let cmd = CoordinatorTcpCmd::AdminStatementCmd(req);

            send_command(&mut conn, &cmd).await.unwrap();

            let path = std::path::Path::new("./down_files");
            let path = path.join(filename);
            println!("=== {:?}; {:?}", path, path.parent().unwrap());
            let _ = tokio::fs::create_dir_all(path.parent().unwrap()).await;
            let mut file = tokio::fs::OpenOptions::new()
                .create(true)
                .truncate(true)
                .read(true)
                .write(true)
                .open(&path)
                .await
                .unwrap();

            let size = conn.read_u64().await.unwrap() as usize;
            println!("===== download file: {}, {}", filename, size);

            let mut read_len = 0;
            let mut buffer = Vec::with_capacity(8 * 1024);
            loop {
                let len = conn.read_buf(&mut buffer).await.unwrap();
                read_len += len;
                file.write_all(&buffer).await.unwrap();

                if len == 0 || read_len >= size {
                    break;
                }

                buffer.clear();
            }

            let name = path.to_string_lossy().to_string();
            let tmp_info = get_file_info(&name).await.unwrap();

            if tmp_info.md5 != info.md5 {
                panic!("download file failed: {}, {}", info.md5, tmp_info.md5);
            }
        }
    }
}
