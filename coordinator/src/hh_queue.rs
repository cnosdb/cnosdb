use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use config::HintedOffConfig;
use models::schema::Precision;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::mpsc::Receiver;
use tokio::sync::oneshot::Sender;
use tokio::sync::RwLock;
use tokio::time::{self, Duration};
use trace::{debug, warn};
use tracing::info;
use tskv::byte_utils;
use tskv::file_system::file_manager::list_dir_names;
use tskv::file_system::queue::{Queue, QueueConfig};
use tskv::file_system::DataBlock;

use crate::errors::*;
use crate::writer::PointWriter;

const SEGMENT_FILE_SUFFIX: &str = "hh";
const SEGMENT_FILE_MAX_SIZE: u64 = 1073741824; // 1 GiB
const HINTEDOFF_BLOCK_HEADER_SIZE: usize = 21; // 8 + 4 + 4 + 4

#[derive(Debug)]
pub struct HintedOffWriteReq {
    pub node_id: u64,
    pub block: HintedOffBlock,
    pub sender: Sender<CoordinatorResult<()>>,
}

#[derive(Debug)]
pub struct HintedOffBlock {
    pub ts: i64,
    pub vnode_id: u32,
    pub tenant_len: u32,
    pub data_len: u32,
    pub precision: Precision,

    pub tenant: String,
    pub data: Vec<u8>,
}

impl HintedOffBlock {
    pub fn new(
        ts: i64,
        vnode_id: u32,
        tenant: String,
        precision: Precision,
        data: Vec<u8>,
    ) -> Self {
        Self {
            ts,
            vnode_id,
            precision,
            tenant_len: tenant.len() as u32,
            tenant,
            data_len: data.len() as u32,
            data,
        }
    }

    pub fn debug(&self) -> String {
        format!(
            "ts:{}, id: {}, data: {}",
            self.ts,
            self.vnode_id,
            String::from_utf8(self.data.clone()).unwrap()
        )
    }
}

#[async_trait::async_trait]
impl DataBlock for HintedOffBlock {
    async fn write(&self, file: &mut tokio::fs::File) -> tskv::Result<usize> {
        let mut written_size = 0;
        written_size += file.write(&self.ts.to_be_bytes()).await?;
        written_size += file.write(&self.vnode_id.to_be_bytes()).await?;
        written_size += file.write(&[self.precision as u8]).await?;
        written_size += file.write(&self.tenant_len.to_be_bytes()).await?;
        written_size += file.write(&self.data_len.to_be_bytes()).await?;

        written_size += file.write(self.tenant.as_bytes()).await?;
        written_size += file.write(&self.data).await?;

        Ok(written_size)
    }

    async fn read(&mut self, file: &mut tokio::fs::File) -> tskv::Result<usize> {
        let mut read_size = 0;
        let mut header_buf = [0_u8; HINTEDOFF_BLOCK_HEADER_SIZE];
        read_size += file.read_exact(&mut header_buf).await?;

        self.ts = byte_utils::decode_be_i64(header_buf[0..8].into());
        self.vnode_id = byte_utils::decode_be_u32(header_buf[8..12].into());
        self.precision = header_buf[12].into();
        self.tenant_len = byte_utils::decode_be_u32(header_buf[13..17].into());
        self.data_len = byte_utils::decode_be_u32(header_buf[17..21].into());

        let mut buf = Vec::new();
        buf.resize((self.data_len + self.tenant_len) as usize, 0);
        read_size += file.read_exact(&mut buf).await?;

        self.tenant =
            unsafe { String::from_utf8_unchecked(buf[0..self.tenant_len as usize].to_vec()) };
        self.data =
            buf[self.tenant_len as usize..(self.tenant_len + self.data_len) as usize].to_vec();

        Ok(read_size)
    }
}

pub struct HintedOffManager {
    config: HintedOffConfig,
    writer: Arc<PointWriter>,
    nodes: RwLock<HashMap<u64, Arc<RwLock<Queue>>>>,
}

impl HintedOffManager {
    pub async fn new(config: HintedOffConfig, writer: Arc<PointWriter>) -> Self {
        let manager = Self {
            config,
            writer,
            nodes: RwLock::new(HashMap::new()),
        };

        let dir = PathBuf::from(manager.config.path.clone());
        for id in list_dir_names(dir).iter() {
            if let Ok(id) = id.parse::<u64>() {
                manager.get_or_create_queue(id).await.unwrap();
            }
        }

        manager
    }

    pub async fn write_handoff_job(
        manager: Arc<HintedOffManager>,
        mut hh_receiver: Receiver<HintedOffWriteReq>,
    ) {
        while let Some(request) = hh_receiver.recv().await {
            if !manager.config.enable {
                request.sender.send(Ok(())).expect("successful");
                continue;
            }

            let result = match manager.get_or_create_queue(request.node_id).await {
                Ok(queue) => queue
                    .write()
                    .await
                    .write(&request.block)
                    .await
                    .map_err(CoordinatorError::from),

                Err(err) => Err(err),
            };

            request.sender.send(result).expect("successful");
        }
    }

    async fn get_or_create_queue(&self, id: u64) -> CoordinatorResult<Arc<RwLock<Queue>>> {
        let mut nodes = self.nodes.write().await;
        if let Some(val) = nodes.get(&id) {
            return Ok(val.clone());
        }

        let dir = PathBuf::from(self.config.path.clone()).join(id.to_string());
        let config = QueueConfig {
            data_path: dir.to_string_lossy().to_string(),
            file_suffix: SEGMENT_FILE_SUFFIX.to_string(),
            max_file_size: SEGMENT_FILE_MAX_SIZE,
        };

        let queue = Queue::new(config).await?;
        let queue = Arc::new(RwLock::new(queue));
        nodes.insert(id, queue.clone());

        tokio::spawn(HintedOffManager::hinted_off_service(
            id,
            self.writer.clone(),
            queue.clone(),
        ));

        Ok(queue)
    }

    async fn hinted_off_service(node_id: u64, writer: Arc<PointWriter>, queue: Arc<RwLock<Queue>>) {
        debug!("hinted_off_service started for node: {}", node_id);

        let mut count = 0;
        let mut block = HintedOffBlock::new(0, 0, "".to_string(), Precision::NS, vec![]);
        loop {
            let read_result = queue.write().await.read(&mut block).await;
            match read_result {
                Ok(_) => {
                    HintedOffManager::write_until_success(
                        queue.clone(),
                        writer.clone(),
                        &block,
                        node_id,
                    )
                    .await;
                    let _ = queue.write().await.commit().await;
                }

                Err(err) => {
                    debug!("read hindoff data: {}", err.to_string());
                    time::sleep(Duration::from_secs(3)).await;
                }
            }

            if count % 10000 == 0 {
                let size = queue.write().await.size().await;
                info!("hinted handoff remain size: {:?}, node: {}", size, node_id)
            }
            count += 1
        }
    }

    async fn write_until_success(
        queue: Arc<RwLock<Queue>>,
        writer: Arc<PointWriter>,
        block: &HintedOffBlock,
        node_id: u64,
    ) {
        loop {
            let result = writer
                .write_to_remote_node(
                    block.vnode_id,
                    node_id,
                    &block.tenant,
                    block.precision,
                    block.data.clone(),
                )
                .await;

            if let Err(CoordinatorError::FailoverNode { id: _ }) = result {
                let size = queue.write().await.size().await;
                warn!(
                    "hinted_off write data to {} failed, try later...; remain size: {:?}",
                    node_id, size
                );

                time::sleep(Duration::from_secs(3)).await;
                continue;
            }

            if result.is_err() {
                info!("hinted_off write data {} failed, {:?}", node_id, result);
            }

            break;
        }
    }
}

impl std::fmt::Debug for HintedOffManager {
    fn fmt(&self, _f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use tokio::io::AsyncSeekExt;
    use tokio::sync::RwLock;
    use tokio::time::{self, Duration};
    use trace::{info, init_default_global_tracing};

    use super::*;

    #[tokio::test]
    #[ignore]
    async fn test_hinted_off_file() {
        init_default_global_tracing("tskv_log", "tskv.log", "debug");

        let dir = "/tmp/cnosdb/hh_test".to_string();
        //let _ = std::fs::remove_dir_all(dir.clone());

        let config = QueueConfig {
            data_path: dir,
            file_suffix: "hh".to_string(),
            max_file_size: 256,
        };

        let queue = Queue::new(config).await.unwrap();
        let queue = Arc::new(RwLock::new(queue));

        tokio::spawn(read_hinted_off_file(queue.clone()));

        for i in 1..100 {
            let data = format!("aaa-datadfdsag-{}-ffdffdfedata-aaa", i);
            let block = HintedOffBlock::new(
                1000 + i,
                123,
                "cnosdb".to_string(),
                Precision::NS,
                data.as_bytes().to_vec(),
            );
            queue.write().await.write(&block).await.unwrap();
            info!("========  write data: {}", block.debug());

            time::sleep(Duration::from_secs(10)).await;
        }

        time::sleep(Duration::from_secs(3000)).await;
    }

    async fn read_hinted_off_file(queue: Arc<RwLock<Queue>>) {
        debug!("read file started.");
        let mut block = HintedOffBlock::new(0, 0, "".to_string(), Precision::NS, vec![]);
        loop {
            match queue.write().await.read(&mut block).await {
                Ok(_) => {
                    info!("======== read block data: {}", block.debug());
                }

                Err(err) => {
                    info!("======== read block err: {}", err);
                }
            }
            let _ = queue.write().await.commit().await;

            time::sleep(Duration::from_secs(8)).await;
        }
    }

    #[tokio::test]
    #[ignore]
    async fn test_read_write_file() {
        use std::io::SeekFrom;

        let file_name =
            "/Users/adminliu/github.com/cnosdb/cnosdb_main/coordinator/src/file.test".to_string();
        let mut file = tokio::fs::OpenOptions::new()
            .create(true)
            .write(true)
            //.append(true)
            .open(file_name)
            .await
            .unwrap();

        file.write_all(b"abc_123456").await.unwrap();
        file.seek(SeekFrom::Start(4)).await.unwrap();
        file.write_all(b"efg").await.unwrap();
    }
}
