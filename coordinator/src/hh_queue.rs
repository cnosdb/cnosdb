use std::{
    collections::HashMap,
    io::SeekFrom,
    marker::PhantomData,
    path::{Path, PathBuf},
    sync::Arc,
};

use config::HintedOffConfig;
use parking_lot::RwLock;
use protos::models as fb_models;
use snafu::prelude::*;
use tokio::{
    sync::mpsc::Receiver,
    sync::oneshot::{self, Sender},
    time::{self, Duration},
};
use trace::{debug, error, info, warn};
use tskv::file_system::file_manager::{self, list_dir_names, FileManager};
use tskv::file_system::{DmaFile, FileCursor, FileSync};
use tskv::{byte_utils, file_utils};

use crate::errors::*;
use crate::writer::PointWriter;

const SEGMENT_FILE_HEADER_SIZE: usize = 8;
const SEGMENT_FILE_MAGIC: [u8; 4] = [0x48, 0x49, 0x4e, 0x54];
const SEGMENT_FILE_MAX_SIZE: u64 = 1073741824; // 1 GiB
const HINTEDOFF_BLOCK_HEADER_SIZE: usize = 16;

#[derive(Debug)]
pub struct HintedOffOption {
    pub enable: bool,
    pub path: String,
    pub node_id: u64,
}

impl HintedOffOption {
    pub fn new(config: &HintedOffConfig, node_id: u64) -> Self {
        Self {
            node_id,
            enable: config.enable,
            path: format!("{}/{}", config.path, node_id),
        }
    }
}

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
    pub data_len: u32,
    pub data: Vec<u8>,
}

impl HintedOffBlock {
    pub fn new(ts: i64, vnode_id: u32, data: Vec<u8>) -> Self {
        Self {
            ts,
            vnode_id,
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

    pub fn size(&self) -> u32 {
        self.data_len + HINTEDOFF_BLOCK_HEADER_SIZE as u32
    }
}

pub struct HintedOffManager {
    config: HintedOffConfig,

    writer: Arc<PointWriter>,

    nodes: RwLock<HashMap<u64, Arc<RwLock<HintedOffQueue>>>>,
}

impl HintedOffManager {
    pub fn new(config: HintedOffConfig, writer: Arc<PointWriter>) -> Self {
        let manager = Self {
            config,
            writer,
            nodes: RwLock::new(HashMap::new()),
        };

        let dir = PathBuf::from(manager.config.path.clone());
        for id in list_dir_names(dir).iter() {
            if let Ok(id) = id.parse::<u64>() {
                manager.get_or_create_queue(id).unwrap();
            }
        }

        manager
    }

    pub async fn write_handoff_job(
        manager: Arc<HintedOffManager>,
        mut hh_receiver: Receiver<HintedOffWriteReq>,
    ) {
        while let Some(request) = hh_receiver.recv().await {
            let result = match manager.get_or_create_queue(request.node_id) {
                Ok(queue) => queue.write().write(&request.block),
                Err(err) => Err(err),
            };

            request.sender.send(result).expect("successful");
        }
    }

    fn get_or_create_queue(&self, id: u64) -> CoordinatorResult<Arc<RwLock<HintedOffQueue>>> {
        let mut nodes = self.nodes.write();
        if let Some(val) = nodes.get(&id) {
            return Ok(val.clone());
        }

        let queue = HintedOffQueue::new(HintedOffOption::new(&self.config, id))?;
        let queue = Arc::new(RwLock::new(queue));
        nodes.insert(id, queue.clone());

        tokio::spawn(HintedOffManager::hinted_off_service(
            id,
            self.writer.clone(),
            queue.clone(),
        ));

        Ok(queue)
    }

    async fn hinted_off_service(
        node_id: u64,
        writer: Arc<PointWriter>,
        queue: Arc<RwLock<HintedOffQueue>>,
    ) {
        debug!("hinted_off_service started for node: {}", node_id);

        loop {
            let block_data = queue.write().read();
            match block_data {
                Ok(block) => {
                    loop {
                        if writer
                            .write_to_remote_node(block.vnode_id, node_id, block.data.clone())
                            .await
                            .is_ok()
                        {
                            break;
                        } else {
                            info!("hinted_off write data to node {} failed", node_id);
                            time::sleep(Duration::from_secs(3)).await;
                        }
                    }

                    if let Err(err) = queue.write().advance_read_offset(0) {
                        info!("advance offset {}", err.to_string());
                    }
                }

                Err(err) => {
                    info!("read hindoff data: {}", err.to_string());
                    time::sleep(Duration::from_secs(3)).await;
                }
            }
        }
    }
}

impl std::fmt::Debug for HintedOffManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Ok(())
    }
}

pub struct HintedOffQueue {
    option: HintedOffOption,

    writer_file: HintedOffWriter,
    reader_file: HintedOffReader,
}

impl HintedOffQueue {
    pub fn new(option: HintedOffOption) -> CoordinatorResult<Self> {
        let data_dir = PathBuf::from(option.path.clone());
        let (last, seq) = match file_utils::get_max_sequence_file_name(
            data_dir.clone(),
            file_utils::get_hinted_off_file_id,
        ) {
            Some((file, seq)) => (data_dir.join(file), seq),
            None => {
                let seq = 1;
                (file_utils::make_hinted_off_file(data_dir.clone(), seq), seq)
            }
        };

        if !file_manager::try_exists(&data_dir) {
            std::fs::create_dir_all(&data_dir)?;
        }

        let writer_file = HintedOffWriter::open(seq, last)?;

        let hh_files = file_manager::list_file_names(&data_dir);
        let read_filename = hh_files[0].clone();
        let read_fileid = file_utils::get_hinted_off_file_id(&read_filename)?;
        let tmp_file = HintedOffWriter::open(read_fileid, data_dir.join(read_filename))?;
        let reader_file = HintedOffReader::new(read_fileid, tmp_file.file.into())?;

        Ok(HintedOffQueue {
            option,
            writer_file,
            reader_file,
        })
    }

    fn roll_hinted_off_write_file(&mut self) -> CoordinatorResult<()> {
        if self.writer_file.size > SEGMENT_FILE_MAX_SIZE {
            debug!(
                "Write Hinted Off '{}' is full , begin rolling.",
                self.writer_file.id
            );

            let new_file_id = self.writer_file.id + 1;
            let new_file_name = file_utils::make_hinted_off_file(&self.option.path, new_file_id);
            let new_file = HintedOffWriter::open(new_file_id, new_file_name)?;
            let mut old_file = std::mem::replace(&mut self.writer_file, new_file);
            old_file.flush()?;

            debug!("Write Hinted Off  '{}' starts write", self.writer_file.id);
        }
        Ok(())
    }

    fn roll_hinted_off_read_file(&mut self) -> CoordinatorResult<()> {
        if self.reader_file.read_over() {
            debug!("Read Hinted Off is read over '{}'", self.reader_file.id);

            let new_file_id = self.reader_file.id + 1;
            let new_file_name = file_utils::make_hinted_off_file(&self.option.path, new_file_id);
            if !file_manager::try_exists(&new_file_name) {
                return Ok(());
            }

            let new_file = HintedOffWriter::open(new_file_id, new_file_name)?;
            self.reader_file = HintedOffReader::new(new_file_id, new_file.file.into())?;
        }

        Ok(())
    }

    pub fn write(&mut self, block: &HintedOffBlock) -> CoordinatorResult<()> {
        if !self.option.enable {
            return Ok(());
        }

        self.roll_hinted_off_write_file()?;

        self.writer_file.write(block)?;

        Ok(())
    }

    pub fn advance_read_offset(&mut self, offset: u32) -> CoordinatorResult<()> {
        self.reader_file.advance_read_offset(offset)
    }

    pub fn read(&mut self) -> CoordinatorResult<HintedOffBlock> {
        self.roll_hinted_off_read_file()?;

        if let Some(val) = self.reader_file.next_hinted_off_block() {
            Ok(val)
        } else {
            Err(CoordinatorError::IOErrors {
                msg: "no data to read".to_string(),
            })
        }
    }

    pub fn close(&mut self) -> CoordinatorResult<()> {
        self.writer_file.flush()
    }
}

struct HintedOffWriter {
    id: u64,

    file: DmaFile,
    size: u64,
}

impl HintedOffWriter {
    pub fn open(id: u64, path: impl AsRef<Path>) -> CoordinatorResult<Self> {
        let path = path.as_ref();

        // Get file and check if new file
        let mut new_file = false;
        let file = if file_manager::try_exists(path) {
            let f = file_manager::get_file_manager().open_file(path)?;
            if f.is_empty() {
                new_file = true;
            }
            f
        } else {
            new_file = true;
            file_manager::get_file_manager().create_file(path)?
        };

        if new_file {
            HintedOffWriter::write_header(&file, SEGMENT_FILE_HEADER_SIZE as u32)?;
        }
        let size = file.len();

        Ok(Self { id, file, size })
    }

    pub fn write_header(file: &DmaFile, offset: u32) -> CoordinatorResult<()> {
        let mut header_buf = [0_u8; SEGMENT_FILE_HEADER_SIZE];
        header_buf[..4].copy_from_slice(SEGMENT_FILE_MAGIC.as_slice());
        header_buf[4..].copy_from_slice(&offset.to_be_bytes());

        file.write_at(0, &header_buf)
            .and_then(|_| file.sync_all(FileSync::Hard))?;

        Ok(())
    }

    fn reade_header(cursor: &mut FileCursor) -> CoordinatorResult<[u8; SEGMENT_FILE_HEADER_SIZE]> {
        let mut header_buf = [0_u8; SEGMENT_FILE_HEADER_SIZE];

        cursor.seek(SeekFrom::Start(0))?;
        let read = cursor.read(&mut header_buf[..])?;

        Ok(header_buf)
    }

    pub fn write(&mut self, block: &HintedOffBlock) -> CoordinatorResult<usize> {
        let mut pos = self.size;

        self.file
            .write_at(pos, &block.ts.to_be_bytes())
            .and_then(|size| {
                pos += size as u64;
                self.file.write_at(pos, &block.vnode_id.to_be_bytes())
            })
            .and_then(|size| {
                // write crc
                pos += size as u64;
                self.file.write_at(pos, &block.data_len.to_be_bytes())
            })
            .and_then(|size| {
                // write data
                pos += size as u64;
                self.file.write_at(pos, &block.data)
            })
            .map(|size| {
                // sync
                pos += size as u64;
            })?;

        debug!(
            "Write hinted off data pos: {}, len: {}",
            self.size,
            (pos - self.size)
        );

        let written_size = (pos - self.size) as usize;
        self.size = pos;

        Ok(written_size)
    }

    pub fn flush(&mut self) -> CoordinatorResult<()> {
        // Do fsync
        self.file.sync_all(FileSync::Hard)?;

        Ok(())
    }
}

pub struct HintedOffReader {
    id: u64,
    cursor: FileCursor,

    body_buf: Vec<u8>,
    header_buf: [u8; HINTEDOFF_BLOCK_HEADER_SIZE],
}

impl HintedOffReader {
    pub fn new(id: u64, mut cursor: FileCursor) -> CoordinatorResult<Self> {
        let header_buf = HintedOffWriter::reade_header(&mut cursor)?;
        let offset = byte_utils::decode_be_u32(&header_buf[4..8]);

        debug!("Read hinted off begin read offset: {}", offset);

        cursor.set_pos(offset as u64);

        Ok(Self {
            id,
            cursor,
            header_buf: [0_u8; HINTEDOFF_BLOCK_HEADER_SIZE],
            body_buf: vec![],
        })
    }

    pub fn read_over(&mut self) -> bool {
        self.cursor.pos() >= self.cursor.len()
    }

    pub fn advance_read_offset(&mut self, mut offset: u32) -> CoordinatorResult<()> {
        if offset == 0 {
            offset = self.cursor.pos() as u32;
        }

        HintedOffWriter::write_header(self.cursor.file_ref(), offset)
    }

    pub fn next_hinted_off_block(&mut self) -> Option<HintedOffBlock> {
        debug!("Read hinted off Reader: cursor.pos={}", self.cursor.pos());

        let read_bytes = match self.cursor.read(&mut self.header_buf[..]) {
            Ok(v) => v,
            Err(e) => {
                error!("failed read block header buf : {:?}", e);
                return None;
            }
        };
        if read_bytes < HINTEDOFF_BLOCK_HEADER_SIZE {
            return None;
        }

        let ts = byte_utils::decode_be_i64(self.header_buf[0..8].into());
        let id = byte_utils::decode_be_u32(self.header_buf[8..12].into());
        let data_len = byte_utils::decode_be_u32(self.header_buf[12..16].into());
        if data_len == 0 {
            return None;
        }
        debug!("Read hinted off Reader: data_len={}", data_len);

        if data_len as usize > self.body_buf.len() {
            self.body_buf.resize(data_len as usize, 0);
        }

        let buf = &mut self.body_buf.as_mut_slice()[0..data_len as usize];
        let read_bytes = match self.cursor.read(buf) {
            Ok(v) => v,
            Err(e) => {
                error!("failed read body buf : {:?}", e);
                return None;
            }
        };

        Some(HintedOffBlock {
            ts,
            vnode_id: id,
            data_len,
            data: buf.to_vec(),
        })
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use super::*;
    use tokio::time::{self, Duration};

    use parking_lot::RwLock;
    use trace::init_default_global_tracing;

    #[tokio::test]
    async fn test_hinted_off_file() {
        init_default_global_tracing("tskv_log", "tskv.log", "debug");

        let dir = "/tmp/cnosdb/hh".to_string();
        //let _ = std::fs::remove_dir_all(dir.clone());

        let option = HintedOffOption::new(
            &HintedOffConfig {
                enable: true,
                path: dir,
            },
            1,
        );
        let queue = HintedOffQueue::new(option).unwrap();
        let queue = Arc::new(RwLock::new(queue));

        tokio::spawn(read_hinted_off_file(queue.clone()));

        for i in 1..100 {
            let data = format!("aaa-datadfdsag{}ffdffdfedata-aaa", i);
            let block = HintedOffBlock::new(1000 + i, 123, data.as_bytes().to_vec());
            queue.write().write(&block).unwrap();

            //time::sleep(Duration::from_secs(3)).await;
        }

        time::sleep(Duration::from_secs(3)).await;
    }

    async fn read_hinted_off_file(queue: Arc<RwLock<HintedOffQueue>>) {
        debug!("read file started.");
        time::sleep(Duration::from_secs(1)).await;
        let mut count = 0;
        loop {
            match queue.write().read() {
                Ok(block) => {
                    count += 1;
                }

                Err(err) => {
                    break;
                }
            }
            let _ = queue.write().advance_read_offset(0);
        }

        if count != 100 {
            panic!("hinted off read write wrong");
        }
    }

    #[tokio::test]
    async fn test_list_dir_names() {
        let list = list_dir_names(PathBuf::from("/tmp/cnosdb".to_string()));

        print!("{:#?}", list);
    }
}
