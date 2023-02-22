use std::collections::HashMap;
use std::io::SeekFrom;
use std::marker::PhantomData;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;
use config::HintedOffConfig;
use protos::models as fb_models;
use snafu::prelude::*;
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use tokio::sync::mpsc::Receiver;
use tokio::sync::oneshot::{self, Sender};
use tokio::sync::RwLock;
use tokio::time::{self, Duration};
use trace::{debug, error, info, warn};

use crate::error::{self, Error, Result};
use crate::file_system::file_manager::{self, list_dir_names, FileManager};
use crate::file_system::{AsyncFile, FileCursor, IFile};
use crate::{byte_utils, file_utils};

const SEGMENT_FILE_HEADER_SIZE: u64 = 12;
const SEGMENT_FILE_MAGIC: [u8; 4] = [0x51, 0x55, 0x51, 0x45];

#[async_trait]
pub trait DataBlock {
    async fn write(&self, file: &mut File) -> Result<usize>;
    async fn read(&mut self, file: &mut File) -> Result<usize>;
}

pub struct QueueConfig {
    pub data_path: String,
    pub file_suffix: String,

    pub max_file_size: u64,
}

pub struct Queue {
    config: QueueConfig,
    write_file: File,
    write_file_id: u64,
    write_file_size: u64,

    read_file: File,
    read_file_id: u64,
}

impl Queue {
    pub async fn new(config: QueueConfig) -> Result<Self> {
        let suffix = config.file_suffix.clone();
        let data_dir = PathBuf::from(config.data_path.clone());
        if !file_manager::try_exists(&data_dir) {
            std::fs::create_dir_all(&data_dir)?;
        }

        let mut min_file_id = 1;
        let mut max_file_id = 1;
        if let Some((min_id, max_id)) = file_utils::get_file_id_range(data_dir.clone(), &suffix) {
            min_file_id = min_id;
            max_file_id = max_id;
        }

        let file_name = file_utils::make_file_name(data_dir.clone(), max_file_id, &suffix);
        let (write_file, write_file_size) = Queue::open_write_file(file_name.clone()).await?;
        info!("queue open write file: {:?}@{}", file_name, write_file_size);

        let file_name = file_utils::make_file_name(data_dir, min_file_id, &suffix);
        let (read_file, read_file_pos) = Queue::open_read_file(file_name.clone()).await?;
        info!("queue open read file: {:?}@{}", file_name, read_file_pos);

        Ok(Self {
            config,
            read_file,
            write_file,
            write_file_size,
            read_file_id: min_file_id,
            write_file_id: max_file_id,
        })
    }

    pub async fn write<Block>(&mut self, block: &Block) -> Result<()>
    where
        Block: DataBlock,
    {
        if self.write_file_size > self.config.max_file_size {
            let _ = self.roll_write_file().await;
        }

        let size = block.write(&mut self.write_file).await?;
        self.write_file_size += size as u64;

        Ok(())
    }

    pub async fn read<Block>(&mut self, block: &mut Block) -> Result<()>
    where
        Block: DataBlock,
    {
        if let Err(e) = block.read(&mut self.read_file).await {
            let seek_cur = self.read_file.seek(SeekFrom::Current(0)).await?;
            let file_size = self.read_file.metadata().await?.len();
            if seek_cur == file_size {
                let _ = self.roll_read_file().await;
            }

            return Err(e);
        }

        Ok(())
    }

    pub async fn commit(&mut self) -> Result<()> {
        let seek_cur = self.read_file.seek(SeekFrom::Current(0)).await?;
        Queue::write_offset(&mut self.read_file, seek_cur).await?;
        debug!("queue commit offset id:{}@{}", self.read_file_id, seek_cur);

        Ok(())
    }

    pub async fn close(&mut self) -> Result<()> {
        self.write_file.flush().await?;

        Ok(())
    }

    async fn open_write_file(file_name: impl AsRef<Path>) -> Result<(File, u64)> {
        let mut file = tokio::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .append(true)
            .open(file_name)
            .await?;

        let mut file_size = file.metadata().await?.len();
        if file_size == 0 {
            let mut header_buf = [0_u8; SEGMENT_FILE_HEADER_SIZE as usize];
            header_buf[..4].copy_from_slice(SEGMENT_FILE_MAGIC.as_slice());
            header_buf[4..].copy_from_slice(&SEGMENT_FILE_HEADER_SIZE.to_be_bytes());
            file.write_all(&header_buf).await?;

            file_size = SEGMENT_FILE_HEADER_SIZE;
        }

        Ok((file, file_size))
    }

    async fn open_read_file(file_name: impl AsRef<Path>) -> Result<(File, u64)> {
        let mut file = tokio::fs::OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(file_name)
            .await?;

        let offset = Queue::read_offset(&mut file).await?;

        file.seek(SeekFrom::Start(offset)).await?;

        Ok((file, offset))
    }

    async fn roll_write_file(&mut self) -> Result<()> {
        debug!("queue file '{}' is full", self.write_file_id);

        let new_file_id = self.write_file_id + 1;
        let new_file_name = file_utils::make_file_name(
            PathBuf::from(self.config.data_path.clone()),
            new_file_id,
            &self.config.file_suffix,
        );

        let (file, size) = Queue::open_write_file(new_file_name.clone()).await?;
        self.write_file = file;
        self.write_file_size = size;
        self.write_file_id = new_file_id;

        info!("queue starts write: {:?}@{}", new_file_name, size);

        Ok(())
    }

    async fn roll_read_file(&mut self) -> Result<()> {
        debug!("queue file: {} read over", self.read_file_id);

        let new_file_id = self.read_file_id + 1;
        let new_file_name = file_utils::make_file_name(
            PathBuf::from(self.config.data_path.clone()),
            new_file_id,
            &self.config.file_suffix,
        );
        if !file_manager::try_exists(&new_file_name) {
            return Ok(());
        }

        let (file, read_pos) = Queue::open_read_file(new_file_name.clone()).await?;
        self.read_file = file;
        self.read_file_id = new_file_id;
        info!("queue starts read: {:?}@{}", new_file_name, read_pos);

        let old_file_name = file_utils::make_file_name(
            PathBuf::from(self.config.data_path.clone()),
            new_file_id - 1,
            &self.config.file_suffix,
        );

        let _ = tokio::fs::remove_file(old_file_name).await;

        Ok(())
    }

    async fn write_offset(file: &mut File, offset: u64) -> std::result::Result<(), std::io::Error> {
        let mut header_buf = [0_u8; SEGMENT_FILE_HEADER_SIZE as usize];
        header_buf[..4].copy_from_slice(SEGMENT_FILE_MAGIC.as_slice());
        header_buf[4..].copy_from_slice(&offset.to_be_bytes());

        let seek_cur = file.seek(SeekFrom::Current(0)).await?;
        file.seek(SeekFrom::Start(0)).await?;
        file.write_all(&header_buf).await?;
        file.seek(SeekFrom::Start(seek_cur)).await?;

        Ok(())
    }

    async fn read_offset(file: &mut File) -> std::result::Result<u64, std::io::Error> {
        let mut header_buf = [0_u8; SEGMENT_FILE_HEADER_SIZE as usize];

        let seek_cur = file.seek(SeekFrom::Current(0)).await?;
        file.seek(SeekFrom::Start(0)).await?;
        file.read_exact(&mut header_buf[..]).await?;
        file.seek(SeekFrom::Start(seek_cur)).await?;

        let offset = byte_utils::decode_be_u64(&header_buf[4..]);

        Ok(offset)
    }
}
