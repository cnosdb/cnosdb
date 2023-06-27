use std::io::{Read, SeekFrom, Write};
use std::path::{Path, PathBuf};

use trace::{debug, error};

use super::{IndexError, IndexResult};
use crate::file_system::file::async_file::AsyncFile;
use crate::file_system::file::cursor::FileCursor;
use crate::file_system::file::IFile;
use crate::file_system::file_manager;
use crate::{byte_utils, file_utils};

const SEGMENT_FILE_HEADER_SIZE: usize = 8;
const SEGMENT_FILE_MAGIC: [u8; 4] = [0x48, 0x49, 0x4e, 0x02];
const SEGMENT_FILE_MAX_SIZE: u64 = 64 * 1024 * 1024;
const BLOCK_HEADER_SIZE: usize = 16;

#[derive(Debug)]
pub struct SeriesKeyBlock {
    pub ts: i64,
    pub series_id: u32,
    pub data_len: u32,
    pub data: Vec<u8>,
}

impl SeriesKeyBlock {
    pub fn new(ts: i64, series_id: u32, data: Vec<u8>) -> Self {
        Self {
            ts,
            series_id,
            data_len: data.len() as u32,
            data,
        }
    }

    pub fn debug(&self) -> String {
        format!(
            "ts:{}, id: {}, data: {}",
            self.ts,
            self.series_id,
            String::from_utf8(self.data.clone()).unwrap()
        )
    }

    pub fn size(&self) -> u32 {
        self.data_len + BLOCK_HEADER_SIZE as u32
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(self.size() as usize);
        buf.extend_from_slice(&self.ts.to_be_bytes());
        buf.extend_from_slice(&self.series_id.to_be_bytes());
        buf.extend_from_slice(&self.data_len.to_be_bytes());
        buf.extend_from_slice(&self.data);

        buf
    }
}

pub struct IndexBinlog {
    path: PathBuf,

    writer_file: BinlogWriter,
}

impl IndexBinlog {
    pub async fn new(path: impl AsRef<Path>) -> IndexResult<Self> {
        let data_dir = path.as_ref();
        let (last, seq) = match file_utils::get_max_sequence_file_name(
            data_dir,
            file_utils::get_index_binlog_file_id,
        ) {
            Some((file, seq)) => (data_dir.join(file), seq),
            None => {
                let seq = 1;
                (file_utils::make_index_binlog_file(data_dir, seq), seq)
            }
        };

        if !file_manager::try_exists(data_dir) {
            std::fs::create_dir_all(data_dir)?;
        }

        let writer_file = BinlogWriter::open(seq, last).await?;

        Ok(IndexBinlog {
            path: data_dir.into(),
            writer_file,
        })
    }

    async fn roll_write_file(&mut self) -> IndexResult<()> {
        if self.writer_file.size > SEGMENT_FILE_MAX_SIZE {
            debug!(
                "Write Binlog '{}' is full , begin rolling.",
                self.writer_file.id
            );

            let new_file_id = self.writer_file.id + 1;
            let new_file_name = file_utils::make_index_binlog_file(&self.path, new_file_id);
            let new_file = BinlogWriter::open(new_file_id, new_file_name).await?;
            let mut old_file = std::mem::replace(&mut self.writer_file, new_file);
            old_file.flush().await?;

            debug!("Write Binlog  '{}' starts write", self.writer_file.id);
        }
        Ok(())
    }

    pub async fn write(&mut self, data: &[u8]) -> IndexResult<()> {
        self.roll_write_file().await?;

        self.writer_file.write(data).await?;

        Ok(())
    }

    pub fn current_write_file_id(&self) -> u64 {
        self.writer_file.id
    }

    pub async fn advance_write_offset(&mut self, offset: u32) -> IndexResult<()> {
        self.writer_file.advance_write_offset(offset).await
    }

    pub async fn close(&mut self) -> IndexResult<()> {
        self.writer_file.flush().await
    }
}

pub struct BinlogWriter {
    id: u64,
    size: u64,

    pub file: AsyncFile,
}

impl BinlogWriter {
    pub async fn open(id: u64, path: impl AsRef<Path>) -> IndexResult<Self> {
        let path = path.as_ref();

        // Get file and check if new file
        let mut new_file = false;
        let file = if file_manager::try_exists(path) {
            let f = file_manager::open_create_file(path)
                .await
                .map_err(|e| IndexError::FileErrors { msg: e.to_string() })?;
            if f.is_empty() {
                new_file = true;
            }
            f
        } else {
            new_file = true;
            file_manager::create_file(path)
                .await
                .map_err(|e| IndexError::FileErrors { msg: e.to_string() })?
        };

        let mut size = file.len();
        if new_file {
            size = 8;
            BinlogWriter::write_header(&file, SEGMENT_FILE_HEADER_SIZE as u32).await?;
        }

        Ok(Self { id, file, size })
    }

    pub async fn write_header(file: &AsyncFile, offset: u32) -> IndexResult<()> {
        let mut header_buf = [0_u8; SEGMENT_FILE_HEADER_SIZE];
        header_buf[..4].copy_from_slice(SEGMENT_FILE_MAGIC.as_slice());
        header_buf[4..].copy_from_slice(&offset.to_be_bytes());

        file.write_at(0, &header_buf).await?;
        file.sync_data().await?;

        Ok(())
    }

    pub async fn advance_write_offset(&mut self, mut offset: u32) -> IndexResult<()> {
        if offset == 0 {
            offset = self.size as u32;
        }

        BinlogWriter::write_header(&self.file, offset).await
    }

    pub async fn write(&mut self, data: &[u8]) -> IndexResult<usize> {
        let mut pos = self.size;
        pos += self.file.write_at(pos, data).await? as u64;

        // pos += self.file.write_at(pos, &block.ts.to_be_bytes()).await? as u64;
        // pos += self
        //     .file
        //     .write_at(pos, &block.series_id.to_be_bytes())
        //     .await? as u64;
        // pos += self
        //     .file
        //     .write_at(pos, &block.data_len.to_be_bytes())
        //     .await? as u64;
        // pos += self.file.write_at(pos, &block.data).await? as u64;

        debug!(
            "Write binlog data pos: {}, len: {}",
            self.size,
            (pos - self.size)
        );

        let written_size = (pos - self.size) as usize;
        self.size = pos;

        Ok(written_size)
    }

    pub async fn flush(&mut self) -> IndexResult<()> {
        // Do fsync
        self.file.sync_data().await?;

        Ok(())
    }
}

pub struct BinlogReader {
    id: u64,
    cursor: FileCursor,

    body_buf: Vec<u8>,
    header_buf: [u8; BLOCK_HEADER_SIZE],
}

impl BinlogReader {
    pub async fn new(id: u64, mut cursor: FileCursor) -> IndexResult<Self> {
        let header_buf = BinlogReader::reade_header(&mut cursor).await?;
        let offset = byte_utils::decode_be_u32(&header_buf[4..8]);

        debug!("Read index binlog begin read offset: {}", offset);

        cursor.set_pos(offset as u64);

        Ok(Self {
            id,
            cursor,
            header_buf: [0_u8; BLOCK_HEADER_SIZE],
            body_buf: vec![],
        })
    }

    async fn reade_header(cursor: &mut FileCursor) -> IndexResult<[u8; SEGMENT_FILE_HEADER_SIZE]> {
        let mut header_buf = [0_u8; SEGMENT_FILE_HEADER_SIZE];

        cursor.seek(SeekFrom::Start(0))?;
        let _read = cursor.read(&mut header_buf[..]).await?;

        Ok(header_buf)
    }

    pub fn read_over(&mut self) -> bool {
        self.cursor.pos() >= self.cursor.len()
    }

    pub async fn advance_read_offset(&mut self, mut offset: u32) -> IndexResult<()> {
        if offset == 0 {
            offset = self.cursor.pos() as u32;
        }

        BinlogWriter::write_header(self.cursor.file_ref(), offset).await
    }

    pub fn read_pos(&self) -> u32 {
        self.cursor.pos() as u32
    }

    pub fn file_len(&self) -> u32 {
        self.cursor.len() as u32
    }

    pub async fn next_block(&mut self) -> IndexResult<Option<SeriesKeyBlock>> {
        if self.read_over() {
            return Ok(None);
        }

        debug!("Read index binlog: cursor.pos={}", self.cursor.pos());

        let read_bytes = self.cursor.read(&mut self.header_buf[..]).await?;
        if read_bytes < BLOCK_HEADER_SIZE {
            return Err(IndexError::FileErrors {
                msg: format!("read header length {} < {}", read_bytes, BLOCK_HEADER_SIZE),
            });
        }

        let ts = byte_utils::decode_be_i64(self.header_buf[0..8].into());
        let id = byte_utils::decode_be_u32(self.header_buf[8..12].into());
        let data_len = byte_utils::decode_be_u32(self.header_buf[12..16].into());
        if data_len == 0 {
            return Ok(Some(SeriesKeyBlock {
                ts,
                series_id: id,
                data_len,
                data: vec![],
            }));
        }
        debug!("Read Binlog Reader: data_len={}", data_len);

        if data_len > (self.file_len() - self.read_pos()) {
            error!(
                "binlog read block error {}, {} {} ",
                data_len,
                self.file_len(),
                self.read_pos()
            );

            return Err(IndexError::FileErrors {
                msg: format!(
                    "{} block data length {} > {}-{}",
                    ts,
                    data_len,
                    self.file_len(),
                    self.read_pos()
                ),
            });
        }

        if data_len as usize > self.body_buf.len() {
            self.body_buf.resize(data_len as usize, 0);
        }

        let buf = &mut self.body_buf.as_mut_slice()[0..data_len as usize];
        let read_bytes = self.cursor.read(buf).await?;
        if read_bytes != data_len as usize {
            return Err(IndexError::FileErrors {
                msg: format!(
                    "{} read block data error {} != {}",
                    ts, read_bytes, data_len
                ),
            });
        }

        Ok(Some(SeriesKeyBlock {
            ts,
            series_id: id,
            data_len,
            data: buf.to_vec(),
        }))
    }
}

pub async fn repair_index_file(file_name: &str) -> IndexResult<()> {
    let tmp_file = BinlogWriter::open(0, PathBuf::from(file_name)).await?;
    let mut reader_file = BinlogReader::new(0, tmp_file.file.into()).await?;

    let file_read_offset = reader_file.read_pos();
    let mut max_can_repair = 0;

    while let Ok(Some(_)) = reader_file.next_block().await {
        max_can_repair = reader_file.read_pos();
    }

    println!(
        "file length: {}, persistence offset: {},  can repair offset: {}",
        reader_file.file_len(),
        file_read_offset,
        max_can_repair
    );

    if file_read_offset >= max_can_repair {
        println!("don't need generate repaire file");
        return Ok(());
    }

    let mut buffer = Vec::new();
    std::fs::File::open(file_name)?.read_to_end(&mut buffer)?;

    let mut file = std::fs::File::create(format!("{}.repair", file_name))?;

    file.write_all(&buffer)?;
    file.set_len(max_can_repair as u64)?;

    Ok(())
}
