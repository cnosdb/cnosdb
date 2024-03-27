use std::io::{Read, SeekFrom, Write};
use std::path::{Path, PathBuf};

use models::{SeriesId, SeriesKey};
use serde::{Deserialize, Serialize};
use trace::{debug, error};

use super::{IndexError, IndexResult};
use crate::file_system::async_filesystem::{LocalFileSystem, LocalFileType};
use crate::file_system::file::stream_reader::FileStreamReader;
use crate::file_system::file::stream_writer::FileStreamWriter;
use crate::file_system::FileSystem;
use crate::{byte_utils, file_utils};

pub const SEGMENT_FILE_HEADER_SIZE: usize = 8;
pub const SEGMENT_FILE_MAGIC: [u8; 4] = [0x48, 0x49, 0x4e, 0x02];
pub const SEGMENT_FILE_MAX_SIZE: usize = 64 * 1024 * 1024;
pub const BLOCK_HEADER_SIZE: usize = 4;

#[derive(Serialize, Deserialize, Debug, PartialEq)]
pub enum IndexBinlogBlock {
    Add(Vec<AddSeries>),
    Delete(DeleteSeries),
    Update(UpdateSeriesKey),
}

#[derive(Serialize, Deserialize, Debug, PartialEq)]
pub struct AddSeries {
    ts: i64,
    series_id: SeriesId,
    data: SeriesKey,
}

impl AddSeries {
    pub fn new(ts: i64, series_id: SeriesId, data: SeriesKey) -> Self {
        Self {
            ts,
            series_id,
            data,
        }
    }

    pub fn ts(&self) -> i64 {
        self.ts
    }

    pub fn series_id(&self) -> SeriesId {
        self.series_id
    }

    pub fn data(&self) -> &SeriesKey {
        &self.data
    }
}

#[derive(Serialize, Deserialize, Debug, PartialEq)]
pub struct DeleteSeries {
    series_id: SeriesId,
}

impl DeleteSeries {
    pub fn new(series_id: SeriesId) -> Self {
        Self { series_id }
    }

    pub fn series_id(&self) -> SeriesId {
        self.series_id
    }
}

#[derive(Serialize, Deserialize, Debug, PartialEq)]
pub struct UpdateSeriesKey {
    // delete keys
    // insert: (keys, ids) + (tag, ids) + (ids + keys)
    old_series: SeriesKey,
    new_series: SeriesKey,
    series_id: SeriesId,
    // 如果是恢复wal执行的update操作(recovering = true)，需要标记为已删除的series key删掉
    // 如果是正常运行过程中的update操作(recovering = false)，需要将待修改的series key加删除标记
    recovering: bool,
}

impl UpdateSeriesKey {
    pub fn new(
        old_series: SeriesKey,
        new_series: SeriesKey,
        series_id: SeriesId,
        recovering: bool,
    ) -> Self {
        Self {
            old_series,
            new_series,
            series_id,
            recovering,
        }
    }

    pub fn old_series(&self) -> &SeriesKey {
        &self.old_series
    }

    pub fn new_series(&self) -> &SeriesKey {
        &self.new_series
    }

    pub fn series_id(&self) -> SeriesId {
        self.series_id
    }

    pub fn recovering(&self) -> bool {
        self.recovering
    }
}

impl IndexBinlogBlock {
    pub fn size_bytes(&self) -> Result<u32, IndexError> {
        let size = bincode::serialized_size(self).map_err(|err| IndexError::EncodeIndexBinlog {
            msg: err.to_string(),
        })? as u32;
        Ok(size)
    }

    pub fn encode<W>(&self, writer: W) -> Result<(), IndexError>
    where
        W: std::io::Write,
    {
        bincode::serialize_into(writer, self).map_err(|err| IndexError::EncodeIndexBinlog {
            msg: err.to_string(),
        })
    }

    pub fn decode(buf: &[u8]) -> Result<Self, IndexError> {
        bincode::deserialize(buf).map_err(|err| IndexError::DecodeIndexBinlog {
            msg: err.to_string(),
        })
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

        if !LocalFileSystem::try_exists(data_dir) {
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

    pub async fn write_blocks(&mut self, blocks: &[IndexBinlogBlock]) -> IndexResult<()> {
        self.roll_write_file().await?;

        let mut buffer = Vec::new();
        for block in blocks {
            let size = block.size_bytes()?;
            buffer.extend(size.to_be_bytes());
            buffer.reserve(size as usize);
            block.encode(&mut buffer)?;
        }

        self.writer_file.write(&buffer).await?;

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
    size: usize,

    pub file: Box<FileStreamWriter>,
}

impl BinlogWriter {
    pub async fn open(id: u64, path: impl AsRef<Path>) -> IndexResult<Self> {
        let file_system = LocalFileSystem::new(LocalFileType::Mmap);
        let mut file = file_system
            .open_file_writer(path.as_ref().to_path_buf())
            .await
            .map_err(|err| IndexError::FileSystemError { source: err })?;

        let mut size = file.len();
        if size < SEGMENT_FILE_HEADER_SIZE {
            size = SEGMENT_FILE_HEADER_SIZE;
            BinlogWriter::write_header(&mut file, SEGMENT_FILE_HEADER_SIZE as u32).await?;
        }

        Ok(Self { id, file, size })
    }

    pub async fn write_header(file: &mut Box<FileStreamWriter>, offset: u32) -> IndexResult<()> {
        let mut header_buf = [0_u8; SEGMENT_FILE_HEADER_SIZE];
        header_buf[..4].copy_from_slice(SEGMENT_FILE_MAGIC.as_slice());
        header_buf[4..].copy_from_slice(&offset.to_be_bytes());

        file.write(&header_buf).await?;
        file.flush().await?;

        Ok(())
    }

    pub async fn advance_write_offset(&mut self, mut offset: u32) -> IndexResult<()> {
        if offset == 0 {
            offset = self.size as u32;
        }

        BinlogWriter::write_header(&mut self.file, offset).await
    }

    pub async fn write(&mut self, data: &[u8]) -> IndexResult<usize> {
        let written_size = self.file.write(data).await?;

        Ok(written_size)
    }

    pub async fn flush(&mut self) -> IndexResult<()> {
        // Do fsync
        self.file.flush().await?;

        Ok(())
    }
}

pub struct BinlogReader {
    id: u64,
    cursor: Box<FileStreamReader>,

    body_buf: Vec<u8>,
    header_buf: [u8; BLOCK_HEADER_SIZE],
}

impl BinlogReader {
    pub async fn open(id: u64, path: impl AsRef<Path>) -> IndexResult<Self> {
        let file_system = LocalFileSystem::new(LocalFileType::Mmap);
        let file = file_system
            .open_file_reader(path.as_ref().to_path_buf())
            .await
            .map_err(|err| IndexError::FileSystemError { source: err })?;

        BinlogReader::new(id, file).await
    }

    pub async fn new(id: u64, mut cursor: Box<FileStreamReader>) -> IndexResult<Self> {
        let header_buf = BinlogReader::reade_header(&mut cursor).await?;
        let offset = byte_utils::decode_be_u32(&header_buf[4..8]);

        debug!("Read index binlog begin read offset: {}", offset);

        cursor.seek(SeekFrom::Start(offset as u64))?;

        Ok(Self {
            id,
            cursor,
            header_buf: [0_u8; BLOCK_HEADER_SIZE],
            body_buf: vec![],
        })
    }

    async fn reade_header(
        cursor: &mut Box<FileStreamReader>,
    ) -> IndexResult<[u8; SEGMENT_FILE_HEADER_SIZE]> {
        let mut header_buf = [0_u8; SEGMENT_FILE_HEADER_SIZE];

        let _read = cursor.read_at(0, &mut header_buf[..]).await?;

        Ok(header_buf)
    }

    pub fn read_over(&mut self) -> bool {
        self.cursor.pos() >= self.cursor.len()
    }

    pub fn pos(&self) -> usize {
        self.cursor.pos()
    }

    pub fn seek(&mut self, pos: u64) -> IndexResult<()> {
        self.cursor.seek(SeekFrom::Start(pos))?;
        Ok(())
    }

    pub async fn advance_read_offset(&mut self, mut offset: u32) -> IndexResult<()> {
        if offset == 0 {
            offset = self.cursor.pos() as u32;
        }

        let file_system = LocalFileSystem::new(LocalFileType::Mmap);
        let mut file = file_system
            .open_file_writer(self.cursor.path())
            .await
            .map_err(|err| IndexError::FileSystemError { source: err })?;

        BinlogWriter::write_header(&mut file, offset).await
    }

    pub fn read_pos(&self) -> usize {
        self.cursor.pos()
    }

    pub fn file_len(&self) -> usize {
        self.cursor.len()
    }

    pub async fn next_block(&mut self) -> IndexResult<Option<IndexBinlogBlock>> {
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

        let data_len = byte_utils::decode_be_u32(self.header_buf[0..BLOCK_HEADER_SIZE].into());

        debug!("Read Binlog Reader: data_len={}", data_len);

        if data_len > (self.file_len() - self.read_pos()) as u32 {
            error!(
                "binlog read block error {}, {} {} ",
                data_len,
                self.file_len(),
                self.read_pos()
            );

            return Err(IndexError::FileErrors {
                msg: format!(
                    "block data length {} > {}-{}",
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
                msg: format!("read block data error {} != {}", read_bytes, data_len),
            });
        }

        let block = IndexBinlogBlock::decode(buf)?;

        Ok(Some(block))
    }
}

pub async fn repair_index_file(file_name: &str) -> IndexResult<()> {
    let flie_system = LocalFileSystem::new(LocalFileType::Mmap);
    let reader_file = flie_system
        .open_file_reader(file_name)
        .await
        .map_err(|err| IndexError::FileSystemError { source: err })?;
    let mut reader_file = BinlogReader::new(0, reader_file).await?;

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

#[cfg(test)]
mod test {
    use models::SeriesKey;

    use super::{AddSeries, IndexBinlogBlock};
    use crate::file_utils::make_index_binlog_file;
    use crate::index::binlog::{BinlogReader, IndexBinlog};

    /// ( timestamp, series_id, data )
    type IndexBinlogBlockDesc<'a> = (i64, u32, &'a str);

    fn build_series_key_blocks(
        series_key_blk_desc: &[IndexBinlogBlockDesc<'_>],
    ) -> Vec<IndexBinlogBlock> {
        let mut blocks = Vec::with_capacity(series_key_blk_desc.len());
        for (ts, sid, data) in series_key_blk_desc {
            let block = AddSeries::new(
                *ts,
                *sid,
                SeriesKey {
                    tags: vec![],
                    table: data.to_string(),
                },
            );
            blocks.push(block);
        }
        vec![IndexBinlogBlock::Add(blocks)]
    }

    #[tokio::test]
    async fn test_index_binlog_read_write() {
        let dir = "/tmp/test/index_binlog/1";
        let _ = std::fs::remove_dir_all(dir);

        #[rustfmt::skip]
        let series_key_block_desc_1: Vec<IndexBinlogBlockDesc> = vec![
            (1001, 101, "abc"),
            (1002, 102, "efg"),
            (1003, 103, "hij"),
        ];
        let series_key_blocks_1 = build_series_key_blocks(&series_key_block_desc_1);

        {
            // Write the first 3 entries;
            let mut index = IndexBinlog::new(dir).await.unwrap();
            index.write_blocks(&series_key_blocks_1).await.unwrap();
            index.close().await.unwrap();
        }

        #[rustfmt::skip]
        let series_key_block_desc_2: Vec<IndexBinlogBlockDesc> = vec![
            (1011, 111, "abcd"),
            (1012, 112, "defg"),
            (1013, 113, "hjkl"),
        ];
        let series_key_blocks_2 = build_series_key_blocks(&series_key_block_desc_2);
        let binlog_id = {
            // Write the second 3 entries;
            let mut index = IndexBinlog::new(dir).await.unwrap();
            index.write_blocks(&series_key_blocks_2).await.unwrap();
            let binlog_id = index.writer_file.id;
            index.close().await.unwrap();
            binlog_id
        };

        // Read the 6 entries and check them.
        let mut index = IndexBinlog::new(dir).await.unwrap();

        let name = make_index_binlog_file(dir, binlog_id);
        let mut reader_file = BinlogReader::open(binlog_id, name).await.unwrap();
        for series_key_block in series_key_blocks_1.iter().chain(series_key_blocks_2.iter()) {
            assert_eq!(
                Some(series_key_block),
                reader_file.next_block().await.unwrap().as_ref()
            );
        }
        assert_eq!(None, reader_file.next_block().await.unwrap());

        index.advance_write_offset(0).await.unwrap();
        assert_eq!(None, reader_file.next_block().await.unwrap());
    }
}
