use std::cmp::Ordering;
use std::path::{Path, PathBuf};

use snafu::ResultExt;

use super::{
    file_crc_source_len, Record, FILE_FOOTER_CRC32_NUMBER_LEN, FILE_FOOTER_LEN,
    FILE_FOOTER_MAGIC_NUMBER_LEN, FILE_MAGIC_NUMBER_LEN, READER_BUF_SIZE, RECORD_CRC32_NUMBER_LEN,
    RECORD_DATA_SIZE_LEN, RECORD_DATA_TYPE_LEN, RECORD_DATA_VERSION_LEN, RECORD_HEADER_LEN,
    RECORD_MAGIC_NUMBER, RECORD_MAGIC_NUMBER_LEN,
};
use crate::byte_utils::decode_be_u32;
use crate::error::{self, Error, Result};
use crate::file_system::file::async_file::AsyncFile;
use crate::file_system::file::IFile;
use crate::file_system::file_manager;

pub struct Reader {
    path: PathBuf,
    file: AsyncFile,
    file_len: u64,
    pos: usize,
    buf: Vec<u8>,
    buf_len: usize,
    buf_use: usize,
    footer: Option<[u8; FILE_FOOTER_LEN]>,
    footer_pos: u64,
}

impl Reader {
    pub async fn open(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        let file = file_manager::open_file(path).await?;
        let (footer_pos, footer) = match Self::read_footer(&path).await {
            Ok((p, f)) => (p, Some(f)),
            Err(Error::NoFooter) => (file.len(), None),
            Err(e) => {
                trace::error!(
                    "Record file: Failed to read footer in '{}': {e}",
                    path.display(),
                );
                return Err(e);
            }
        };
        let file_len = file.len();
        let records_len = if footer_pos == file_len {
            // If there is no footer
            file_len - FILE_MAGIC_NUMBER_LEN as u64
        } else {
            file_len - FILE_FOOTER_LEN as u64 - FILE_MAGIC_NUMBER_LEN as u64
        };
        let buf_size = records_len.min(READER_BUF_SIZE as u64) as usize;
        Ok(Reader {
            path: path.to_path_buf(),
            file,
            file_len,
            pos: FILE_MAGIC_NUMBER_LEN,
            buf: vec![0_u8; buf_size],
            buf_len: 0,
            buf_use: 0,
            footer,
            footer_pos,
        })
    }

    /// Set self.pos, load buffer if needed.
    async fn set_pos(&mut self, pos: usize) -> Result<()> {
        if self.pos - self.buf_use == pos {
            self.pos = pos;
            self.buf_use = 0;
            return Ok(());
        }
        if pos as u64 > self.file_len {
            return Err(Error::Eof);
        }

        match self.pos.cmp(&pos) {
            Ordering::Greater => {
                let size = self.pos - pos;
                self.pos = pos;
                match self.buf_use.cmp(&size) {
                    Ordering::Greater => {
                        self.buf_use -= size;
                        Ok(())
                    }
                    _ => self.load_buf().await,
                }
            }
            Ordering::Less => {
                let size = pos - self.pos;
                self.pos = pos;
                match (self.buf_len - self.buf_use).cmp(&size) {
                    Ordering::Greater => {
                        self.buf_use += size;
                        Ok(())
                    }
                    _ => self.load_buf().await,
                }
            }
            Ordering::Equal => Ok(()),
        }
    }

    /// Returns a position where to read the header and the header slice.
    async fn find_record_header(&mut self) -> Result<(usize, &[u8])> {
        loop {
            let magic_number_sli = self.read_buf(RECORD_MAGIC_NUMBER_LEN).await?;
            let magic_number = decode_be_u32(magic_number_sli);
            if magic_number == RECORD_MAGIC_NUMBER {
                let pos = self.pos;
                let header = self.read_buf(RECORD_HEADER_LEN).await?;
                return Ok((pos, header));
            } else {
                self.set_pos(self.pos + 1).await?;
            }
        }
    }

    /// Read the next record, return Ok(record) if succeeded.
    /// There are three special error type:
    /// - Eof - file reads to a end.
    /// - RecordFileInvalidDataSize - file is broken that data size is too big.
    /// - RecordFileHashCheckFailed - file may be broken that data crc not match.
    pub async fn read_record(&mut self) -> Result<Record> {
        // The previous position, if the previous error is not the HashCheckFailed,
        // this value will be updated.
        if (self.pos + RECORD_HEADER_LEN) as u64 >= self.footer_pos {
            return Err(Error::Eof);
        }
        let (header_pos, header) = self.find_record_header().await?;

        let mut p = RECORD_MAGIC_NUMBER_LEN;
        let data_version = header[p];
        p += RECORD_DATA_VERSION_LEN;
        let data_type = header[p];
        p += RECORD_DATA_TYPE_LEN;
        let data_size = decode_be_u32(&header[p..p + RECORD_DATA_SIZE_LEN]);
        p += RECORD_DATA_SIZE_LEN;
        let data_crc = decode_be_u32(&header[p..p + RECORD_CRC32_NUMBER_LEN]);

        // A hasher for record header and record data.
        let mut hasher = crc32fast::Hasher::new();
        // Hash record header (Exclude magic number and crc32 number)
        hasher
            .update(&header[RECORD_MAGIC_NUMBER_LEN..RECORD_HEADER_LEN - RECORD_CRC32_NUMBER_LEN]);

        // TODO: Check if data_size is too large.
        let data_pos = header_pos + RECORD_HEADER_LEN;
        self.set_pos(data_pos).await?;
        let data = match self.read_buf(data_size as usize).await {
            Ok(record_bytes) => {
                let record_data = record_bytes.to_vec();
                if record_bytes.len() < data_size as usize {
                    trace::error!(
                        "Record file: Failed to read data: Data size ({}) at pos {} is greater than bytes readed ({})",
                        data_size, data_pos, record_bytes.len()
                    );
                    return Err(Error::RecordFileInvalidDataSize {
                        pos: header_pos as u64,
                        len: data_size,
                    });
                }
                self.set_pos(data_pos + data_size as usize).await?;
                record_data
            }
            Err(e) => {
                trace::error!(
                    "Record file: Failed to read data at {header_pos} for {} bytes: {e}",
                    data_size + RECORD_HEADER_LEN as u32
                );
                self.set_pos(header_pos + 1).await?;
                return Err(e);
            }
        };

        // Hash record data
        hasher.update(&data);
        // check crc32 number
        let data_crc_calculated = hasher.finalize();
        if data_crc_calculated != data_crc {
            // If crc not match, try to get header from the next pos.
            trace::error!(
                "Record file: Failed to read data: Data crc check failed at {header_pos} for {} bytes",
                data_size + RECORD_HEADER_LEN as u32
            );
            self.set_pos(header_pos + 1).await?;
            return Err(Error::RecordFileHashCheckFailed {
                crc: data_crc,
                crc_calculated: data_crc_calculated,
                record: Record {
                    data_type,
                    data_version,
                    data,
                    pos: header_pos as u64,
                },
            });
        }

        Ok(Record {
            data_type,
            data_version,
            data,
            pos: header_pos as u64,
        })
    }

    pub async fn read_record_at(&mut self, pos: usize) -> Result<Record> {
        self.set_pos(pos).await?;
        self.read_record().await
    }

    /// Returns footer position and footer data.
    pub async fn read_footer(path: impl AsRef<Path>) -> Result<(u64, [u8; FILE_FOOTER_LEN])> {
        let path = path.as_ref();
        let file = file_manager::open_file(&path).await?;
        if file.len() < (FILE_MAGIC_NUMBER_LEN + FILE_FOOTER_LEN) as u64 {
            return Err(Error::NoFooter);
        }

        // Get file crc
        let mut buf = vec![0_u8; file_crc_source_len(file.len(), FILE_FOOTER_LEN)];
        if let Err(e) = file.read_at(FILE_MAGIC_NUMBER_LEN as u64, &mut buf).await {
            return Err(Error::ReadFile {
                path: path.to_path_buf(),
                source: e,
            });
        }
        let crc = crc32fast::hash(&buf);

        // Read footer
        let footer_pos = file.len() - FILE_FOOTER_LEN as u64;
        let mut footer = [0_u8; FILE_FOOTER_LEN];
        if let Err(e) = file.read_at(footer_pos, &mut footer[..]).await {
            return Err(Error::ReadFile {
                path: path.to_path_buf(),
                source: e,
            });
        }

        // Check file crc
        let footer_crc = decode_be_u32(
            &footer[FILE_FOOTER_MAGIC_NUMBER_LEN
                ..FILE_FOOTER_MAGIC_NUMBER_LEN + FILE_FOOTER_CRC32_NUMBER_LEN],
        );

        // If crc doesn't match, this file may not contain a footer.
        if crc != footer_crc {
            Err(Error::NoFooter)
        } else {
            Ok((footer_pos, footer))
        }
    }

    /// Returns a clone of file footer.
    pub fn footer(&self) -> Option<[u8; FILE_FOOTER_LEN]> {
        self.footer
    }

    /// Load buf from self.pos, reset self.buf_len and self.buf_use.
    async fn load_buf(&mut self) -> Result<()> {
        trace::trace!(
            "Record file: Trying load buf at {} for {} bytes",
            self.pos,
            self.buf.len()
        );
        self.buf_len = self
            .file
            .read_at(self.pos as u64, &mut self.buf)
            .await
            .map_err(|e| Error::ReadFile {
                path: self.path.clone(),
                source: e,
            })?;
        self.buf_use = 0;
        Ok(())
    }

    async fn read_buf(&mut self, size: usize) -> Result<&[u8]> {
        if self.buf_len - self.buf_use < size {
            self.load_buf().await?;
        }
        let right_bound = (self.buf_use + size).min(self.buf_len);
        Ok(&self.buf[self.buf_use..right_bound])
    }

    pub async fn reload_metadata(&mut self) -> Result<()> {
        let meta = tokio::fs::metadata(&self.path)
            .await
            .context(error::IOSnafu)?;
        self.file_len = meta.len();
        if self.footer.is_none() {
            self.footer_pos = self.file_len;
        }
        Ok(())
    }

    pub fn path(&self) -> PathBuf {
        self.path.clone()
    }

    pub fn len(&self) -> u64 {
        self.file_len
    }

    pub fn is_empty(&self) -> bool {
        self.file.is_empty()
    }

    pub fn pos(&self) -> usize {
        self.pos
    }
}

#[cfg(test)]
pub(crate) mod test {
    use std::path::{Path, PathBuf};

    use super::Reader;
    use crate::byte_utils;
    use crate::error::Error;
    use crate::record_file::writer::test::record_length;
    use crate::record_file::{
        RecordDataType, Writer, FILE_FOOTER_LEN, FILE_MAGIC_NUMBER, FILE_MAGIC_NUMBER_LEN,
        RECORD_CRC32_NUMBER_LEN, RECORD_DATA_SIZE_LEN, RECORD_DATA_TYPE_LEN,
        RECORD_DATA_VERSION_LEN, RECORD_HEADER_LEN, RECORD_MAGIC_NUMBER, RECORD_MAGIC_NUMBER_LEN,
    };

    /// Read a record_file and see if records in file are the same as given records.
    pub async fn assert_record_file_data_eq<L, R, D>(
        path: impl AsRef<Path>,
        records: L,
        has_footer: bool,
    ) where
        D: AsRef<[u8]>,
        R: AsRef<[D]>,
        L: AsRef<[R]>,
    {
        let records = records.as_ref();

        let file_content = std::fs::read(&path).unwrap();
        assert_eq!(
            &file_content[..FILE_MAGIC_NUMBER_LEN],
            &FILE_MAGIC_NUMBER.to_be_bytes()
        );

        let max_pos = if has_footer {
            if file_content.len() < FILE_FOOTER_LEN {
                panic!(
                    "Record file size ({})is less than {FILE_FOOTER_LEN}",
                    file_content.len()
                );
            }
            file_content.len() - FILE_FOOTER_LEN
        } else {
            file_content.len()
        };
        let mut pos = FILE_MAGIC_NUMBER_LEN;
        let mut i = 0_usize;
        while pos < max_pos {
            if pos + RECORD_HEADER_LEN >= max_pos {
                break;
            }
            let record_to_check = match records.get(i) {
                Some(d) => d.as_ref(),
                None => break,
            };
            i += 1;

            // Skip magic number.
            pos += RECORD_MAGIC_NUMBER_LEN;

            let mut hasher = crc32fast::Hasher::new();
            hasher.update(&file_content[pos..pos + RECORD_DATA_VERSION_LEN + RECORD_DATA_TYPE_LEN]);
            pos += RECORD_DATA_VERSION_LEN + RECORD_DATA_TYPE_LEN;

            // Check data size.
            let data_size_sli = &file_content[pos..pos + RECORD_DATA_SIZE_LEN];
            hasher.update(data_size_sli);
            let data_size = byte_utils::decode_be_u32(data_size_sli) as usize;
            assert_eq!(
                data_size,
                record_to_check
                    .iter()
                    .map(|d| d.as_ref().len())
                    .sum::<usize>(),
                "unexpected data_size at pos {pos}"
            );
            pos += RECORD_DATA_SIZE_LEN;

            // Check crc.
            let crc_record_file =
                byte_utils::decode_be_u32(&file_content[pos..pos + RECORD_CRC32_NUMBER_LEN]);
            for d in record_to_check {
                hasher.update(d.as_ref());
            }
            let crc_to_check = hasher.finalize();
            assert_eq!(crc_record_file, crc_to_check, "unexpected crc at pos {pos}");
            pos += RECORD_CRC32_NUMBER_LEN;

            // Skip data in file.
            pos += data_size;
        }
        if i < records.len() {
            panic!(
                "Given record data is more than that in record file: index({i}) < max_index({})",
                records.len()
            );
        }
        if pos < max_pos {
            panic!("Given record data is less than that in record file: pos({pos}) < max_pos({max_pos})");
        }
    }

    /// Call reader.set_pos(pos) and then do check.
    async fn set_pos_and_check(reader: &mut Reader, pos: usize) {
        reader.set_pos(pos).await.unwrap();
        assert_eq!(reader.pos, pos, "unexpected pos after set_pos({pos}");
    }

    /// Call reader.set_pos(from_pos) by check_set_pos() if from_pos is set,
    /// then call reader.find_record_header() and then do check.
    ///
    /// - If from_pos is none, then do not call reader.set_pos().
    /// - If assumed_header_pos is nont, then reader.find_record_header need return Error::Eof.
    async fn find_record_header_and_check(
        reader: &mut Reader,
        from_pos: Option<usize>,
        assumed_header_pos: Option<usize>,
    ) {
        let str_set_pos_and: String = if let Some(pos) = from_pos {
            set_pos_and_check(reader, pos).await;
            format!("set_pos({pos}) and")
        } else {
            String::new()
        };

        let ret = reader.find_record_header().await;
        if let Some(assumed_header_pos) = assumed_header_pos {
            let (found_header_pos, found_header) = ret.unwrap();
            assert_eq!(
                found_header_pos, assumed_header_pos,
                "unexpected found header pos"
            );
            assert_eq!(
                &found_header[..4],
                &RECORD_MAGIC_NUMBER.to_be_bytes(),
                "unexpected found header after {str_set_pos_and} find_record_header()"
            );
            // drop(found_header);
            assert_eq!(
                reader.pos, found_header_pos,
                "unexpected pos after {str_set_pos_and} find_record_header()",
            );
        } else {
            // EOF expected, `Error` is not able to Eq, so use match.
            match ret {
                Err(Error::Eof) => {}
                Err(e) => panic!("Unexpected error: {e}, Error::EOF was expected."),
                Ok((header_pos, _)) => {
                    panic!("Unexpected Ok((header_pos, header)) at {header_pos}, Error::EOF was expected.",)
                }
            }
        }
    }

    #[tokio::test]
    async fn test_record_reader_open_and_read() {
        let dir = PathBuf::from("/tmp/test/record_file/reader/1");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        {
            // Test open a file not exist.
            let ret = Reader::open(dir.join("no.record")).await;
            assert!(ret.is_err());
        }
        {
            let path = dir.join("1.log");
            let data = b"hello world";
            let data_version = 1_u8;
            let data_type = 1_u8;
            let mut footer = [0_u8; FILE_FOOTER_LEN];
            {
                let mut writer = Writer::open(&path, RecordDataType::Summary).await.unwrap();
                let data_size = writer
                    .write_record(data_version, data_type, &[data])
                    .await
                    .unwrap();
                assert_eq!(data_size, record_length(11));
                writer.write_footer(&mut footer).await.unwrap();
                assert_record_file_data_eq(&path, [[data]], true).await;
            }
            // Test open file.
            let mut reader = Reader::open(&path).await.unwrap();
            assert_eq!(reader.pos, 4);
            assert_eq!(
                reader.buf.len() as u64,
                reader.len() - FILE_MAGIC_NUMBER_LEN as u64 - FILE_FOOTER_LEN as u64
            );
            assert_eq!(reader.buf_len, 0);
            assert_eq!(reader.buf_use, 0);
            assert_eq!(reader.footer, Some(footer));
            assert_eq!(reader.footer_pos, reader.len() - FILE_FOOTER_LEN as u64);
            // Test read record.
            let record = reader.read_record().await.unwrap();
            assert_eq!(record.pos, 4);
            assert_eq!(record.data_version, data_version);
            assert_eq!(record.data_type, data_type);
            assert_eq!(&record.data, data);
            // Test read twice, EOF expected, `Error` is not able to Eq, so use match.
            match reader.read_record().await {
                Err(Error::Eof) => {}
                Err(e) => panic!("Unexpected error: {e}, Error::EOF was expected."),
                Ok(r) => panic!(
                    "Unexpected Ok(record) at pos {}, Error::EOF was expected.",
                    r.pos
                ),
            }
        }
    }

    #[tokio::test]
    async fn test_record_file_find_record_header() {
        let dir = PathBuf::from("/tmp/test/record_file/reader/2");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        let path = dir.join("1.log");
        {
            // Write record file, 1804 bytes:
            // - header(0-4): RECO(4)
            // - record(4-1804):
            //   14 bytes header: FlOg(4), version(1), type(1), size(4), crc(4)
            //   4 bytes data: 0000 - 0099
            let mut writer = Writer::open(&path, RecordDataType::Summary).await.unwrap();
            for i in 0..100 {
                let data = format!("{i:04}");
                writer.write_record(1, 1, &[data]).await.unwrap();
            }
            writer.close().await.unwrap();
        }
        let mut reader = Reader::open(&path).await.unwrap();
        // For this test case, make buf_len smaller
        const BUF_LEN: usize = 64;
        reader.buf = vec![0_u8; BUF_LEN];

        // Test load buffer from pos 4.
        assert_eq!(reader.pos, 4);
        reader.load_buf().await.unwrap();
        assert_eq!(reader.pos, 4);
        assert_eq!(reader.buf.len(), BUF_LEN);
        assert_eq!(reader.buf_len, BUF_LEN); // [4..68]
        assert_eq!(reader.buf_use, 0);

        // Test find the first header
        find_record_header_and_check(&mut reader, None, Some(4)).await;
        // Test find header that need to load half of the buffer.
        find_record_header_and_check(&mut reader, Some(63), Some(76)).await;
        // Test find header that need to load entire of the buffer.
        find_record_header_and_check(&mut reader, Some(1024), Some(1030)).await;
        // Test find the last header
        find_record_header_and_check(&mut reader, Some(1786), Some(1786)).await;
        // Test find previous header.
        find_record_header_and_check(&mut reader, Some(1024), Some(1030)).await;
        // Test find header and return Err(EOF).
        find_record_header_and_check(&mut reader, Some(1787), None).await;

        // Test set_pos
        set_pos_and_check(&mut reader, 0).await; // old buf, pos: 62
        set_pos_and_check(&mut reader, 67).await; // old buf, pos: 63
        set_pos_and_check(&mut reader, 68).await; // new buf, pos: 0
        set_pos_and_check(&mut reader, 69).await; // new buf, pos: 1

        // Re-run those find_record_header() tests.
        find_record_header_and_check(&mut reader, Some(0), Some(4)).await;
        find_record_header_and_check(&mut reader, Some(63), Some(76)).await;
        find_record_header_and_check(&mut reader, Some(1024), Some(1030)).await;
        find_record_header_and_check(&mut reader, Some(1786), Some(1786)).await;
        find_record_header_and_check(&mut reader, Some(1787), None).await;
    }
}
