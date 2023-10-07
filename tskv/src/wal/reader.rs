use std::path::{Path, PathBuf};

use models::codec::Encoding;
use models::meta_data::VnodeId;
use models::schema::Precision;
use openraft::EntryPayload;
use protos::models_helper::print_points;
use snafu::ResultExt;

use super::{
    raft_store, WalType, WAL_DATABASE_SIZE_LEN, WAL_FOOTER_MAGIC_NUMBER, WAL_HEADER_LEN,
    WAL_PRECISION_LEN, WAL_TENANT_SIZE_LEN, WAL_VNODE_ID_LEN,
};
use crate::byte_utils::{decode_be_u32, decode_be_u64};
use crate::file_system::file_manager;
use crate::tsm::codec::get_str_codec;
use crate::{error, record_file, Error, Result};

/// Reads a wal file and parse footer, returns sequence range
pub async fn read_footer(path: impl AsRef<Path>) -> Result<Option<(u64, u64)>> {
    if file_manager::try_exists(&path) {
        let reader = WalReader::open(path).await?;
        Ok(Some((reader.min_sequence, reader.max_sequence)))
    } else {
        Ok(None)
    }
}

/// Parses wal footer, returns sequence range.
pub fn parse_footer(footer: [u8; record_file::FILE_FOOTER_LEN]) -> Option<(u64, u64)> {
    let magic_number = decode_be_u32(&footer[0..4]);
    if magic_number != WAL_FOOTER_MAGIC_NUMBER {
        // There is no footer in wal file.
        return None;
    }
    let min_sequence = decode_be_u64(&footer[16..24]);
    let max_sequence = decode_be_u64(&footer[24..32]);
    Some((min_sequence, max_sequence))
}

pub struct WalReader {
    inner: record_file::Reader,
    /// Min write sequence in the wal file, may be 0 if wal file is new or
    /// CnosDB was crushed or force-killed.
    min_sequence: u64,
    /// Max write sequence in the wal file, may be 0 if wal file is new or
    /// CnosDB was crushed or force-killed.
    max_sequence: u64,
    has_footer: bool,
}

impl WalReader {
    pub async fn open(path: impl AsRef<Path>) -> Result<Self> {
        let reader = record_file::Reader::open(&path).await?;

        let mut has_footer = true;
        let (min_sequence, max_sequence) = match reader.footer() {
            Some(footer) => parse_footer(footer).unwrap_or((0_u64, 0_u64)),
            None => {
                has_footer = false;
                (0_u64, 0_u64)
            }
        };

        Ok(Self {
            inner: reader,
            min_sequence,
            max_sequence,
            has_footer,
        })
    }

    pub(super) fn new(
        record_reader: record_file::Reader,
        min_sequence: u64,
        max_sequence: u64,
        has_footer: bool,
    ) -> Self {
        Self {
            inner: record_reader,
            min_sequence,
            max_sequence,
            has_footer,
        }
    }

    pub fn has_footer(&self) -> bool {
        self.has_footer
    }

    pub async fn next_wal_entry(&mut self) -> Result<Option<WalRecordData>> {
        loop {
            let data = match self.inner.read_record().await {
                Ok(r) => r.data,
                Err(Error::Eof) => {
                    return Ok(None);
                }
                Err(Error::RecordFileHashCheckFailed { .. }) => continue,
                Err(e) => {
                    trace::error!("Error reading wal: {:?}", e);
                    return Err(Error::WalTruncated);
                }
            };
            return Ok(Some(WalRecordData::new(data)));
        }
    }

    pub async fn read_wal_record_data(&mut self, pos: u64) -> Result<Option<WalRecordData>> {
        self.inner.reload_metadata().await?;
        match self.inner.read_record_at(pos as usize).await {
            Ok(r) => Ok(Some(WalRecordData::new(r.data))),
            Err(Error::Eof) => Ok(None),
            Err(e @ Error::RecordFileHashCheckFailed { .. }) => Err(e),
            Err(e) => {
                trace::error!("Error reading wal: {:?}", e);
                Err(Error::WalTruncated)
            }
        }
    }

    pub fn min_sequence(&self) -> u64 {
        self.min_sequence
    }

    pub fn max_sequence(&self) -> u64 {
        self.max_sequence
    }

    pub fn path(&self) -> PathBuf {
        self.inner.path()
    }

    pub fn len(&self) -> u64 {
        self.inner.len()
    }

    /// If this record file has some records in it.
    pub fn is_empty(&self) -> bool {
        match self
            .len()
            .checked_sub((record_file::FILE_MAGIC_NUMBER_LEN + record_file::FILE_FOOTER_LEN) as u64)
        {
            Some(d) => d == 0,
            None => true,
        }
    }

    pub fn take_record_reader(self) -> record_file::Reader {
        self.inner
    }
}

pub struct WalRecordData {
    pub typ: WalType,
    pub seq: u64,
    pub block: Block,
}

impl WalRecordData {
    pub fn new(buf: Vec<u8>) -> WalRecordData {
        if buf.len() < WAL_HEADER_LEN {
            return Self {
                typ: WalType::Unknown,
                seq: 0,
                block: Block::Unknown,
            };
        }
        let seq = decode_be_u64(&buf[1..9]);
        let entry_type: WalType = buf[0].into();
        let block = match entry_type {
            WalType::Write => Block::Write(WriteBlock::new(buf)),
            WalType::DeleteVnode => Block::DeleteVnode(DeleteVnodeBlock::new(buf)),
            WalType::DeleteTable => Block::DeleteTable(DeleteTableBlock::new(buf)),
            WalType::RaftBlankLog | WalType::RaftNormalLog | WalType::RaftMembershipLog => {
                match raft_store::new_raft_entry(&buf[WAL_HEADER_LEN..]) {
                    Ok(e) => Block::RaftLog(e),
                    Err(e) => {
                        trace::error!("Failed to decode raft entry from wal: {e}");
                        return Self {
                            typ: WalType::Unknown,
                            seq: 0,
                            block: Block::Unknown,
                        };
                    }
                }
            }
            WalType::Unknown => Block::Unknown,
        };
        Self {
            typ: entry_type,
            seq,
            block,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum Block {
    Write(WriteBlock),
    DeleteVnode(DeleteVnodeBlock),
    DeleteTable(DeleteTableBlock),
    RaftLog(raft_store::RaftEntry),
    Unknown,
}

/// buf:
/// - header: WAL_HEADER_LEN
/// - vnode_id: WAL_VNODE_ID_LEN
/// - precision: WAL_PRECISION_LEN
/// - tenant_size: WAL_TENANT_SIZE_LEN
/// - tenant: tenant_size
/// - data: ..
#[derive(Debug, Clone, PartialEq)]
pub struct WriteBlock {
    buf: Vec<u8>,
    tenant_size: usize,
}

impl WriteBlock {
    pub fn new(buf: Vec<u8>) -> WriteBlock {
        let tenatn_size_pos = WAL_HEADER_LEN + WAL_VNODE_ID_LEN + WAL_PRECISION_LEN;
        let tenant_size =
            decode_be_u64(&buf[tenatn_size_pos..tenatn_size_pos + WAL_TENANT_SIZE_LEN]) as usize;
        Self { buf, tenant_size }
    }

    pub fn check_buf_size(size: usize) -> bool {
        size >= WAL_HEADER_LEN + WAL_VNODE_ID_LEN + WAL_PRECISION_LEN + WAL_TENANT_SIZE_LEN
    }

    pub fn vnode_id(&self) -> VnodeId {
        decode_be_u32(&self.buf[WAL_HEADER_LEN..WAL_HEADER_LEN + WAL_VNODE_ID_LEN])
    }

    pub fn precision(&self) -> Precision {
        Precision::from(self.buf[WAL_HEADER_LEN + WAL_VNODE_ID_LEN])
    }

    pub fn tenant(&self) -> &[u8] {
        let tenant_pos =
            WAL_HEADER_LEN + WAL_VNODE_ID_LEN + WAL_PRECISION_LEN + WAL_TENANT_SIZE_LEN;
        &self.buf[tenant_pos..tenant_pos + self.tenant_size]
    }

    pub fn tenant_utf8(&self) -> Result<&str> {
        std::str::from_utf8(self.tenant()).with_context(|_| error::InvalidUtf8Snafu {
            message: "wal::WriteBlock::tenant",
        })
    }

    pub fn points(&self) -> &[u8] {
        let points_pos = WAL_HEADER_LEN
            + WAL_VNODE_ID_LEN
            + WAL_PRECISION_LEN
            + WAL_TENANT_SIZE_LEN
            + self.tenant_size;
        &self.buf[points_pos..]
    }
}

/// buf:
/// - header: WAL_HEADER_LEN
/// - vnode_id: WAL_VNODE_ID_LEN
/// - tenant_size: WAL_TENANT_SIZE_LEN
/// - tenant: tenant_size
/// - database: ..
#[derive(Debug, Clone, PartialEq)]
pub struct DeleteVnodeBlock {
    buf: Vec<u8>,
    tenant_size: usize,
}

impl DeleteVnodeBlock {
    pub fn new(buf: Vec<u8>) -> Self {
        let tenant_len = decode_be_u64(
            &buf[WAL_HEADER_LEN + WAL_VNODE_ID_LEN
                ..WAL_HEADER_LEN + WAL_VNODE_ID_LEN + WAL_TENANT_SIZE_LEN],
        ) as usize;
        Self {
            buf,
            tenant_size: tenant_len,
        }
    }

    pub fn check_buf_size(size: usize) -> bool {
        size > WAL_HEADER_LEN + WAL_VNODE_ID_LEN + WAL_TENANT_SIZE_LEN
    }

    pub fn vnode_id(&self) -> VnodeId {
        decode_be_u32(&self.buf[WAL_HEADER_LEN..WAL_HEADER_LEN + WAL_VNODE_ID_LEN])
    }

    pub fn tenant(&self) -> &[u8] {
        let tenant_pos = WAL_HEADER_LEN + WAL_VNODE_ID_LEN + WAL_TENANT_SIZE_LEN;
        &self.buf[tenant_pos..tenant_pos + self.tenant_size]
    }

    pub fn tenant_utf8(&self) -> Result<&str> {
        std::str::from_utf8(self.tenant()).with_context(|_| error::InvalidUtf8Snafu {
            message: "wal::DeleteVnodeBlock::tenant",
        })
    }

    pub fn database(&self) -> &[u8] {
        let database_pos =
            WAL_HEADER_LEN + WAL_VNODE_ID_LEN + WAL_TENANT_SIZE_LEN + self.tenant_size;
        &self.buf[database_pos..]
    }

    pub fn database_utf8(&self) -> Result<&str> {
        std::str::from_utf8(self.database()).with_context(|_| error::InvalidUtf8Snafu {
            message: "wal::DeleteVnodeBlock::database",
        })
    }
}

/// buf:
/// - header: WAL_HEADER_LEN
/// - tenant_size: WAL_TENANT_SIZE_LEN
/// - database_size: WAL_DATABASE_SIZE_LEN
/// - tenant: tenant_size
/// - database: database_size
/// - table: ..
#[derive(Debug, Clone, PartialEq)]
pub struct DeleteTableBlock {
    buf: Vec<u8>,
    tenant_len: usize,
    database_len: usize,
}

impl DeleteTableBlock {
    pub fn new(buf: Vec<u8>) -> DeleteTableBlock {
        let tenant_len =
            decode_be_u64(&buf[WAL_HEADER_LEN..WAL_HEADER_LEN + WAL_TENANT_SIZE_LEN]) as usize;
        let database_len_pos = WAL_HEADER_LEN + WAL_TENANT_SIZE_LEN;
        let database_len =
            decode_be_u32(&buf[database_len_pos..database_len_pos + WAL_DATABASE_SIZE_LEN])
                as usize;
        Self {
            buf,
            tenant_len,
            database_len,
        }
    }

    pub fn check_buf_size(size: usize) -> bool {
        size >= WAL_HEADER_LEN + WAL_TENANT_SIZE_LEN + WAL_DATABASE_SIZE_LEN
    }

    pub fn tenant(&self) -> &[u8] {
        let tenant_pos = WAL_HEADER_LEN + WAL_TENANT_SIZE_LEN + WAL_DATABASE_SIZE_LEN;
        &self.buf[tenant_pos..tenant_pos + self.tenant_len]
    }

    pub fn tenant_utf8(&self) -> Result<&str> {
        std::str::from_utf8(self.tenant()).with_context(|_| error::InvalidUtf8Snafu {
            message: "wal::DeleteTableBlock::tenant",
        })
    }

    pub fn database(&self) -> &[u8] {
        let database_pos =
            WAL_HEADER_LEN + WAL_TENANT_SIZE_LEN + WAL_DATABASE_SIZE_LEN + self.tenant_len;
        &self.buf[database_pos..database_pos + self.database_len]
    }

    pub fn database_utf8(&self) -> Result<&str> {
        std::str::from_utf8(self.database()).with_context(|_| error::InvalidUtf8Snafu {
            message: "wal::DeleteTableBlock::database",
        })
    }

    pub fn table(&self) -> &[u8] {
        let table_pos = WAL_HEADER_LEN
            + WAL_TENANT_SIZE_LEN
            + WAL_DATABASE_SIZE_LEN
            + self.tenant_len
            + self.database_len;
        &self.buf[table_pos..]
    }

    pub fn table_utf8(&self) -> Result<&str> {
        std::str::from_utf8(self.table()).with_context(|_| error::InvalidUtf8Snafu {
            message: "wal::DeleteTableBlock::tenant",
        })
    }
}

pub async fn print_wal_statistics(path: impl AsRef<Path>) {
    use protos::models as fb_models;

    let mut reader = WalReader::open(path).await.unwrap();
    let decoder = get_str_codec(Encoding::Zstd);
    loop {
        match reader.next_wal_entry().await {
            Ok(Some(entry_block)) => {
                println!("============================================================");
                println!("Seq: {}, Typ: {}", entry_block.seq, entry_block.typ);
                match entry_block.block {
                    Block::Write(blk) => {
                        println!(
                            "Tenant: {}, VnodeId: {}, Precision: {}",
                            std::str::from_utf8(blk.tenant()).unwrap(),
                            blk.vnode_id(),
                            blk.precision(),
                        );
                        let ety_points = blk.points();
                        let mut data_buf = Vec::with_capacity(ety_points.len());
                        decoder.decode(ety_points, &mut data_buf).unwrap();
                        match flatbuffers::root::<fb_models::Points>(&data_buf[0]) {
                            Ok(points) => {
                                print_points(points);
                            }
                            Err(e) => panic!("unexpected data: '{:?}'", e),
                        }
                    }
                    Block::DeleteVnode(blk) => {
                        println!(
                            "Tenant: {}, Database: {}, VnodeId: {}",
                            std::str::from_utf8(blk.tenant()).unwrap(),
                            std::str::from_utf8(blk.database()).unwrap(),
                            blk.vnode_id(),
                        );
                    }
                    Block::DeleteTable(blk) => {
                        println!(
                            "Tenant: {}, VnodeId: {}, Precision: {}",
                            std::str::from_utf8(blk.tenant()).unwrap(),
                            std::str::from_utf8(blk.database()).unwrap(),
                            std::str::from_utf8(blk.table()).unwrap(),
                        );
                    }
                    Block::RaftLog(entry) => match entry.payload {
                        EntryPayload::Blank => {
                            println!("Raft log: empty");
                        }
                        EntryPayload::Normal(_data) => {
                            println!("Raft log: normal");
                        }
                        EntryPayload::Membership(_data) => {
                            println!("Raft log: membership");
                        }
                    },
                    Block::Unknown => {
                        println!("Unknown WAL entry type.");
                    }
                }
            }
            Ok(None) => {
                println!("============================================================");
                break;
            }
            Err(Error::WalTruncated) => {
                println!("============================================================");
                println!("WAL file truncated");
                break;
            }
            Err(e) => {
                panic!("Failed to read wal file: {:?}", e);
            }
        }
    }
}

#[cfg(test)]
mod test {
    use models::meta_data::VnodeId;
    use models::schema::Precision;

    use crate::wal::reader::{DeleteTableBlock, DeleteVnodeBlock, WriteBlock};
    use crate::wal::WalType;

    impl WriteBlock {
        pub fn build(
            seq: u64,
            tenant: &str,
            vnode_id: VnodeId,
            precision: Precision,
            points: Vec<u8>,
        ) -> Self {
            let mut buf = Vec::new();
            buf.push(WalType::Write as u8);
            buf.extend_from_slice(&seq.to_be_bytes());
            buf.extend_from_slice(&vnode_id.to_be_bytes());
            buf.push(precision as u8);
            buf.extend_from_slice(&(tenant.len() as u64).to_be_bytes());
            buf.extend_from_slice(tenant.as_bytes());
            buf.extend_from_slice(&points);

            Self {
                buf,
                tenant_size: tenant.len(),
            }
        }
    }

    impl DeleteVnodeBlock {
        pub fn build(seq: u64, tenant: &str, database: &str, vnode_id: VnodeId) -> Self {
            let mut buf = Vec::new();
            let tenant_bytes = tenant.as_bytes();
            buf.push(WalType::DeleteVnode as u8);
            buf.extend_from_slice(&seq.to_be_bytes());
            buf.extend_from_slice(&vnode_id.to_be_bytes());
            buf.extend_from_slice(&(tenant_bytes.len() as u64).to_be_bytes());
            buf.extend_from_slice(tenant_bytes);
            buf.extend_from_slice(database.as_bytes());

            Self {
                buf,
                tenant_size: tenant_bytes.len(),
            }
        }
    }

    impl DeleteTableBlock {
        pub fn build(seq: u64, tenant: &str, database: &str, table: &str) -> Self {
            let mut buf = Vec::new();
            let tenant_bytes = tenant.as_bytes();
            let database_bytes = database.as_bytes();
            let table_bytes = table.as_bytes();
            buf.push(WalType::DeleteTable as u8);
            buf.extend_from_slice(&seq.to_be_bytes());
            buf.extend_from_slice(&(tenant_bytes.len() as u64).to_be_bytes());
            buf.extend_from_slice(&(database_bytes.len() as u32).to_be_bytes());
            buf.extend_from_slice(tenant_bytes);
            buf.extend_from_slice(database_bytes);
            buf.extend_from_slice(table_bytes);

            Self {
                buf,
                tenant_len: tenant_bytes.len(),
                database_len: database_bytes.len(),
            }
        }
    }

    #[test]
    fn test_wal_blocks() {
        {
            let block = WriteBlock::build(1, "tenant", 2, Precision::MS, vec![]);
            assert_eq!(block.tenant(), b"tenant");
            assert_eq!(block.tenant_utf8().unwrap(), "tenant");
            assert_eq!(block.vnode_id(), 2);
            assert_eq!(block.precision(), Precision::MS);
        }
        {
            let block = DeleteVnodeBlock::build(3, "tenant", "database", 4);
            assert_eq!(block.tenant(), b"tenant");
            assert_eq!(block.tenant_utf8().unwrap(), "tenant");
            assert_eq!(block.database(), b"database");
            assert_eq!(block.database_utf8().unwrap(), "database");
        }
        {
            let block = DeleteTableBlock::build(5, "tenant", "database", "table");
            assert_eq!(block.tenant(), b"tenant");
            assert_eq!(block.tenant_utf8().unwrap(), "tenant");
            assert_eq!(block.database(), b"database");
            assert_eq!(block.database_utf8().unwrap(), "database");
            assert_eq!(block.table(), b"table");
            assert_eq!(block.table_utf8().unwrap(), "table");
        }
    }
}
