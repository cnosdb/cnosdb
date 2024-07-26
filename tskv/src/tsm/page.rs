use models::column_data::PrimaryColumnData;
use models::field_value::FieldVal;
use models::schema::tskv_table_schema::{PhysicalCType, TableColumn};
use models::PhysicalDType;
use serde::{Deserialize, Serialize};
use snafu::{OptionExt, ResultExt};
use utils::bitset::ImmutBitSet;

use super::statistics::ValueStatistics;
use crate::byte_utils::{decode_be_u32, decode_be_u64};
use crate::error::{
    DecodeSnafu, EncodeSnafu, TskvResult, TsmPageFileHashCheckFailedSnafu, TsmPageSnafu,
    UnsupportedDataTypeSnafu,
};
use crate::tsm::codec::{
    get_bool_codec, get_encoding, get_f64_codec, get_i64_codec, get_str_codec, get_u64_codec,
};
use crate::tsm::data_block::MutableColumn;

#[derive(Debug)]
pub struct Page {
    /// 4 bits for bitset len
    /// 8 bits for data len
    /// 4 bits for crc32 len
    /// bitset len bits for BitSet
    /// the bits of rest for data
    pub(crate) bytes: bytes::Bytes,
    pub(crate) meta: PageMeta,
}

impl Page {
    pub fn new(bytes: bytes::Bytes, meta: PageMeta) -> Self {
        Self { bytes, meta }
    }

    pub fn bytes(&self) -> &bytes::Bytes {
        &self.bytes
    }

    pub fn meta(&self) -> &PageMeta {
        &self.meta
    }

    pub fn desc(&self) -> &TableColumn {
        &self.meta.column
    }

    pub fn crc_validation(&self) -> TskvResult<Page> {
        let bytes = self.bytes().clone();
        let meta = self.meta().clone();
        let data_crc = decode_be_u32(&bytes[12..16]);
        let mut hasher = crc32fast::Hasher::new();
        let bitset_len = decode_be_u32(&self.bytes[0..4]) as usize;
        hasher.update(&bytes[16 + bitset_len..]);
        let data_crc_calculated = hasher.finalize();
        if data_crc != data_crc_calculated {
            // If crc not match, try to return error.
            return Err(TsmPageFileHashCheckFailedSnafu {
                crc: data_crc,
                crc_calculated: data_crc_calculated,
                page: Page { bytes, meta },
            }
            .build());
        }
        Ok(Page { bytes, meta })
    }

    pub fn null_bitset(&self) -> ImmutBitSet<'_> {
        let data_len = decode_be_u64(&self.bytes[4..12]) as usize;
        let bitset_buffer = self.null_bitset_slice();
        ImmutBitSet::new_without_check(data_len, bitset_buffer)
    }

    pub fn null_bitset_slice(&self) -> &[u8] {
        let bitset_len = decode_be_u32(&self.bytes[0..4]) as usize;
        &self.bytes[16..16 + bitset_len]
    }

    pub fn data_buffer(&self) -> &[u8] {
        let bitset_len = decode_be_u32(&self.bytes[0..4]) as usize;
        &self.bytes[16 + bitset_len..]
    }

    pub fn to_column(&self) -> TskvResult<MutableColumn> {
        let col_type = self.meta.column.column_type.to_physical_type();
        let mut col =
            MutableColumn::empty_with_cap(self.meta.column.clone(), self.meta.num_values as usize)?;
        let data_buffer = self.data_buffer();
        let bitset = self.null_bitset();
        match col_type {
            PhysicalCType::Tag => {
                return Err(TsmPageSnafu {
                    reason: "tag column not support now".to_string(),
                }
                .build());
            }
            PhysicalCType::Time(_) | PhysicalCType::Field(PhysicalDType::Integer) => {
                let encoding = get_encoding(data_buffer);
                let ts_codec = get_i64_codec(encoding);
                let mut target = Vec::new();
                ts_codec
                    .decode(data_buffer, &mut target)
                    .context(DecodeSnafu)?;

                let mut target = target.into_iter();
                for i in 0..bitset.len() {
                    if bitset.get(i) {
                        col.push(Some(FieldVal::Integer(target.next().context(
                            TsmPageSnafu {
                                reason: "data buffer not enough".to_string(),
                            },
                        )?)))?;
                    } else {
                        col.push(None)?;
                    }
                }
            }
            PhysicalCType::Field(PhysicalDType::Float) => {
                let encoding = get_encoding(data_buffer);
                let ts_codec = get_f64_codec(encoding);
                let mut target = Vec::new();
                ts_codec
                    .decode(data_buffer, &mut target)
                    .context(DecodeSnafu)?;

                let mut target = target.into_iter();
                for i in 0..bitset.len() {
                    if bitset.get(i) {
                        col.push(Some(FieldVal::Float(target.next().context(
                            TsmPageSnafu {
                                reason: "data buffer not enough".to_string(),
                            },
                        )?)))?;
                    } else {
                        col.push(None)?;
                    }
                }
            }
            PhysicalCType::Field(PhysicalDType::Unsigned) => {
                let encoding = get_encoding(data_buffer);
                let ts_codec = get_u64_codec(encoding);
                let mut target = Vec::new();
                ts_codec
                    .decode(data_buffer, &mut target)
                    .context(DecodeSnafu)?;
                let mut target = target.into_iter();
                for i in 0..bitset.len() {
                    if bitset.get(i) {
                        col.push(Some(FieldVal::Unsigned(target.next().context(
                            TsmPageSnafu {
                                reason: "data buffer not enough".to_string(),
                            },
                        )?)))?;
                    } else {
                        col.push(None)?;
                    }
                }
            }
            PhysicalCType::Field(PhysicalDType::Boolean) => {
                let encoding = get_encoding(data_buffer);
                let ts_codec = get_bool_codec(encoding);
                let mut target = Vec::new();
                ts_codec
                    .decode(data_buffer, &mut target)
                    .context(DecodeSnafu)?;

                let mut target = target.into_iter();
                for i in 0..bitset.len() {
                    if bitset.get(i) {
                        col.push(Some(FieldVal::Boolean(target.next().context(
                            TsmPageSnafu {
                                reason: "data buffer not enough".to_string(),
                            },
                        )?)))?;
                    } else {
                        col.push(None)?;
                    }
                }
            }
            PhysicalCType::Field(PhysicalDType::String) => {
                let encoding = get_encoding(data_buffer);
                let ts_codec = get_str_codec(encoding);
                let mut target = Vec::new();
                ts_codec
                    .decode(data_buffer, &mut target)
                    .context(DecodeSnafu)?;

                let mut target = target.into_iter();
                for i in 0..bitset.len() {
                    if bitset.get(i) {
                        col.push(Some(FieldVal::Bytes(target.next().context(
                            TsmPageSnafu {
                                reason: "data buffer not enough".to_string(),
                            },
                        )?)))?;
                    } else {
                        col.push(None)?;
                    }
                }
            }
            PhysicalCType::Field(PhysicalDType::Unknown) => {
                return Err(UnsupportedDataTypeSnafu {
                    dt: "unknown".to_string(),
                }
                .build());
            }
        }
        Ok(col)
    }

    pub fn col_to_page(column: &MutableColumn) -> TskvResult<Page> {
        let null_count = 1;
        let len_bitset = column.valid().byte_len() as u32;
        let data_len = column.valid().len() as u64;
        let mut buf = vec![];
        let statistics = match column.data() {
            PrimaryColumnData::F64(array, min, max) => {
                let target_array = array
                    .iter()
                    .enumerate()
                    .filter_map(|(idx, val)| {
                        if column.valid().get(idx) {
                            Some(*val)
                        } else {
                            None
                        }
                    })
                    .collect::<Vec<_>>();
                let encoder = get_f64_codec(column.column_desc().encoding);
                encoder
                    .encode(&target_array, &mut buf)
                    .context(EncodeSnafu)?;

                PageStatistics::F64(ValueStatistics::new(
                    Some(*min),
                    Some(*max),
                    None,
                    null_count,
                ))
            }
            PrimaryColumnData::I64(array, min, max) => {
                let target_array = array
                    .iter()
                    .enumerate()
                    .filter_map(|(idx, val)| {
                        if column.valid().get(idx) {
                            Some(*val)
                        } else {
                            None
                        }
                    })
                    .collect::<Vec<_>>();
                let encoder = get_i64_codec(column.column_desc().encoding);
                encoder
                    .encode(&target_array, &mut buf)
                    .context(EncodeSnafu)?;

                PageStatistics::I64(ValueStatistics::new(
                    Some(*min),
                    Some(*max),
                    None,
                    null_count,
                ))
            }
            PrimaryColumnData::U64(array, min, max) => {
                let target_array = array
                    .iter()
                    .enumerate()
                    .filter_map(|(idx, val)| {
                        if column.valid().get(idx) {
                            Some(*val)
                        } else {
                            None
                        }
                    })
                    .collect::<Vec<_>>();
                let encoder = get_u64_codec(column.column_desc().encoding);
                encoder
                    .encode(&target_array, &mut buf)
                    .context(EncodeSnafu)?;

                PageStatistics::U64(ValueStatistics::new(
                    Some(*min),
                    Some(*max),
                    None,
                    null_count,
                ))
            }
            PrimaryColumnData::String(array, min, max) => {
                let target_array = array
                    .iter()
                    .enumerate()
                    .filter_map(|(idx, val)| {
                        if column.valid().get(idx) {
                            Some(val.as_bytes())
                        } else {
                            None
                        }
                    })
                    .collect::<Vec<_>>();
                let encoder = get_str_codec(column.column_desc().encoding);
                encoder
                    .encode(&target_array, &mut buf)
                    .context(EncodeSnafu)?;

                PageStatistics::Bytes(ValueStatistics::new(
                    Some(min.as_bytes().to_vec()),
                    Some(max.as_bytes().to_vec()),
                    None,
                    null_count,
                ))
            }
            PrimaryColumnData::Bool(array, min, max) => {
                let target_array = array
                    .iter()
                    .enumerate()
                    .filter_map(|(idx, val)| {
                        if column.valid().get(idx) {
                            Some(*val)
                        } else {
                            None
                        }
                    })
                    .collect::<Vec<_>>();
                let encoder = get_bool_codec(column.column_desc().encoding);
                encoder
                    .encode(&target_array, &mut buf)
                    .context(EncodeSnafu)?;

                PageStatistics::Bool(ValueStatistics::new(
                    Some(*min),
                    Some(*max),
                    None,
                    null_count,
                ))
            }
        };
        let mut data = vec![];
        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&buf);
        let data_crc = hasher.finalize().to_be_bytes();
        data.extend_from_slice(&len_bitset.to_be_bytes());
        data.extend_from_slice(&data_len.to_be_bytes());
        data.extend_from_slice(&data_crc);
        data.extend_from_slice(column.valid().bytes());
        data.extend_from_slice(&buf);
        let bytes = bytes::Bytes::from(data);
        let meta = PageMeta {
            num_values: column.valid().len() as u32,
            column: column.column_desc().clone(),
            statistics,
        };
        Ok(Page { bytes, meta })
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PageMeta {
    pub(crate) num_values: u32,
    pub(crate) column: TableColumn,
    pub(crate) statistics: PageStatistics,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PageStatistics {
    Bool(ValueStatistics<bool>),
    F64(ValueStatistics<f64>),
    I64(ValueStatistics<i64>),
    U64(ValueStatistics<u64>),
    Bytes(ValueStatistics<Vec<u8>>),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PageWriteSpec {
    pub(crate) offset: u64,
    pub(crate) size: u64,
    pub(crate) meta: PageMeta,
}

impl PageWriteSpec {
    pub fn new(offset: u64, size: u64, meta: PageMeta) -> Self {
        Self { offset, size, meta }
    }

    pub fn offset(&self) -> u64 {
        self.offset
    }

    pub fn size(&self) -> u64 {
        self.size
    }

    /// todo: dont copy meta
    pub fn meta(&self) -> &PageMeta {
        &self.meta
    }
}

#[cfg(test)]
mod test {
    use arrow::datatypes::ToByteSlice;
    use models::schema::tskv_table_schema::{ColumnType, TableColumn};
    use models::ValueType;
    use utils::bitset::BitSet;

    use crate::tsm::page::{Page, PageMeta, PageStatistics};
    use crate::tsm::statistics::ValueStatistics;

    fn create_test_page() -> Page {
        let field_column = TableColumn::new(
            1,
            "field1".to_string(),
            ColumnType::Field(ValueType::Integer),
            Default::default(),
        );

        let pagemeta = PageMeta {
            num_values: 1,
            column: field_column,
            statistics: PageStatistics::I64(ValueStatistics::new(Some(1), Some(3), None, 1)),
        };

        let buf = b"hello world".to_byte_slice();
        let data_len = 1_u64;
        let valid = BitSet::new();
        let len_bitset = valid.byte_len() as u32;

        let mut hasher = crc32fast::Hasher::new();
        hasher.update(buf);
        let data_crc = hasher.finalize().to_be_bytes();

        let mut data = vec![];
        data.extend_from_slice(&len_bitset.to_be_bytes());
        data.extend_from_slice(&data_len.to_be_bytes());
        data.extend_from_slice(&data_crc);
        data.extend_from_slice(valid.bytes());
        data.extend_from_slice(buf);

        let bytes = bytes::Bytes::from(data);
        Page::new(bytes, pagemeta.clone())
    }

    #[test]
    fn test_page_crc_validation() {
        let page = create_test_page();
        let result = page.crc_validation();
        assert!(result.is_ok());
    }
}
