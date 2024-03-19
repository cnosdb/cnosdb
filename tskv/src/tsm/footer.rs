use datafusion::parquet::data_type::AsBytes;
use models::predicate::domain::TimeRange;
use models::SeriesId;
use serde::{Deserialize, Serialize};
use utils::BloomFilter;

use crate::TskvError;

#[derive(Debug, Default, Clone, Serialize, Deserialize, Eq, PartialEq)]
pub struct Footer {
    pub(crate) version: u8,
    pub(crate) time_range: TimeRange,
    pub(crate) table: TableMeta,
    pub(crate) series: SeriesMeta,
}

impl Footer {
    pub fn new(version: u8, time_range: TimeRange, table: TableMeta, series: SeriesMeta) -> Self {
        Self {
            version,
            time_range,
            table,
            series,
        }
    }

    pub fn table(&self) -> &TableMeta {
        &self.table
    }

    pub fn series(&self) -> &SeriesMeta {
        &self.series
    }

    pub fn time_range(&self) -> &TimeRange {
        &self.time_range
    }

    pub fn version(&self) -> u8 {
        self.version
    }

    pub fn serialize(&self) -> crate::TskvResult<Vec<u8>> {
        bincode::serialize(&self).map_err(|e| TskvError::Serialize { source: e.into() })
    }

    pub fn deserialize(bytes: &[u8]) -> crate::TskvResult<Self> {
        bincode::deserialize(bytes).map_err(|e| TskvError::Deserialize { source: e.into() })
    }

    pub fn maybe_series_exist(&self, series_id: &SeriesId) -> bool {
        self.series
            .bloom_filter
            .maybe_contains((*series_id).as_bytes().as_ref())
    }
}

///  7 + 8 + 8 = 23
#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
pub struct TableMeta {
    // todo: bloomfilter, store table object id
    // bloom_filter: BloomFilter,
    chunk_group_offset: u64,
    chunk_group_size: usize,
}

impl TableMeta {
    pub fn new(chunk_group_offset: u64, chunk_group_size: usize) -> Self {
        Self {
            chunk_group_offset,
            chunk_group_size,
        }
    }

    pub fn chunk_group_offset(&self) -> u64 {
        self.chunk_group_offset
    }

    pub fn chunk_group_size(&self) -> usize {
        self.chunk_group_size
    }
}

/// 16 + 8 + 8 = 32
#[derive(Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
pub struct SeriesMeta {
    bloom_filter: BloomFilter,
    // 16 Byte
    chunk_offset: u64,
    chunk_size: u64,
}

impl SeriesMeta {
    pub fn new(bloom_filter: Vec<u8>, chunk_offset: u64, chunk_size: u64) -> Self {
        let bloom_filter = BloomFilter::with_data(&bloom_filter);
        Self {
            bloom_filter,
            chunk_offset,
            chunk_size,
        }
    }

    pub fn serialize(&self) -> crate::TskvResult<Vec<u8>> {
        bincode::serialize(&self).map_err(|e| TskvError::Serialize { source: e.into() })
    }

    pub fn deserialize(bytes: &[u8]) -> crate::TskvResult<Self> {
        bincode::deserialize(bytes).map_err(|e| TskvError::Deserialize { source: e.into() })
    }

    pub fn bloom_filter(&self) -> &BloomFilter {
        &self.bloom_filter
    }

    pub fn chunk_offset(&self) -> u64 {
        self.chunk_offset
    }

    pub fn chunk_size(&self) -> u64 {
        self.chunk_size
    }
}

#[cfg(test)]
mod test {
    use models::predicate::domain::TimeRange;
    use utils::BloomFilter;

    use crate::tsm::footer::{Footer, SeriesMeta, TableMeta};
    use crate::tsm::BLOOM_FILTER_BITS;

    #[test]
    fn test1() {
        let table_meta = TableMeta {
            chunk_group_offset: 100,
            chunk_group_size: 100,
        };
        let expect_footer = Footer::new(
            1,
            TimeRange {
                min_ts: 0,
                max_ts: 100,
            },
            table_meta,
            SeriesMeta::new(
                BloomFilter::new(BLOOM_FILTER_BITS).bytes().to_vec(),
                100,
                100,
            ),
        );
        let bytess = expect_footer.serialize().unwrap();
        println!("bytes: {:?}", bytess.len());
        let footer = Footer::deserialize(&bytess).unwrap();
        assert_eq!(footer, expect_footer);
    }
}
