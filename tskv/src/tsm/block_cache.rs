use std::sync::Arc;

use cache::{ShardedSyncCache, SyncCache};
use datafusion::execution::memory_pool::UnboundedMemoryPool;
use memory_pool::{MemoryConsumer, MemoryPool, MemoryPoolRef, MemoryReservation};
use metrics::count::U64Counter;
use metrics::duration::DurationCounter;
use metrics::gauge::U64Gauge;
use metrics::metric_register::MetricsRegister;
use models::schema::split_owner;
use parking_lot::Mutex;

use crate::file_system::file::async_file::AsyncFile;
use crate::kv_option::StorageOptions;
use crate::tsm::{read_data_block, BlockMeta, DataBlock, DataBlockId};
use crate::{Error, TseriesFamilyId};

#[derive(Debug)]
pub struct DataBlockCache {
    hits: U64Counter,
    visits: U64Counter,
    load_duration: DurationCounter,
    size_gauge: U64Gauge,
    max_cap: usize,
    cache: Arc<ShardedSyncCache<DataBlockId, DataBlockWrap>>,
    memory_reservation: Mutex<MemoryReservation>,
}

#[derive(Debug)]
pub struct DataBlockCacheFactory {
    database_owner: Arc<String>,
    register: Arc<MetricsRegister>,
    memory_pool: MemoryPoolRef,
    opt: Arc<StorageOptions>,
}

impl DataBlockCacheFactory {
    pub fn new(
        database_owner: Arc<String>,
        register: Arc<MetricsRegister>,
        memory_pool: MemoryPoolRef,
        opt: Arc<StorageOptions>,
    ) -> Self {
        Self {
            database_owner,
            register,
            memory_pool,
            opt,
        }
    }
    pub fn create_cache(&self, tsf_id: TseriesFamilyId) -> DataBlockCache {
        DataBlockCache::create(
            &self.register,
            &self.database_owner,
            tsf_id,
            &self.opt,
            &self.memory_pool,
        )
    }
}

impl Default for DataBlockCache {
    fn default() -> Self {
        let pool = Arc::new(UnboundedMemoryPool::default()) as Arc<dyn MemoryPool>;
        let consumer = MemoryConsumer::new("default data block cache");
        let reservation = consumer.register(&pool);

        Self {
            hits: Default::default(),
            visits: Default::default(),
            load_duration: Default::default(),
            size_gauge: Default::default(),
            cache: Arc::new(ShardedSyncCache::create_lru_sharded_cache_unbounded()),
            max_cap: usize::MAX,
            memory_reservation: Mutex::new(reservation),
        }
    }
}

#[derive(Debug, Clone)]
pub struct DataBlockWrap {
    block: Arc<DataBlock>,
    size: usize,
}

impl DataBlockWrap {
    pub fn new(block: Arc<DataBlock>) -> Self {
        let size = block.memory_size();
        Self { block, size }
    }
    pub fn get(self) -> Arc<DataBlock> {
        self.block
    }
    pub fn size(&self) -> usize {
        self.size
    }
}

impl DataBlockCache {
    pub fn new(
        hits: U64Counter,
        visits: U64Counter,
        load_duration: DurationCounter,
        size_gauge: U64Gauge,
        max_cap: usize,
        cache: Arc<ShardedSyncCache<DataBlockId, DataBlockWrap>>,
        memory_reservation: Mutex<MemoryReservation>,
    ) -> Self {
        Self {
            hits,
            visits,
            load_duration,
            size_gauge,
            max_cap,
            cache,
            memory_reservation,
        }
    }

    pub fn create(
        metrics: &MetricsRegister,
        database: &str,
        tsf_id: TseriesFamilyId,
        cache_option: &StorageOptions,
        memory_pool: &MemoryPoolRef,
    ) -> Self {
        let (tenant, db) = split_owner(database);
        let vnode_id = tsf_id.to_string();
        let labels = [
            ("tenant", tenant),
            ("database", db),
            ("vnode_id", vnode_id.as_str()),
        ];
        let hits_metric = metrics.metric::<U64Counter>("block_cache_hits", "");
        let visits_metric = metrics.metric::<U64Counter>("block_cache_visits", "");
        let load_duration_metric =
            metrics.metric::<DurationCounter>("block_cache_load_duration", "");
        let size_metric = metrics.metric::<U64Gauge>("block_cache_size", "");

        let hits = hits_metric.recorder(labels);
        let visits = visits_metric.recorder(labels);
        let load_duration = load_duration_metric.recorder(labels);
        let size_gauge = size_metric.recorder(labels);
        let memory_cap = cache_option.max_data_block_cache_size;
        let consumer = MemoryConsumer::new(format!("data block cache for tsm {tsf_id}"));
        let memory_reservation = Mutex::new(consumer.register(memory_pool));
        Self {
            hits,
            visits,
            load_duration,
            size_gauge,
            max_cap: memory_cap as usize,
            cache: Arc::new(ShardedSyncCache::create_lru_sharded_cache_unbounded()),
            memory_reservation,
        }
    }

    pub async fn get(&self, id: &DataBlockId) -> Option<Arc<DataBlock>> {
        self.cache.get(id).map(|a| a.block)
    }

    pub async fn get_data_block_or_read(
        &self,
        block_meta: &BlockMeta,
        file: Arc<AsyncFile>,
    ) -> Result<Arc<DataBlock>, Error> {
        self.visits.inc_one();
        let id = DataBlockId::new(block_meta.tsm_file_id(), block_meta.offset());
        match self.cache.get(&id) {
            // hits!
            Some(res) => {
                self.hits.inc_one();
                Ok(res.get())
            }
            None => {
                let start = std::time::Instant::now();
                let mut buf = vec![0_u8; block_meta.size() as usize];
                let blk = read_data_block(
                    file,
                    &mut buf,
                    block_meta.field_type(),
                    block_meta.offset(),
                    block_meta.val_off(),
                )
                .await?;
                let blk = Arc::new(blk);
                self.insert_data_block(id, blk.clone())?;
                self.load_duration.inc(start.elapsed());
                Ok(blk)
            }
        }
    }

    fn insert_data_block(&self, id: DataBlockId, blk: Arc<DataBlock>) -> Result<(), Error> {
        let mut memory = self.memory_reservation.lock();

        let blk = DataBlockWrap::new(blk);
        while self.max_cap - memory.size() < blk.size() {
            if let Some((_, b)) = self.cache.pop_shard(&id).or_else(|| self.cache.pop()) {
                memory.shrink(b.size())
            } else {
                return Ok(());
            }
        }

        memory
            .try_grow(blk.size())
            .map_err(|_| Error::MemoryExhausted)?;
        self.size_gauge.set(memory.size() as u64);
        drop(memory);
        self.cache.insert(id, blk);
        Ok(())
    }

    pub fn clear(&self) {
        let mut reservation = self.memory_reservation.lock();
        self.cache.clear();
        reservation.resize(0)
    }
}

#[cfg(test)]
mod data_block_cache_tests {
    use std::sync::Arc;

    use models::PhysicalDType;

    use crate::tsm::{DataBlock, DataBlockCache, DataBlockId};

    #[tokio::test]
    async fn test_data_block_cache() {
        let mut cache = DataBlockCache::default();
        let block = Arc::new(DataBlock::new(1000, PhysicalDType::Integer));
        cache.max_cap = block.memory_size();

        cache
            .insert_data_block(DataBlockId::new(0, 0), block.clone())
            .unwrap();
        cache
            .insert_data_block(DataBlockId::new(1, 1), block.clone())
            .unwrap();
        assert!(cache.get(&DataBlockId::new(0, 0)).await.is_none());
        assert!(cache.get(&DataBlockId::new(1, 1)).await.is_some());
        cache.clear();
        assert!(cache.get(&DataBlockId::new(1, 1)).await.is_none());

        cache.max_cap = block.memory_size() * 2;

        cache
            .insert_data_block(DataBlockId::new(0, 0), block.clone())
            .unwrap();
        cache
            .insert_data_block(DataBlockId::new(1, 1), block.clone())
            .unwrap();
        assert!(cache.get(&DataBlockId::new(0, 0)).await.is_some());
        assert!(cache.get(&DataBlockId::new(1, 1)).await.is_some());
    }
}
