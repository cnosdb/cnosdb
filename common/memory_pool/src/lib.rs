use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Instant;

use datafusion::common::{DataFusionError, Result};
pub use datafusion::execution::memory_pool::{MemoryConsumer, MemoryPool, MemoryReservation};
use parking_lot::RwLock;

pub type MemoryPoolRef = Arc<dyn MemoryPool>;

const WARNING_PERCENT: f64 = 0.8;
const LOG_INTERVAL_SECONDS: u64 = 5;

/// A [`MemoryPool`] that implements a greedy first-come first-serve limit
#[derive(Debug)]
pub struct GreedyMemoryPool {
    pool_size: usize,
    used: AtomicUsize,
    last_log_time: RwLock<Instant>,
}

impl Default for GreedyMemoryPool {
    fn default() -> Self {
        Self::new(1024 * 1024 * 1024)
    }
}
impl GreedyMemoryPool {
    /// Allocate up to `limit` bytes
    pub fn new(pool_size: usize) -> Self {
        Self {
            pool_size,
            used: AtomicUsize::new(0),
            last_log_time: RwLock::new(Instant::now()),
        }
    }
}

impl MemoryPool for GreedyMemoryPool {
    fn grow(&self, _reservation: &MemoryReservation, additional: usize) {
        self.used.fetch_add(additional, Ordering::Relaxed);
    }

    fn shrink(&self, _reservation: &MemoryReservation, shrink: usize) {
        self.used.fetch_sub(shrink, Ordering::Relaxed);
    }

    fn try_grow(&self, reservation: &MemoryReservation, additional: usize) -> Result<()> {
        self.used
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |used| {
                let new_used = used + additional;
                (new_used <= self.pool_size).then_some(new_used)
            })
            .map_err(|used| {
                insufficient_capacity_err(reservation, additional, self.pool_size - used)
            })?;
        // add warn message, for CAICT test
        if self
            .last_log_time
            .read()
            .duration_since(Instant::now())
            .as_secs()
            >= LOG_INTERVAL_SECONDS
        {
            *self.last_log_time.write() = Instant::now();
            if self.used.load(Ordering::Relaxed) as f64 > self.pool_size as f64 * WARNING_PERCENT {
                trace::warn!(
                    "{}",
                    format!(
                        "Memory used over {} percent, after allocate additional {} ",
                        WARNING_PERCENT * 100_f64,
                        additional
                    )
                );
            }
        }

        Ok(())
    }

    fn reserved(&self) -> usize {
        self.used.load(Ordering::Relaxed)
    }
}

fn insufficient_capacity_err(
    reservation: &MemoryReservation,
    additional: usize,
    available: usize,
) -> DataFusionError {
    DataFusionError::ResourcesExhausted(format!("Failed to allocate additional {} bytes with {} bytes already allocated - maximum available is {}", additional, reservation.size(), available))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::execution::memory_pool::MemoryConsumer;

    use super::*;

    #[test]
    fn it_works() {
        let pool = Arc::new(GreedyMemoryPool::new(50)) as _;
        let mut a1 = MemoryConsumer::new("a1").register(&pool);
        assert_eq!(pool.reserved(), 0);

        a1.grow(100);
        assert_eq!(pool.reserved(), 100);

        assert_eq!(a1.free(), 100);
        assert_eq!(pool.reserved(), 0);

        a1.try_grow(100).unwrap_err();
        assert_eq!(pool.reserved(), 0);

        a1.try_grow(30).unwrap();
        assert_eq!(pool.reserved(), 30);

        let mut a2 = MemoryConsumer::new("a2").register(&pool);
        a2.try_grow(25).unwrap_err();
        assert_eq!(pool.reserved(), 30);

        drop(a1);
        assert_eq!(pool.reserved(), 0);

        a2.try_grow(25).unwrap();
        assert_eq!(pool.reserved(), 25);
    }
}
