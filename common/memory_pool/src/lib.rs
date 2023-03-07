use std::sync::Arc;

pub use datafusion::execution::memory_pool::*;

pub type MemoryPoolRef = Arc<dyn MemoryPool>;

#[cfg(test)]
mod tests {
    use std::sync::Arc;

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
