pub mod tskv_compaction;

pub trait MetricStore: Send {
    fn begin(&mut self);

    fn end(&mut self, is_finished: bool);
}

pub struct FakeMetricStore;

impl MetricStore for FakeMetricStore {
    fn begin(&mut self) {}

    fn end(&mut self, _: bool) {}
}
