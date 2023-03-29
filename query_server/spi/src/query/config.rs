use std::time::Duration;

#[derive(Debug, Clone)]
pub enum StreamTriggerInterval {
    Once,
    Interval(Duration),
}
