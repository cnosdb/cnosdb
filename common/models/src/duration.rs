const MILLIS_PER_SEC: u64 = 1000;
const SECS_PER_MINUTE: u64 = 60;
const MINUTES_PER_HOUR: u64 = 60;
const HOURS_PER_DAY: u64 = 24;

pub const UNIT: u64 = 1;
pub const MS: u64 = UNIT;
pub const SECOND: u64 = MS * MILLIS_PER_SEC;
pub const MINUTE: u64 = SECOND * SECS_PER_MINUTE;
pub const HOUR: u64 = MINUTE * MINUTES_PER_HOUR;
pub const DAY: u64 = HOUR * HOURS_PER_DAY;
