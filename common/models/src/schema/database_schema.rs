use std::fmt::{self, Display};

use datafusion::arrow::datatypes::TimeUnit;
use serde::{Deserialize, Serialize};

use crate::schema::utils::{Duration, DurationUnit};
use crate::Timestamp;

#[derive(Serialize, Deserialize, Debug, Clone, Default, PartialEq, Eq, Hash)]
pub struct DatabaseSchema {
    tenant: String,
    database: String,
    pub config: DatabaseOptions,
}

impl DatabaseSchema {
    pub fn new(tenant_name: &str, database_name: &str) -> Self {
        DatabaseSchema {
            tenant: tenant_name.to_string(),
            database: database_name.to_string(),
            config: DatabaseOptions::default(),
        }
    }

    pub fn new_with_options(
        tenant_name: &str,
        database_name: &str,
        options: DatabaseOptions,
    ) -> Self {
        DatabaseSchema {
            tenant: tenant_name.to_string(),
            database: database_name.to_string(),
            config: options,
        }
    }

    pub fn database_name(&self) -> &str {
        &self.database
    }

    pub fn tenant_name(&self) -> &str {
        &self.tenant
    }

    pub fn owner(&self) -> String {
        make_owner(&self.tenant, &self.database)
    }

    pub fn is_empty(&self) -> bool {
        if self.tenant.is_empty() && self.database.is_empty() {
            return true;
        }

        false
    }

    pub fn options(&self) -> &DatabaseOptions {
        &self.config
    }

    // return the min timestamp value database allowed to store
    pub fn time_to_expired(&self) -> i64 {
        let (ttl, now) = match self.config.precision_or_default() {
            Precision::MS => (
                self.config.ttl_or_default().to_millisecond(),
                crate::utils::now_timestamp_millis(),
            ),
            Precision::US => (
                self.config.ttl_or_default().to_microseconds(),
                crate::utils::now_timestamp_micros(),
            ),
            Precision::NS => (
                self.config.ttl_or_default().to_nanoseconds(),
                crate::utils::now_timestamp_nanos(),
            ),
        };
        now - ttl
    }
}

pub fn make_owner(tenant_name: &str, database_name: &str) -> String {
    format!("{}.{}", tenant_name, database_name)
}

/// "tenant.database" -> ("tenant", "database")
pub fn split_owner(owner: &str) -> (&str, &str) {
    owner
        .find('.')
        .map(|index| {
            (index < owner.len())
                .then(|| (&owner[..index], &owner[(index + 1)..]))
                .unwrap_or((owner, ""))
        })
        .unwrap_or_default()
}

#[derive(Serialize, Deserialize, Debug, Default, Clone, PartialEq, Eq, Hash)]
pub struct DatabaseOptions {
    // data keep time
    ttl: Option<Duration>,

    shard_num: Option<u64>,
    // shard coverage time range
    vnode_duration: Option<Duration>,

    replica: Option<u64>,
    // timestamp precision
    precision: Option<Precision>,

    db_is_hidden: bool,
}

impl DatabaseOptions {
    pub const DEFAULT_TTL: Duration = Duration {
        time_num: 0,
        unit: DurationUnit::Inf,
    };
    pub const DEFAULT_SHARD_NUM: u64 = 1;
    pub const DEFAULT_REPLICA: u64 = 1;
    pub const DEFAULT_VNODE_DURATION: Duration = Duration {
        time_num: 365,
        unit: DurationUnit::Day,
    };
    pub const DEFAULT_PRECISION: Precision = Precision::NS;

    pub fn new(
        ttl: Option<Duration>,
        shard_num: Option<u64>,
        vnode_duration: Option<Duration>,
        replica: Option<u64>,
        precision: Option<Precision>,
    ) -> Self {
        DatabaseOptions {
            ttl,
            shard_num,
            vnode_duration,
            replica,
            precision,
            db_is_hidden: false,
        }
    }

    pub fn ttl(&self) -> &Option<Duration> {
        &self.ttl
    }

    pub fn ttl_or_default(&self) -> &Duration {
        self.ttl.as_ref().unwrap_or(&DatabaseOptions::DEFAULT_TTL)
    }

    pub fn shard_num(&self) -> &Option<u64> {
        &self.shard_num
    }

    pub fn shard_num_or_default(&self) -> u64 {
        self.shard_num.unwrap_or(DatabaseOptions::DEFAULT_SHARD_NUM)
    }

    pub fn vnode_duration(&self) -> &Option<Duration> {
        &self.vnode_duration
    }

    pub fn vnode_duration_or_default(&self) -> &Duration {
        self.vnode_duration
            .as_ref()
            .unwrap_or(&DatabaseOptions::DEFAULT_VNODE_DURATION)
    }

    pub fn replica(&self) -> &Option<u64> {
        &self.replica
    }

    pub fn replica_or_default(&self) -> u64 {
        self.replica.unwrap_or(DatabaseOptions::DEFAULT_REPLICA)
    }

    pub fn precision(&self) -> &Option<Precision> {
        &self.precision
    }

    pub fn precision_or_default(&self) -> &Precision {
        self.precision
            .as_ref()
            .unwrap_or(&DatabaseOptions::DEFAULT_PRECISION)
    }

    pub fn with_ttl(&mut self, ttl: Duration) {
        self.ttl = Some(ttl);
    }

    pub fn with_shard_num(&mut self, shard_num: u64) {
        self.shard_num = Some(shard_num);
    }

    pub fn with_vnode_duration(&mut self, vnode_duration: Duration) {
        self.vnode_duration = Some(vnode_duration);
    }

    pub fn with_replica(&mut self, replica: u64) {
        self.replica = Some(replica);
    }

    pub fn with_precision(&mut self, precision: Precision) {
        self.precision = Some(precision)
    }

    pub fn get_db_is_hidden(&self) -> bool {
        self.db_is_hidden
    }

    pub fn set_db_is_hidden(&mut self, db_is_hidden: bool) {
        self.db_is_hidden = db_is_hidden;
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum Precision {
    MS = 0,
    US,
    NS,
}

impl From<u8> for Precision {
    fn from(value: u8) -> Self {
        match value {
            0 => Precision::MS,
            1 => Precision::US,
            2 => Precision::NS,
            _ => Precision::NS,
        }
    }
}

impl Default for Precision {
    fn default() -> Self {
        Self::NS
    }
}

impl From<TimeUnit> for Precision {
    fn from(value: TimeUnit) -> Self {
        match value {
            TimeUnit::Millisecond => Precision::MS,
            TimeUnit::Microsecond => Precision::US,
            TimeUnit::Nanosecond => Precision::NS,
            _ => Precision::NS,
        }
    }
}

impl From<Precision> for TimeUnit {
    fn from(value: Precision) -> Self {
        match value {
            Precision::MS => TimeUnit::Millisecond,
            Precision::US => TimeUnit::Microsecond,
            Precision::NS => TimeUnit::Nanosecond,
        }
    }
}

impl Precision {
    pub fn new(text: &str) -> Option<Self> {
        match text.to_uppercase().as_str() {
            "MS" => Some(Precision::MS),
            "US" => Some(Precision::US),
            "NS" => Some(Precision::NS),
            _ => None,
        }
    }
}

pub fn timestamp_convert(from: Precision, to: Precision, ts: Timestamp) -> Option<Timestamp> {
    match (from, to) {
        (Precision::NS, Precision::US) | (Precision::US, Precision::MS) => Some(ts / 1_000),
        (Precision::MS, Precision::US) | (Precision::US, Precision::NS) => ts.checked_mul(1_000),
        (Precision::NS, Precision::MS) => Some(ts / 1_000_000),
        (Precision::MS, Precision::NS) => ts.checked_mul(1_000_000),
        _ => Some(ts),
    }
}

impl Display for Precision {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Precision::MS => f.write_str("MS"),
            Precision::US => f.write_str("US"),
            Precision::NS => f.write_str("NS"),
        }
    }
}
