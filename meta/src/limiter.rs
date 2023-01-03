// extern crate serde_derive;

use crate::error::{MetaError, MetaResult};
use std::fmt::{Debug, Formatter};

use models::limiter::{CountLimiter, RateLimiter};
use models::schema::{DatabaseSchema, LimiterConfig};

use serde::{Deserialize, Serialize};

pub trait Limiter: Send + Sync + Debug {
    fn check_create_db(&self, db_number: usize, db_schema: &mut DatabaseSchema) -> MetaResult<()>;
    fn check_add_user(&self, user_number: usize) -> MetaResult<()>;
    fn check_data_in(&self, data_len: usize) -> MetaResult<()>;
    fn check_data_out(&self, data_len: usize) -> MetaResult<()>;
    fn check_query(&self) -> MetaResult<()>;
    fn check_write(&self) -> MetaResult<()>;
}

#[derive(Serialize, Deserialize, Debug)]
pub struct LimiterImpl {
    // add user limit
    max_users_number: Option<usize>,
    /// create database limit
    max_databases: Option<usize>,
    max_shard_number: Option<usize>,
    max_replicate_number: Option<usize>,
    max_retention_time: Option<usize>,

    data_in_rate: RateLimiter,
    data_out_rate: RateLimiter,
    //
    data_in_size: CountLimiter,
    data_out_size: CountLimiter,
    //
    // storage_size: CountLimiter,
    //
    queries_rate: RateLimiter,
    queries: CountLimiter,
    //
    writes_rate: RateLimiter,
    writes: CountLimiter,
}
#[derive(Default)]
pub struct NoneLimiter;

impl Debug for NoneLimiter {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str("NoneLimiter")
    }
}

impl Limiter for NoneLimiter {
    fn check_create_db(
        &self,
        _db_number: usize,
        _db_schema: &mut DatabaseSchema,
    ) -> MetaResult<()> {
        Ok(())
    }

    fn check_add_user(&self, _user_number: usize) -> MetaResult<()> {
        Ok(())
    }

    fn check_data_in(&self, _data_len: usize) -> MetaResult<()> {
        Ok(())
    }

    fn check_data_out(&self, _data_len: usize) -> MetaResult<()> {
        Ok(())
    }

    fn check_query(&self) -> MetaResult<()> {
        Ok(())
    }

    fn check_write(&self) -> MetaResult<()> {
        Ok(())
    }
}

impl From<&LimiterConfig> for LimiterImpl {
    fn from(value: &LimiterConfig) -> Self {
        Self {
            max_users_number: value.max_users_number,
            max_databases: value.max_databases,
            max_shard_number: value.max_shard_number,
            max_replicate_number: value.max_shard_number,
            max_retention_time: value.max_retention_time,

            data_in_rate: value.data_in_rate.build(),
            data_out_rate: value.data_out_rate.build(),

            data_in_size: value.data_in_size.build(),
            data_out_size: value.data_out_size.build(),

            queries_rate: value.queries_rate.build(),
            queries: value.queries.build(),

            writes_rate: value.writes_rate.build(),
            writes: value.writes.build(),
        }
    }
}

impl Limiter for LimiterImpl {
    fn check_create_db(&self, db_number: usize, db_schema: &mut DatabaseSchema) -> MetaResult<()> {
        if let Some(max) = &self.max_databases {
            if db_number >= *max {
                return Err(MetaError::Limit {
                    action: "create database".to_string(),
                    name: "db number".to_string(),
                    max: format!("{}", max),
                });
            }
        }

        let replica = db_schema.config.replica_or_default();
        if let Some(max) = &self.max_replicate_number {
            if replica as usize > *max {
                return Err(MetaError::Limit {
                    action: "create database".to_string(),
                    name: "replica number".to_string(),
                    max: format!("{}", max),
                });
            }
        }

        let shard = db_schema.config.shard_num_or_default();
        if let Some(max) = &self.max_shard_number {
            if shard as usize > *max {
                return Err(MetaError::Limit {
                    action: "create database".to_string(),
                    name: "shard number".to_string(),
                    max: format!("{}", max),
                });
            }
        }

        match (db_schema.config.ttl(), self.max_retention_time) {
            (Some(ttl), Some(day)) => {
                let ttl = ttl.to_nanoseconds();
                let max = models::schema::Duration::new_with_day(day as u64);
                if ttl > max.to_nanoseconds() {
                    return Err(MetaError::Limit {
                        action: "create database".to_string(),
                        name: "retention time".to_string(),
                        max: format!("{}", max),
                    });
                }
            }
            (None, Some(day)) => db_schema
                .config
                .with_ttl(models::schema::Duration::new_with_day(day as u64)),
            _ => {}
        }

        Ok(())
    }

    fn check_add_user(&self, user_number: usize) -> MetaResult<()> {
        if let Some(max) = &self.max_users_number {
            if user_number >= *max {
                return Err(MetaError::Limit {
                    action: "add user".to_string(),
                    name: "user number".to_string(),
                    max: format!("{}", max),
                });
            }
        }
        Ok(())
    }

    fn check_data_in(&self, data_len: usize) -> MetaResult<()> {
        self.data_in_rate
            .acquire(data_len)
            .map_err(|_| MetaError::Limit {
                action: "data write".to_string(),
                name: "traffic".to_string(),
                max: self.data_in_rate.to_traffic_string(),
            })?;
        self.data_in_size
            .try_inc(data_len)
            .map_err(|max| MetaError::Limit {
                action: "data write".to_string(),
                name: "traffic".to_string(),
                max: format!("{}B", max),
            })?;
        Ok(())
    }

    fn check_data_out(&self, data_len: usize) -> MetaResult<()> {
        self.data_out_rate
            .acquire(data_len)
            .map_err(|_| MetaError::Limit {
                action: "data write".to_string(),
                name: "traffic".to_string(),
                max: self.data_out_rate.to_traffic_string(),
            })?;
        self.data_out_size
            .try_inc(data_len)
            .map_err(|max| MetaError::Limit {
                action: "data write".to_string(),
                name: "traffic".to_string(),
                max: format!("{}B", max),
            })?;
        Ok(())
    }

    fn check_query(&self) -> MetaResult<()> {
        self.queries_rate
            .acquire_one()
            .map_err(|_| MetaError::Limit {
                action: "data query".to_string(),
                name: "query".to_string(),
                max: self.queries_rate.to_traffic_string(),
            })?;
        self.queries.try_inc_one().map_err(|max| MetaError::Limit {
            action: "data query".to_string(),
            name: "query".to_string(),
            max: format!("{}", max),
        })?;
        Ok(())
    }

    fn check_write(&self) -> MetaResult<()> {
        self.writes_rate
            .acquire_one()
            .map_err(|_| MetaError::Limit {
                action: "data write".to_string(),
                name: "write".to_string(),
                max: self.writes_rate.to_traffic_string(),
            })?;
        self.writes.try_inc_one().map_err(|max| MetaError::Limit {
            action: "data write".to_string(),
            name: "write".to_string(),
            max: format!("{}", max),
        })?;
        Ok(())
    }
}
