use std::cmp::Ordering;
use std::collections::HashMap;

use chrono::Utc;
use coordinator::service::CoordinatorRef;
use dateparser;
use models::auth::privilege::{DatabasePrivilege, Privilege, TenantObjectPrivilege};
use models::auth::user::{UserInfo, ROOT, ROOT_PWD};
use models::oid::Identifier;
use models::schema::{DEFAULT_CATALOG, DEFAULT_DATABASE};
use models::utils::now_timestamp_nanos;
use protocol_parser::line_protocol::parser::Parser;
use protos::vector::event_wrapper::Event;
use protos::vector::metric::Value as MetricValue;
use protos::vector::sketch::{AgentDdSketch, Sketch};
use protos::vector::value::Kind;
use protos::vector::vector_server::Vector;
use protos::vector::{
    DistributionSample, EventWrapper, HealthCheckRequest, HealthCheckResponse, Log, Metric,
    PushEventsRequest, PushEventsResponse, ServingStatus, Timestamp, Value,
};
use spi::server::dbms::DBMSRef;
use tonic::{Request, Response, Status};
use utils::precision::Precision;

use crate::server;
use crate::server::Error;

const TENANT_FIELD: &str = "_tenant";
const DATABASE_FIELD: &str = "_database";
const TABLE_FIELD: &str = "_table";
const USERNAME_FIELD: &str = "_user";
const PASSWORD_FIELD: &str = "_password";

const VECTOR_LOG_TABLE: &str = "__vector_log";
const VECTOR_LOG_HOST_TAG: &str = "host";
const VECTOR_LOG_MESSAGE_FIELD: &str = "message";
const VECTOR_LOG_TIMESTAMP: &str = "timestamp";

const INVALID_FIELD_OR_TAG: &str = "time";

const VECTOR_TYPE_TAG_KEY: &str = "metric_type";
const VECTOR_LOG_TYPE_TAG_VALUE: &str = "logs";

pub struct VectorService {
    pub coord: CoordinatorRef,
    pub dbms: DBMSRef,
}

impl VectorService {
    pub fn new(coord: CoordinatorRef, dbms: DBMSRef) -> Self {
        Self { coord, dbms }
    }

    pub async fn get_tenant_db_and_check_privilege(
        &self,
        event: &EventWrapper,
    ) -> Result<(String, String), Status> {
        let event = event
            .event
            .clone()
            .ok_or_else(|| Status::invalid_argument("missing event"))?;
        match event {
            Event::Log(log) => {
                let db = log
                    .fields
                    .get(DATABASE_FIELD)
                    .map(|v| vector_value_to_string(v.clone()))
                    .unwrap_or(DEFAULT_DATABASE.to_string())
                    .trim_matches('\"')
                    .to_string();
                let tenant = log
                    .fields
                    .get(TENANT_FIELD)
                    .map(|v| vector_value_to_string(v.clone()))
                    .unwrap_or(DEFAULT_CATALOG.to_string())
                    .trim_matches('\"')
                    .to_string();
                let user = log
                    .fields
                    .get(USERNAME_FIELD)
                    .map(|v| vector_value_to_string(v.clone()))
                    .unwrap_or(ROOT.to_string())
                    .trim_matches('\"')
                    .to_string();
                let password = log
                    .fields
                    .get(PASSWORD_FIELD)
                    .map(|v| vector_value_to_string(v.clone()))
                    .unwrap_or(ROOT_PWD.to_string())
                    .trim_matches('\"')
                    .to_string();
                self.privilege_check(&tenant, &db, &user, &password).await?;
                Ok((tenant, db))
            }
            Event::Metric(metric) => {
                let tenant = metric
                    .tags_v1
                    .get(TENANT_FIELD)
                    .map(|v| v.trim_matches('\"'))
                    .unwrap_or(DEFAULT_CATALOG);
                let db = metric
                    .tags_v1
                    .get(DATABASE_FIELD)
                    .map(|v| v.trim_matches('\"'))
                    .unwrap_or(DEFAULT_DATABASE);
                let user = metric
                    .tags_v1
                    .get(USERNAME_FIELD)
                    .map(|v| v.trim_matches('\"'))
                    .unwrap_or(ROOT);
                let password = metric
                    .tags_v1
                    .get(PASSWORD_FIELD)
                    .map(|v| v.trim_matches('\"'))
                    .unwrap_or(ROOT_PWD);
                self.privilege_check(tenant, db, user, password).await?;
                Ok((tenant.to_string(), db.to_string()))
            }
            Event::Trace(_) => Err(Status::invalid_argument(
                "trace is not supported yet".to_string(),
            )),
        }
    }

    async fn privilege_check(
        &self,
        tenant: &str,
        db: &str,
        user: &str,
        password: &str,
    ) -> Result<(), Status> {
        let tenant_id = *self
            .coord
            .tenant_meta(tenant)
            .await
            .ok_or_else(|| Status::invalid_argument("invalid tenant"))?
            .tenant()
            .id();
        let privilege = Privilege::TenantObject(
            TenantObjectPrivilege::Database(DatabasePrivilege::Write, Some(db.to_string())),
            Some(tenant_id),
        );
        let user_info = UserInfo {
            user: user.to_string(),
            password: password.to_string(),
            private_key: None,
        };
        let user = self
            .dbms
            .authenticate(&user_info, tenant)
            .await
            .map_err(|e| Status::permission_denied(e.to_string()))?;
        if !user.check_privilege(&privilege) {
            return Err(Status::permission_denied(format!(
                "user {} has no privilege {:?}",
                user_info.user, privilege
            )));
        }
        Ok(())
    }
}

#[tonic::async_trait]
impl Vector for VectorService {
    async fn push_events(
        &self,
        request: Request<PushEventsRequest>,
    ) -> Result<Response<PushEventsResponse>, Status> {
        let response = PushEventsResponse {};

        let mut lines = String::new();
        let request_inner = request.into_inner();
        let event_simple = request_inner.events.first();
        let (tenant, db) = match event_simple {
            None => return Ok(Response::new(response)),
            Some(event) => self.get_tenant_db_and_check_privilege(event).await?,
        };

        for event in request_inner.events {
            let line = handle_vector(event).map_err(|e| Status::invalid_argument(e.to_string()))?;
            lines.push_str(&line);
            lines.push('\n');
        }
        let parser = Parser::new(Utc::now().timestamp_millis());
        let lines = parser
            .parse(lines.as_str())
            .map_err(|e| Status::invalid_argument(e.to_string()))?;
        self.coord
            .write_lines(&tenant, &db, Precision::NS, lines, None)
            .await
            .map_err(|e| Status::internal(format!("failed to write lines to database {}", e)))?;
        Ok(Response::new(response))
    }

    async fn health_check(
        &self,
        _request: Request<HealthCheckRequest>,
    ) -> Result<Response<HealthCheckResponse>, Status> {
        let response = HealthCheckResponse {
            status: ServingStatus::Serving as i32,
        };

        Ok(Response::new(response))
    }
}

const AGENT_DEFAULT_BIN_LIMIT: u16 = 4096;
const AGENT_DEFAULT_EPS: f64 = 1.0 / 128.0;
const AGENT_DEFAULT_MIN_VALUE: f64 = 1.0e-9;

#[derive(Debug)]
pub struct DistributionStatistic {
    pub min: f64,
    pub max: f64,
    pub median: f64,
    pub avg: f64,
    pub sum: f64,
    pub count: u64,
}

struct Bin {
    pub k: i32,
    pub n: u32,
}

#[derive(Copy, Clone, Debug, PartialEq)]
pub struct Config {
    bin_limit: u16,
    // gamma_ln is the natural log of gamma_v, used to speed up calculating log base gamma.
    gamma_v: f64,
    gamma_ln: f64,
    // Min and max values representable by a sketch with these params.
    //
    // key(x) =
    //    0 : -min > x < min
    //    1 : x == min
    //   -1 : x == -min
    // +Inf : x > max
    // -Inf : x < -max.
    norm_min: f64,
    // Bias of the exponent, used to ensure key(x) >= 1.
    norm_bias: i32,
}

impl Config {
    #[allow(clippy::cast_possible_truncation)]
    pub(self) fn new(mut eps: f64, min_value: f64, bin_limit: u16) -> Self {
        assert!(eps > 0.0 && eps < 1.0, "eps must be between 0.0 and 1.0");
        assert!(min_value > 0.0, "min value must be greater than 0.0");
        assert!(bin_limit > 0, "bin limit must be greater than 0");

        eps *= 2.0;
        let gamma_v = 1.0 + eps;
        let gamma_ln = eps.ln_1p();

        // SAFETY: We expect `log_gamma` to return a value between -2^16 and 2^16, so it will always
        // fit in an i32.
        let norm_eff_min = log_gamma(gamma_ln, min_value).floor() as i32;
        let norm_bias = -norm_eff_min + 1;

        let norm_min = lower_bound(gamma_v, norm_bias, 1);

        assert!(
            norm_min <= min_value,
            "norm min should not exceed min_value"
        );

        Self {
            bin_limit,
            gamma_v,
            gamma_ln,
            norm_min,
            norm_bias,
        }
    }

    /// Gets the value lower bound of the bin at the given key.
    pub fn bin_lower_bound(&self, k: i32) -> f64 {
        lower_bound(self.gamma_v, self.norm_bias, k)
    }
}

#[inline]
fn pow_gamma(gamma_v: f64, y: f64) -> f64 {
    gamma_v.powf(y)
}

#[inline]
fn log_gamma(gamma_ln: f64, v: f64) -> f64 {
    v.ln() / gamma_ln
}

#[inline]
fn lower_bound(gamma_v: f64, bias: i32, k: i32) -> f64 {
    if k < 0 {
        return -lower_bound(gamma_v, bias, -k);
    }

    if k == i32::MAX {
        return f64::INFINITY;
    }

    if k == 0 {
        return 0.0;
    }

    pow_gamma(gamma_v, f64::from(k - bias))
}

fn handle_vector(event: EventWrapper) -> server::Result<String> {
    let event = event.event.ok_or_else(|| Error::Common {
        reason: "event is none".to_string(),
    })?;
    match event {
        Event::Log(log) => handle_vector_log_trace(log),
        Event::Metric(metric) => handle_vector_metric(metric),
        Event::Trace(_) => Err(Error::Common {
            reason: "not support trace".to_string(),
        }),
    }
}

fn handle_vector_metric(mut metric: Metric) -> server::Result<String> {
    let mut line = String::new();
    let name = metric.name;
    let namespace = metric.namespace;
    let table = format!("{}.{},", namespace, name);
    let timestamp = metric
        .timestamp
        .map(convert_timestamp)
        .unwrap_or(now_timestamp_nanos());
    line.push_str(&table);

    metric.tags_v1.remove(TENANT_FIELD);
    metric.tags_v1.remove(DATABASE_FIELD);
    metric.tags_v1.remove(USERNAME_FIELD);
    metric.tags_v1.remove(PASSWORD_FIELD);

    for (key, value) in metric.tags_v1.iter() {
        let key = if key.as_str() == INVALID_FIELD_OR_TAG {
            format!("{}_metric", key)
        } else {
            key.to_string()
        };
        line.push_str(&format!("{}={},", key, value.replace(' ', "_")));
    }

    match metric.value.ok_or_else(|| Error::Common {
        reason: "missing value in metric".to_string(),
    })? {
        MetricValue::Counter(counter) => {
            line.push_str("metric_type=counter ");
            line.push_str(&format!("value={} ", counter.value));
            line.push_str(&format!("{}", timestamp));
        }
        MetricValue::Gauge(guage) => {
            line.push_str("metric_type=gauge ");
            line.push_str(&format!("value={} ", guage.value));
            line.push_str(&format!("{}", timestamp));
        }
        MetricValue::Set(set) => {
            line.push_str("metric_type=set ");
            line.push_str(&format!("value={}u ", set.values.len()));
            line.push_str(&format!("{}", timestamp));
        }
        MetricValue::Distribution1(distribution) => {
            line.push_str("metric_type=distribution1 ");
            let simple = distribution
                .sample_rates
                .iter()
                .zip(distribution.values.iter())
                .map(|(rate, value)| DistributionSample {
                    rate: *rate,
                    value: *value,
                })
                .collect::<Vec<_>>();
            let statics = distribution_statics(simple).ok_or_else(|| Error::Common {
                reason: "distribution statics failed".to_string(),
            })?;
            line.push_str(&format!("min={},", statics.min));
            line.push_str(&format!("max={},", statics.max));
            line.push_str(&format!("median={},", statics.median));
            line.push_str(&format!("avg={},", statics.avg));
            line.push_str(&format!("sum={},", statics.sum));
            line.push_str(&format!("count={}u ", statics.count));
            line.push_str(&format!("{}", timestamp));
        }
        MetricValue::AggregatedHistogram1(histogram) => {
            line.push_str("metric_type=aggregated_histogram1 ");
            histogram
                .buckets
                .iter()
                .zip(histogram.counts.iter())
                .for_each(|(bucket, count)| {
                    line.push_str(&format!("bucket_{}={}u,", bucket, count));
                });
            line.push_str(&format!("count={}u,", histogram.count));
            line.push_str(&format!("sum={} ", histogram.sum));
            line.push_str(&format!("{}", timestamp));
        }
        MetricValue::AggregatedSummary1(summary) => {
            line.push_str("metric_type=aggregated_summary1 ");
            summary
                .quantiles
                .iter()
                .zip(summary.values.iter())
                .for_each(|(quantile, value)| {
                    line.push_str(&format!("quantile_{}={},", quantile, value));
                });
            line.push_str(&format!("count={}u,", summary.count));
            line.push_str(&format!("sum={} ", summary.sum));
            line.push_str(&format!("{}", timestamp));
        }
        MetricValue::Distribution2(distribution) => {
            line.push_str("metric_type=distribution2 ");
            let statics = distribution_statics(distribution.samples.to_vec()).ok_or_else(|| {
                Error::Common {
                    reason: "distribution statics failed".to_string(),
                }
            })?;
            line.push_str(&format!("min={},", statics.min));
            line.push_str(&format!("max={},", statics.max));
            line.push_str(&format!("median={},", statics.median));
            line.push_str(&format!("avg={},", statics.avg));
            line.push_str(&format!("sum={},", statics.sum));
            line.push_str(&format!("count={}u ", statics.count));
            line.push_str(&format!("{}", timestamp));
        }
        MetricValue::AggregatedHistogram2(histogram) => {
            line.push_str("metric_type=aggregated_histogram2 ");
            histogram.buckets.iter().for_each(|bucket| {
                line.push_str(&format!("bucket_{}={}u,", bucket.upper_limit, bucket.count));
            });
            line.push_str(&format!("count={}u,", histogram.count));
            line.push_str(&format!("sum={} ", histogram.sum));
            line.push_str(&format!("{}", timestamp));
        }
        MetricValue::AggregatedSummary2(summary) => {
            line.push_str("metric_type=aggregated_summary2 ");
            summary.quantiles.iter().for_each(|quantile| {
                line.push_str(&format!(
                    "quantile_{}={},",
                    quantile.quantile, quantile.value
                ));
            });
            line.push_str(&format!("count={}u,", summary.count));
            line.push_str(&format!("sum={} ", summary.sum));
            line.push_str(&format!("{}", timestamp));
        }
        MetricValue::Sketch(sketch) => {
            line.push_str("metric_type=sketch ");
            let sketch = sketch.sketch.ok_or_else(|| Error::Common {
                reason: "missing sketch in metric".to_string(),
            })?;
            match sketch {
                Sketch::AgentDdSketch(ddsketch) => {
                    let config = Config::new(
                        AGENT_DEFAULT_EPS,
                        AGENT_DEFAULT_MIN_VALUE,
                        AGENT_DEFAULT_BIN_LIMIT,
                    );
                    let quantiles = [0.5, 0.75, 0.9, 0.95, 0.99];
                    let quantiles_string = quantiles
                        .iter()
                        .map(|q| {
                            q.to_string()
                                .chars()
                                .take(5)
                                .filter(|c| *c != '.')
                                .collect::<String>()
                        })
                        .collect::<Vec<_>>();
                    let quantiles_values = quantiles
                        .iter()
                        .map(|q| quantile_values(&ddsketch, *q, &config).unwrap_or(0.0))
                        .collect::<Vec<_>>();
                    quantiles_string
                        .iter()
                        .zip(quantiles_values.iter())
                        .for_each(|(quantile, value)| {
                            line.push_str(&format!("{}={},", quantile, value));
                        });
                    line.push_str(&format!("count={}u,", ddsketch.count));
                    line.push_str(&format!("sum={},", ddsketch.sum));
                    line.push_str(&format!("min={},", ddsketch.min));
                    line.push_str(&format!("max={},", ddsketch.max));
                    line.push_str(&format!("avg={} ", ddsketch.avg));
                    line.push_str(&format!("{}", timestamp));
                }
            }
        }
        MetricValue::AggregatedHistogram3(histogram) => {
            line.push_str("metric_type=aggregated_histogram3 ");
            histogram.buckets.iter().for_each(|bucket| {
                line.push_str(&format!("bucket_{}={}u,", bucket.upper_limit, bucket.count));
            });
            line.push_str(&format!("count={}u,", histogram.count));
            line.push_str(&format!("sum={} ", histogram.sum));
            line.push_str(&format!("{}", timestamp));
        }
        MetricValue::AggregatedSummary3(summary) => {
            line.push_str("metric_type=aggregated_summary3 ");
            summary.quantiles.iter().for_each(|quantile| {
                line.push_str(&format!(
                    "quantile_{}={},",
                    quantile.quantile, quantile.value
                ));
            });
            line.push_str(&format!("count={}u,", summary.count));
            line.push_str(&format!("sum={} ", summary.sum));
            line.push_str(&format!("{}", timestamp));
        }
    }
    Ok(line)
}

fn handle_vector_log_trace(mut log: Log) -> server::Result<String> {
    let mut line = String::new();
    let table = log
        .fields
        .remove(TABLE_FIELD)
        .map(vector_value_to_string)
        .unwrap_or(VECTOR_LOG_TABLE.to_string())
        .trim_matches('"')
        .to_string();

    line.push_str(table.as_str());
    line.push(',');

    let host = log
        .fields
        .remove(VECTOR_LOG_HOST_TAG)
        .map(vector_value_to_string)
        .unwrap_or_default();
    let timestamp = log
        .fields
        .remove(VECTOR_LOG_TIMESTAMP)
        .map(vector_value_timestamp)
        .unwrap_or_else(|| now_timestamp_nanos().to_string());

    log.fields.remove(TENANT_FIELD);
    log.fields.remove(DATABASE_FIELD);
    log.fields.remove(USERNAME_FIELD);
    log.fields.remove(PASSWORD_FIELD);
    line.push_str(&format!(
        "{}={},{}={} ",
        VECTOR_LOG_HOST_TAG, host, VECTOR_TYPE_TAG_KEY, VECTOR_LOG_TYPE_TAG_VALUE
    ));
    for (key, value) in log.fields {
        let key = if key.as_str() == INVALID_FIELD_OR_TAG {
            format!("{}_log", key)
        } else {
            key
        };
        line.push_str(&format!("{}={},", key, vector_value_to_string(value)));
    }
    if line.ends_with(',') {
        line.pop();
    }
    line.push(' ');
    line.push_str(&timestamp);

    Ok(line)
}

pub fn quantile_values(ddsketch: &AgentDdSketch, q: f64, config: &Config) -> Option<f64> {
    if ddsketch.count == 0 {
        return None;
    }

    if q <= 0.0 {
        return Some(ddsketch.min);
    }

    if q >= 1.0 {
        return Some(ddsketch.max);
    }

    let mut n = 0.0;
    let mut estimated = None;
    let wanted_rank = rank(ddsketch.count, q);
    let bins = ddsketch
        .k
        .iter()
        .zip(ddsketch.n.iter())
        .map(|(k, n)| Bin { k: *k, n: *n })
        .collect::<Vec<_>>();

    for (i, bin) in bins.iter().enumerate() {
        n += f64::from(bin.n);
        if n <= wanted_rank {
            continue;
        }

        let weight = (n - wanted_rank) / f64::from(bin.n);
        let mut v_low = config.bin_lower_bound(bin.k);
        let mut v_high = v_low * config.gamma_v;

        if i == bins.len() {
            v_high = ddsketch.max;
        } else if i == 0 {
            v_low = ddsketch.min;
        }

        estimated = Some(v_low * weight + v_high * (1.0 - weight));
        break;
    }

    estimated
        .map(|v| v.clamp(ddsketch.min, ddsketch.max))
        .or(Some(f64::NAN))
}

fn rank(count: u32, q: f64) -> f64 {
    round_to_even(q * f64::from(count - 1))
}

#[allow(clippy::cast_possible_truncation)]
#[inline]
fn capped_u64_shift(shift: u64) -> u32 {
    if shift >= 64 {
        u32::MAX
    } else {
        // SAFETY: There's no way that we end up truncating `shift`, since we cap it to 64 above.
        shift as u32
    }
}

// take from vector
fn round_to_even(v: f64) -> f64 {
    // Taken from Go: src/math/floor.go
    //
    // Copyright (c) 2009 The Go Authors. All rights reserved.
    //
    // Redistribution and use in source and binary forms, with or without
    // modification, are permitted provided that the following conditions are
    // met:
    //
    //    * Redistributions of source code must retain the above copyright
    // notice, this list of conditions and the following disclaimer.
    //    * Redistributions in binary form must reproduce the above
    // copyright notice, this list of conditions and the following disclaimer
    // in the documentation and/or other materials provided with the
    // distribution.
    //    * Neither the name of Google Inc. nor the names of its
    // contributors may be used to endorse or promote products derived from
    // this software without specific prior written permission.
    //
    // THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
    // "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
    // LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
    // A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
    // OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
    // SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
    // LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
    // DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
    // THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
    // (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
    // OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

    // HEAR YE: There's a non-zero chance that we could rewrite this function in a way that is far
    // more Rust like, rather than porting over the particulars of how Go works, but we're
    // aiming for compatibility with a Go implementation, and so we've ported this over as
    // faithfully as possible.  With that said, here are the specifics we're dealing with:
    // - in Go, subtraction of unsigned numbers implicitly wraps around i.e. 1u64 - u64::MAX == 2
    // - in Go, right shifts are capped at the bitwidth of the left operand, which means that if
    //   you shift a 64-bit unsigned integers by 65 or above (some_u64 >> 65), instead of being told that
    //   you're doing something wrong, Go just caps the shift amount, and you end up with 0,
    //   whether you shift by 64 or by 9 million
    // - in Rust, it's not possible to directly do `some_u64 >> 64` even, as this would be
    //   considered an overflow
    // - in Rust, there are methods on unsigned integer primitives that allow for safely
    //   shifting by amounts greater than the bitwidth of the primitive type, although they mask off
    //   the bits in the shift amount that are higher than the bitwidth i.e. shift values above 64,
    //   for shifting a u64, are masked such that the resulting shift amount is 0, effectively
    //   not shifting the operand at all
    //
    // With all of this in mind, the code below compensates for that by doing wrapped
    // subtraction and shifting, which is straightforward, but also by utilizing
    // `capped_u64_shift` to approximate the behavior Go has when shifting by amounts larger
    // than the bitwidth of the left operand.
    //
    // I'm really proud of myself for reverse engineering this, but I sincerely hope we can
    // flush it down the toilet in the near future for something vastly simpler.
    const MASK: u64 = 0x7ff;
    const BIAS: u64 = 1023;
    const SHIFT: u64 = 64 - 11 - 1;
    const SIGN_MASK: u64 = 1 << 63;
    const FRAC_MASK: u64 = (1 << SHIFT) - 1;
    #[allow(clippy::unreadable_literal)]
    const UV_ONE: u64 = 0x3FF0000000000000;
    const HALF_MINUS_ULP: u64 = (1 << (SHIFT - 1)) - 1;

    let mut bits = v.to_bits();
    let mut e = (bits >> SHIFT) & MASK;
    if e >= BIAS {
        e = e.wrapping_sub(BIAS);
        let shift_amount = SHIFT.wrapping_sub(e);
        let shifted = bits.wrapping_shr(capped_u64_shift(shift_amount));
        let plus_ulp = HALF_MINUS_ULP + (shifted & 1);
        bits += plus_ulp.wrapping_shr(capped_u64_shift(e));
        bits &= !(FRAC_MASK.wrapping_shr(capped_u64_shift(e)));
    } else if e == BIAS - 1 && bits & FRAC_MASK != 0 {
        // Round 0.5 < abs(x) < 1.
        bits = bits & SIGN_MASK | UV_ONE; // +-1
    } else {
        // Round abs(x) <= 0.5 including denormals.
        bits &= SIGN_MASK; // +-0
    }
    f64::from_bits(bits)
}

// take from vector
/// `bins` is a cumulative histogram
/// We are using R-3 (without choosing the even integer in the case of a tie),
/// it might be preferable to use a more common function, such as R-7.
///
/// List of quantile functions:
/// <https://en.wikipedia.org/wiki/Quantile#Estimating_quantiles_from_a_sample>
fn find_quantile(bins: &[DistributionSample], p: f64) -> f64 {
    let count = bins.last().expect("bins is empty").rate;
    find_sample(bins, (p * count as f64).round() as u32)
}

//take from vector
/// `bins` is a cumulative histogram
/// Return the i-th smallest value,
/// i starts from 1 (i == 1 mean the smallest value).
/// i == 0 is equivalent to i == 1.
fn find_sample(bins: &[DistributionSample], i: u32) -> f64 {
    let index = match bins.binary_search_by_key(&i, |sample| sample.rate) {
        Ok(index) => index,
        Err(index) => index,
    };
    bins[index].value
}

fn distribution_statics(source: Vec<DistributionSample>) -> Option<DistributionStatistic> {
    let mut bins = source
        .iter()
        .filter(|sample| sample.rate > 0)
        .cloned()
        .collect::<Vec<_>>();
    match bins.len() {
        0 => None,
        1 => Some({
            let val = bins[0].value;
            let count = bins[0].rate;
            DistributionStatistic {
                min: val,
                max: val,
                median: val,
                avg: val,
                sum: val * count as f64,
                count: count as u64,
            }
        }),
        _ => Some({
            bins.sort_unstable_by(|a, b| a.value.partial_cmp(&b.value).unwrap_or(Ordering::Equal));

            let min = bins.first().unwrap().value;
            let max = bins.last().unwrap().value;
            let sum = bins
                .iter()
                .map(|sample| sample.value * sample.rate as f64)
                .sum::<f64>();

            for i in 1..bins.len() {
                bins[i].rate += bins[i - 1].rate;
            }

            let count = bins.last().unwrap().rate;
            let avg = sum / count as f64;

            let median = find_quantile(&bins, 0.5);

            DistributionStatistic {
                min,
                max,
                median,
                avg,
                sum,
                count: count as u64,
            }
        }),
    }
}

fn vector_value_to_string(value: Value) -> String {
    match value.kind {
        None => String::new(),
        Some(kind) => match kind {
            Kind::RawBytes(v) => {
                let temp = String::from_utf8(v).unwrap_or_default();
                let mut temp = temp.replace('"', "\\\"");
                temp.push('"');
                temp.insert(0, '"');
                temp
            }
            Kind::Timestamp(v) => convert_timestamp(v).to_string() + "i",
            Kind::Integer(v) => v.to_string() + "i",
            Kind::Float(v) => v.to_string(),
            Kind::Boolean(v) => v.to_string(),
            Kind::Map(map) => {
                let values = map
                    .fields
                    .iter()
                    .map(|(k, v)| {
                        let k = k.replace('"', "\\\"");
                        (k, vector_value_to_string(v.clone()))
                    })
                    .collect::<HashMap<_, _>>();
                format!("\"{:?}\"", values)
            }
            Kind::Array(array) => {
                let values = array
                    .items
                    .iter()
                    .map(|v| vector_value_to_string(v.clone()))
                    .collect::<Vec<_>>();
                format!("\"{:?}\"", values)
            }
            Kind::Null(_) => String::new(),
        },
    }
}

fn vector_value_timestamp(value: Value) -> String {
    match value.kind {
        Some(Kind::Timestamp(v)) => convert_timestamp(v).to_string(),
        Some(Kind::RawBytes(v)) => {
            let s = String::from_utf8(v).unwrap_or_default();
            if let Ok(v) = s.parse::<i64>() {
                v.to_string()
            } else if let Ok(dt) = dateparser::parse(s.as_str()) {
                return dt.timestamp_nanos_opt().unwrap_or_default().to_string();
            } else {
                return now_timestamp_nanos().to_string();
            }
        }
        Some(Kind::Integer(v)) => v.to_string(),
        _ => now_timestamp_nanos().to_string(),
    }
}

fn convert_timestamp(t: Timestamp) -> i64 {
    t.seconds * 1_000_000_000 + t.nanos as i64
}

#[cfg(test)]
mod test {
    use std::collections::HashMap;

    use protos::vector::metric::Value as MetricValue;
    use protos::vector::value::Kind;
    use protos::vector::{
        sketch, AggregatedHistogram1, AggregatedHistogram2, AggregatedHistogram3,
        AggregatedSummary1, AggregatedSummary2, AggregatedSummary3, Counter, Distribution1,
        Distribution2, DistributionSample, Gauge, HistogramBucket, HistogramBucket3, Log, Metric,
        Set, Sketch, SummaryQuantile, Timestamp, Value, ValueArray, ValueMap,
    };

    use crate::vector::vector_server::{
        distribution_statics, handle_vector_log_trace, handle_vector_metric, vector_value_to_string,
    };

    #[test]
    fn test_vector_value_to_string() {
        let value = Value {
            kind: Some(Kind::RawBytes("hello".as_bytes().to_vec())),
        };
        assert_eq!(vector_value_to_string(value), "\"hello\"");
    }

    #[test]
    fn test_vector_value_to_string_timestamp() {
        let value = Value {
            kind: Some(Kind::Timestamp(Timestamp {
                seconds: 1619712000,
                nanos: 0,
            })),
        };
        assert_eq!(vector_value_to_string(value), "1619712000000000000i");
    }

    #[test]
    fn test_vector_value_to_string_integer() {
        let value = Value {
            kind: Some(Kind::Integer(123)),
        };
        assert_eq!(vector_value_to_string(value), "123i");
    }

    #[test]
    fn test_vector_value_to_string_float() {
        let value = Value {
            kind: Some(Kind::Float(123.0)),
        };
        assert_eq!(vector_value_to_string(value), "123");
    }

    #[test]
    fn test_vector_value_to_string_boolean() {
        let value = Value {
            kind: Some(Kind::Boolean(true)),
        };
        assert_eq!(vector_value_to_string(value), "true");
    }

    #[test]
    fn test_vector_value_to_string_map() {
        let value = Value {
            kind: Some(Kind::Map(ValueMap {
                fields: HashMap::from([(
                    "key1".to_string(),
                    Value {
                        kind: Some(Kind::RawBytes("value1".as_bytes().to_vec())),
                    },
                )]),
            })),
        };
        assert_eq!(
            vector_value_to_string(value),
            "\"{\"key1\": \"\\\"value1\\\"\"}\""
        );
    }

    #[test]
    fn test_vector_value_to_string_array() {
        let value = Value {
            kind: Some(Kind::Array(ValueArray {
                items: vec![
                    Value {
                        kind: Some(Kind::RawBytes("value1".as_bytes().to_vec())),
                    },
                    Value {
                        kind: Some(Kind::RawBytes("value2".as_bytes().to_vec())),
                    },
                ],
            })),
        };
        assert_eq!(
            vector_value_to_string(value),
            "\"[\"\\\"value1\\\"\", \"\\\"value2\\\"\"]\""
        );
    }

    #[test]
    fn test_distribution_statics() {
        let source = vec![
            DistributionSample {
                value: 1.0,
                rate: 1,
            },
            DistributionSample {
                value: 2.0,
                rate: 2,
            },
            DistributionSample {
                value: 3.0,
                rate: 3,
            },
            DistributionSample {
                value: 4.0,
                rate: 4,
            },
            DistributionSample {
                value: 5.0,
                rate: 5,
            },
        ];
        let result = distribution_statics(source).unwrap();
        assert_eq!(result.min, 1.0);
        assert_eq!(result.max, 5.0);
        assert_eq!(result.median, 4.0);
        assert_eq!(result.avg, 3.6666666666666665);
        assert_eq!(result.sum, 55.0);
        assert_eq!(result.count, 15);
    }

    #[test]
    fn test_handle_log_event() {
        let log_event = Log {
            fields: HashMap::from([
                (
                    "timestamp".to_string(),
                    Value {
                        kind: Some(Kind::Timestamp(Timestamp {
                            seconds: 1619712000,
                            nanos: 0,
                        })),
                    },
                ),
                (
                    "message".to_string(),
                    Value {
                        kind: Some(Kind::RawBytes("hello".as_bytes().to_vec())),
                    },
                ),
                (
                    "host".to_string(),
                    Value {
                        kind: Some(Kind::RawBytes("localhost".as_bytes().to_vec())),
                    },
                ),
            ]),
            value: None,
            metadata: None,
        };
        let result = handle_vector_log_trace(log_event).unwrap();
        let s =
            r#"__vector_log,host="localhost",metric_type=logs message="hello" 1619712000000000000"#
                .to_string();
        assert_eq!(result, s);
    }

    #[test]
    fn test_handle_metric_event_counter() {
        let metric_event = Metric {
            name: "test".to_string(),
            timestamp: Some(Timestamp {
                seconds: 1619712000,
                nanos: 0,
            }),
            tags_v1: HashMap::from([
                ("host".to_string(), "localhost".to_string()),
                ("service".to_string(), "test".to_string()),
            ]),
            tags_v2: Default::default(),
            kind: 0,
            namespace: "test".to_string(),
            value: Some(MetricValue::Counter(Counter { value: 1.0 })),
            metadata: None,
            interval_ms: 0,
        };
        let result = handle_vector_metric(metric_event).unwrap();
        let s1 = r#"test.test,service=test,host=localhost,metric_type=counter value=1 1619712000000000000"#.to_string();
        let s2 = r#"test.test,host=localhost,service=test,metric_type=counter value=1 1619712000000000000"#.to_string();
        assert!((result == s1 || result == s2));
    }

    #[test]
    fn test_handle_metric_event_gauge() {
        let metric_event = Metric {
            name: "test".to_string(),
            timestamp: Some(Timestamp {
                seconds: 1619712000,
                nanos: 0,
            }),
            tags_v1: HashMap::from([
                ("host".to_string(), "localhost".to_string()),
                ("service".to_string(), "test".to_string()),
            ]),
            tags_v2: Default::default(),
            kind: 0,
            namespace: "test".to_string(),
            value: Some(MetricValue::Gauge(Gauge { value: 1.0 })),
            metadata: None,
            interval_ms: 0,
        };
        let result = handle_vector_metric(metric_event).unwrap();
        let s1 = r#"test.test,service=test,host=localhost,metric_type=gauge value=1 1619712000000000000"#.to_string();
        let s2 = r#"test.test,host=localhost,service=test,metric_type=gauge value=1 1619712000000000000"#.to_string();
        assert!((result == s1 || result == s2));
    }

    #[test]
    fn test_handle_metric_event_histogram1() {
        let metric_event = Metric {
            name: "test".to_string(),
            timestamp: Some(Timestamp {
                seconds: 1619712000,
                nanos: 0,
            }),
            tags_v1: HashMap::from([("host".to_string(), "localhost".to_string())]),
            tags_v2: Default::default(),
            kind: 0,
            namespace: "test".to_string(),
            value: Some(MetricValue::AggregatedHistogram1(AggregatedHistogram1 {
                buckets: vec![1.0, 2.0, 3.0, 4.0, 5.0],
                count: 1,
                sum: 1.0,
                counts: vec![1, 1, 1, 1, 1],
            })),
            metadata: None,
            interval_ms: 0,
        };
        let result = handle_vector_metric(metric_event).unwrap();
        assert_eq!(result, r#"test.test,host=localhost,metric_type=aggregated_histogram1 bucket_1=1u,bucket_2=1u,bucket_3=1u,bucket_4=1u,bucket_5=1u,count=1u,sum=1 1619712000000000000"#.to_string());
    }

    #[test]
    fn test_handle_metric_event_histogram2() {
        let metric_event = Metric {
            name: "test".to_string(),
            timestamp: Some(Timestamp {
                seconds: 1619712000,
                nanos: 0,
            }),
            tags_v1: HashMap::from([("host".to_string(), "localhost".to_string())]),
            tags_v2: Default::default(),
            kind: 0,
            namespace: "test".to_string(),
            value: Some(MetricValue::AggregatedHistogram2(AggregatedHistogram2 {
                buckets: vec![
                    HistogramBucket {
                        upper_limit: 1.0,
                        count: 1,
                    },
                    HistogramBucket {
                        upper_limit: 2.0,
                        count: 1,
                    },
                    HistogramBucket {
                        upper_limit: 3.0,
                        count: 1,
                    },
                    HistogramBucket {
                        upper_limit: 4.0,
                        count: 1,
                    },
                    HistogramBucket {
                        upper_limit: 5.0,
                        count: 1,
                    },
                ],
                count: 1,
                sum: 1.0,
            })),
            metadata: None,
            interval_ms: 0,
        };
        let result = handle_vector_metric(metric_event).unwrap();
        assert_eq!(result, r#"test.test,host=localhost,metric_type=aggregated_histogram2 bucket_1=1u,bucket_2=1u,bucket_3=1u,bucket_4=1u,bucket_5=1u,count=1u,sum=1 1619712000000000000"#.to_string());
    }

    #[test]
    fn test_handle_metric_event_histogram3() {
        let metric_event = Metric {
            name: "test".to_string(),
            timestamp: Some(Timestamp {
                seconds: 1619712000,
                nanos: 0,
            }),
            tags_v1: HashMap::from([("host".to_string(), "localhost".to_string())]),
            tags_v2: Default::default(),
            kind: 0,
            namespace: "test".to_string(),
            value: Some(MetricValue::AggregatedHistogram3(AggregatedHistogram3 {
                buckets: vec![
                    HistogramBucket3 {
                        upper_limit: 1.0,
                        count: 1,
                    },
                    HistogramBucket3 {
                        upper_limit: 2.0,
                        count: 1,
                    },
                    HistogramBucket3 {
                        upper_limit: 3.0,
                        count: 1,
                    },
                    HistogramBucket3 {
                        upper_limit: 4.0,
                        count: 1,
                    },
                    HistogramBucket3 {
                        upper_limit: 5.0,
                        count: 1,
                    },
                ],
                count: 1,
                sum: 1.0,
            })),
            metadata: None,
            interval_ms: 0,
        };
        let result = handle_vector_metric(metric_event).unwrap();
        assert_eq!(result, r#"test.test,host=localhost,metric_type=aggregated_histogram3 bucket_1=1u,bucket_2=1u,bucket_3=1u,bucket_4=1u,bucket_5=1u,count=1u,sum=1 1619712000000000000"#.to_string());
    }

    #[test]
    fn test_handle_metric_event_set() {
        let metric_event = Metric {
            name: "test".to_string(),
            timestamp: Some(Timestamp {
                seconds: 1619712000,
                nanos: 0,
            }),
            tags_v1: HashMap::from([("host".to_string(), "localhost".to_string())]),
            tags_v2: Default::default(),
            kind: 0,
            namespace: "test".to_string(),
            value: Some(MetricValue::Set(Set {
                values: vec!["test".to_string(), "test2".to_string()],
            })),
            metadata: None,
            interval_ms: 0,
        };
        let result = handle_vector_metric(metric_event).unwrap();
        assert_eq!(
            result,
            r#"test.test,host=localhost,metric_type=set value=2u 1619712000000000000"#.to_string()
        );
    }

    #[test]
    fn test_handle_metric_event_summary1() {
        let metric_event = Metric {
            name: "test".to_string(),
            timestamp: Some(Timestamp {
                seconds: 1619712000,
                nanos: 0,
            }),
            tags_v1: HashMap::from([("host".to_string(), "localhost".to_string())]),
            tags_v2: Default::default(),
            kind: 0,
            namespace: "test".to_string(),
            value: Some(MetricValue::AggregatedSummary1(AggregatedSummary1 {
                quantiles: vec![0.5, 0.9, 0.99],
                values: vec![1.0, 2.0, 3.0],
                count: 1,
                sum: 1.0,
            })),
            metadata: None,
            interval_ms: 0,
        };
        let result = handle_vector_metric(metric_event).unwrap();
        assert_eq!(result, r#"test.test,host=localhost,metric_type=aggregated_summary1 quantile_0.5=1,quantile_0.9=2,quantile_0.99=3,count=1u,sum=1 1619712000000000000"#.to_string());
    }

    #[test]
    fn test_handle_metric_event_summary2() {
        let metric_event = Metric {
            name: "test".to_string(),
            timestamp: Some(Timestamp {
                seconds: 1619712000,
                nanos: 0,
            }),
            tags_v1: HashMap::from([("host".to_string(), "localhost".to_string())]),
            tags_v2: Default::default(),
            kind: 0,
            namespace: "test".to_string(),
            value: Some(MetricValue::AggregatedSummary2(AggregatedSummary2 {
                quantiles: vec![
                    SummaryQuantile {
                        quantile: 0.5,
                        value: 1.0,
                    },
                    SummaryQuantile {
                        quantile: 0.9,
                        value: 2.0,
                    },
                    SummaryQuantile {
                        quantile: 0.99,
                        value: 3.0,
                    },
                ],
                count: 1,
                sum: 1.0,
            })),
            metadata: None,
            interval_ms: 0,
        };
        let result = handle_vector_metric(metric_event).unwrap();
        assert_eq!(result, r#"test.test,host=localhost,metric_type=aggregated_summary2 quantile_0.5=1,quantile_0.9=2,quantile_0.99=3,count=1u,sum=1 1619712000000000000"#.to_string());
    }

    #[test]
    fn test_handle_metric_event_summary3() {
        let metric_event = Metric {
            name: "test".to_string(),
            timestamp: Some(Timestamp {
                seconds: 1619712000,
                nanos: 0,
            }),
            tags_v1: HashMap::from([("host".to_string(), "localhost".to_string())]),
            tags_v2: Default::default(),
            kind: 0,
            namespace: "test".to_string(),
            value: Some(MetricValue::AggregatedSummary3(AggregatedSummary3 {
                quantiles: vec![
                    SummaryQuantile {
                        quantile: 0.5,
                        value: 1.0,
                    },
                    SummaryQuantile {
                        quantile: 0.9,
                        value: 2.0,
                    },
                    SummaryQuantile {
                        quantile: 0.99,
                        value: 3.0,
                    },
                ],
                count: 1,
                sum: 1.0,
            })),
            metadata: None,
            interval_ms: 0,
        };
        let result = handle_vector_metric(metric_event).unwrap();
        assert_eq!(result, r#"test.test,host=localhost,metric_type=aggregated_summary3 quantile_0.5=1,quantile_0.9=2,quantile_0.99=3,count=1u,sum=1 1619712000000000000"#.to_string());
    }

    #[test]
    fn test_handle_metric_event_distribution1() {
        let metric_event = Metric {
            name: "test".to_string(),
            timestamp: Some(Timestamp {
                seconds: 1619712000,
                nanos: 0,
            }),
            tags_v1: HashMap::from([("host".to_string(), "localhost".to_string())]),
            tags_v2: Default::default(),
            kind: 0,
            namespace: "test".to_string(),
            value: Some(MetricValue::Distribution1(Distribution1 {
                values: vec![1.0, 2.0, 3.0],
                sample_rates: vec![1, 1, 1],
                statistic: 0,
            })),
            metadata: None,
            interval_ms: 0,
        };
        let result = handle_vector_metric(metric_event).unwrap();
        assert_eq!(result, r#"test.test,host=localhost,metric_type=distribution1 min=1,max=3,median=2,avg=2,sum=6,count=3u 1619712000000000000"#.to_string());
    }

    #[test]
    fn test_handle_metric_event_distribution2() {
        let metric_event = Metric {
            name: "test".to_string(),
            timestamp: Some(Timestamp {
                seconds: 1619712000,
                nanos: 0,
            }),
            tags_v1: HashMap::from([("host".to_string(), "localhost".to_string())]),
            tags_v2: Default::default(),
            kind: 0,
            namespace: "test".to_string(),
            value: Some(MetricValue::Distribution2(Distribution2 {
                statistic: 0,
                samples: vec![
                    DistributionSample {
                        value: 1.0,
                        rate: 1,
                    },
                    DistributionSample {
                        value: 2.0,
                        rate: 1,
                    },
                    DistributionSample {
                        value: 3.0,
                        rate: 1,
                    },
                ],
            })),
            metadata: None,
            interval_ms: 0,
        };
        let result = handle_vector_metric(metric_event).unwrap();
        assert_eq!(result, r#"test.test,host=localhost,metric_type=distribution2 min=1,max=3,median=2,avg=2,sum=6,count=3u 1619712000000000000"#.to_string());
    }

    #[test]
    fn test_handle_metric_event_sketch() {
        let metric_event = Metric {
            name: "test".to_string(),
            timestamp: Some(Timestamp {
                seconds: 1619712000,
                nanos: 0,
            }),
            tags_v1: HashMap::from([("host".to_string(), "localhost".to_string())]),
            tags_v2: Default::default(),
            kind: 0,
            namespace: "test".to_string(),
            value: Some(MetricValue::Sketch(Sketch {
                sketch: Some(sketch::Sketch::AgentDdSketch(sketch::AgentDdSketch {
                    min: -10000.0,
                    max: 3000000.0,
                    avg: 2.0,
                    sum: 6.0,
                    count: 3,
                    k: vec![-1, 2, -100],
                    n: vec![100, 150, 300],
                })),
            })),
            metadata: None,
            interval_ms: 0,
        };
        let result = handle_vector_metric(metric_event).unwrap();
        assert_eq!(result, r#"test.test,host=localhost,metric_type=sketch 05=-9900.000000000011,075=-9800.00000000002,09=-9800.00000000002,095=-9800.00000000002,099=-9800.00000000002,count=3u,sum=6,min=-10000,max=3000000,avg=2 1619712000000000000"#.to_string());
    }
}
