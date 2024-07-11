use std::borrow::Cow;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;

use coordinator::Coordinator;
use datafusion::arrow::array::{Array, StringArray, TimestampNanosecondArray};
use datafusion::arrow::record_batch::RecordBatch;
use futures::TryStreamExt;
use models::arrow::TimeUnit;
use models::predicate::domain::Predicate;
use models::schema::tskv_table_schema::{ColumnType, TableColumn, TskvTableSchema};
use models::schema::{CLUSTER_SCHEMA, DEFAULT_CATALOG};
use models::utils::now_timestamp_nanos;
use models::ValueType;
use protocol_parser::Line;
use protos::FieldValue;
use snafu::ResultExt;
use spi::query::session::SessionCtx;
use spi::service::protocol::QueryId;
use spi::{CoordinatorSnafu, ModelsSnafu, PersistQuerySnafu, QueryError};
use trace::debug;
use tskv::reader::QueryOption;
use utils::precision::Precision;

use crate::data_source::split::tskv::TableLayoutHandle;
use crate::data_source::split::SplitManager;

pub type WatermarkTrackerRef = Arc<WatermarkTracker>;

const PERSISTER_SQL_TABLE: &str = "sql_watermark_persister";

/// Real-time tracking of watermark during query running, which can be recovered after system restart.
/// Information is logged to table.
#[derive(Default, Debug)]
pub struct WatermarkTracker {
    global_watermark_ns: AtomicI64,
    query_id: QueryId,
    timestamp: i64,
}

impl WatermarkTracker {
    pub async fn try_new(
        query_id: QueryId,
        coord: Arc<dyn Coordinator>,
        session: SessionCtx,
        is_old: bool,
    ) -> Result<Self, QueryError> {
        let mut watermark_ns = i64::MIN;
        let mut timestamp = now_timestamp_nanos();

        if is_old {
            let table_columns = vec![
                TableColumn::new_time_column(0, TimeUnit::Nanosecond),
                TableColumn::new(
                    1,
                    "query_id".to_string(),
                    ColumnType::Field(ValueType::String),
                    Default::default(),
                ),
                TableColumn::new(
                    2,
                    "watermark".to_string(),
                    ColumnType::Field(ValueType::String),
                    Default::default(),
                ),
            ];
            let tskv_table_schema = Arc::new(TskvTableSchema::new(
                DEFAULT_CATALOG.to_string(),
                CLUSTER_SCHEMA.to_string(),
                PERSISTER_SQL_TABLE.to_string(),
                table_columns,
            ));
            let schema = tskv_table_schema.to_arrow_schema();
            let table_layout = TableLayoutHandle {
                table: tskv_table_schema.clone(),
                predicate: Arc::new(
                    Predicate::push_down_filter(
                        None,
                        &*tskv_table_schema.to_df_schema()?,
                        &schema,
                        None,
                    )
                    .context(ModelsSnafu)?,
                ),
            };
            let split_manager = SplitManager::new(coord.clone());
            let splits = split_manager.splits(session.inner(), table_layout).await?;

            let mut record_batch_vec: Vec<RecordBatch> = Vec::new();
            for split in splits {
                let query_opt = QueryOption::new(
                    100_usize,
                    split.clone(),
                    None,
                    schema.clone(),
                    tskv_table_schema.clone(),
                    tskv_table_schema.meta(),
                );
                let mut iter = coord
                    .table_scan(query_opt, session.get_span_ctx())
                    .context(CoordinatorSnafu)?;
                while let Some(record_batch) = iter
                    .try_next()
                    .await
                    .map_err(|source| QueryError::Coordinator { source })?
                {
                    record_batch_vec.push(record_batch);
                }
            }

            'outer: for batch in record_batch_vec {
                let column = batch
                    .column(1)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or(
                        PersistQuerySnafu {
                            reason: "column 1 is not StringArray".to_string(),
                        }
                        .build(),
                    )?;
                for i in 0..column.len() {
                    if query_id.get().to_string().eq(column.value(i)) {
                        let column = batch
                            .column(0)
                            .as_any()
                            .downcast_ref::<TimestampNanosecondArray>()
                            .ok_or(
                                PersistQuerySnafu {
                                    reason: "column 0 is not TimestampNanosecondArray".to_string(),
                                }
                                .build(),
                            )?;
                        timestamp = column.value(i);
                        let column = batch
                            .column(2)
                            .as_any()
                            .downcast_ref::<StringArray>()
                            .ok_or(
                                PersistQuerySnafu {
                                    reason: "column 2 is not StringArray".to_string(),
                                }
                                .build(),
                            )?;
                        watermark_ns = column.value(i).parse().expect("Not a valid number");
                        break 'outer;
                    }
                }
            }
        }

        debug!(
            "timestamp: {}, query_id: {}, watermark_ns: {}",
            timestamp, query_id, watermark_ns
        );
        Ok(Self {
            global_watermark_ns: AtomicI64::new(watermark_ns),
            query_id,
            timestamp,
        })
    }

    pub fn current_watermark_ns(&self) -> i64 {
        self.global_watermark_ns.load(Ordering::Relaxed)
    }

    pub fn update_watermark(&self, event_time: i64, _delay: i64) {
        // TODO _delay needs to be processed after kv supports offset
        // self.global_watermark_ns
        //     .store(event_time - delay, Ordering::Relaxed);
        self.global_watermark_ns
            .store(event_time, Ordering::Relaxed);
    }

    /// Persist watermark to table.
    /// The purpose is to not output duplicate data after the system restarts.
    pub async fn commit(
        &self,
        _batch_id: i64,
        coord: Arc<dyn Coordinator>,
    ) -> Result<(), QueryError> {
        let line = Line {
            hash_id: 0,
            table: Cow::Owned(PERSISTER_SQL_TABLE.to_string()),
            tags: vec![],
            fields: vec![
                (
                    Cow::Owned("query_id".to_string()),
                    FieldValue::Str(self.query_id.to_string().as_bytes().to_owned()),
                ),
                (
                    Cow::Owned("watermark".to_string()),
                    FieldValue::Str(
                        self.global_watermark_ns
                            .load(Ordering::Relaxed)
                            .to_string()
                            .as_bytes()
                            .to_owned(),
                    ),
                ),
            ],
            timestamp: self.timestamp,
        };

        coord
            .write_lines(
                DEFAULT_CATALOG,
                CLUSTER_SCHEMA,
                Precision::NS,
                vec![line],
                None,
            )
            .await
            .map_err(|source| QueryError::Coordinator { source })?;

        debug!(
            "timestamp: {}, query_id: {}, watermark_ns: {}",
            self.timestamp,
            self.query_id,
            self.global_watermark_ns.load(Ordering::Relaxed)
        );

        Ok(())
    }
}
