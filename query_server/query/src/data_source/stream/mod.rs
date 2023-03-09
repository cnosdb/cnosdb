use std::collections::HashMap;
use std::time::Duration;

use spi::QueryError;
use utils::duration::parse_duration;

pub mod tskv;

// Table option keys
const EVENT_TIME_COLUMN_OPTION: &str = "event_time_column";
const WATERMARK_DELAY_OPTION: &str = "watermark_delay";

pub fn get_event_time_column<'a>(
    table: &'a str,
    options: &'a HashMap<String, String>,
) -> Result<&'a str, QueryError> {
    options
        .get(EVENT_TIME_COLUMN_OPTION)
        .ok_or_else(|| QueryError::MissingTableOptions {
            option_name: EVENT_TIME_COLUMN_OPTION.into(),
            table_name: table.into(),
        })
        .map(|e| e.as_ref())
}

pub fn get_watermark_delay<'a>(
    table: &'a str,
    options: &'a HashMap<String, String>,
) -> Result<Option<Duration>, QueryError> {
    options
        .get(WATERMARK_DELAY_OPTION)
        .map(|e| {
            parse_duration(e).map_err(|err| QueryError::InvalidTableOption {
                option_name: WATERMARK_DELAY_OPTION.into(),
                table_name: table.into(),
                reason: err.to_string(),
            })
        })
        .transpose()
}
