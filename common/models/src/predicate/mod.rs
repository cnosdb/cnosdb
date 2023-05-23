use std::sync::Arc;

use serde::{Deserialize, Serialize};

use self::domain::{ColumnDomains, PredicateRef, TimeRange, TimeRanges};
use crate::schema::{ColumnType, TskvTableSchemaRef};

pub mod domain;
pub mod transformation;
pub mod utils;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Split {
    // partition id
    id: usize,
    time_ranges: Arc<TimeRanges>,
    tags_filter: ColumnDomains<String>,
    fields_filter: ColumnDomains<String>,
    limit: Option<usize>,
}

impl Split {
    pub fn new(
        id: usize,
        table: TskvTableSchemaRef,
        time_ranges: Vec<TimeRange>,
        predicate: PredicateRef,
    ) -> Self {
        let domains_filter = predicate
            .filter()
            .translate_column(|c| table.column(&c.name).cloned());

        let tags_filter = domains_filter.translate_column(|e| match e.column_type {
            ColumnType::Tag => Some(e.name.clone()),
            _ => None,
        });

        let fields_filter = domains_filter.translate_column(|e| match e.column_type {
            ColumnType::Field(_) => Some(e.name.clone()),
            _ => None,
        });

        let limit = predicate.limit();

        Self {
            id,
            time_ranges: Arc::new(TimeRanges::new(time_ranges)),
            tags_filter,
            fields_filter,
            limit,
        }
    }

    pub fn id(&self) -> usize {
        self.id
    }

    pub fn time_ranges(&self) -> Arc<TimeRanges> {
        self.time_ranges.clone()
    }

    pub fn tags_filter(&self) -> &ColumnDomains<String> {
        &self.tags_filter
    }

    pub fn fields_filter(&self) -> &ColumnDomains<String> {
        &self.fields_filter
    }

    pub fn limit(&self) -> Option<usize> {
        self.limit
    }
}
