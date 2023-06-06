use std::sync::Arc;

use serde::{Deserialize, Serialize};

use self::domain::{
    ColumnDomains, PredicateRef, ResolvedPredicate, ResolvedPredicateRef, TimeRange, TimeRanges,
};
use crate::meta_data::VnodeInfo;
use crate::schema::{ColumnType, TskvTableSchemaRef};

pub mod domain;
pub mod transformation;
pub mod utils;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Split {
    // partition id
    id: usize,
    predicate: ResolvedPredicateRef,
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

        let predicate = Arc::new(ResolvedPredicate::new(
            Arc::new(TimeRanges::new(time_ranges)),
            tags_filter,
            fields_filter,
        ));

        Self {
            id,
            predicate,
            limit,
        }
    }

    pub fn id(&self) -> usize {
        self.id
    }

    pub fn time_ranges(&self) -> Arc<TimeRanges> {
        self.predicate.time_ranges()
    }

    pub fn tags_filter(&self) -> &ColumnDomains<String> {
        self.predicate.tags_filter()
    }

    pub fn fields_filter(&self) -> &ColumnDomains<String> {
        self.predicate.fields_filter()
    }

    pub fn limit(&self) -> Option<usize> {
        self.limit
    }
}

impl From<PlacedSplit> for Split {
    fn from(v: PlacedSplit) -> Self {
        v.split
    }
}

/// The split of the allocated shard
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlacedSplit {
    split: Split,

    vnode: VnodeInfo,
}

impl PlacedSplit {
    pub fn new(
        id: usize,
        predicate: ResolvedPredicateRef,
        limit: Option<usize>,
        vnode: VnodeInfo,
    ) -> Self {
        let split = Split {
            id,
            predicate,
            limit,
        };

        Self { split, vnode }
    }

    pub fn from_split(split: Split, vnode: VnodeInfo) -> Self {
        Self { split, vnode }
    }

    pub fn id(&self) -> usize {
        self.split.id
    }

    pub fn time_ranges(&self) -> Arc<TimeRanges> {
        self.split.predicate.time_ranges()
    }

    pub fn tags_filter(&self) -> &ColumnDomains<String> {
        self.split.predicate.tags_filter()
    }

    pub fn fields_filter(&self) -> &ColumnDomains<String> {
        self.split.predicate.fields_filter()
    }

    pub fn limit(&self) -> Option<usize> {
        self.split.limit
    }

    pub fn vnode(&self) -> &VnodeInfo {
        &self.vnode
    }
}
