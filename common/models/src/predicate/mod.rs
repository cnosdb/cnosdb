use std::sync::Arc;

use datafusion::physical_expr::PhysicalExpr;
use datafusion_proto::protobuf::PhysicalExprNode;
use serde::{Deserialize, Serialize};

use self::domain::{ColumnDomains, PredicateRef, TimeRange, TimeRanges};
use crate::meta_data::{ReplicationSet, ReplicationSetId, VnodeInfo};
use crate::predicate::domain::{ResolvedPredicate, ResolvedPredicateRef};
use crate::schema::tskv_table_schema::{ColumnType, TskvTableSchemaRef};
use crate::ModelResult;

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
        filter: Option<Arc<dyn PhysicalExpr>>,
    ) -> ModelResult<Self> {
        let domains_filter = predicate
            .filter()
            .translate_column(|c| table.column(&c.name).cloned());

        let tags_filter = domains_filter.translate_column(|e| match e.column_type {
            ColumnType::Tag => Some(e.name.clone()),
            _ => None,
        });

        let limit = predicate.limit();

        let predicate = Arc::new(ResolvedPredicate::new(
            Arc::new(TimeRanges::new(time_ranges)),
            tags_filter,
            filter,
        )?);

        Ok(Self {
            id,
            predicate,
            limit,
        })
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

    pub fn filter(&self) -> &PhysicalExprNode {
        self.predicate.filter()
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

    repl_set: ReplicationSet,
}

impl PlacedSplit {
    pub fn new(
        id: usize,
        predicate: ResolvedPredicateRef,
        limit: Option<usize>,
        repl_set: ReplicationSet,
    ) -> Self {
        let split = Split {
            id,
            predicate,
            limit,
        };

        Self { split, repl_set }
    }

    pub fn from_split(split: Split, repl_set: ReplicationSet) -> Self {
        Self { split, repl_set }
    }

    pub fn id(&self) -> usize {
        self.split.id
    }

    pub fn time_ranges(&self) -> Arc<TimeRanges> {
        self.split.time_ranges()
    }

    pub fn tags_filter(&self) -> &ColumnDomains<String> {
        self.split.tags_filter()
    }

    pub fn filter(&self) -> &PhysicalExprNode {
        self.split.filter()
    }

    pub fn limit(&self) -> Option<usize> {
        self.split.limit
    }

    pub fn pop_front(&mut self) -> Option<VnodeInfo> {
        if self.repl_set.vnodes.is_empty() {
            None
        } else {
            Some(self.repl_set.vnodes.remove(0))
        }
    }

    pub fn replica_id(&self) -> ReplicationSetId {
        self.repl_set.id
    }
}
