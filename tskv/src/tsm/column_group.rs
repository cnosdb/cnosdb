use models::predicate::domain::TimeRange;
use serde::{Deserialize, Serialize};
use snafu::OptionExt;

use crate::error::TsmColumnGroupSnafu;
use crate::tsm::page::PageWriteSpec;
use crate::tsm::ColumnGroupID;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ColumnGroup {
    column_group_id: ColumnGroupID,

    pages_offset: u64,
    size: u64,
    time_range: TimeRange,
    pages: Vec<PageWriteSpec>,
}

impl ColumnGroup {
    pub fn new(id: u64) -> Self {
        Self {
            column_group_id: id,
            pages_offset: 0,
            size: 0,
            time_range: TimeRange::none(),
            pages: Vec::new(),
        }
    }

    pub fn column_group_id(&self) -> ColumnGroupID {
        self.column_group_id
    }

    pub fn time_range_merge(&mut self, time_range: &TimeRange) {
        self.time_range.merge(time_range)
    }

    pub fn pages_offset(&self) -> u64 {
        self.pages_offset
    }

    pub fn size(&self) -> u64 {
        self.size
    }

    pub fn time_range(&self) -> &TimeRange {
        &self.time_range
    }

    pub fn pages(&self) -> &[PageWriteSpec] {
        &self.pages
    }

    pub fn push(&mut self, page: PageWriteSpec) {
        if self.pages_offset == 0 {
            self.pages_offset = page.offset;
        }
        if self.size != 0 {
            debug_assert_eq!(self.pages_offset + self.size, page.offset);
        }
        self.size += page.size;
        self.pages.push(page);
    }

    pub fn row_len(&self) -> usize {
        self.pages
            .first()
            .map(|p| p.meta.num_values as usize)
            .unwrap_or(0)
    }

    pub fn time_page_write_spec(&self) -> crate::TskvResult<PageWriteSpec> {
        self.pages
            .iter()
            .find(|p| p.meta.column.column_type.is_time())
            .cloned()
            .context(TsmColumnGroupSnafu {
                reason: format!("column group: {} not found time page", self.column_group_id),
            })
    }
}
