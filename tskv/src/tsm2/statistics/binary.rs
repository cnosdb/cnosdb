use std::sync::Arc;

use models::PhysicalDType;

use super::Statistics;
use crate::error::Result;
use crate::tsm2::page::PageStatistics;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BinaryStatistics {
    pub null_count: Option<i64>,
    pub distinct_count: Option<i64>,
    pub max_value: Option<Vec<u8>>,
    pub min_value: Option<Vec<u8>>,
}

impl Statistics for BinaryStatistics {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn physical_type(&self) -> &PhysicalDType {
        &PhysicalDType::String
    }

    fn null_count(&self) -> Option<i64> {
        self.null_count
    }
}

pub fn read(v: &PageStatistics) -> Result<Arc<dyn Statistics>> {
    Ok(Arc::new(BinaryStatistics {
        null_count: v.null_count,
        distinct_count: v.distinct_count,
        max_value: v.max_value.clone(),
        min_value: v.min_value.clone(),
    }))
}

pub fn write(v: &BinaryStatistics) -> PageStatistics {
    PageStatistics {
        primitive_type: *v.physical_type(),
        null_count: v.null_count,
        distinct_count: v.distinct_count,
        max_value: v.max_value.clone(),
        min_value: v.min_value.clone(),
    }
}
