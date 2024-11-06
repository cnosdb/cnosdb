use models::column_data::{ColumnData, PrimaryColumnData};
use models::field_value::FieldVal;
use models::schema::tskv_table_schema::TableColumn;
use utils::bitset::BitSet;

use crate::{TskvError, TskvResult};

#[derive(Debug, Clone, PartialEq)]
pub struct MutableColumn {
    column_desc: TableColumn,
    column_data: ColumnData,
}

impl MutableColumn {
    pub fn empty(column_desc: TableColumn) -> TskvResult<MutableColumn> {
        let column_type = column_desc.column_type.to_physical_data_type();
        let data =
            ColumnData::new(column_type).map_err(|e| TskvError::ColumnDataError { source: e })?;
        let column = Self {
            column_desc,
            column_data: data,
        };
        Ok(column)
    }

    pub fn empty_with_cap(column_desc: TableColumn, cap: usize) -> TskvResult<MutableColumn> {
        let column_type = column_desc.column_type.to_physical_data_type();
        let data = ColumnData::with_cap(column_type, cap)
            .map_err(|e| TskvError::ColumnDataError { source: e })?;
        let column = Self {
            column_desc,
            column_data: data,
        };
        Ok(column)
    }

    pub fn chunk(&self, start: usize, end: usize) -> TskvResult<MutableColumn> {
        let column = self
            .column_data
            .chunk(start, end)
            .map_err(|e| TskvError::ColumnDataError { source: e })?;
        Ok(MutableColumn {
            column_desc: self.column_desc.clone(),
            column_data: column,
        })
    }

    pub fn valid(&self) -> &BitSet {
        &self.column_data.valid
    }

    pub fn mut_valid(&mut self) -> &mut BitSet {
        &mut self.column_data.valid
    }

    pub fn data(&self) -> &PrimaryColumnData {
        &self.column_data.primary_data
    }

    pub fn into_data(self) -> ColumnData {
        self.column_data
    }

    pub fn column_desc(&self) -> &TableColumn {
        &self.column_desc
    }

    pub fn push(&mut self, value: Option<FieldVal>) -> TskvResult<()> {
        self.column_data
            .push(value)
            .map_err(|e| TskvError::ColumnDataError { source: e })
    }
}
