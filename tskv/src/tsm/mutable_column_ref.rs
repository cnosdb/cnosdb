use models::column_data_ref::ColumnDataRef;
use models::field_value::FieldVal;
use models::schema::tskv_table_schema::TableColumn;

use crate::{TskvError, TskvResult};

#[derive(Debug, Clone, PartialEq)]
pub struct MutableColumnRef<'a> {
    pub column_desc: TableColumn,
    pub column_data: ColumnDataRef<'a>,
}

impl<'a> MutableColumnRef<'a> {
    pub fn empty(column_desc: TableColumn) -> TskvResult<MutableColumnRef<'a>> {
        let column_type = column_desc.column_type.to_physical_data_type();
        let data = ColumnDataRef::new(column_type, 0)
            .map_err(|e| TskvError::ColumnDataError { source: e })?;
        let column = Self {
            column_desc,
            column_data: data,
        };
        Ok(column)
    }

    pub fn push_ts(&mut self, value: i64) -> TskvResult<()> {
        self.column_data
            .push_ts(value)
            .map_err(|e| TskvError::ColumnDataError { source: e })
    }

    pub fn push(&mut self, value: Option<&'a FieldVal>) -> TskvResult<()> {
        self.column_data
            .push(value)
            .map_err(|e| TskvError::ColumnDataError { source: e })
    }
}
