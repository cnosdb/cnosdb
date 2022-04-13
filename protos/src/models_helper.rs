use std::{borrow::BorrowMut, time};

use flatbuffers::{self, Vector, WIPOffset};

use crate::models::{self, *};

pub fn build_row_field<'a>(
    _fbb: &mut flatbuffers::FlatBufferBuilder<'a>,
    field_id: u64,
    field_type: FieldType,
    value: Option<WIPOffset<Vector<u8>>>,
) -> WIPOffset<RowField<'a>> {
    RowField::create(
        _fbb,
        &RowFieldArgs {
            field_id,
            type_: field_type,
            value,
        },
    )
}

pub fn build_row<'a>(
    _fbb: &mut flatbuffers::FlatBufferBuilder<'a>,
    series_id: u64,
    timestamp: u64,
    fields: &Vec<WIPOffset<RowField<'a>>>,
) -> WIPOffset<Row<'a>> {
    let fields = _fbb.create_vector(fields);

    let mut row_builder = RowBuilder::new(_fbb);
    row_builder.add_key(&RowKey::new(series_id, timestamp));
    row_builder.add_fields(fields);
    row_builder.finish()
}
