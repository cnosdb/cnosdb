use std::{borrow::BorrowMut, time};

use flatbuffers::{self, Vector, WIPOffset};

use crate::models::*;

pub fn build_row_field<'a>(
    _fbb: &mut flatbuffers::FlatBufferBuilder<'a>,
    field_id: u64,
    field_type: FieldType,
    value: Option<WIPOffset<Vector<u8>>>,
) -> WIPOffset<RowField<'a>> {
    let fbb = _fbb.borrow_mut();
    RowField::create(
        fbb,
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
    let fbb = _fbb.borrow_mut();

    let fields = fbb.create_vector(fields);

    let mut row_builder = RowBuilder::new(fbb);
    row_builder.add_key(&RowKey::new(series_id, timestamp));
    row_builder.add_fields(fields);
    row_builder.finish()
}

pub fn build_write_wal_entry<'a>(
    _fbb: &mut flatbuffers::FlatBufferBuilder<'a>,
    rows: &Vec<WIPOffset<Row<'a>>>,
) -> WIPOffset<WriteWALEntry<'a>> {
    let fbb = _fbb.borrow_mut();

    let rows = Some(fbb.create_vector(rows));

    WriteWALEntry::create(fbb, &WriteWALEntryArgs { rows })
}

pub fn build_delete_wal_entry_item(series_id: u64, field_id: u64) -> DeleteWALEntryItem {
    DeleteWALEntryItem::new(series_id, field_id)
}

pub fn build_delete_wal_entry<'a>(
    _fbb: &mut flatbuffers::FlatBufferBuilder<'a>,
    items: &'a Vec<DeleteWALEntryItem>,
) -> WIPOffset<DeleteWALEntry<'a>> {
    let fbb = _fbb.borrow_mut();

    let items = Some(fbb.create_vector(items));

    DeleteWALEntry::create(fbb, &DeleteWALEntryArgs { items })
}

pub fn build_delete_range_wal_entry<'a>(
    _fbb: &mut flatbuffers::FlatBufferBuilder<'a>,
    items: &Vec<DeleteWALEntryItem>,
    min: u64,
    max: u64,
) -> WIPOffset<DeleteRangeWALEntry<'a>> {
    let fbb = _fbb.borrow_mut();

    let items = Some(fbb.create_vector(items));

    DeleteRangeWALEntry::create(fbb, &DeleteRangeWALEntryArgs { items, min, max })
}

pub fn build_wal_entry<'a>(
    _fbb: &mut flatbuffers::FlatBufferBuilder<'a>,
    series_id: u64,
    entry_type: WALEntryType,
    value_type: WALEntryUnion,
    value: Option<flatbuffers::WIPOffset<flatbuffers::UnionWIPOffset>>,
) -> WIPOffset<WALEntry<'a>> {
    let fbb = _fbb.borrow_mut();

    WALEntry::create(
        fbb,
        &WALEntryArgs {
            type_: entry_type,
            series_id,
            value_type,
            value,
        },
    )
}

#[test]
fn test_build_write_wal_entry() {
    for _ in 0..10 {
        let mut fbb = flatbuffers::FlatBufferBuilder::new();

        let float_value = Some(fbb.create_vector(3.14_f64.to_le_bytes().as_slice()));
        let float_field = build_row_field(&mut fbb, 1, FieldType::Float, float_value);

        let row = build_row(&mut fbb, 1, 1000, &vec![float_field]);

        let entry = build_write_wal_entry(&mut fbb, &vec![row]);
        fbb.finish(entry, None);

        let bytes = fbb.finished_data();
        println!("{:?}", bytes);

        // let de_entry = flatbuffers::root::<WALEntry>(bytes).unwrap();
        // println!("{:?}", de_entry);
    }
}
