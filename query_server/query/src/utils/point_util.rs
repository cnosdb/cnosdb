use datafusion::arrow::array::{
    Array, ArrayRef, BooleanArray, Float64Array, Int64Array, StringArray,
    TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
    TimestampSecondArray, UInt64Array,
};
use datafusion::arrow::datatypes::{DataType as ArrowDataType, Field, TimeUnit};
use datafusion::arrow::record_batch::RecordBatch;
use flatbuffers::{self, FlatBufferBuilder, Vector, WIPOffset};
use models::schema::{is_time_column, ColumnType, TskvTableSchemaRef, TIME_FIELD_NAME};
use models::ValueType;
use paste::paste;
use protos::models::{
    FieldBuilder, FieldType, Point, PointArgs, Points, PointsArgs, TableBuilder, TagBuilder,
};
use protos::{build_fb_schema_offset, init_fields_and_nullbits, init_tags_and_nullbits, FbSchema};
use spi::{QueryError, Result};

type Datum<'fbb> = WIPOffset<Vector<'fbb, u8>>;

macro_rules! arrow_array_to_specific_offset_array {
    ($fbb:ident, $col_array:ident, $Builder: ident) => {{
        Ok(cast_arrow_array::<$Builder>($col_array)?
            .iter()
            .map(|e| e.map(|e| $fbb.create_vector(&e.to_be_bytes())))
            .collect())
    }};
}

/// convert arrow::array:Array to Vec<Option<WIPOfset>>
///
/// only support Timestamp/Float64/Int64/UInt64/Utf8/Boolean
macro_rules! arrow_array_to_offset_array {
    ($fbb:ident, $col_array:ident) => {{
        match $col_array.data_type() {
            ArrowDataType::Timestamp(unit, _) => match unit {
                TimeUnit::Second => {
                    arrow_array_to_specific_offset_array!($fbb, $col_array, TimestampSecondArray)
                }
                TimeUnit::Millisecond => arrow_array_to_specific_offset_array!(
                    $fbb,
                    $col_array,
                    TimestampMillisecondArray
                ),
                TimeUnit::Microsecond => arrow_array_to_specific_offset_array!(
                    $fbb,
                    $col_array,
                    TimestampMicrosecondArray
                ),
                TimeUnit::Nanosecond => arrow_array_to_specific_offset_array!(
                    $fbb,
                    $col_array,
                    TimestampNanosecondArray
                ),
            },
            ArrowDataType::Float64 => {
                arrow_array_to_specific_offset_array!($fbb, $col_array, Float64Array)
            }
            ArrowDataType::Int64 => {
                arrow_array_to_specific_offset_array!($fbb, $col_array, Int64Array)
            }
            ArrowDataType::UInt64 => {
                arrow_array_to_specific_offset_array!($fbb, $col_array, UInt64Array)
            }
            ArrowDataType::Utf8 => Ok(cast_arrow_array::<StringArray>($col_array)?
                .iter()
                .map(|e| e.map(|e| $fbb.create_vector(e.as_bytes())))
                .collect()),
            ArrowDataType::Boolean => Ok(cast_arrow_array::<BooleanArray>($col_array)?
                .iter()
                .map(|e| e.map(|e| $fbb.create_vector(if e { &[1_u8][..] } else { &[0_u8][..] })))
                .collect()),
            other => Err(QueryError::PointErrorDataTypeNotSupport {
                type_: other.to_string(),
            }),
        }
    }};
}

pub fn record_batch_to_points_flat_buffer(
    record_batch: &RecordBatch,
    table_schema: TskvTableSchemaRef,
) -> Result<(Vec<u8>, TimeUnit)> {
    let mut fbb = FlatBufferBuilder::new();

    let record_schema = record_batch.schema();
    let column_schemas = record_schema.fields();

    let mut columns_wip_offset_without_time_col = Vec::with_capacity(column_schemas.len());
    let mut column_schemas_without_time_col = Vec::with_capacity(column_schemas.len());

    let mut time_col_array_with_unit = None;

    // Convert each column to a datum array
    for (col_idx, col_array) in record_batch.columns().iter().enumerate() {
        let col = unsafe { column_schemas.get_unchecked(col_idx) };
        // The time column requires special handling
        if is_time_column(col) {
            time_col_array_with_unit = Some(extract_time_column_from(col, col_array)?);
            continue;
        }
        // Get wip offset of flatbuffer through arrow::Array of non-time column
        let wip_offset_array = arrow_array_to_offset_array!(fbb, col_array);
        // Save column metadata in field order of record_batch, without time column
        column_schemas_without_time_col.push(col);
        // Save wip offset
        columns_wip_offset_without_time_col.push(wip_offset_array?)
    }
    // must contain the time column
    let (time_col_array, unit) =
        time_col_array_with_unit.ok_or_else(|| QueryError::ColumnNotFound {
            col: "time".to_string(),
        })?;

    trace::trace!(
        "time: {:?}, record num: {}, col num: {}",
        time_col_array,
        record_batch.num_rows(),
        record_batch.num_columns()
    );

    let points = construct_row_based_points(
        &mut fbb,
        columns_wip_offset_without_time_col,
        &column_schemas_without_time_col,
        time_col_array,
        record_batch.num_rows(),
        table_schema,
    )?;
    Ok((points, unit))
}

/// Construct row-based points flatbuffer from column-based data wip_offset
fn construct_row_based_points(
    fbb: &mut FlatBufferBuilder,
    columns_datum: Vec<Vec<Option<Datum>>>,
    column_schemas: &[&Field],
    time_col_array: Vec<Option<i64>>,
    num_rows: usize,
    schema: TskvTableSchemaRef,
) -> Result<Vec<u8>> {
    let mut point_offsets = Vec::with_capacity(num_rows);

    let fb_schema = build_fb_schema(&schema);
    // row-based
    for row_idx in 0..num_rows {
        let time = unsafe { time_col_array.get_unchecked(row_idx) };

        // Extract tags and fields
        let (mut tags, mut tags_nullbit) = init_tags_and_nullbits(fbb, &fb_schema);
        let (mut fields, mut fields_nullbits) = init_fields_and_nullbits(fbb, &fb_schema);

        for (col_idx, df_field) in column_schemas.iter().enumerate() {
            let name = df_field.name().as_ref();

            let field = schema
                .column(name)
                .ok_or_else(|| QueryError::ColumnNotFound {
                    col: name.to_string(),
                })?;

            let value = unsafe { columns_datum.get_unchecked(col_idx).get_unchecked(row_idx) };

            if let Some(datum) = value {
                match field.column_type {
                    ColumnType::Time(_) => {
                        continue;
                    }
                    ColumnType::Tag => {
                        let idx = match fb_schema.tag_names().get(name) {
                            None => continue,
                            Some(v) => *v,
                        };
                        let mut tag_builder = TagBuilder::new(fbb);
                        tag_builder.add_value(datum.to_owned());

                        tags[idx] = tag_builder.finish();
                        tags_nullbit.set(idx);
                    }
                    ColumnType::Field(_) => {
                        let idx = match fb_schema.field().get(name) {
                            None => continue,
                            Some(v) => *v,
                        };

                        let mut field_builder = FieldBuilder::new(fbb);
                        field_builder.add_value(datum.to_owned());

                        fields[idx] = field_builder.finish();
                        fields_nullbits.set(idx)
                    }
                }
            }
        }

        let time = time.ok_or_else(|| QueryError::ColumnNotFound {
            col: TIME_FIELD_NAME.to_string(),
        })?;

        let point_args = PointArgs {
            tags: Some(fbb.create_vector(&tags)),
            tags_nullbit: Some(fbb.create_vector(tags_nullbit.bytes())),
            fields: Some(fbb.create_vector(&fields)),
            fields_nullbit: Some(fbb.create_vector(fields_nullbits.bytes())),
            timestamp: time,
        };

        point_offsets.push(Point::create(fbb, &point_args));
    }

    let fb_schema_off = build_fb_schema_offset(fbb, &fb_schema);
    let points = fbb.create_vector(&point_offsets);
    let table_name = fbb.create_vector(schema.name.as_bytes());

    let mut table_builder = TableBuilder::new(fbb);

    table_builder.add_points(points);
    table_builder.add_schema(fb_schema_off);
    table_builder.add_tab(table_name);
    table_builder.add_num_rows(num_rows as u64);

    let table_offset = table_builder.finish();

    let fbb_db = fbb.create_vector(schema.db.as_bytes());
    let tables = fbb.create_vector(&[table_offset]);
    let points = Points::create(
        fbb,
        &PointsArgs {
            db: Some(fbb_db),
            tables: Some(tables),
        },
    );
    fbb.finish(points, None);

    Ok(fbb.finished_data().to_vec())
}

fn build_fb_schema(tskv_schema: &TskvTableSchemaRef) -> FbSchema<'_> {
    let mut schema = FbSchema::default();
    for column in tskv_schema.columns() {
        match column.column_type {
            ColumnType::Tag => {
                schema.add_tag(&column.name);
            }
            ColumnType::Time(_) => continue,
            ColumnType::Field(field_type) => schema.add_field(
                &column.name,
                convert_value_type_to_fb_field_type(field_type),
            ),
        }
    }
    schema
}

fn convert_value_type_to_fb_field_type(type_: ValueType) -> FieldType {
    match type_ {
        ValueType::Unknown => FieldType::Unknown,
        ValueType::Float => FieldType::Float,
        ValueType::Integer => FieldType::Integer,
        ValueType::Unsigned => FieldType::Unsigned,
        ValueType::Boolean => FieldType::Boolean,
        ValueType::String => FieldType::String,
    }
}

fn cast_arrow_array<T: 'static>(array: &ArrayRef) -> Result<&T> {
    array
        .as_any()
        .downcast_ref::<T>()
        .ok_or_else(|| QueryError::InvalidArrayType {
            expected: "UNKNOWN".to_string(),
            found: array.data_type().to_string(),
        })
}

macro_rules! define_extract_time_column_from_func {
    ($($Kind: ident), *) => {
        paste! {
            /// Convert the value of the time column to the offset of the flatbuffer
            ///
            /// Throws an exception `PointUtilError::NotNullConstraint` if it contains a null value
            ///
            /// Throws an exception `PointUtilError::InvalidArrayType` if types do not match
            fn extract_time_column_from(col: &Field, array: &ArrayRef) -> Result<(Vec<Option<i64>>, TimeUnit)> {
                // time column cannot contain null values
                if array.null_count() > 0 {
                    return Err(QueryError::PointErrorNotNullConstraint {
                        col: col.name().clone(),
                    });
                }

                match array.data_type() {
                    ArrowDataType::Timestamp(unit, _) => match unit {
                        $(
                            TimeUnit::$Kind => Ok((cast_arrow_array::<[<Timestamp $Kind Array>]>(array)?.iter().collect(), unit.clone())),
                        )*
                    },
                    ArrowDataType::Int64 => Ok((cast_arrow_array::<Int64Array>(array)?.iter().collect(), TimeUnit::Nanosecond)),
                    other => Err(QueryError::InvalidArrayType {
                        expected: "ArrowDataType for Timestamp".to_string(),
                        found: other.to_string(),
                    }),
                }
            }
        }
    }
}

define_extract_time_column_from_func!(Second, Millisecond, Microsecond, Nanosecond);
