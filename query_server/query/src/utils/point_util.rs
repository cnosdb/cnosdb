use datafusion::arrow::{
    array::{
        Array, ArrayRef, BooleanArray, Float64Array, Int64Array, StringArray,
        TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
        TimestampSecondArray, UInt64Array,
    },
    datatypes::{DataType as ArrowDataType, Field, TimeUnit},
    record_batch::RecordBatch,
};
use flatbuffers::{self, FlatBufferBuilder, Vector, WIPOffset};
use models::schema::{is_time_column, ColumnType, TableFiled, TableSchema, TIME_FIELD_NAME};
use models::{define_result, ValueType};
use paste::paste;
use protos::models::Point;
use protos::models::{FieldBuilder, FieldType, PointArgs, Points, PointsArgs, TagBuilder};
use snafu::Snafu;
use trace::debug;

define_result!(PointUtilError);

#[derive(Debug, Snafu)]
pub enum PointUtilError {
    #[snafu(display("Failed to write points flat buffer, err: {}", err))]
    ToPointsFlatBuffer { err: String },

    #[snafu(display("Invalid array type, expected: {}, found: {}", expected, found))]
    InvalidArrayType { expected: String, found: String },

    #[snafu(display("Column {} not found.", col))]
    ColumnNotFound { col: String },

    #[snafu(display("Data type {} not support.", type_))]
    DataTypeNotSupport { type_: String },

    #[snafu(display("Column {} cannot be null.", col))]
    NotNullConstraint { col: String },
}

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
            other => Err(PointUtilError::DataTypeNotSupport {
                type_: other.to_string(),
            }),
        }
    }};
}

pub fn record_batch_to_points_flat_buffer(
    record_batch: &RecordBatch,
    table_schema: TableSchema,
) -> Result<Vec<u8>> {
    let mut fbb = FlatBufferBuilder::new();

    let record_schema = record_batch.schema();
    let column_schemas = record_schema.fields();

    let mut columns_wip_offset_without_time_col = Vec::with_capacity(column_schemas.len());
    let mut column_schemas_without_time_col = Vec::with_capacity(column_schemas.len());

    let mut time_col_array = None;

    // Convert each column to a datum array
    for (col_idx, col_array) in record_batch.columns().iter().enumerate() {
        let col = unsafe { column_schemas.get_unchecked(col_idx) };
        // The time column requires special handling
        if is_time_column(col) {
            time_col_array = Some(extract_time_column_from(col, col_array)?);
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
    let time_col_array = time_col_array.ok_or_else(|| PointUtilError::ColumnNotFound {
        col: TIME_FIELD_NAME.to_string(),
    })?;

    debug!(
        "time: {:?}, record num: {}, col num: {}",
        time_col_array,
        record_batch.num_rows(),
        record_batch.num_columns()
    );

    construct_row_based_points(
        &mut fbb,
        columns_wip_offset_without_time_col,
        &column_schemas_without_time_col,
        time_col_array,
        record_batch.num_rows(),
        table_schema,
    )
}

/// Construct row-based points flatbuffer from column-based data wip_offset
fn construct_row_based_points(
    fbb: &mut FlatBufferBuilder,
    columns_datum: Vec<Vec<Option<Datum>>>,
    column_schemas: &[&Field],
    time_col_array: Vec<Option<i64>>,
    num_rows: usize,
    schema: TableSchema,
) -> Result<Vec<u8>> {
    let mut point_offsets = Vec::with_capacity(num_rows);
    // row-based
    for row_idx in 0..num_rows {
        let time = unsafe { time_col_array.get_unchecked(row_idx) };

        // Extract tags and fields
        let mut tags = Vec::new();
        let mut fields = Vec::new();
        for (col_idx, df_field) in column_schemas.iter().enumerate() {
            let name = df_field.name();

            let field = schema
                .fields
                .get(name)
                .ok_or_else(|| PointUtilError::ColumnNotFound {
                    col: name.to_owned(),
                })?;

            let value = unsafe { columns_datum.get_unchecked(col_idx).get_unchecked(row_idx) };

            if let Some(datum) = value {
                match field.column_type {
                    ColumnType::Time => {
                        continue;
                    }
                    ColumnType::Tag => {
                        let fbk = fbb.create_vector(name.as_bytes());
                        // let fbv = fbb.create_vector(datum);
                        let mut tag_builder = TagBuilder::new(fbb);
                        tag_builder.add_key(fbk);
                        tag_builder.add_value(datum.to_owned());
                        tags.push(tag_builder.finish());
                    }
                    ColumnType::Field(type_) => {
                        let fbv_type = match type_ {
                            ValueType::Unknown => FieldType::Unknown,
                            ValueType::Float => FieldType::Float,
                            ValueType::Integer => FieldType::Integer,
                            ValueType::Unsigned => FieldType::Unsigned,
                            ValueType::Boolean => FieldType::Boolean,
                            ValueType::String => FieldType::String,
                        };

                        let fbk = fbb.create_vector(name.as_bytes());
                        // let fbv = fbb.create_vector(datum);
                        let mut field_builder = FieldBuilder::new(fbb);
                        field_builder.add_name(fbk);
                        field_builder.add_type_(fbv_type);
                        field_builder.add_value(datum.to_owned());
                        fields.push(field_builder.finish());
                    }
                }
            }
        }

        let time = time.ok_or_else(|| PointUtilError::ColumnNotFound {
            col: TIME_FIELD_NAME.to_string(),
        })?;

        let point_args = PointArgs {
            db: Some(fbb.create_vector(schema.db.as_bytes())),
            table: Some(fbb.create_vector(schema.name.as_bytes())),
            tags: Some(fbb.create_vector(&tags)),
            fields: Some(fbb.create_vector(&fields)),
            timestamp: time,
        };

        point_offsets.push(Point::create(fbb, &point_args));
    }

    let fbb_db = fbb.create_vector(schema.db.as_bytes());
    let points_raw = fbb.create_vector(&point_offsets);
    let points = Points::create(
        fbb,
        &PointsArgs {
            database: Some(fbb_db),
            points: Some(points_raw),
        },
    );
    fbb.finish(points, None);

    Ok(fbb.finished_data().to_vec())
}

fn cast_arrow_array<T: 'static>(array: &ArrayRef) -> Result<&T> {
    array
        .as_any()
        .downcast_ref::<T>()
        .ok_or_else(|| PointUtilError::InvalidArrayType {
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
            fn extract_time_column_from(col: &Field, array: &ArrayRef) -> Result<Vec<Option<i64>>> {
                // time column cannot contain null values
                if array.null_count() > 0 {
                    return Err(PointUtilError::NotNullConstraint {
                        col: col.name().clone(),
                    });
                }

                match array.data_type() {
                    ArrowDataType::Timestamp(unit, _) => match unit {
                        $(
                            TimeUnit::$Kind => Ok(cast_arrow_array::<[<Timestamp $Kind Array>]>(array)?.iter().collect()),
                        )*
                    },
                    ArrowDataType::Int64 => Ok(cast_arrow_array::<Int64Array>(array)?.iter().collect()),
                    other => Err(PointUtilError::InvalidArrayType {
                        expected: ArrowDataType::from(TableFiled::time_field(0).column_type).to_string(),
                        found: other.to_string(),
                    }),
                }
            }
        }
    }
}

define_extract_time_column_from_func!(Second, Millisecond, Microsecond, Nanosecond);
