mod generated;
pub use generated::*;
use tonic::codec::CompressionEncoding;
pub mod models_helper;
pub mod prompb;
pub mod test_helper;

use core::time;
use std::fmt::{Display, Formatter};
use std::time::Duration;

use flatbuffers::{ForwardsUOffset, Vector};
use snafu::{Backtrace, Location, OptionExt, Snafu};
use tonic::transport::{Channel, Endpoint};
use tower::timeout::Timeout;

use crate::kv_service::tskv_service_client::TskvServiceClient;
use crate::models::{Column, Points, Table};
use crate::raft_service::raft_service_client::RaftServiceClient;

// Default 100 MB
pub const DEFAULT_GRPC_SERVER_MESSAGE_LEN: usize = 100 * 1024 * 1024;

type PointsResult<T> = Result<T, PointsError>;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum PointsError {
    #[snafu(display("{}", msg))]
    Points {
        msg: String,
        location: Location,
        backtrace: Backtrace,
    },

    #[snafu(display("Flatbuffers 'Points' missing database name (db)"))]
    PointsMissingDatabaseName {
        location: Location,
        backtrace: Backtrace,
    },

    #[snafu(display("Flatbuffers 'Points' missing tables data (tables)"))]
    PointsMissingTables {
        location: Location,
        backtrace: Backtrace,
    },

    #[snafu(display("Flatbuffers 'Table' missing table name (tab)"))]
    TableMissingName {
        location: Location,
        backtrace: Backtrace,
    },

    #[snafu(display("Flatbuffers 'Table' missing points data (points)"))]
    TableMissingColumns {
        location: Location,
        backtrace: Backtrace,
    },

    #[snafu(display("Flatbuffers 'Point' missing tags data (tags)"))]
    PointMissingTags {
        location: Location,
        backtrace: Backtrace,
    },

    #[snafu(display("Flatbuffers 'Column' missing values"))]
    ColumnMissingValues {
        location: Location,
        backtrace: Backtrace,
    },

    #[snafu(display("Flatbuffers 'Column' missing names"))]
    ColumnMissingNames {
        location: Location,
        backtrace: Backtrace,
    },

    #[snafu(display("Flatbuffers 'Column' missing nullbits"))]
    ColumnMissingNullbits {
        location: Location,
        backtrace: Backtrace,
    },
}

#[derive(Debug, PartialEq, Clone)]
pub enum FieldValue {
    U64(u64),
    I64(i64),
    Str(Vec<u8>),
    F64(f64),
    Bool(bool),
}

impl<'a> Points<'a> {
    pub fn db_ext(&'a self) -> PointsResult<&'a str> {
        self.db().context(PointsMissingDatabaseNameSnafu)
    }

    pub fn tables_iter_ext(&'a self) -> PointsResult<impl Iterator<Item = Table<'a>>> {
        Ok(self.tables().context(PointsMissingTablesSnafu)?.iter())
    }
}

impl<'a> Table<'a> {
    pub fn tab_ext(&'a self) -> PointsResult<&'a str> {
        let name = self.tab().context(TableMissingNameSnafu)?;
        Ok(name)
    }

    pub fn columns_iter_ext(&'a self) -> PointsResult<impl Iterator<Item = Column<'a>>> {
        Ok(self.columns().context(TableMissingColumnsSnafu)?.iter())
    }
}

impl<'a> Column<'a> {
    pub fn name_ext(&'a self) -> PointsResult<&'a str> {
        let name = self.name().context(ColumnMissingNamesSnafu)?;
        Ok(name)
    }

    pub fn nullbit_ext(&self) -> PointsResult<Vector<u8>> {
        let nullbit = self.nullbits().context(ColumnMissingNullbitsSnafu)?;
        Ok(nullbit)
    }

    pub fn string_values_len(&self) -> PointsResult<usize> {
        let len = self
            .col_values()
            .context(ColumnMissingValuesSnafu)?
            .string_value()
            .map(|v| v.len())
            .unwrap_or(0);
        Ok(len)
    }

    pub fn string_values(&self) -> PointsResult<Vector<ForwardsUOffset<&str>>> {
        let values = self
            .col_values()
            .context(ColumnMissingValuesSnafu)?
            .string_value()
            .unwrap_or_default();
        Ok(values)
    }

    pub fn bool_values_len(&self) -> PointsResult<usize> {
        let len = self
            .col_values()
            .context(ColumnMissingValuesSnafu)?
            .bool_value()
            .map(|v| v.len())
            .unwrap_or(0);
        Ok(len)
    }

    pub fn bool_values(&self) -> PointsResult<Vector<bool>> {
        let values = self
            .col_values()
            .context(ColumnMissingValuesSnafu)?
            .bool_value()
            .unwrap_or_default();
        Ok(values)
    }

    pub fn int_values_len(&self) -> PointsResult<usize> {
        let len = self
            .col_values()
            .context(ColumnMissingValuesSnafu)?
            .int_value()
            .map(|v| v.len())
            .unwrap_or(0);
        Ok(len)
    }

    pub fn int_values(&self) -> PointsResult<Vector<i64>> {
        let values = self
            .col_values()
            .context(ColumnMissingValuesSnafu)?
            .int_value()
            .unwrap_or_default();
        Ok(values)
    }

    pub fn float_values_len(&self) -> PointsResult<usize> {
        let len = self
            .col_values()
            .context(ColumnMissingValuesSnafu)?
            .float_value()
            .map(|v| v.len())
            .unwrap_or(0);
        Ok(len)
    }

    pub fn float_values(&self) -> PointsResult<Vector<f64>> {
        let values = self
            .col_values()
            .context(ColumnMissingValuesSnafu)?
            .float_value()
            .unwrap_or_default();
        Ok(values)
    }

    pub fn uint_values_len(&self) -> PointsResult<usize> {
        let len = self
            .col_values()
            .context(ColumnMissingValuesSnafu)?
            .uint_value()
            .map(|v| v.len())
            .unwrap_or(0);
        Ok(len)
    }

    pub fn uint_values(&self) -> PointsResult<Vector<u64>> {
        let values = self
            .col_values()
            .context(ColumnMissingValuesSnafu)?
            .uint_value()
            .unwrap_or_default();
        Ok(values)
    }
}

impl<'a> Display for Points<'a> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "==============================")?;
        writeln!(f, "Database: {}", self.db_ext().unwrap_or("{!BAD_DB_NAME}"))?;
        writeln!(f, "------------------------------")?;
        match self.tables_iter_ext() {
            Ok(tables) => {
                for table in tables {
                    write!(
                        f,
                        "Table: {}",
                        table.tab_ext().unwrap_or("{!BAD_TABLE_NAME}")
                    )?;
                    writeln!(f, "{}", table)?;
                    writeln!(f, "------------------------------")?;
                }
            }
            Err(_) => {
                writeln!(f, "No tables")?;
            }
        }

        Ok(())
    }
}

impl<'a> Display for Table<'a> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let columns = match self.columns_iter_ext() {
            Ok(p) => p,
            Err(_) => {
                writeln!(f, "{{!BAD_TABLE_POINTS}}")?;
                return Ok(());
            }
        };
        for column in columns {
            write!(
                f,
                "\nColumn: {}",
                column.name_ext().unwrap_or("{!BAD_COLUMN_NAME}")
            )?;
            writeln!(f, "\nColumn Type: {:?}", column.column_type())?;
            writeln!(f, "\nField Type: {:?}", column.field_type())?;
            writeln!(f, "\nColumn Value: {:?}", column.col_values())?;
            writeln!(f, "\nNullBits: {:?}", column.nullbit_ext())?;
        }
        Ok(())
    }
}

pub fn tskv_service_time_out_client(
    channel: Channel,
    time_out: Duration,
    max_message_size: usize,
    grpc_enable_gzip: bool,
) -> TskvServiceClient<Timeout<Channel>> {
    let timeout_channel = Timeout::new(channel, time_out);
    let client = TskvServiceClient::<Timeout<Channel>>::new(timeout_channel);
    let client = TskvServiceClient::max_decoding_message_size(client, max_message_size);
    if grpc_enable_gzip {
        TskvServiceClient::max_encoding_message_size(client, max_message_size)
            .accept_compressed(CompressionEncoding::Gzip)
            .send_compressed(CompressionEncoding::Gzip)
    } else {
        TskvServiceClient::max_encoding_message_size(client, max_message_size)
    }
}

pub fn raft_service_time_out_client(
    channel: Channel,
    time_out: Duration,
    max_message_size: usize,
    grpc_enable_gzip: bool,
) -> RaftServiceClient<Timeout<Channel>> {
    let timeout_channel = Timeout::new(channel, time_out);
    let client = RaftServiceClient::<Timeout<Channel>>::new(timeout_channel);
    let client = RaftServiceClient::max_decoding_message_size(client, max_message_size);
    if grpc_enable_gzip {
        RaftServiceClient::max_encoding_message_size(client, max_message_size)
            .accept_compressed(CompressionEncoding::Gzip)
            .send_compressed(CompressionEncoding::Gzip)
    } else {
        RaftServiceClient::max_encoding_message_size(client, max_message_size)
    }
}

pub async fn tskv_service_ping(addr: &str) -> Result<(), String> {
    let connector = Endpoint::from_shared(format!("http://{}", addr)).map_err(|e| e.to_string())?;
    let channel = connector
        .connect()
        .await
        .map_err(|e| format!("connect to {} failed: {}", addr, e))?;

    let mut client =
        tskv_service_time_out_client(channel, time::Duration::from_secs(3), 1024 * 1024, false);

    let mut fbb = flatbuffers::FlatBufferBuilder::new();
    let payload = fbb.create_vector(b"hello world");
    let mut builder = models::PingBodyBuilder::new(&mut fbb);
    builder.add_payload(payload);
    let root = builder.finish();
    fbb.finish(root, None);
    let data = fbb.finished_data();

    let resp = client
        .ping(tonic::Request::new(kv_service::PingRequest {
            version: 1,
            body: data.to_vec(),
        }))
        .await;

    if let Err(status) = resp {
        return Err(format!("request failed srtatus: {:?}", status));
    }

    Ok(())
}

#[cfg(all(test, feature = "test"))]
pub mod test {
    use std::collections::HashMap;

    use flatbuffers::FlatBufferBuilder;

    use crate::models::{FieldType, Points};
    use crate::models_helper::create_const_points;

    #[test]
    #[ignore = "Checked by human"]
    fn test_format_fb_model_points() {
        let mut fbb = FlatBufferBuilder::new();
        let points = create_const_points(
            &mut fbb,
            "test_database",
            "test_table",
            vec![("ta", "1111"), ("tb", "22222")],
            vec![
                ("i1", 2_i64.to_be_bytes().as_slice()),
                ("f2", 2.0_f64.to_be_bytes().as_slice()),
                ("s3", "111111".as_bytes()),
            ],
            HashMap::from([
                ("i1", FieldType::Integer),
                ("f2", FieldType::Float),
                ("s3", FieldType::String),
            ]),
            0,
            10,
        );
        fbb.finish(points, None);
        let points_bytes = fbb.finished_data().to_vec();
        let fb_points = flatbuffers::root::<Points>(&points_bytes).unwrap();
        let fb_points_str = format!("{fb_points}");
        // println!("{fb_points}");
        assert_eq!(
            &fb_points_str,
            r#"==============================
Database: test_database
------------------------------
Table: test_table
Column: ta
Column Type: Tag

Field Type: String

Column Value: Some(Values { float_value: None, int_value: None, uint_value: None, bool_value: None, string_value: Some(["1111", "1111", "1111", "1111", "1111", "1111", "1111", "1111", "1111", "1111"]) })

NullBits: Ok([255, 3])

Column: tb
Column Type: Tag

Field Type: String

Column Value: Some(Values { float_value: None, int_value: None, uint_value: None, bool_value: None, string_value: Some(["22222", "22222", "22222", "22222", "22222", "22222", "22222", "22222", "22222", "22222"]) })

NullBits: Ok([255, 3])

Column: i1
Column Type: Field

Field Type: Integer

Column Value: Some(Values { float_value: None, int_value: Some([2, 2, 2, 2, 2, 2, 2, 2, 2, 2]), uint_value: None, bool_value: None, string_value: None })

NullBits: Ok([255, 3])

Column: f2
Column Type: Field

Field Type: Float

Column Value: Some(Values { float_value: Some([2.0, 2.0, 2.0, 2.0, 2.0, 2.0, 2.0, 2.0, 2.0, 2.0]), int_value: None, uint_value: None, bool_value: None, string_value: None })

NullBits: Ok([255, 3])

Column: s3
Column Type: Field

Field Type: String

Column Value: Some(Values { float_value: None, int_value: None, uint_value: None, bool_value: None, string_value: Some(["111111", "111111", "111111", "111111", "111111", "111111", "111111", "111111", "111111", "111111"]) })

NullBits: Ok([255, 3])

Column: time
Column Type: Field

Field Type: Boolean

Column Value: Some(Values { float_value: None, int_value: Some([10, 10, 10, 10, 10, 10, 10, 10, 10, 10]), uint_value: None, bool_value: None, string_value: None })

NullBits: Ok([255, 3])

------------------------------
"#
        );
    }
}
