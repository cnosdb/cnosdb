use std::sync::Arc;

use datafusion::datasource::MemTable;
use meta::error::MetaError;
use meta::model::MetaClientRef;
use models::auth::user::User;
use models::oid::Identifier;
use models::schema::{ColumnType, ExternalTableSchema, StreamTable, TableSchema, TskvTableSchema};
use models::ValueType;

use crate::dispatcher::query_tracker::QueryTracker;
use crate::metadata::information_schema_provider::builder::columns::InformationSchemaColumnsBuilder;
use crate::metadata::information_schema_provider::InformationSchemaTableFactory;

const INFORMATION_SCHEMA_COLUMNS: &str = "COLUMNS";

/// This view only displays the column information of tables under the database that the current user has Read permission or higher.
pub struct ColumnsFactory {}

#[async_trait::async_trait]
impl InformationSchemaTableFactory for ColumnsFactory {
    fn table_name(&self) -> &'static str {
        INFORMATION_SCHEMA_COLUMNS
    }

    async fn create(
        &self,
        user: &User,
        metadata: MetaClientRef,
        _query_tracker: Arc<QueryTracker>,
    ) -> std::result::Result<Arc<MemTable>, MetaError> {
        let mut builder = InformationSchemaColumnsBuilder::default();

        let tenant = metadata.tenant();

        let dbs = metadata.list_databases()?;
        let tenant_id = tenant.id();
        let tenant_name = tenant.name();

        for db in dbs {
            // Check if the current user has at least read permission on this db, skip if not
            if !user.can_read_database(*tenant_id, &db) {
                continue;
            }

            let tables = metadata.list_tables(&db)?;
            for table in tables {
                if let Some(table) = metadata.get_table_schema(&db, &table)? {
                    match table {
                        TableSchema::TsKvTableSchema(t) => {
                            append_tskv_table(tenant_name, &db, t.clone(), &mut builder);
                        }
                        TableSchema::ExternalTableSchema(t) => {
                            append_external_table(tenant_name, &db, t.clone(), &mut builder);
                        }
                        TableSchema::StreamTableSchema(t) => {
                            append_stream_table(tenant_name, &db, t.clone(), &mut builder);
                        }
                    }
                }
            }
        }

        let mem_table = MemTable::try_from(builder)
            .map_err(|e| MetaError::CommonError { msg: e.to_string() })?;
        Ok(Arc::new(mem_table))
    }
}

fn append_tskv_table(
    tenant_name: &str,
    database_name: &str,
    table: Arc<TskvTableSchema>,
    builder: &mut InformationSchemaColumnsBuilder,
) {
    for (idx, col) in table.columns().iter().enumerate() {
        builder.append_row(
            tenant_name,
            database_name,
            &table.name,
            &col.name,
            col.column_type.as_column_type_str(),
            idx as u64,
            "NULL",
            col.nullable(),
            col.column_type.to_sql_type_str(),
            Some(col.encoding.as_str()),
        );
    }
}

fn append_external_table(
    tenant_name: &str,
    database_name: &str,
    table: Arc<ExternalTableSchema>,
    builder: &mut InformationSchemaColumnsBuilder,
) {
    for (idx, col) in table.schema.all_fields().iter().enumerate() {
        builder.append_row(
            tenant_name,
            database_name,
            &table.name,
            col.name(),
            // The fields of the external table are all type FIELD
            ColumnType::Field(ValueType::Unknown).as_column_type_str(),
            idx as u64,
            "NULL",
            col.is_nullable(),
            col.data_type().to_string(),
            None::<String>,
        );
    }
}

fn append_stream_table(
    tenant_name: &str,
    database_name: &str,
    table: Arc<StreamTable>,
    builder: &mut InformationSchemaColumnsBuilder,
) {
    for (idx, col) in table.schema().all_fields().iter().enumerate() {
        builder.append_row(
            tenant_name,
            database_name,
            table.name(),
            col.name(),
            // The fields of the external table are all type FIELD
            ColumnType::Field(ValueType::Unknown).as_column_type_str(),
            idx as u64,
            "UNKNOWN",
            col.is_nullable(),
            col.data_type().to_string(),
            None::<String>,
        );
    }
}
