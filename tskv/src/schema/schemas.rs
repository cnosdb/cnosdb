use crate::schema::error::{MetaSnafu, Result, SchemaError};
use meta::meta_client::{MetaClientRef, MetaRef};
use models::auth::AuthError::Metadata;
use models::codec::Encoding;
use models::schema::{
    ColumnType, DatabaseSchema, TableColumn, TableSchema, TenantOptions, TskvTableSchema,
};
use models::{ColumnId, SeriesId};
use parking_lot::RwLock;
use protos::models::Point;
use snafu::ResultExt;
use std::collections::HashMap;
use trace::{error, info, warn};

const TIME_STAMP_NAME: &str = "time";

#[derive(Debug)]
pub struct DBschemas {
    tenant_name: String,
    database_name: String,
    client: MetaClientRef,
}

impl DBschemas {
    pub fn new(db_schema: DatabaseSchema, meta: MetaRef) -> Result<Self> {
        let table_schemas: HashMap<String, TskvTableSchema> = HashMap::new();
        let client = meta
            .tenant_manager()
            .tenant_meta(db_schema.tenant_name())
            .ok_or(SchemaError::TenantNotFound {
                tenant: db_schema.tenant_name().to_string(),
            })?;
        if client
            .get_db_schema(db_schema.database_name())
            .context(MetaSnafu)?
            .is_none()
        {
            client.create_db(db_schema.clone()).context(MetaSnafu)?;
        }
        Ok(Self {
            tenant_name: db_schema.tenant_name().to_string(),
            database_name: db_schema.database_name().to_string(),
            client,
        })
    }

    pub fn alter_db_schema(&self, db_schema: DatabaseSchema) -> Result<()> {
        // todo: client need alter db action
        Ok(())
    }

    pub fn check_field_type_from_cache(&self, series_id: u64, info: &Point) -> Result<()> {
        let table_name =
            unsafe { String::from_utf8_unchecked(info.tab().unwrap().bytes().to_vec()) };
        let schema = self
            .client
            .get_tskv_table_schema(&self.database_name, &table_name)
            .context(MetaSnafu)?
            .ok_or(SchemaError::DatabaseNotFound {
                database: self.database_name.clone(),
            })?;
        for field in info.fields().unwrap() {
            let field_name = String::from_utf8(field.name().unwrap().bytes().to_vec()).unwrap();
            if let Some(v) = schema.column(&field_name) {
                if field.type_().0 != v.column_type.field_type() as i32 {
                    error!(
                        "type mismatch, point: {}, schema: {}",
                        field.type_().0,
                        v.column_type.field_type()
                    );
                    return Err(SchemaError::FieldType);
                }
            } else {
                return Err(SchemaError::NotFoundField);
            }
        }
        for tag in info.tags().unwrap() {
            let tag_name: String = String::from_utf8(tag.key().unwrap().bytes().to_vec()).unwrap();
            if let Some(v) = schema.column(&tag_name) {
                if ColumnType::Tag != v.column_type {
                    error!("type mismatch, point: tag, schema: {}", &v.column_type);
                    return Err(SchemaError::FieldType);
                }
            } else {
                return Err(SchemaError::NotFoundField);
            }
        }
        Ok(())
    }

    pub fn check_field_type_or_else_add(&self, series_id: u64, info: &Point) -> Result<()> {
        //load schema first from cache,or else from storage and than cache it!
        let table_name =
            unsafe { String::from_utf8_unchecked(info.tab().unwrap().bytes().to_vec()) };
        let db_name = unsafe { String::from_utf8_unchecked(info.db().unwrap().bytes().to_vec()) };
        let schema = self
            .client
            .get_tskv_table_schema(&db_name, &table_name)
            .context(MetaSnafu)?;
        let mut new_schema = false;
        let mut schema = match schema {
            None => {
                let mut schema = TskvTableSchema::default();
                schema.tenant = self.tenant_name.clone();
                schema.db = db_name;
                schema.name = table_name;
                new_schema = true;
                schema
            }
            Some(schema) => schema,
        };

        let mut schema_change = false;
        let mut check_fn = |field: &mut TableColumn| -> Result<()> {
            let encoding = match schema.column(&field.name) {
                None => Encoding::Default,
                Some(v) => v.encoding,
            };
            field.encoding = encoding;

            match schema.column(&field.name) {
                Some(v) => {
                    if field.column_type != v.column_type {
                        trace::debug!(
                            "type mismatch, point: {}, schema: {}",
                            &field.column_type,
                            &v.column_type
                        );
                        trace::debug!("type mismatch, schema: {:?}", &schema);
                        return Err(SchemaError::FieldType);
                    }
                }
                None => {
                    schema_change = true;
                    field.id = (schema.columns().len() + 1) as ColumnId;
                    schema.add_column(field.clone());
                }
            }
            Ok(())
        };
        //check timestamp
        check_fn(&mut TableColumn::new_with_default(
            TIME_STAMP_NAME.to_string(),
            ColumnType::Time,
        ))?;

        //check tags
        for tag in info.tags().unwrap() {
            let tag_key =
                unsafe { String::from_utf8_unchecked(tag.key().unwrap().bytes().to_vec()) };
            check_fn(&mut TableColumn::new_with_default(tag_key, ColumnType::Tag))?
        }

        //check fields
        for field in info.fields().unwrap() {
            let field_name =
                unsafe { String::from_utf8_unchecked(field.name().unwrap().bytes().to_vec()) };
            check_fn(&mut TableColumn::new_with_default(
                field_name,
                ColumnType::from_i32(field.type_().0),
            ))?
        }

        //schema changed store it
        if new_schema {
            schema.schema_id = 0;
            self.client
                .create_table(&TableSchema::TsKvTableSchema(schema.clone()))
                .context(MetaSnafu)?;
        } else if schema_change {
            schema.schema_id += 1;
            self.client
                .update_table(&TableSchema::TsKvTableSchema(schema.clone()))
                .context(MetaSnafu)?;
        }
        Ok(())
    }

    pub fn get_table_schema(&self, tab: &str) -> Result<Option<TskvTableSchema>> {
        let schema = self
            .client
            .get_tskv_table_schema(&self.database_name, tab)
            .context(MetaSnafu)?;

        //todo get schema from meta
        Ok(schema)
    }

    // pub fn list_tables(&self) -> Result<Vec<String>> {
    //     let tables = self
    //         .client
    //         .list_tables(&self.database_name)
    //         .context(MetaSnafu)?;
    //     Ok(tables)
    // }

    pub fn del_table_schema(&self, tab: &str) -> Result<()> {
        self.client
            .drop_table(&self.database_name, tab)
            .context(MetaSnafu)?;
        Ok(())
    }

    // pub fn create_table(&self, schema: &TskvTableSchema) -> Result<()> {
    //     self.client
    //         .create_table(&TableSchema::TsKvTableSchema(schema.clone()))
    //         .context(MetaSnafu)?;

    //     Ok(())
    // }

    pub fn db_schema(&self) -> Result<DatabaseSchema> {
        let db_schema = self
            .client
            .get_db_schema(&self.database_name)
            .context(MetaSnafu)?
            .ok_or(SchemaError::DatabaseNotFound {
                database: self.database_name.clone(),
            })?;
        Ok(db_schema)
    }

    pub fn add_table_column(&self, tab: &str, mut column: TableColumn) -> Result<()> {
        let mut schema = self
            .client
            .get_tskv_table_schema(&self.database_name, tab)
            .context(MetaSnafu)?
            .ok_or(SchemaError::TableNotFound {
                table: tab.to_string(),
            })?;
        if schema.column(&column.name).is_some() {
            return Err(SchemaError::ColumnAlreadyExists { name: column.name });
        }
        column.id = schema.next_column_id();
        schema.add_column(column);
        schema.schema_id += 1;
        self.client
            .update_table(&TableSchema::TsKvTableSchema(schema))
            .context(MetaSnafu)?;
        Ok(())
    }

    pub fn drop_table_column(&self, tab: &str, name: &str) -> Result<()> {
        let mut schema = self
            .client
            .get_tskv_table_schema(&self.database_name, tab)
            .context(MetaSnafu)?
            .ok_or(SchemaError::TableNotFound {
                table: tab.to_string(),
            })?;
        if schema.column(name).is_none() {
            return Err(SchemaError::NotFoundField);
        }
        schema.drop_column(name);
        schema.schema_id += 1;
        self.client
            .update_table(&TableSchema::TsKvTableSchema(schema))
            .context(MetaSnafu)?;
        Ok(())
    }

    pub fn change_table_column(
        &self,
        tab: &str,
        name: &str,
        new_column: TableColumn,
    ) -> Result<()> {
        let mut schema = self
            .client
            .get_tskv_table_schema(&self.database_name, tab)
            .context(MetaSnafu)?
            .ok_or(SchemaError::TableNotFound {
                table: tab.to_string(),
            })?;
        if schema.column(name).is_none() {
            return Err(SchemaError::NotFoundField);
        }
        schema.change_column(name, new_column);
        schema.schema_id += 1;
        self.client
            .update_table(&TableSchema::TsKvTableSchema(schema))
            .context(MetaSnafu)?;
        Ok(())
    }
}
