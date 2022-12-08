use crate::schema::error::{MetaSnafu, Result, SchemaError};
use meta::meta_client::{MetaClientRef, MetaRef};
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
    db_schema: RwLock<DatabaseSchema>,
    table_schema: RwLock<HashMap<String, TskvTableSchema>>,
    meta: MetaRef,
}

impl DBschemas {
    pub fn new(db_schema: DatabaseSchema, meta: MetaRef) -> Result<Self> {
        let mut table_schemas: HashMap<String, TskvTableSchema> = HashMap::new();
        let client = meta
            .tenant_manager()
            .tenant_meta(db_schema.tenant_name())
            .ok_or(SchemaError::TenantNotFound {
                tenant: db_schema.tenant_name().to_string(),
            })?;
        match client
            .get_db_schema(db_schema.database_name())
            .context(MetaSnafu)?
        {
            None => {
                client.create_db(&db_schema).context(MetaSnafu)?;
                Ok(Self {
                    db_schema: RwLock::new(db_schema),
                    table_schema: RwLock::new(table_schemas),
                    meta,
                })
            }
            Some(schema) => {
                let tables = client
                    .list_tables(schema.database_name())
                    .context(MetaSnafu)?;
                for table in tables {
                    if let Some(table_schema) = client
                        .get_tskv_table_schema(schema.database_name(), &table)
                        .context(MetaSnafu)?
                    {
                        table_schemas.insert(table, table_schema);
                    }
                }
                Ok(Self {
                    db_schema: RwLock::new(schema),
                    table_schema: RwLock::new(table_schemas),
                    meta,
                })
            }
        }
    }

    pub fn alter_db_schema(&self, db_schema: DatabaseSchema) -> Result<()> {
        let client = self
            .meta
            .tenant_manager()
            .tenant_meta(db_schema.tenant_name())
            .ok_or(SchemaError::DatabaseNotFound {
                database: db_schema.database_name().to_string(),
            })?;
        // todo: client need alter db action
        *self.db_schema.write() = db_schema;
        Ok(())
    }

    pub fn check_field_type_from_cache(&self, series_id: u64, info: &Point) -> Result<()> {
        let table_name = unsafe { String::from_utf8_unchecked(info.tab().unwrap().to_vec()) };
        if let Some(schema) = self.table_schema.read().get(&table_name) {
            for field in info.fields().unwrap() {
                let field_name = String::from_utf8(field.name().unwrap().to_vec()).unwrap();
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
                let tag_name: String = String::from_utf8(tag.key().unwrap().to_vec()).unwrap();
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
        } else {
            Err(SchemaError::NotFoundField)
        }
    }

    pub fn check_field_type_or_else_add(&self, series_id: u64, info: &Point) -> Result<()> {
        //load schema first from cache,or else from storage and than cache it!
        let table_name = unsafe { String::from_utf8_unchecked(info.tab().unwrap().to_vec()) };
        let db_name = unsafe { String::from_utf8_unchecked(info.db().unwrap().to_vec()) };
        let mut fields = self.table_schema.write();
        let mut new_schema = false;
        let schema = fields.entry(table_name.clone()).or_insert_with(|| {
            let mut schema = TskvTableSchema::default();
            schema.tenant = self.db_schema.read().tenant_name().to_string();
            schema.name = table_name.clone();
            schema.db = db_name.clone();
            new_schema = true;
            schema
        });

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
            let tag_key = unsafe { String::from_utf8_unchecked(tag.key().unwrap().to_vec()) };
            check_fn(&mut TableColumn::new_with_default(tag_key, ColumnType::Tag))?
        }

        //check fields
        for field in info.fields().unwrap() {
            let field_name = unsafe { String::from_utf8_unchecked(field.name().unwrap().to_vec()) };
            check_fn(&mut TableColumn::new_with_default(
                field_name,
                ColumnType::from_i32(field.type_().0),
            ))?
        }

        let client = self
            .meta
            .tenant_manager()
            .tenant_meta(&schema.tenant)
            .ok_or(SchemaError::DatabaseNotFound {
                database: self.db_schema.read().database_name().to_string(),
            })?;
        //schema changed store it
        if new_schema {
            schema.schema_id = 0;
            client
                .create_table(&TableSchema::TsKvTableSchema(schema.clone()))
                .context(MetaSnafu)?;
        } else if schema_change {
            schema.schema_id += 1;
            client
                .update_table(&TableSchema::TsKvTableSchema(schema.clone()))
                .context(MetaSnafu)?;
        }
        Ok(())
    }

    pub fn get_table_schema(&self, tab: &str) -> Result<Option<TskvTableSchema>> {
        if let Some(schema) = self.table_schema.read().get(tab) {
            return Ok(Some(schema.clone()));
        }
        let db_schema = self.db_schema.read();
        let client = self
            .meta
            .tenant_manager()
            .tenant_meta(db_schema.tenant_name())
            .ok_or(SchemaError::DatabaseNotFound {
                database: db_schema.database_name().to_string(),
            })?;
        let schema = client
            .get_tskv_table_schema(db_schema.database_name(), tab)
            .context(MetaSnafu)?;

        //todo get schema from meta
        Ok(schema)
    }

    pub fn list_tables(&self) -> Vec<String> {
        let mut tables = Vec::new();
        for (table, _) in self.table_schema.read().iter() {
            tables.push(table.clone())
        }

        tables
    }

    pub fn del_table_schema(&self, tab: &str) -> Result<()> {
        let db_schema = self.db_schema.read();
        let client = self
            .meta
            .tenant_manager()
            .tenant_meta(db_schema.tenant_name())
            .ok_or(SchemaError::DatabaseNotFound {
                database: db_schema.database_name().to_string(),
            })?;
        client
            .drop_table(db_schema.database_name(), tab)
            .context(MetaSnafu)?;
        self.table_schema.write().remove(tab);
        Ok(())
    }

    pub fn create_table(&self, schema: &TskvTableSchema) -> Result<()> {
        let db_schema = self.db_schema.read();
        let client = self
            .meta
            .tenant_manager()
            .tenant_meta(db_schema.tenant_name())
            .ok_or(SchemaError::DatabaseNotFound {
                database: db_schema.database_name().to_string(),
            })?;
        client
            .create_table(&TableSchema::TsKvTableSchema(schema.clone()))
            .context(MetaSnafu)?;
        self.table_schema
            .write()
            .insert(schema.name.clone(), schema.clone());
        Ok(())
    }

    pub fn db_schema(&self) -> DatabaseSchema {
        self.db_schema.read().clone()
    }

    pub fn add_table_column(&self, tab: &str, mut column: TableColumn) -> Result<()> {
        // todo : meta need alter method
        let mut schema = self
            .get_table_schema(tab)?
            .ok_or(SchemaError::TableNotFound {
                table: tab.to_string(),
            })?;
        if schema.column(&column.name).is_some() {
            return Err(SchemaError::ColumnAlreadyExists { name: column.name });
        }
        column.id = schema.next_column_id();
        schema.add_column(column);
        schema.schema_id += 1;
        Ok(())
    }

    pub fn drop_table_column(&self, tab: &str, name: &str) -> Result<()> {
        // todo : meta need alter method
        let mut schema = self
            .get_table_schema(tab)?
            .ok_or(SchemaError::TableNotFound {
                table: tab.to_string(),
            })?;
        if schema.column(name).is_none() {
            return Err(SchemaError::NotFoundField);
        }
        schema.drop_column(name);
        schema.schema_id += 1;
        Ok(())
    }

    pub fn change_table_column(
        &self,
        tab: &str,
        name: &str,
        new_column: TableColumn,
    ) -> Result<()> {
        // todo : meta need alter method
        let mut schema = self
            .get_table_schema(tab)?
            .ok_or(SchemaError::TableNotFound {
                table: tab.to_string(),
            })?;
        if schema.column(name).is_none() {
            return Err(SchemaError::NotFoundField);
        }
        schema.change_column(name, new_column);
        schema.schema_id += 1;
        Ok(())
    }
}
