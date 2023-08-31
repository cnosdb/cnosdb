use std::sync::Arc;

use meta::error::{MetaError, MetaResult};
use meta::model::{MetaClientRef, MetaRef};
use models::codec::Encoding;
use models::schema::{
    ColumnType, DatabaseSchema, TableColumn, TableSchema, TskvTableSchema, TskvTableSchemaRef,
};
use models::ColumnId;
use protos::models::FieldType;
use trace::error;

use crate::schema::error::{Result, SchemaError};

const TIME_STAMP_NAME: &str = "time";

#[derive(Debug)]
pub struct DBschemas {
    tenant_name: String,
    database_name: String,
    client: MetaClientRef,
}

impl DBschemas {
    pub async fn new(db_schema: DatabaseSchema, meta: MetaRef) -> Result<Self> {
        let client =
            meta.tenant_meta(db_schema.tenant_name())
                .await
                .ok_or(SchemaError::TenantNotFound {
                    tenant: db_schema.tenant_name().to_string(),
                })?;
        if client.get_db_schema(db_schema.database_name())?.is_none() {
            client.create_db(db_schema.clone()).await?;
        }
        Ok(Self {
            tenant_name: db_schema.tenant_name().to_string(),
            database_name: db_schema.database_name().to_string(),
            client,
        })
    }

    pub fn database_name(&self) -> String {
        self.database_name.clone()
    }

    pub fn check_field_type_from_cache(
        &self,
        table_name: &str,
        tag_names: &[&str],
        field_names: &[&str],
        field_type: &[FieldType],
    ) -> Result<()> {
        let schema = self
            .client
            .get_tskv_table_schema(&self.database_name, table_name)?
            .ok_or(SchemaError::DatabaseNotFound {
                database: self.database_name.clone(),
            })?;

        for (field_name, field_type) in field_names.iter().zip(field_type) {
            if let Some(v) = schema.column(field_name) {
                if field_type.0 != v.column_type.field_type() as i32 {
                    error!(
                        "type mismatch, point: {}, schema: {}",
                        field_type.0,
                        v.column_type.field_type()
                    );
                    return Err(SchemaError::FieldType {
                        field: field_name.to_string(),
                    });
                }
            } else {
                return Err(SchemaError::FieldNotFound {
                    database: self.database_name(),
                    table: table_name.to_string(),
                    field: field_name.to_string(),
                });
            }
        }

        for tag_name in tag_names {
            if let Some(v) = schema.column(tag_name) {
                if ColumnType::Tag != v.column_type {
                    error!("type mismatch, point: tag, schema: {}", &v.column_type);
                    return Err(SchemaError::FieldType {
                        field: tag_name.to_string(),
                    });
                }
            } else {
                return Err(SchemaError::FieldNotFound {
                    database: self.database_name(),
                    table: table_name.to_string(),
                    field: tag_name.to_string(),
                });
            }
        }
        Ok(())
    }

    pub async fn check_field_type_or_else_add(
        &self,
        db_name: &str,
        table_name: &str,
        tag_names: &[&str],
        field_names: &[&str],
        field_type: &[FieldType],
    ) -> Result<bool> {
        //load schema first from cache,or else from storage and than cache it!
        let schema = self.client.get_tskv_table_schema(db_name, table_name)?;
        let db_schema =
            self.client
                .get_db_schema(db_name)?
                .ok_or(SchemaError::DatabaseNotFound {
                    database: db_name.to_string(),
                })?;
        let precision = db_schema.config.precision_or_default();
        let mut new_schema = false;
        let mut schema = match schema {
            None => {
                let mut schema = TskvTableSchema::default();
                schema.tenant = self.tenant_name.clone();
                schema.db = db_name.to_string();
                schema.name = table_name.to_string();
                new_schema = true;
                schema
            }
            Some(schema) => schema.as_ref().clone(),
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
                        return Err(SchemaError::FieldType {
                            field: field.name.to_owned(),
                        });
                    }
                }
                None => {
                    schema_change = true;
                    field.id = schema.columns().len() as ColumnId;
                    schema.add_column(field.clone());
                }
            }
            Ok(())
        };
        //check timestamp
        check_fn(&mut TableColumn::new_with_default(
            TIME_STAMP_NAME.to_string(),
            ColumnType::Time((*precision).into()),
        ))?;

        //check tags
        for tag_name in tag_names {
            check_fn(&mut TableColumn::new_with_default(
                tag_name.to_string(),
                ColumnType::Tag,
            ))?
        }

        //check fields
        for (field_name, field_type) in field_names.iter().zip(field_type) {
            check_fn(&mut TableColumn::new_with_default(
                field_name.to_string(),
                ColumnType::from_i32(field_type.0),
            ))?
        }

        //schema changed store it
        if new_schema {
            schema.schema_id = 0;
            let schema: TskvTableSchemaRef = schema.into();
            let res = self
                .client
                .create_table(&TableSchema::TsKvTableSchema(schema.clone()))
                .await;
            self.check_create_table_res(res, schema).await?;
        } else if schema_change {
            schema.schema_id += 1;
            self.client
                .update_table(&TableSchema::TsKvTableSchema(schema.into()))
                .await?;
        }
        Ok(new_schema || schema_change)
    }

    async fn check_create_table_res(
        &self,
        res: MetaResult<()>,
        schema: TskvTableSchemaRef,
    ) -> Result<()> {
        match res {
            Ok(_) => Ok(()),
            Err(e) => {
                if let MetaError::TableAlreadyExists { .. } = e {
                    let schema_get = self
                        .client
                        .get_tskv_table_schema(&schema.db, &schema.name)
                        .map_err(|e| MetaError::Retry { msg: e.to_string() })?;
                    let schema_get = match schema_get {
                        None => self
                            .client
                            .get_tskv_table_schema_by_meta(&schema.db, &schema.name)
                            .await
                            .map_err(|e| MetaError::Retry { msg: e.to_string() })?
                            .ok_or(MetaError::Retry {
                                msg: "Table not found after TableAlreadyExists".to_string(),
                            })?,
                        Some(schema) => schema,
                    };

                    if schema.tenant == schema_get.tenant
                        && schema.db == schema_get.db
                        && schema.columns() == schema_get.columns()
                    {
                        Ok(())
                    } else {
                        for _ in 0..3 {
                            let schema_get = self
                                .client
                                .get_tskv_table_schema(&schema.db, &schema.name)
                                .map_err(|e| MetaError::Retry { msg: e.to_string() })?;
                            let schema_get = match schema_get {
                                None => self
                                    .client
                                    .get_tskv_table_schema_by_meta(&schema.db, &schema.name)
                                    .await
                                    .map_err(|e| MetaError::Retry { msg: e.to_string() })?
                                    .ok_or(MetaError::Retry {
                                        msg: "Table not found when retry update table schema"
                                            .to_string(),
                                    })?,
                                Some(schema) => schema,
                            };
                            let mut schema = schema.as_ref().clone();
                            schema.schema_id = schema_get.schema_id + 1;
                            let schema = Arc::new(schema);
                            if self
                                .client
                                .update_table(&TableSchema::TsKvTableSchema(schema))
                                .await
                                .is_ok()
                            {
                                return Ok(());
                            }
                        }
                        Err(e.into())
                    }
                } else {
                    Err(e.into())
                }
            }
        }
    }

    pub fn get_table_schema(&self, tab: &str) -> Result<Option<TskvTableSchemaRef>> {
        let schema = self
            .client
            .get_tskv_table_schema(&self.database_name, tab)?;

        Ok(schema)
    }

    pub async fn get_table_schema_by_meta(&self, tab: &str) -> Result<Option<TskvTableSchemaRef>> {
        let schema = self
            .client
            .get_tskv_table_schema_by_meta(&self.database_name, tab)
            .await?;

        Ok(schema)
    }

    pub fn list_tables(&self) -> Result<Vec<String>> {
        let tables = self.client.list_tables(&self.database_name)?;
        Ok(tables)
    }

    pub async fn del_table_schema(&self, tab: &str) -> Result<()> {
        self.client.drop_table(&self.database_name, tab).await?;
        Ok(())
    }

    pub fn db_schema(&self) -> Result<DatabaseSchema> {
        let db_schema =
            self.client
                .get_db_schema(&self.database_name)?
                .ok_or(MetaError::DatabaseNotFound {
                    database: self.database_name.clone(),
                })?;
        Ok(db_schema)
    }
}
