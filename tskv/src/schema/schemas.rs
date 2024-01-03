use std::borrow::Cow;
use std::sync::Arc;

use async_recursion::async_recursion;
use meta::error::{MetaError, MetaResult};
use meta::model::{MetaClientRef, MetaRef};
use models::codec::Encoding;
use models::schema::{
    ColumnType, DatabaseSchema, TableColumn, TableSchema, TskvTableSchema, TskvTableSchemaRef,
};

use crate::database::FbSchema;
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

    pub fn database_name(&self) -> &str {
        &self.database_name
    }

    pub fn tenant_name(&self) -> &str {
        &self.tenant_name
    }
    #[async_recursion]
    async fn check_field_type_or_else_add_impl<'a>(
        &self,
        fb_schema: &'a FbSchema<'a>,
        get_schema_from_meta: bool,
    ) -> Result<TskvTableSchemaRef> {
        let mut schema_changed = false;
        let mut new_schema = false;
        if get_schema_from_meta {
            self.get_table_schema_by_meta(fb_schema.table).await?;
        } else {
            self.get_table_schema(fb_schema.table)?;
        }
        let opt_schema = self.get_table_schema(fb_schema.table)?;
        let mut schema = match opt_schema.as_ref() {
            Some(schema) => Cow::Borrowed(schema.as_ref()),
            None => {
                let mut schema = TskvTableSchema::new(
                    self.tenant_name().into(),
                    self.database_name().into(),
                    fb_schema.table.to_string(),
                    vec![],
                );
                let db_schema = self.db_schema()?;
                let precision = db_schema.config.precision_or_default();
                schema.add_column(TableColumn::new_time_column(
                    schema.next_column_id(),
                    (*precision).into(),
                ));
                new_schema = true;
                Cow::Owned(schema)
            }
        };

        for tag_name in &fb_schema.tag_names {
            match schema.column(tag_name) {
                Some(column) => {
                    if !column.column_type.is_tag() {
                        return Err(SchemaError::ColumnTypeError {
                            column: tag_name.to_string(),
                            found: ColumnType::Tag,
                            expected: column.column_type.clone(),
                        });
                    }
                }
                None => {
                    let next_column_id = schema.next_column_id();
                    schema.to_mut().add_column(TableColumn::new_tag_column(
                        next_column_id,
                        tag_name.to_string(),
                    ));
                    schema_changed = true;
                }
            };
        }

        for (field_name, field_type) in fb_schema
            .field_names
            .iter()
            .zip(fb_schema.field_types.iter())
        {
            match schema.column(field_name) {
                Some(column) => {
                    let column_type =
                        models::schema::ColumnType::from_proto_field_type(*field_type);
                    if !column.column_type.matches_type(&column_type) {
                        return Err(SchemaError::ColumnTypeError {
                            column: field_name.to_string(),
                            found: column_type,
                            expected: column.column_type.clone(),
                        });
                    }
                }
                None => {
                    let next_column_id = schema.next_column_id();
                    schema.to_mut().add_column(TableColumn::new(
                        next_column_id,
                        field_name.to_string(),
                        ColumnType::from_proto_field_type(*field_type),
                        Encoding::Default,
                    ));
                    schema_changed = true;
                }
            }
        }
        //schema changed store it
        let schema = if new_schema {
            schema.to_mut().schema_id = 0;
            let schema: TskvTableSchemaRef = schema.into_owned().into();
            let res = self
                .client
                .create_table(&TableSchema::TsKvTableSchema(schema.clone()))
                .await;
            if let Err(MetaError::TableAlreadyExists { .. }) = res.as_ref() {
                self.check_field_type_or_else_add_impl(fb_schema, true)
                    .await?;
            } else {
                res?;
            }
            schema
        } else if schema_changed {
            schema.to_mut().schema_id += 1;
            let schema: TskvTableSchemaRef = schema.into_owned().into();
            let res = self
                .client
                .update_table(&TableSchema::TsKvTableSchema(schema.clone()))
                .await;
            if let Err(MetaError::UpdateTableConflict { .. }) = &res {
                self.check_field_type_or_else_add_impl(fb_schema, true)
                    .await?;
            } else {
                res?;
            }
            schema
        } else {
            // Safety
            // if opt_schema is none then create new schema
            unsafe { opt_schema.unwrap_unchecked() }
        };
        Ok(schema)
    }

    pub async fn check_field_type_or_else_add(
        &self,
        fb_schema: &FbSchema<'_>,
    ) -> Result<TskvTableSchemaRef> {
        //load schema first from cache
        self.check_field_type_or_else_add_impl(fb_schema, false)
            .await
    }

    pub fn client(&self) -> &MetaClientRef {
        &self.client
    }

    pub async fn check_create_table_res(
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

    async fn check_update_table_res(
        &self,
        res: MetaResult<()>,
        schema: TskvTableSchemaRef,
    ) -> Result<()> {
        let mut schema_id = schema.schema_id;
        if let Err(ref e) = res {
            if matches!(e, MetaError::UpdateTableConflict { .. }) {
                for _ in 0..3 {
                    let mut schema = schema.as_ref().clone();
                    schema_id += 1;
                    schema.schema_id = schema_id;
                    if self
                        .client
                        .update_table(&TableSchema::TsKvTableSchema(Arc::new(schema)))
                        .await
                        .is_ok()
                    {
                        return Ok(());
                    }
                }
            } else {
                return res.map_err(|e| e.into());
            }
        }
        Ok(())
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
