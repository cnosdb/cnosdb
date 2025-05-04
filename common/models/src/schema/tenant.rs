use config::common::{RequestLimiterConfig, TenantLimiterConfig, TenantObjectLimiterConfig};
use datafusion::sql::sqlparser::ast::escape_quoted_string;
use derive_builder::Builder;
use serde::{Deserialize, Serialize};
use utils::duration::CnosDuration;

use crate::errors::DumpSnafu;
use crate::oid::{Identifier, Oid};
use crate::sql::write_sql_with_option_kv;
use crate::ModelError;

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Tenant {
    id: Oid,
    name: String,
    options: TenantOptions,
}

impl Identifier<Oid> for Tenant {
    fn id(&self) -> &Oid {
        &self.id
    }

    fn name(&self) -> &str {
        &self.name
    }
}

impl Tenant {
    pub fn new(id: Oid, name: String, options: TenantOptions) -> Self {
        Self { id, name, options }
    }

    pub fn options(&self) -> &TenantOptions {
        &self.options
    }

    pub fn into_options(self) -> TenantOptions {
        self.options
    }
}

#[derive(Debug, Default, Clone, Builder, Serialize, Deserialize)]
#[builder(setter(into, strip_option), default)]
pub struct TenantOptions {
    ///
    pub comment: Option<String>,
    ///
    pub limiter_config: Option<TenantLimiterConfig>,
    ///
    pub drop_after: Option<CnosDuration>,
    ///
    pub tenant_is_hidden: bool,
}

impl TenantOptionsBuilder {
    pub fn unset_comment(&mut self) {
        self.comment = None
    }

    pub fn unset_limiter_config(&mut self) {
        self.limiter_config = None
    }

    pub fn unset_drop_after(&mut self) {
        self.drop_after = None;
    }
}

impl From<TenantOptions> for TenantOptionsBuilder {
    fn from(value: TenantOptions) -> Self {
        let mut builder = TenantOptionsBuilder::default();
        if let Some(comment) = value.comment.clone() {
            builder.comment(comment);
        }
        if let Some(config) = value.limiter_config.clone() {
            builder.limiter_config(config);
        }
        if let Some(duration) = value.drop_after() {
            builder.drop_after(duration.clone());
        }
        builder.tenant_is_hidden(false);
        builder
    }
}

impl TenantOptions {
    pub fn object_config(&self) -> Option<&TenantObjectLimiterConfig> {
        self.limiter_config.as_ref()?.object_config.as_ref()
    }

    pub fn request_config(&self) -> Option<&RequestLimiterConfig> {
        self.limiter_config.as_ref()?.request_config.as_ref()
    }

    pub fn tenant_is_hidden(&self) -> bool {
        self.tenant_is_hidden
    }

    pub fn set_tenant_is_hidden(&mut self, tenant_is_hidden: bool) {
        self.tenant_is_hidden = tenant_is_hidden;
    }

    pub fn drop_after(&self) -> Option<&CnosDuration> {
        self.drop_after.as_ref()
    }

    fn is_empty(&self) -> bool {
        self.comment.is_none() && self.limiter_config.is_none() && self.drop_after.is_none()
    }

    /// Write the options to a SQL string for DUMP action.
    /// If the options are empty, nothing will be written.
    ///
    /// The format of the SQL string is:
    /// ```SQL
    /// [<SPACE> with
    ///   [comment='<STRING>']
    ///   [, drop_after='<DURATION>']
    ///   [, _limiter='<JSON_STRING>']
    /// ]
    /// ```
    pub fn write_as_dump_sql(&self, buf: &mut String) -> Result<(), ModelError> {
        if self.is_empty() {
            return Ok(());
        }

        let mut did_write = false;
        if let Some(ref comment) = self.comment {
            write_sql_with_option_kv(
                buf,
                &mut did_write,
                "comment",
                escape_quoted_string(comment, '\''),
            );
        }
        if let Some(ref dur) = self.drop_after {
            write_sql_with_option_kv(
                buf,
                &mut did_write,
                "drop_after",
                escape_quoted_string(&dur.to_string(), '\''),
            );
        }
        if let Some(ref cfg) = self.limiter_config {
            match serde_json::to_string(cfg) {
                Ok(cfg_json) => {
                    write_sql_with_option_kv(
                        buf,
                        &mut did_write,
                        "_limiter",
                        escape_quoted_string(&cfg_json, '\''),
                    );
                }
                Err(e) => {
                    return DumpSnafu {
                        msg: format!("Failed to serialize limiter config: {e}"),
                    }
                    .fail()
                }
            }
        }

        Ok(())
    }
}
