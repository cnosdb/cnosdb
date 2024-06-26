use std::fmt::Display;

use config::common::{RequestLimiterConfig, TenantLimiterConfig, TenantObjectLimiterConfig};
use derive_builder::Builder;
use serde::{Deserialize, Serialize};

use crate::oid::{Identifier, Oid};
use crate::schema::utils::CnosDuration;

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

    pub fn to_own_options(self) -> TenantOptions {
        self.options
    }
}

#[derive(Debug, Default, Clone, Builder, Serialize, Deserialize)]
#[builder(setter(into, strip_option), default)]
pub struct TenantOptions {
    pub comment: Option<String>,
    pub limiter_config: Option<TenantLimiterConfig>,
    drop_after: Option<CnosDuration>,
    tenant_is_hidden: bool,
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
        if let Some(drop_after) = value.get_drop_after() {
            builder.drop_after(drop_after);
        }
        builder.tenant_is_hidden(false);
        builder
    }
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

impl TenantOptions {
    pub fn object_config(&self) -> Option<&TenantObjectLimiterConfig> {
        match self.limiter_config {
            Some(ref limit_config) => limit_config.object_config.as_ref(),
            None => None,
        }
    }

    pub fn request_config(&self) -> Option<&RequestLimiterConfig> {
        match self.limiter_config {
            Some(ref limit_config) => limit_config.request_config.as_ref(),
            None => None,
        }
    }

    pub fn get_tenant_is_hidden(&self) -> bool {
        self.tenant_is_hidden
    }

    pub fn set_tenant_is_hidden(&mut self, tenant_is_hidden: bool) {
        self.tenant_is_hidden = tenant_is_hidden;
    }

    pub fn get_drop_after(&self) -> Option<CnosDuration> {
        self.drop_after.clone()
    }
}

impl Display for TenantOptions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(ref e) = self.comment {
            write!(f, "comment={},", e)?;
        }

        if let Some(ref e) = self.limiter_config {
            write!(f, "limiter={e:?},")?;
        } else {
            write!(f, "limiter=None,")?;
        }

        Ok(())
    }
}
