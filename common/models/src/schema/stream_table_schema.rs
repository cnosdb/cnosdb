use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration as StdDuration;

use datafusion::arrow::datatypes::SchemaRef;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Hash)]
pub struct Watermark {
    pub column: String,
    pub delay: StdDuration,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct StreamTable {
    tenant: Arc<str>,
    db: Arc<str>,
    name: Arc<str>,
    schema: SchemaRef,
    stream_type: String,
    watermark: Watermark,
    extra_options: HashMap<String, String>,
}

impl StreamTable {
    pub fn new(
        tenant: impl Into<Arc<str>>,
        db: impl Into<Arc<str>>,
        name: impl Into<Arc<str>>,
        schema: SchemaRef,
        stream_type: impl Into<String>,
        watermark: Watermark,
        extra_options: HashMap<String, String>,
    ) -> Self {
        Self {
            tenant: tenant.into(),
            db: db.into(),
            name: name.into(),
            schema,
            stream_type: stream_type.into(),
            watermark,
            extra_options,
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn tenant(&self) -> &str {
        &self.tenant
    }

    pub fn db(&self) -> &str {
        &self.db
    }

    pub fn stream_type(&self) -> &str {
        &self.stream_type
    }

    pub fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    pub fn watermark(&self) -> &Watermark {
        &self.watermark
    }

    pub fn extra_options(&self) -> &HashMap<String, String> {
        &self.extra_options
    }
}
