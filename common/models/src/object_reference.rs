use std::fmt::Display;
use std::sync::Arc;

use datafusion::common::Result as DFResult;
use datafusion::error::DataFusionError;
use datafusion::sql::TableReference;

pub trait Resolve {
    fn resolve_object(
        self,
        default_catalog: Arc<str>,
        default_schema: Arc<str>,
    ) -> DFResult<ResolvedTable>;
}

impl Resolve for TableReference {
    fn resolve_object(
        self,
        default_catalog: Arc<str>,
        default_schema: Arc<str>,
    ) -> DFResult<ResolvedTable> {
        let result = match self {
            Self::Full { .. } => {
                // check table reference name
                return Err(DataFusionError::Plan(format!(
                    "Database object names must have at most two parts, but found: '{}'",
                    self
                )));
            }
            Self::Partial { schema, table } => ResolvedTable {
                tenant: default_catalog,
                database: schema,
                table,
            },
            Self::Bare { table } => ResolvedTable {
                tenant: default_catalog,
                database: default_schema,
                table,
            },
        };

        Ok(result)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedTable {
    tenant: Arc<str>,
    database: Arc<str>,
    table: Arc<str>,
}

impl ResolvedTable {
    pub fn tenant(&self) -> &str {
        self.tenant.as_ref()
    }

    pub fn database(&self) -> &str {
        self.database.as_ref()
    }

    pub fn table(&self) -> &str {
        self.table.as_ref()
    }
}

impl Display for ResolvedTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let Self {
            tenant,
            database,
            table,
        } = self;
        write!(f, "{tenant}.{database}.{table}")
    }
}
