use std::fmt::Display;

use datafusion::common::Result as DFResult;
use datafusion::error::DataFusionError;
use datafusion::sql::TableReference;

pub trait Resolve {
    fn resolve_object(self, default_catalog: &str, default_schema: &str)
        -> DFResult<ResolvedTable>;
}

impl Resolve for TableReference<'_> {
    fn resolve_object(
        self,
        default_catalog: &str,
        default_schema: &str,
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
                tenant: default_catalog.into(),
                database: schema.into(),
                table: table.into(),
            },
            Self::Bare { table } => ResolvedTable {
                tenant: default_catalog.into(),
                database: default_schema.into(),
                table: table.into(),
            },
        };

        Ok(result)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedTable {
    tenant: String,
    database: String,
    table: String,
}

impl ResolvedTable {
    pub fn tenant(&self) -> &str {
        &self.tenant
    }

    pub fn database(&self) -> &str {
        &self.database
    }

    pub fn table(&self) -> &str {
        &self.table
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
