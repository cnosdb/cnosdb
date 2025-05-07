use std::fmt::{self, Debug};
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use datafusion::common::{DFSchemaRef, OwnedTableReference, ToDFSchema};
use datafusion::logical_expr::{LogicalPlan, TableSource, UserDefinedLogicalNodeCore};
use datafusion::prelude::{Column, Expr};
use spi::DFResult;

#[derive(Clone)]
pub struct UpdateNode {
    pub table_name: OwnedTableReference,
    // table for update
    pub table_source: Arc<dyn TableSource>,
    // set
    pub assigns: Vec<(Column, Expr)>,
    // where
    pub filter: Expr,
    // The schema description of the output
    pub schema: DFSchemaRef,
}

impl Hash for UpdateNode {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.table_name.hash(state);
        self.assigns.hash(state);
        self.filter.hash(state);
        self.schema.hash(state);
    }
}

impl PartialEq for UpdateNode {
    fn eq(&self, other: &Self) -> bool {
        self.table_name == other.table_name
            && self.assigns == other.assigns
            && self.filter == other.filter
            && self.schema == other.schema
    }
}

impl Eq for UpdateNode {}

impl UpdateNode {
    pub fn try_new(
        table_name: impl Into<OwnedTableReference>,
        table_source: Arc<dyn TableSource>,
        assigns: Vec<(Column, Expr)>,
        filter: Expr,
    ) -> DFResult<Self> {
        let schema = Arc::new(table_source.schema().to_dfschema()?);

        Ok(Self {
            table_name: table_name.into(),
            table_source,
            assigns,
            filter,
            schema,
        })
    }
}

impl Debug for UpdateNode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.fmt_for_explain(f)
    }
}

impl UserDefinedLogicalNodeCore for UpdateNode {
    fn name(&self) -> &str {
        "Update"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![]
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }

    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let assigns_str = self
            .assigns
            .iter()
            .map(|(col, val)| format!("{col}={val}"))
            .collect::<Vec<_>>()
            .join(", ");

        write!(
            f,
            "Update: table={}, set=[{assigns_str}], where=[{}]",
            self.table_name, self.filter
        )
    }

    fn with_exprs_and_inputs(&self, exprs: Vec<Expr>, inputs: Vec<LogicalPlan>) -> Self {
        assert_eq!(inputs.len(), 0, "input size inconsistent");

        Self {
            table_name: self.table_name.clone(),
            table_source: self.table_source.clone(),
            assigns: self.assigns.clone(),
            filter: self.filter.clone(),
            schema: self.schema.clone(),
        }
    }
}
