use std::fmt::{self, Debug};
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use datafusion::common::{DFSchemaRef, TableReference, ToDFSchema};
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::logical_expr::{LogicalPlan, TableSource, UserDefinedLogicalNodeCore};
use datafusion::prelude::{Column, Expr};

#[derive(Clone)]
pub struct UpdateNode {
    pub table_name: TableReference,
    // table for update
    pub table_source: Arc<dyn TableSource>,
    // set
    pub assigns: Vec<(Column, Expr)>,
    // where
    pub filter: Expr,
    // The schema description of the output
    pub schema: DFSchemaRef,
}

impl PartialOrd for UpdateNode {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        match self.table_name.partial_cmp(&other.table_name) {
            Some(core::cmp::Ordering::Equal) => {}
            ord => return ord,
        }
        match self.assigns.partial_cmp(&other.assigns) {
            Some(core::cmp::Ordering::Equal) => {}
            ord => return ord,
        }
        match self.filter.partial_cmp(&other.filter) {
            Some(core::cmp::Ordering::Equal) => {}
            ord => return ord,
        }
        self.schema.fields().partial_cmp(other.schema.fields())
    }
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
        table_name: impl Into<TableReference>,
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

    fn with_exprs_and_inputs(&self, _exprs: Vec<Expr>, inputs: Vec<LogicalPlan>) -> DFResult<Self> {
        if !inputs.is_empty() {
            return Err(DataFusionError::Plan(
                "UpdateNode should have exactly one input".to_string(),
            ));
        }

        Ok(Self {
            table_name: self.table_name.clone(),
            table_source: self.table_source.clone(),
            assigns: self.assigns.clone(),
            filter: self.filter.clone(),
            schema: self.schema.clone(),
        })
    }
}
