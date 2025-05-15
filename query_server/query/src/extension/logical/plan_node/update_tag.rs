use std::fmt::{self, Debug};
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use datafusion::common::{DFSchema, DFSchemaRef};
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::logical_expr::utils::exprlist_to_fields;
use datafusion::logical_expr::{Extension, LogicalPlan, TableSource, UserDefinedLogicalNodeCore};
use datafusion::prelude::{Column, Expr};

#[derive(PartialOrd)]
pub struct UpdateTagPlanNode {
    pub table_name: String,
    pub table_source: Arc<dyn TableSource>,
    pub scan: Arc<LogicalPlan>,
    pub assigns: Vec<(Column, Expr)>,
    pub schema: DFSchemaRef,
    pub exprs: Vec<Expr>,
}

impl UpdateTagPlanNode {
    pub fn try_new(
        table_name: String,
        table_source: Arc<dyn TableSource>,
        scan: Arc<LogicalPlan>,
        assigns: Vec<(Column, Expr)>,
        exprs: Vec<Expr>,
    ) -> Result<Self, DataFusionError> {
        let schema = Arc::new(DFSchema::new_with_metadata(
            exprlist_to_fields(&exprs, scan.as_ref())?,
            scan.schema().metadata().clone(),
        )?);

        Ok(Self {
            table_name,
            table_source,
            scan,
            assigns,
            schema,
            exprs,
        })
    }
}

impl Debug for UpdateTagPlanNode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.fmt_for_explain(f)
    }
}

impl Hash for UpdateTagPlanNode {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.table_name.hash(state);
        self.scan.hash(state);
        self.assigns.hash(state);
        self.schema.hash(state);
        self.exprs.hash(state);
    }
}

impl PartialEq for UpdateTagPlanNode {
    fn eq(&self, other: &Self) -> bool {
        self.table_name == other.table_name
            && self.scan == other.scan
            && self.assigns == other.assigns
            && self.schema == other.schema
            && self.exprs == other.exprs
    }
}

impl Eq for UpdateTagPlanNode {}

impl UserDefinedLogicalNodeCore for UpdateTagPlanNode {
    fn name(&self) -> &str {
        "UpdateTagValuePlanNode"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.scan]
    }

    fn schema(&self) -> &datafusion::common::DFSchemaRef {
        &self.schema
    }

    fn expressions(&self) -> Vec<Expr> {
        self.exprs.clone()
    }

    fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let out_exprs: Vec<String> = self.exprs.iter().map(|e| e.to_string()).collect();
        write!(
            f,
            "UpdateTagValue: table={}, {}",
            self.table_name,
            out_exprs.join(",")
        )?;

        Ok(())
    }

    fn with_exprs_and_inputs(&self, exprs: Vec<Expr>, inputs: Vec<LogicalPlan>) -> DFResult<Self> {
        if inputs.len() != 1 {
            return Err(DataFusionError::Plan(
                "UpdateTagPlanNode only supports one input".to_string(),
            ));
        }
        Ok(UpdateTagPlanNode {
            table_name: self.table_name.clone(),
            table_source: self.table_source.clone(),
            scan: Arc::new(inputs[0].clone()),
            assigns: self.assigns.clone(),
            exprs,
            schema: self.schema.clone(),
        })
    }
}

impl From<UpdateTagPlanNode> for LogicalPlan {
    fn from(value: UpdateTagPlanNode) -> Self {
        LogicalPlan::Extension(Extension {
            node: Arc::new(value),
        })
    }
}
