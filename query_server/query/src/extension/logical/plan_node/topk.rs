use std::any::Any;
use std::fmt::{self, Debug, Display};
use std::sync::Arc;

use datafusion::common::DFSchemaRef;
use datafusion::logical_expr::{LogicalPlan, UserDefinedLogicalNode};
use datafusion::prelude::Expr;

#[derive(Debug, Clone, Copy)]
pub enum TopKStep {
    SINGLE,
    PARTIAL,
    FINAL,
}

#[derive(Debug, Clone)]
pub struct TopKOptions {
    /// The maxium number of values
    pub skip: Option<usize>,
    /// The maxium number of values
    pub k: usize,
    pub step: TopKStep,
}

impl Display for TopKOptions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "TopKOptions: k={}, skip={}, step={:#?}",
            self.k,
            self.skip.map_or("None".to_string(), |x| x.to_string()),
            self.step,
        )
    }
}

pub struct TopKPlanNode {
    /// The sort expressions
    expr: Vec<Expr>,
    /// The incoming logical plan
    input: Arc<LogicalPlan>,

    options: TopKOptions,
}

impl TopKPlanNode {
    pub fn new(expr: Vec<Expr>, input: Arc<LogicalPlan>, skip: Option<usize>, k: usize) -> Self {
        Self {
            expr,
            input,
            options: TopKOptions {
                skip,
                k,
                step: TopKStep::SINGLE,
            },
        }
    }

    pub fn options(&self) -> &TopKOptions {
        &self.options
    }

    pub fn k(&self) -> usize {
        self.options.k
    }

    pub fn skip(&self) -> &Option<usize> {
        &self.options.skip
    }

    pub fn step(&self) -> &TopKStep {
        &self.options.step
    }

    pub fn input(&self) -> &Arc<LogicalPlan> {
        &self.input
    }

    pub fn expr(&self) -> &[Expr] {
        &self.expr
    }
}

impl Debug for TopKPlanNode {
    /// For TopK, use explain format for the Debug format. Other types
    /// of nodes may
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.fmt_for_explain(f)
    }
}

impl UserDefinedLogicalNode for TopKPlanNode {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.input]
    }

    /// Schema for TopK is the same as the input
    fn schema(&self) -> &DFSchemaRef {
        self.input.schema()
    }

    fn expressions(&self) -> Vec<Expr> {
        self.expr.clone()
    }

    /// For example: `TopK: k=10, skip=None`
    fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let expr: Vec<String> = self.expr.iter().map(|e| e.to_string()).collect();

        write!(f, "TopK: [{}], {}", expr.join(","), self.options,)
    }

    fn from_template(
        &self,
        exprs: &[Expr],
        inputs: &[LogicalPlan],
    ) -> Arc<dyn UserDefinedLogicalNode> {
        assert_eq!(inputs.len(), 1, "input size inconsistent");
        Arc::new(TopKPlanNode {
            input: Arc::new(inputs[0].clone()),
            expr: exprs.to_vec(),
            options: self.options.clone(),
        })
    }
}
