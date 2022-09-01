use super::{
    ast::{ExtStatement, ObjectType},
    session::IsiphoSessionCtx,
    Result,
};

use datafusion::logical_plan::LogicalPlan as DFPlan;

#[derive(Debug, Clone)]
pub enum Plan {
    /// Query plan
    Query(QueryPlan),
    /// Drop table plan
    Drop(DropPlan),
}

#[derive(Debug, Clone)]
pub struct QueryPlan {
    pub df_plan: DFPlan,
}

#[derive(Debug, Clone)]
pub struct DropPlan {
    /// Table name
    pub object_name: String,
    /// If exists
    pub if_exist: bool,
    ///ObjectType
    pub obj_type: ObjectType,
}

pub trait LogicalPlanner {
    fn create_logical_plan(
        &self,
        statement: ExtStatement,
        session: &IsiphoSessionCtx,
    ) -> Result<Plan>;
}
