use models::predicate::domain::PredicateRef;
use models::schema::TskvTableSchemaRef;

pub struct TableLayoutHandle {
    pub table: TskvTableSchemaRef,
    pub predicate: PredicateRef,
}
