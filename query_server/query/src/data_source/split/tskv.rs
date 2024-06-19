use models::predicate::domain::PredicateRef;
use models::schema::tskv_table_schema::TskvTableSchemaRef;

pub struct TableLayoutHandle {
    pub table: TskvTableSchemaRef,
    pub predicate: PredicateRef,
}
