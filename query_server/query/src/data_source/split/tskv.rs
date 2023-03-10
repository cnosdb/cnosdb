use models::meta_data::DatabaseInfoRef;
use models::predicate::domain::PredicateRef;
use models::schema::TskvTableSchemaRef;

pub struct TableLayoutHandle {
    pub db: DatabaseInfoRef,
    pub table: TskvTableSchemaRef,
    pub predicate: PredicateRef,
}
