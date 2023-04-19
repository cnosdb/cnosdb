use models::meta_data::DatabaseInfo;
use models::predicate::domain::PredicateRef;
use models::schema::TskvTableSchemaRef;

pub struct TableLayoutHandle {
    pub db: DatabaseInfo,
    pub table: TskvTableSchemaRef,
    pub predicate: PredicateRef,
}
