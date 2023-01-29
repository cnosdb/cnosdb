use models::{
    meta_data::DatabaseInfoRef, predicate::domain::PredicateRef, schema::TskvTableSchemaRef,
};

pub struct TableLayoutHandle {
    pub db: DatabaseInfoRef,
    pub table: TskvTableSchemaRef,
    pub predicate: PredicateRef,
}
