use arrow_schema::DataType;
use once_cell::sync::OnceCell;

pub mod stream;

pub fn str_dict_data_type() -> &'static DataType {
    static REE_DATA_TYPE: OnceCell<DataType> = OnceCell::new();
    REE_DATA_TYPE
        .get_or_init(|| DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)))
}
