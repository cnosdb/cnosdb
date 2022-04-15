/*
use crate::forward_index::field_info::{FieldInfo, ValueType};
use crate::forward_index::series_info::SeriesInfo;
use crate::forward_index::tags::Tag;
use crate::forward_index::ForwardIndex;
use ValueType::Float;

#[test]
fn test_add_series() {
    let mut fidx = ForwardIndex::new("/tmp/test");

    let mut fields = Vec::<FieldInfo>::new();
    fields.push(FieldInfo {
        id: 1,
        value_type: Float,
    });
    let mut tags = Vec::<Tag>::new();
    tags.push(Tag {
        key: Vec::<u8>::from("app"),
        value: Vec::<u8>::from("test"),
    });
    fidx.add_series(SeriesInfo {
        id: 123,
        fields,
        tags,
    });

    fidx.close();
}
*/