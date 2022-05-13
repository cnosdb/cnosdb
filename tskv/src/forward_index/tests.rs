use super::*;

#[tokio::test]
async fn test_forward_index() {
    let mut fidx = ForwardIndex::from("/tmp/test.fidx");
    fidx.load_cache_file().await.unwrap();

    let mut series_info = SeriesInfo::new();
    series_info.tags.push(Tag::from_parts("hello", "123"));
    series_info.field_infos.push(FieldInfo::from_parts("cpu", ValueType::Float));
    series_info.field_infos.push(FieldInfo::from_parts("mem", ValueType::Float));

    fidx.add_series_info_if_not_exists(series_info).await.unwrap();

    fidx.close().await.unwrap();
}