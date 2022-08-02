use models::{FieldInfo, SeriesInfo, Tag, ValueType};

use crate::index::forward_index::ForwardIndex;

#[tokio::test]
async fn test_forward_index() {
    let mut fidx = ForwardIndex::from("/tmp/test.fidx");
    fidx.load_cache_file().await.unwrap();

    let series_info = SeriesInfo::new(
        vec![Tag::new(b"hello".to_vec(), b"123".to_vec())],
        vec![
            FieldInfo::new(0, b"cpu".to_vec(), ValueType::Float),
            FieldInfo::new(0, b"mem".to_vec(), ValueType::Float),
        ],
    );
    fidx.add_series_info_if_not_exists(series_info)
        .await
        .unwrap();

    fidx.close().await.unwrap();
}
