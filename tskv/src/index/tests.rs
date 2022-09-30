// #![cfg(test)]
//
// use std::{
//     collections::{HashMap, HashSet},
//     fs,
// };
//
// use models::{FieldInfo, SeriesInfo, Tag, ValueType};
// use rand::random;
//
// use super::*;
//
// /// Generate some random SeriesInfos.
// fn generate_serie_infos(database: String, table: String) -> Vec<SeriesInfo> {
//     let tag_keys = ["region", "host"];
//     #[rustfmt::skip]
//     let tag_values = [
//         vec!["rg_1", "rg_2", "rg_3", "rg_4", "rg_5", "rg_6"],
//         vec!["192.168.0.1", "192.168.0.2", "192.168.0.3", "192.168.0.4", "192.168.0.5", "192.168.0.6"],
//     ];
//     let field_keys = ["cpu", "mem"];
//
//     let mut infos: Vec<SeriesInfo> = Vec::new();
//     for t0 in tag_values[0].iter() {
//         for t1 in tag_values[1].iter() {
//             let mut fields = Vec::<FieldInfo>::new();
//             for fk in field_keys {
//                 fields.push(FieldInfo::new(
//                     0,
//                     fk.as_bytes().to_vec(),
//                     ValueType::Float,
//                     0,
//                 ));
//             }
//             infos.push(SeriesInfo::new(
//                 database.clone(),
//                 table.clone(),
//                 vec![
//                     Tag::new(tag_keys[0].as_bytes().to_vec(), t0.as_bytes().to_vec()),
//                     Tag::new(tag_keys[1].as_bytes().to_vec(), t1.as_bytes().to_vec()),
//                 ],
//                 fields,
//             ));
//         }
//     }
//
//     infos
// }
//
// #[tokio::test]
// async fn test_index_add_del() {
//     let _ = fs::remove_dir_all("/tmp/index_test/db_test1");
//     let mut index = db_index::DBIndex::from("/tmp/index_test/db_test1");
//
//     let database = "db_test".to_string();
//     let table = "table_test".to_string();
//
//     let mut series_infos = generate_serie_infos(database, table.clone());
//     let mut sids_1: Vec<u64> = Vec::new();
//     let mut sid_info_map: HashMap<u64, &SeriesInfo> = HashMap::new();
//
//     // Test add sries
//     series_infos.iter_mut().for_each(|info| {
//         let sid = index.add_series_if_not_exists(info).unwrap();
//         sids_1.push(sid);
//         sid_info_map.insert(sid, &*info);
//     });
//     sids_1.sort();
//
//     // Test get series id list
//     let mut sids_2 = index.get_series_id_list(&table, &[]).unwrap();
//     sids_2.sort();
//     assert_eq!(&sids_1, &sids_2);
//
//     // Test get series by series id
//     for sid in sids_2.iter() {
//         let key = index.get_series_key(*sid).unwrap().unwrap();
//         let info = sid_info_map.get(sid).unwrap();
//         assert_eq!(info.tags(), key.tags());
//     }
//
//     // Test delete series by series id
//     let mut deleted_sid = HashSet::new();
//     for sid in sids_2.iter() {
//         index.del_series_info(*sid).unwrap();
//         let key = index.get_series_key(*sid).unwrap();
//         assert_eq!(key, None);
//         deleted_sid.insert(*sid);
//     }
//     println!("Deleted {} series ids.", sids_2.len());
//     let sids_3 = index.get_series_id_list(&table, &[]).unwrap();
//     let remained_sid: HashSet<u64> = HashSet::from_iter(sids_3.iter().copied());
//     println!(
//         "Remained {} series ids after Deleted {} series ids. And remained - deleted = {:?}.",
//         deleted_sid.len(),
//         remained_sid.len(),
//         remained_sid.difference(&deleted_sid),
//     );
//     assert!(sids_3.is_empty());
//
//     // Test get table schema
//     let schema_1 = index.get_table_schema(&table).unwrap().unwrap();
//     for field in schema_1 {
//         if field.is_tag() {
//             assert_eq!(field.value_type(), ValueType::Unknown);
//         } else {
//             assert_eq!(field.value_type(), ValueType::Float);
//         }
//     }
//
//     // Test delete table schema
//     index.del_table_schema(&table).unwrap();
//     let schema_2 = index.get_table_schema(&table).unwrap();
//     assert_eq!(schema_2, None);
//
//     index.flush().unwrap();
// }
//
// #[tokio::test]
// async fn test_index_id_list() {
//     let _ = fs::remove_dir_all("/tmp/index_test/db_test2");
//     let mut index = db_index::DBIndex::from("/tmp/index_test/db_test2");
//
//     let mut info1 = SeriesInfo::new(
//         "db_test".to_string(),
//         "table_test".to_string(),
//         vec![
//             Tag::new(b"loc".to_vec(), b"bj".to_vec()),
//             Tag::new(b"host".to_vec(), b"h1".to_vec()),
//         ],
//         vec![
//             FieldInfo::new(0, b"cpu".to_vec(), ValueType::Float, 0),
//             FieldInfo::new(0, b"mem".to_vec(), ValueType::Float, 0),
//         ],
//     );
//
//     let mut info2 = SeriesInfo::new(
//         "db_test".to_string(),
//         "table_test".to_string(),
//         vec![
//             Tag::new(b"loc".to_vec(), b"bj".to_vec()),
//             Tag::new(b"host".to_vec(), b"h2".to_vec()),
//         ],
//         vec![FieldInfo::new(0, b"mem".to_vec(), ValueType::Float, 0)],
//     );
//
//     let mut info3 = SeriesInfo::new(
//         "db_test".to_string(),
//         "table_test".to_string(),
//         vec![
//             Tag::new(b"loc".to_vec(), b"bj".to_vec()),
//             Tag::new(b"host".to_vec(), b"h3".to_vec()),
//         ],
//         vec![FieldInfo::new(0, b"mem".to_vec(), ValueType::Float, 0)],
//     );
//
//     let id1 = index.add_series_if_not_exists(&mut info1).unwrap();
//     let id2 = index.add_series_if_not_exists(&mut info2).unwrap();
//     let id3 = index.add_series_if_not_exists(&mut info3).unwrap();
//
//     let key1 = index.get_series_key(id1).unwrap().unwrap();
//     let key2 = index.get_series_key(id2).unwrap().unwrap();
//
//     let enc = utils::encode_inverted_index_key("table_test", "tag".as_bytes(), "val".as_bytes());
//     assert_eq!(enc, "table_test.tag=val".as_bytes());
//
//     let tags = vec![
//         Tag::new(b"loc".to_vec(), b"bj".to_vec()),
//         Tag::new(b"host".to_vec(), b"h2".to_vec()),
//     ];
//
//     let list = index.get_series_id_list("table_test", &tags).unwrap();
//     assert_eq!(vec![id2], list);
//
//     let list = index.get_series_id_list("table_test", &tags[0..0]).unwrap();
//     assert_eq!(vec![id1, id2, id3], list);
//
//     index.flush().unwrap();
// }
//
// #[tokio::test]
// async fn test_field_type() {
//     let _ = fs::remove_dir_all("/tmp/index_test/db_test3");
//     let mut index = db_index::DBIndex::from("/tmp/index_test/db_test3");
//
//     let mut info1 = SeriesInfo::new(
//         "db_test".to_string(),
//         "table_test".to_string(),
//         vec![
//             Tag::new(b"loc".to_vec(), b"bj".to_vec()),
//             Tag::new(b"host".to_vec(), b"h1".to_vec()),
//         ],
//         vec![
//             FieldInfo::new(0, b"cpu".to_vec(), ValueType::Float, 0),
//             FieldInfo::new(0, b"mem".to_vec(), ValueType::Float, 0),
//         ],
//     );
//
//     let mut info2 = SeriesInfo::new(
//         "db_test".to_string(),
//         "table_test".to_string(),
//         vec![
//             Tag::new(b"loc".to_vec(), b"bj".to_vec()),
//             Tag::new(b"host".to_vec(), b"h2".to_vec()),
//         ],
//         vec![FieldInfo::new(0, b"mem".to_vec(), ValueType::Unsigned, 0)],
//     );
//     let id1 = index.add_series_if_not_exists(&mut info1).unwrap();
//     let id2 = index.add_series_if_not_exists(&mut info2);
//
//     let schema = index.get_table_schema("table_test");
//
//     index.flush().unwrap();
// }
