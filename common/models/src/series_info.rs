use crate::Error::InvalidFlatbufferMessage;
use crate::{
    errors::{Error, Result},
    tag, SeriesId, Tag,
};
use protos::models as fb_models;
use serde::{Deserialize, Serialize};
use utils::BkdrHasher;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct SeriesKey {
    id: SeriesId,
    tags: Vec<Tag>,
    table: String,
    db: String,
}

impl SeriesKey {
    pub fn id(&self) -> SeriesId {
        self.id
    }

    pub fn set_id(&mut self, id: SeriesId) {
        self.id = id;
    }
    pub fn tags(&self) -> &Vec<Tag> {
        &self.tags
    }

    pub fn table(&self) -> &String {
        &self.table
    }

    pub fn encode(&self) -> Vec<u8> {
        bincode::serialize(self).unwrap()
    }

    pub fn tag_val(&self, key: &str) -> Vec<u8> {
        for tag in &self.tags {
            if tag.key == key.as_bytes() {
                return tag.value.clone();
            }
        }

        vec![]
    }

    pub fn hash(&self) -> u64 {
        let mut hasher = BkdrHasher::new();
        hasher.hash_with(self.table.as_bytes());
        for tag in &self.tags {
            hasher.hash_with(&tag.key);
            hasher.hash_with(&tag.value);
        }

        hasher.number()
    }

    pub fn decode(data: &[u8]) -> Result<SeriesKey> {
        let key = bincode::deserialize(data);
        match key {
            Ok(key) => Ok(key),
            Err(err) => Err(Error::InvalidSerdeMessage {
                err: format!("Invalid serde message: {}", err),
            }),
        }
    }

    pub fn string(&self) -> String {
        let mut str = self.table.clone() + ".";
        for tag in &self.tags {
            str = str + &String::from_utf8(tag.key.to_vec()).unwrap() + "=";
            str = str + &String::from_utf8(tag.value.to_vec()).unwrap() + ".";
        }

        str
    }
    pub fn from_flatbuffer(point: &fb_models::Point) -> Result<Self> {
        let mut tags = match point.tags() {
            Some(tags_inner) => {
                let mut tags = Vec::with_capacity(tags_inner.len());
                for t in tags_inner.into_iter() {
                    tags.push(Tag::from_flatbuffers(&t)?);
                }
                tags
            }

            None => {
                return Err(InvalidFlatbufferMessage {
                    err: "Point tags cannot be empty".to_string(),
                })
            }
        };

        let db = match point.db() {
            Some(db) => String::from_utf8(db.to_vec()).map_err(|err| InvalidFlatbufferMessage {
                err: err.to_string(),
            })?,

            None => {
                return Err(Error::InvalidFlatbufferMessage {
                    err: "Point db name cannot be empty".to_string(),
                })
            }
        };

        let table = match point.table() {
            Some(table) => {
                String::from_utf8(table.to_vec()).map_err(|err| InvalidFlatbufferMessage {
                    err: err.to_string(),
                })?
            }

            None => {
                return Err(InvalidFlatbufferMessage {
                    err: "Point table name cannot be empty".to_string(),
                })
            }
        };

        tag::sort_tags(&mut tags);

        Ok(Self {
            id: 0,
            tags,
            db,
            table,
        })
    }
}

// impl From<&SeriesInfo> for SeriesKey {
//     fn from(value: &SeriesInfo) -> Self {
//         SeriesKey {
//             id: 0,
//             tags: value.tags().to_vec(),
//             table: value.table().clone(),
//         }
//     }
// }

impl PartialEq for SeriesKey {
    fn eq(&self, other: &Self) -> bool {
        if self.table != other.table {
            return false;
        }

        if self.tags != other.tags {
            return false;
        }

        true
    }
}

// #[derive(Serialize, Deserialize, Debug, PartialEq)]
// pub struct SeriesInfo {
//     id: SeriesId,
//
//     tags: Vec<Tag>,
//     field_infos: Vec<FieldInfo>,
//     db: String,
//     table: String,
//
//     field_fill: Vec<FieldInfo>,
//     schema_id: u32,
// }

// impl Display for SeriesInfo {
//     fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
//         let mut tags = String::new();
//         if !self.tags.is_empty() {
//             for t in self.tags.iter() {
//                 tags.push_str(&String::from_utf8(t.key.clone()).unwrap());
//                 tags.push('=');
//                 tags.push_str(&String::from_utf8(t.value.clone()).unwrap());
//                 tags.push(',');
//             }
//             tags.truncate(tags.len() - 1);
//         }
//
//         let mut field_infos = String::new();
//         if !self.field_infos.is_empty() {
//             for t in self.field_infos.iter() {
//                 field_infos.push_str(&String::from_utf8(t.name().clone()).unwrap());
//                 field_infos.push_str("_t_");
//                 let _ = write!(field_infos, "{}", t.value_type());
//                 field_infos.push(',');
//             }
//             field_infos.truncate(tags.len() - 1);
//         }
//
//         write!(
//             f,
//             "SeriesInfo: {{id: {}, tags: [{}], field_infos: [{}], table: {}}}",
//             self.id, tags, field_infos, self.table
//         )
//     }
// }

// impl SeriesInfo {
//     pub fn new(db: String, table: String, tags: Vec<Tag>, field_infos: Vec<FieldInfo>) -> Self {
//         let mut si = Self {
//             id: 0,
//             db,
//             table,
//             tags,
//             field_infos,
//
//             schema_id: 0,
//             field_fill: vec![],
//         };
//
//         si.sort_tags();
//         si
//     }
//
//     pub fn from_flatbuffers(point: &fb_models::Point) -> Result<Self> {
//         let tags = match point.tags() {
//             Some(tags_inner) => {
//                 let mut tags = Vec::with_capacity(tags_inner.len());
//                 for t in tags_inner.into_iter() {
//                     tags.push(Tag::from_flatbuffers(&t)?);
//                 }
//                 tags
//             }
//             None => {
//                 return Err(Error::InvalidFlatbufferMessage {
//                     err: "Point tags cannot be empty".to_string(),
//                 })
//             }
//         };
//         let field_infos = match point.fields() {
//             Some(fields_inner) => {
//                 let mut fields = Vec::with_capacity(fields_inner.len());
//                 for f in fields_inner.into_iter() {
//                     fields.push(FieldInfo::from_flatbuffers(&f)?);
//                 }
//                 fields
//             }
//             None => {
//                 return Err(Error::InvalidFlatbufferMessage {
//                     err: "Point fields cannot be empty".to_string(),
//                 })
//             }
//         };
//
//         let db = match point.db() {
//             Some(db) => {
//                 String::from_utf8(db.to_vec()).map_err(|err| Error::InvalidFlatbufferMessage {
//                     err: err.to_string(),
//                 })?
//             }
//
//             None => {
//                 return Err(Error::InvalidFlatbufferMessage {
//                     err: "Point db name cannot be empty".to_string(),
//                 })
//             }
//         };
//
//         let table = match point.table() {
//             Some(table) => String::from_utf8(table.to_vec()).map_err(|err| {
//                 Error::InvalidFlatbufferMessage {
//                     err: err.to_string(),
//                 }
//             })?,
//
//             None => {
//                 return Err(Error::InvalidFlatbufferMessage {
//                     err: "Point table name cannot be empty".to_string(),
//                 })
//             }
//         };
//
//         let mut info = Self {
//             id: 0,
//             db,
//             table,
//             tags,
//             field_infos,
//
//             schema_id: 0,
//             field_fill: vec![],
//         };
//         info.sort_tags();
//         Ok(info)
//     }
//
//     pub fn sort_tags(&mut self) {
//         tag::sort_tags(&mut self.tags);
//     }
//
//     pub fn series_id(&self) -> SeriesId {
//         self.id
//     }
//
//     pub fn tags(&self) -> &Vec<Tag> {
//         &self.tags
//     }
//
//     pub fn table(&self) -> &String {
//         &self.table
//     }
//
//     pub fn db(&self) -> &String {
//         &self.db
//     }
//
//     pub fn get_schema_id(&self) -> u32 {
//         self.schema_id
//     }
//
//     pub fn set_schema_id(&mut self, id: u32) {
//         self.schema_id = id;
//     }
//
//     pub fn field_infos(&mut self) -> &mut Vec<FieldInfo> {
//         &mut self.field_infos
//     }
//
//     pub fn field_fill(&mut self) -> &mut Vec<FieldInfo> {
//         &mut self.field_fill
//     }
//
//     pub fn push_field_info(&mut self, field_info: FieldInfo) {
//         self.field_infos.push(field_info)
//     }
//
//     pub fn push_field_fill(&mut self, field_info: FieldInfo) {
//         self.field_fill.push(field_info)
//     }
//
//     pub fn field_info_with_id(&self, field_id: FieldId) -> Vec<&FieldInfo> {
//         self.field_infos
//             .iter()
//             .filter(|f| f.field_id().cmp(&field_id).is_eq())
//             .collect()
//     }
//
//     pub fn field_info_with_name(&self, field_name: &FieldName) -> Vec<&FieldInfo> {
//         self.field_infos
//             .iter()
//             .filter(|f| f.name().cmp(field_name).is_eq())
//             .collect()
//     }
//
//     pub fn encode(&self) -> Vec<u8> {
//         bincode::serialize(self).unwrap()
//     }
//
//     pub fn decode(data: &[u8]) -> SeriesInfo {
//         bincode::deserialize(data).unwrap()
//     }
// }

#[cfg(test)]
mod tests_series_info {
    // use protos::models;
    //
    // use crate::{FieldInfo, Tag, ValueType};
    // #[test]
    // fn test_series_info_encode_and_decode() {
    //     let info = SeriesInfo::new(
    //         "db_test".to_string(),
    //         "tab_test".to_string(),
    //         vec![Tag::new(b"col_a".to_vec(), b"val_a".to_vec())],
    //         vec![FieldInfo::new(1, b"col_b".to_vec(), ValueType::Integer, 0)],
    //     );
    //     let data = info.encode();
    //     let new_info = SeriesInfo::decode(&data);
    //     assert_eq!(info, new_info);
    // }
    //
    // #[test]
    // fn test_from() {
    //     let mut fb = flatbuffers::FlatBufferBuilder::new();
    //
    //     // build tag
    //     let tag_k = fb.create_vector("tag_k".as_bytes());
    //     let tag_v = fb.create_vector("tag_v".as_bytes());
    //     let tag = models::Tag::create(
    //         &mut fb,
    //         &models::TagArgs {
    //             key: Some(tag_k),
    //             value: Some(tag_v),
    //         },
    //     );
    //     // build field
    //     let f_n = fb.create_vector("field_name".as_bytes());
    //     let f_v = fb.create_vector("field_value".as_bytes());
    //
    //     let field = models::Field::create(
    //         &mut fb,
    //         &models::FieldArgs {
    //             name: Some(f_n),
    //             type_: models::FieldType::Integer,
    //             value: Some(f_v),
    //         },
    //     );
    //     // build series_info
    //     let db = Some(fb.create_vector("test_db".as_bytes()));
    //     let table = Some(fb.create_vector("test_tab".as_bytes()));
    //     let fields = Some(fb.create_vector(&[field]));
    //     let tags = Some(fb.create_vector(&[tag]));
    //     // build point
    //     let point = models::Point::create(
    //         &mut fb,
    //         &models::PointArgs {
    //             db,
    //             table,
    //             tags,
    //             fields,
    //             timestamp: 1,
    //         },
    //     );
    //
    //     fb.finish(point, None);
    //     let buf = fb.finished_data();
    //
    //     let p = flatbuffers::root::<models::Point>(buf).unwrap();
    //     println!("Point info {:?}", p);
    //
    //     let s = SeriesInfo::from_flatbuffers(&p).unwrap();
    //     println!("Series info {:?}", s);
    // }
}
