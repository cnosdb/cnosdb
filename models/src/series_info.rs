use protos::models as fb_models;
use serde::{Deserialize, Serialize};
use utils::BkdrHasher;

use crate::{
    errors::{Error, Result},
    tag, FieldId, FieldInfo, FieldName, SeriesId, Tag,
};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct SeriesKey {
    id: SeriesId,
    tags: Vec<Tag>,
    table: String,
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
                err: format!("Invalid serde message: {}", err.to_string()),
            }),
        }
    }

    pub fn string(&self) -> String {
        let mut str = self.table.clone() + ".";
        for tag in &self.tags {
            str = str + &String::from_utf8(tag.key.to_vec()).unwrap() + "=";
            str = str + &String::from_utf8(tag.value.to_vec()).unwrap() + ".";
        }

        return str;
    }
}

impl PartialEq for SeriesKey {
    fn eq(&self, other: &Self) -> bool {
        if self.table != other.table {
            return false;
        }

        if self.tags != other.tags {
            return false;
        }

        return true;
    }
}

#[derive(Serialize, Deserialize, Debug, PartialEq)]
pub struct SeriesInfo {
    id: SeriesId,
    tags: Vec<Tag>,
    field_infos: Vec<FieldInfo>,

    db: String,
    table: String,
}

impl SeriesInfo {
    pub fn new(db: String, table: String, tags: Vec<Tag>, field_infos: Vec<FieldInfo>) -> Self {
        let mut si = Self {
            id: 0,
            db,
            table,
            tags,
            field_infos,
        };

        si.sort_tags();
        si
    }

    pub fn from_flatbuffers(point: &fb_models::Point) -> Result<Self> {
        let tags = match point.tags() {
            Some(tags_inner) => {
                let mut tags = Vec::with_capacity(tags_inner.len());
                for t in tags_inner.into_iter() {
                    tags.push(Tag::from_flatbuffers(&t)?);
                }
                tags
            }
            None => {
                return Err(Error::InvalidFlatbufferMessage {
                    err: "Point tags cannot be empty".to_string(),
                })
            }
        };
        let field_infos = match point.fields() {
            Some(fields_inner) => {
                let mut fields = Vec::with_capacity(fields_inner.len());
                for f in fields_inner.into_iter() {
                    fields.push(FieldInfo::from_flatbuffers(&f)?);
                }
                fields
            }
            None => {
                return Err(Error::InvalidFlatbufferMessage {
                    err: "Point fields cannot be empty".to_string(),
                })
            }
        };

        let db = match point.db() {
            Some(db) => {
                String::from_utf8(db.to_vec()).map_err(|err| Error::InvalidFlatbufferMessage {
                    err: err.to_string(),
                })?
            }

            None => {
                return Err(Error::InvalidFlatbufferMessage {
                    err: "Point db name cannot be empty".to_string(),
                })
            }
        };

        let table = match point.table() {
            Some(table) => String::from_utf8(table.to_vec()).map_err(|err| {
                Error::InvalidFlatbufferMessage {
                    err: err.to_string(),
                }
            })?,

            None => {
                return Err(Error::InvalidFlatbufferMessage {
                    err: "Point table name cannot be empty".to_string(),
                })
            }
        };

        let mut info = Self {
            id: 0,
            db,
            table,
            tags,
            field_infos,
        };
        info.sort_tags();
        Ok(info)
    }

    pub fn sort_tags(&mut self) {
        tag::sort_tags(&mut self.tags);
    }

    pub fn series_id(&self) -> SeriesId {
        self.id
    }

    pub fn tags(&self) -> &Vec<Tag> {
        &self.tags
    }

    pub fn table(&self) -> &String {
        &self.table
    }

    pub fn db(&self) -> &String {
        &self.db
    }

    pub fn field_infos(&mut self) -> &mut Vec<FieldInfo> {
        &mut self.field_infos
    }

    pub fn push_field_info(&mut self, field_info: FieldInfo) {
        self.field_infos.push(field_info)
    }

    pub fn field_info_with_id(&self, field_id: FieldId) -> Vec<&FieldInfo> {
        self.field_infos
            .iter()
            .filter(|f| f.field_id().cmp(&field_id).is_eq())
            .collect()
    }

    pub fn field_info_with_name(&self, field_name: &FieldName) -> Vec<&FieldInfo> {
        self.field_infos
            .iter()
            .filter(|f| f.name().cmp(field_name).is_eq())
            .collect()
    }

    pub fn encode(&self) -> Vec<u8> {
        bincode::serialize(self).unwrap()
    }

    pub fn decode(data: &[u8]) -> SeriesInfo {
        bincode::deserialize(data).unwrap()
    }
}

impl From<&SeriesInfo> for SeriesKey {
    fn from(value: &SeriesInfo) -> Self {
        SeriesKey {
            id: 0,
            tags: value.tags().to_vec(),
            table: value.table().clone(),
        }
    }
}

pub fn generate_series_id(tags: &[Tag]) -> SeriesId {
    let mut hasher = BkdrHasher::new();
    for tag in tags {
        hasher.hash_with(&tag.key);
        hasher.hash_with(&tag.value);
    }
    hasher.number()
}

#[cfg(test)]
mod tests_series_info {
    use protos::models;

    use crate::{FieldInfo, SeriesInfo, Tag, ValueType};

    #[test]
    fn test_series_info_encode_and_decode() {
        let info = SeriesInfo::new(
            "db_test".to_string(),
            "tab_test".to_string(),
            vec![Tag::new(b"col_a".to_vec(), b"val_a".to_vec())],
            vec![FieldInfo::new(1, b"col_b".to_vec(), ValueType::Integer)],
        );
        let data = info.encode();
        let new_info = SeriesInfo::decode(&data);
        assert_eq!(info, new_info);
    }

    #[test]
    fn test_from() {
        let mut fb = flatbuffers::FlatBufferBuilder::new();

        // build tag
        let tag_k = fb.create_vector("tag_k".as_bytes());
        let tag_v = fb.create_vector("tag_v".as_bytes());
        let tag = models::Tag::create(
            &mut fb,
            &models::TagArgs {
                key: Some(tag_k),
                value: Some(tag_v),
            },
        );
        // build field
        let f_n = fb.create_vector("field_name".as_bytes());
        let f_v = fb.create_vector("field_value".as_bytes());

        let field = models::Field::create(
            &mut fb,
            &models::FieldArgs {
                name: Some(f_n),
                type_: models::FieldType::Integer,
                value: Some(f_v),
            },
        );
        // build series_info
        let db = Some(fb.create_vector("test_db".as_bytes()));
        let table = Some(fb.create_vector("test_tab".as_bytes()));
        let fields = Some(fb.create_vector(&[field]));
        let tags = Some(fb.create_vector(&[tag]));
        // build point
        let point = models::Point::create(
            &mut fb,
            &models::PointArgs {
                db,
                table,
                tags,
                fields,
                timestamp: 1,
            },
        );

        fb.finish(point, None);
        let buf = fb.finished_data();

        let p = flatbuffers::root::<models::Point>(buf).unwrap();
        println!("Point info {:?}", p);

        let s = SeriesInfo::from_flatbuffers(&p).unwrap();
        println!("Series info {:?}", s);
    }
}
