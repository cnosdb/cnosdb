use std::cmp::Ordering;

use protos::models::Point;
use utils::bkdr_hash::{Hash, HashWith};

use super::*;

pub type SeriesID = u64;

#[derive(Serialize, Deserialize, Debug, PartialEq)]
pub struct SeriesInfo {
    pub id: SeriesID,
    pub tags: Vec<Tag>,
    pub field_infos: Vec<FieldInfo>,
}

impl SeriesInfo {
    pub fn new() -> Self {
        SeriesInfo { id: 0, tags: Vec::new(), field_infos: Vec::new() }
    }

    pub fn add_tag(&mut self, tag: Tag) -> Result<(), String> {
        tag.format_check()?;
        self.tags.push(tag);
        self.update_id();
        Ok(())
    }

    pub fn del_tag(&mut self, key: TagKey) {
        for i in 0..self.tags.len() {
            if self.tags[i].key == key {
                self.tags.remove(i);
                break;
            }
        }
    }

    pub fn add_field_info(&mut self, mut field_info: FieldInfo) -> Result<(), String> {
        field_info.format_check()?;
        field_info.update_id(self.id);
        self.field_infos.push(field_info);
        Ok(())
    }

    pub fn del_field_info_by_name(&mut self, name: FieldName) {
        for i in 0..self.field_infos.len() {
            if self.field_infos[i].name == name {
                self.field_infos.remove(i);
                break;
            }
        }
    }

    pub fn del_field_info_by_id(&mut self, id: FieldID) {
        for i in 0..self.field_infos.len() {
            if self.field_infos[i].id == id {
                self.field_infos.remove(i);
                break;
            }
        }
    }

    pub fn sort_tags(tags: &mut Vec<Tag>) {
        tags.sort_by(|a, b| -> Ordering {
                if a.key < b.key {
                    Ordering::Less
                } else if a.key > b.key {
                    Ordering::Greater
                } else {
                    Ordering::Equal
                }
            })
    }

    pub fn update_id(&mut self) {
        //series id
        self.sort_tags();
        let mut data = Vec::<u8>::new();
        for tag in self.tags.iter_mut() {
            data.append(&mut tag.bytes())
        }
        self.id = Hash::new().hash_with(&data).number();

        // field id
        for field_info in &mut self.field_infos {
            field_info.update_id(self.id);
        }
    }

    pub fn encode(&self) -> Vec<u8> {
        bincode::serialize(self).unwrap()
    }

    pub fn decoded(data: &Vec<u8>) -> SeriesInfo {
        bincode::deserialize(&data[..]).unwrap()
    }
}

impl From<Point<'_>> for SeriesInfo {
    fn from(p: Point<'_>) -> Self {
        let mut fileds = Vec::new();
        let mut tags = Vec::new();

        for fit in p.fields().into_iter() {
            for f in fit.into_iter() {
                let field_name = f.name().unwrap().to_vec();
                let t = f.type_().into();
                // let val = f.value().unwrap().to_vec();
                let filed = FieldInfo::new(0, field_name, t);
                fileds.push(filed);
            }
        }
        for tit in p.tags().into_iter() {
            for t in tit.into_iter() {
                let k = t.key().unwrap().to_vec();
                let v = t.value().unwrap().to_vec();
                let tag = Tag::new(k, v);
                tags.push(tag);
            }
        }

        let mut info = SeriesInfo { id: 0, tags, field_infos: fileds };
        info.update_id();
        info
    }
}

#[cfg(test)]
mod tests_series_info {
    use crate::{FieldInfo, TagFromParts, FieldInfoFromParts, SeriesInfo, Tag, ValueType};

    #[test]
    fn test_series_info_encode_and_decode() {
        let mut info = SeriesInfo::new();
        info.add_tag(Tag::from_parts("hello", "123")).unwrap();
        info.add_field_info(FieldInfo::from_parts(Vec::from("cpu"), ValueType::Integer)).unwrap();
        let data = info.encode();
        let new_info = SeriesInfo::decoded(&data);
        assert_eq!(info, new_info);
    }

    #[test]
    fn test_from() {
        let mut fb = flatbuffers::FlatBufferBuilder::new();

        // build tag
        let tag_k = fb.create_vector("tag_k".as_bytes());
        let tag_v = fb.create_vector("tag_v".as_bytes());
        let tag =
            models::Tag::create(&mut fb, &models::TagArgs { key: Some(tag_k), value: Some(tag_v) });
        // build filed
        let f_n = fb.create_vector("filed_name".as_bytes());
        let f_v = fb.create_vector("filed_value".as_bytes());

        let filed =
            models::Field::create(&mut fb,
                                  &models::FieldArgs { name: Some(f_n),
                                                       type_:
                                                           protos::models::FieldType::Integer,
                                                       value: Some(f_v) });
        // build series_info
        let fields = Some(fb.create_vector(&[filed]));
        let tags = Some(fb.create_vector(&[tag]));
        // build point
        let point =
            models::Point::create(&mut fb, &models::PointArgs { tags, fields, timestamp: 1 });

        fb.finish(point, None);
        let buf = fb.finished_data();

        let p = flatbuffers::root::<models::Point>(buf).unwrap();
        println!("Point info {:?}", p);

        let s = SeriesInfo::from(p);
        println!("Series info {:?}", s);
    }
}
