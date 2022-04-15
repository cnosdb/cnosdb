use super::*;
use std::cmp::Ordering;
use utils::bkdr_hash::{Hash, HashWith};

pub type SeriesID = u64;

#[derive(Serialize, Deserialize, Debug, PartialEq)]
pub struct SeriesInfo {
    pub id: SeriesID,
    pub tags: Vec<Tag>,
    pub field_infos: Vec<FieldInfo>,
}

impl SeriesInfo {
    pub fn new() -> Self {
        SeriesInfo {
            id: 0,
            tags: Vec::new(),
            field_infos: Vec::new(),
        }
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

    pub fn sort_tags(&mut self) {
        self.tags.sort_by(|a, b| -> Ordering {
            return if a.key < b.key {
                Ordering::Less
            } else if a.key > b.key {
                Ordering::Greater
            } else {
                Ordering::Equal
            };
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

        //field id
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

#[cfg(test)]
mod tests_series_info {
    use crate::{FieldInfo, FieldInfoFromParts, SeriesInfo, Tag, TagFromParts, ValueType};

    #[test]
    fn test_series_info_encode_and_decode() {
        let mut info = SeriesInfo::new();
        info.add_tag(Tag::from_parts("hello", "123")).unwrap();
        info.add_field_info(FieldInfo::from_parts(Vec::from("cpu"), ValueType::Integer))
            .unwrap();
        let data = info.encode();
        let new_info = SeriesInfo::decoded(&data);
        assert_eq!(info, new_info);
    }
}
