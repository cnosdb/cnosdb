/*
use crate::forward_index::field_info::{FieldID, FieldInfo};
use crate::forward_index::tags::Tag;
use bincode;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use utils::bkdr_hash::hash_with_bytes;

pub type SeriesID = u64;

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct SeriesInfo {
    pub id: SeriesID,
    pub fields: Vec<FieldInfo>,
    pub tags: Vec<Tag>,
}

impl SeriesInfo {
    pub fn new() -> SeriesInfo {
        SeriesInfo {
            id: 0,
            tags: Vec::<Tag>::new(),
            fields: Vec::<FieldInfo>::new(),
        }
    }

    pub fn new_with(tags: Vec<Tag>) -> SeriesInfo {
        let mut s = SeriesInfo {
            id: 0,
            tags,
            fields: Vec::<FieldInfo>::new(),
        };
        s.calculation_series_id();
        s
    }

    pub fn calculation_series_id(&mut self) {
        //series id
        /*
        self.tags.sort_by(|a, b| -> Ordering {
            return if a.key < b.key {
                Ordering::Less
            } else if a.key > b.key {
                Ordering::Greater
            } else {
                Ordering::Equal
            };
        });
        */
        let mut data = Vec::<u8>::new();
        for tag in self.tags.iter_mut() {
            data.append(&mut tag.bytes())
        }
        self.id = hash_with_bytes(&data);
    }

    pub fn simplified(&self) -> SeriesInfoSimplified {
        let mut field_ids = Vec::<FieldID>::new();
        for field in self.fields.iter() {
            field_ids.push(field.id);
        }

        SeriesInfoSimplified {
            id: self.id,
            offset: 0,
            field_ids,
        }
    }

    pub fn encode(&self) -> Vec<u8> {
        bincode::serialize(self).unwrap()
    }

    pub fn decoded(data: &Vec<u8>) -> SeriesInfo {
        bincode::deserialize(&data[..]).unwrap()
    }
}

#[derive(Debug)]
pub struct SeriesInfoSimplified {
    pub id: SeriesID,
    pub offset: usize,
    pub field_ids: Vec<FieldID>, //temporarily use vec
}

impl SeriesInfoSimplified {
    pub fn set_offset(&mut self, offset: usize) {
        self.offset = offset;
    }

    pub fn add_field_id(&mut self, id: FieldID) {
        match self.field_ids.binary_search(&id) {
            Err(..) => {
                self.field_ids.push(id);
            }
            _ => {}
        }
    }

    pub fn del_field_id(&mut self, id: FieldID) {
        match self.field_ids.binary_search(&id) {
            Ok(i) => {
                self.field_ids.remove(i);
            }
            _ => {}
        }
    }
}
*/