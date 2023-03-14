use protos::models as fb_models;
use serde::{Deserialize, Serialize};
use utils::bitset::BitSet;
use utils::BkdrHasher;

use crate::errors::{Error, Result};
use crate::{tag, SeriesId, Tag, TagValue};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct SeriesKey {
    pub id: SeriesId,
    pub tags: Vec<Tag>,
    pub table: String,
    pub db: String,
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

    pub fn tag_val(&self, key: &str) -> Option<TagValue> {
        for tag in &self.tags {
            if tag.key == key.as_bytes() {
                return Some(tag.value.clone());
            }
        }
        None
    }

    pub fn tag_string_val(&self, key: &str) -> Result<Option<String>> {
        match self.tag_val(key) {
            Some(v) => Ok(Some(String::from_utf8(v)?)),
            None => Ok(None),
        }
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

    pub fn build_series_key(
        db_name: &str,
        tab_name: &str,
        tag_names: &[&str],
        point: &fb_models::Point,
    ) -> Result<Self> {
        let tag_nullbit_buffer = point.tags_nullbit().ok_or(Error::InvalidTag {
            err: "point tag null bit".to_string(),
        })?;
        let len = tag_names.len();
        let tag_nullbit = BitSet::new_without_check(len, tag_nullbit_buffer.bytes());
        let mut tags = Vec::new();
        for (idx, (tag_key, tag_value)) in tag_names
            .iter()
            .zip(point.tags().ok_or(Error::InvalidTag {
                err: "point tag value".to_string(),
            })?)
            .enumerate()
        {
            if !tag_nullbit.get(idx) {
                continue;
            }
            tags.push(Tag::new(
                tag_key.as_bytes().to_vec(),
                tag_value
                    .value()
                    .ok_or(Error::InvalidTag {
                        err: "tag missing value".to_string(),
                    })?
                    .bytes()
                    .into(),
            ))
        }

        tag::sort_tags(&mut tags);

        Ok(Self {
            id: 0,
            tags,
            table: tab_name.to_string(),
            db: db_name.to_string(),
        })
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

        true
    }
}
