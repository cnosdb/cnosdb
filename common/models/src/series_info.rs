use protos::models as fb_models;
use serde::{Deserialize, Serialize};
use utils::BkdrHasher;

use crate::errors::{Error, Result};
use crate::Error::InvalidFlatbufferMessage;
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
            Some(db) => {
                String::from_utf8(db.bytes().to_vec()).map_err(|err| InvalidFlatbufferMessage {
                    err: err.to_string(),
                })?
            }

            None => {
                return Err(Error::InvalidFlatbufferMessage {
                    err: "Point db name cannot be empty".to_string(),
                })
            }
        };

        let table = match point.tab() {
            Some(table) => String::from_utf8(table.bytes().to_vec()).map_err(|err| {
                InvalidFlatbufferMessage {
                    err: err.to_string(),
                }
            })?,

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
