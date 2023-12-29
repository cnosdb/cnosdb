use flatbuffers::{ForwardsUOffset, Vector};
use protos::models::Column;
use serde::{Deserialize, Serialize};
use utils::bitset::ImmutBitSet;
use utils::BkdrHasher;

use crate::errors::{Error, Result};
use crate::schema::TskvTableSchema;
use crate::{tag, Tag, TagValue};

#[derive(Serialize, Deserialize, Debug, Clone, Hash, PartialEq, Eq)]
pub struct SeriesKey {
    pub tags: Vec<Tag>,
    pub table: String,
}

impl SeriesKey {
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

    /// Returns a string with format `{table}[,{tag.key}={tag.value}]`.
    pub fn string(&self) -> String {
        let buf_len = self.tags.iter().fold(self.table.len(), |acc, tag| {
            acc + tag.key.len() + tag.value.len() + 2 // ,{key}={value}
        });
        let mut buf = Vec::with_capacity(buf_len);
        buf.extend_from_slice(self.table.as_bytes());
        for tag in self.tags.iter() {
            buf.extend(b",");
            buf.extend_from_slice(&tag.key);
            buf.extend_from_slice(b"=");
            buf.extend_from_slice(&tag.value);
        }

        String::from_utf8(buf).unwrap()
    }

    pub fn build_series_key(
        tab_name: &str,
        columns: &Vector<ForwardsUOffset<Column>>,
        table_schema: &TskvTableSchema,
        tag_idx: &[usize],
        row_count: usize,
    ) -> Result<Self> {
        let mut tags = Vec::new();
        for idx in tag_idx {
            let column = columns.get(*idx);

            let tag_nullbit_buffer = column.nullbits().ok_or(Error::InvalidTag {
                err: "missing column null bit".to_string(),
            })?;
            let len = column
                .string_values_len()
                .map_err(|e| Error::InvalidPoint { err: e.to_string() })?;
            let column_nullbit = ImmutBitSet::new_without_check(len, tag_nullbit_buffer.bytes());
            if !column_nullbit.get(row_count) {
                continue;
            }

            let tags_value = column
                .string_values()
                .map_err(|e| Error::InvalidPoint { err: e.to_string() })?;
            let tag_value = tags_value.get(row_count);
            let tag_key = column.name().ok_or(Error::InvalidTag {
                err: "missing column name".to_string(),
            })?;
            let id = table_schema
                .column(tag_key)
                .ok_or_else(|| Error::InvalidTag {
                    err: format!("tag not found {}", tag_key),
                })?
                .id;

            tags.push(Tag::new_with_column_id(id, tag_value.as_bytes().to_vec()));
        }

        tag::sort_tags(&mut tags);

        Ok(Self {
            tags,
            table: tab_name.to_string(),
        })
    }
}

impl std::fmt::Display for SeriesKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", &self.table)?;
        for tag in self.tags.iter() {
            write!(
                f,
                ",{}={}",
                std::str::from_utf8(&tag.key).map_err(|_| std::fmt::Error)?,
                std::str::from_utf8(&tag.value).map_err(|_| std::fmt::Error)?,
            )?;
        }

        Ok(())
    }
}
