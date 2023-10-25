use std::cmp::Ordering;
use std::hash::Hash;

use serde::{Deserialize, Serialize};
use utils::BkdrHasher;

use crate::{ColumnId, TagKey, TagValue};

pub fn sort_tags(tags: &mut [Tag]) {
    tags.sort_by(|a, b| -> Ordering { a.key.partial_cmp(&b.key).unwrap() })
}

pub fn tags_hash_id(name: &String, tags: &[Tag]) -> u64 {
    let mut hasher = BkdrHasher::new();
    hasher.hash_with(name.as_bytes());
    for tag in tags {
        hasher.hash_with(&tag.key);
        hasher.hash_with(&tag.value);
    }

    hasher.number()
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Hash, Clone, Eq)]
pub struct Tag {
    pub key: TagKey,
    pub value: TagValue,
}

impl Tag {
    pub fn new_with_column_id(key: ColumnId, value: TagValue) -> Self {
        Self {
            key: format!("{key}").into_bytes(),
            value,
        }
    }

    pub fn new(key: TagKey, value: TagValue) -> Self {
        Self { key, value }
    }

    #[cfg(test)]
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(self.key.len() + self.value.len() + 1);
        buf.extend_from_slice(&self.key);
        buf.extend_from_slice(b"=");
        buf.extend_from_slice(&self.value);
        buf
    }

    #[cfg(test)]
    pub fn check(&self) -> crate::Result<()> {
        const TAG_KEY_MAX_LEN: usize = 512;
        const TAG_VALUE_MAX_LEN: usize = 4096;
        use crate::Error;
        if self.key.is_empty() {
            return Err(Error::InvalidTag {
                err: "Tag key cannot be empty".to_string(),
            });
        }
        if self.value.is_empty() {
            return Err(Error::InvalidTag {
                err: "Tag value cannot be empty".to_string(),
            });
        }
        if self.key.len() > TAG_KEY_MAX_LEN {
            return Err(Error::InvalidTag {
                err: format!("Tag key exceeds the TAG_KEY_MAX_LEN({})", TAG_KEY_MAX_LEN),
            });
        }
        if self.value.len() > TAG_VALUE_MAX_LEN {
            return Err(Error::InvalidTag {
                err: format!(
                    "Tag value exceeds the TAG_VALUE_MAX_LEN({})",
                    TAG_VALUE_MAX_LEN
                ),
            });
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests_tag {
    use crate::Tag;

    #[test]
    fn test_tag_bytes() {
        let tag = Tag::new(b"hello".to_vec(), b"123".to_vec());
        assert_eq!(tag.to_bytes(), Vec::from("hello=123"));
    }

    #[test]
    fn test_tag_format_check() {
        let tag = Tag::new(b"hello".to_vec(), b"123".to_vec());
        tag.check().unwrap();
    }
}
