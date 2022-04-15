use super::*;

const TAG_KEY_MAX_LEN: usize = 512;
const TAG_VALUE_MAX_LEN: usize = 4096;

pub type TagKey = Vec<u8>;
pub type TagValue = Vec<u8>;

#[derive(Serialize, Deserialize, Debug, PartialEq)]
pub struct Tag {
    pub key: TagKey,
    pub value: TagValue,
}

impl Tag {
    pub fn format_check(&self) -> Result<(), String> {
        if self.key.len() == 0 {
            return Err(String::from("Key cannot be empty"));
        }
        if self.value.len() == 0 {
            return Err(String::from("Value cannot be empty"));
        }
        if self.key.len() > TAG_KEY_MAX_LEN {
            return Err(String::from("TagKey exceeds the TAG_KEY_MAX_LEN"));
        }
        if self.value.len() > TAG_VALUE_MAX_LEN {
            return Err(String::from("TagValue exceeds the TAG_VALUE_MAX_LEN"));
        }
        Ok(())
    }

    pub fn bytes(&mut self) -> Vec<u8> {
        let mut data = Vec::<u8>::new();
        data.append(&mut self.key);
        data.append(&mut self.value);
        data
    }
}

pub trait TagFromParts<T1, T2> {
    fn from_parts(key: T1, value: T2) -> Self;
}

impl TagFromParts<&str, &str> for Tag {
    fn from_parts(key: &str, value: &str) -> Self {
        Tag {
            key: key.as_bytes().to_vec(),
            value: value.as_bytes().to_vec(),
        }
    }
}

impl TagFromParts<TagKey, TagValue> for Tag {
    fn from_parts(key: TagKey, value: TagValue) -> Self {
        Tag { key, value }
    }
}

#[cfg(test)]
mod tests_tag {
    use crate::{Tag, TagFromParts};

    #[test]
    fn test_tag_bytes() {
        let mut tag = Tag::from_parts("hello", "123");
        assert_eq!(tag.bytes(), Vec::from("hello123"));
    }

    #[test]
    fn test_tag_format_check() {
        let tag = Tag::from_parts("hello", "123");
        tag.format_check().unwrap();
    }
}
