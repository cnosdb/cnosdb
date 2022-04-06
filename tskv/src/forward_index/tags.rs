use serde::{Serialize, Deserialize};

#[derive(Serialize, Deserialize, Debug, PartialEq)]
pub struct Tag {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}

impl Tag {
    pub fn bytes(&mut self) -> Vec<u8> {
        let mut data = Vec::<u8>::new();
        data.append(&mut self.key);
        data.append(&mut self.value);
        data
    }
}