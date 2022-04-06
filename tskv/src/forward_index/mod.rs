mod series_info;
mod field_info;
mod tags;
mod tests;

use std::any::Any;
use std::borrow::Borrow;
use std::collections::HashMap;
use std::fs;
use std::fs::OpenOptions;
use std::path::PathBuf;
use hashbrown::HashMap;
use series_info::{SeriesID, SeriesInfo, SeriesInfoSimplified};
use util::direct_fio;
use util::direct_fio::{File, FileSync};
use util::direct_fio::FileSync::{Hard, Soft};
use crate::file_manager::FileManager;
use crc32fast;
use libc::munlock;

pub struct ForwardIndex {
    series_info_set: HashMap<SeriesID, SeriesInfoSimplified>,
    file_path: PathBuf,
    file: direct_fio::File,
}

impl ForwardIndex {
    pub fn new(path: &str) -> Self {
        let file = FileManager::get_instance().open_file_with(
            path, &fs::OpenOptions::new().read(true).write(true).create(true)).unwrap();

        ForwardIndex {
            file_path: PathBuf::from(path),
            series_info_set: HashMap::new(),
            file,
        }
    }

    /*
        |  2  |    1    |  1   |   4   | len  |
        +-----+---------+------+-------+------+
        | len | version | type | crc32 | data |
        +-----+---------+------+-------+------+
    */
    fn encode(&self, elem_type: ElemType, data: Vec<u8>) -> Vec<u8> {
        let mut buffer = Vec::<u8>::new();
        //len
        buffer.append(&mut data.len().to_be_bytes().to_vec());
        //version
        buffer.append(&mut 1_u8.to_be_bytes().to_vec());
        //type
        buffer.append(&mut elem_type.u8_number().to_be_bytes().to_vec());
        //crc32
        let crc32_number = crc32fast::hash(data.borrow());
        buffer.append(&mut crc32_number.to_be_bytes().to_vec());
        //data
        buffer.append(&mut data.to_vec());

        buffer
    }

    fn add_encode(&self, info: &SeriesInfo) -> Vec<u8> {
        self.encode(ElemType::Add, info.encode())
    }

    fn del_encode(&self, id: SeriesID) -> Vec<u8> {
        self.encode(ElemType::Del, id.encode_to_vec())
    }

    pub fn add_series(&mut self, info: SeriesInfo) {
        let size = self.file.len();

        let mut info_simplified = info.simplified();
        info_simplified.set_offset(size as usize);

        self.file.write_at(size, &self.add_encode(&info)).unwrap();
        //TODO del
        // self.file.sync_all(FileSync::Soft);

        self.series_info_set.insert(info.id, info_simplified);
    }

    pub fn del_series(&mut self, id: SeriesID) {
        self.file.write_at(self.file.len(), &self.del_encode(id)).unwrap();
        //TODO del
        // self.file.sync_all(FileSync::Soft);

        self.series_info_set.remove(id.borrow());
    }

    pub fn get_series(&self, id: SeriesID) -> Option<SeriesInfo> {
        match self.series_info_set.get(&id) {
            /*
            Some(info) => {
                //todo read_file
                SeriesInfo
            }
             */
            Some(..) => {
                Option::None
            }
            None => {
                Option::None
            }
        }
    }

    pub fn get_file_path(&self) -> PathBuf {
        self.file_path.clone()
    }
}

#[derive(Copy, Clone)]
enum ElemType {
    Add = 1,
    Del,
}

impl ElemType {
    fn u8_number(&self) -> u8 {
        *self as u8
    }
}

fn f(foo: &Foo) -> u8 {
    *foo as u8
}