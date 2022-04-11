mod series_info;
mod field_info;
mod tags;
mod tests;

use std::any::Any;
use std::borrow::Borrow;
use std::fs;
use std::fs::OpenOptions;
use std::path::PathBuf;
use hashbrown::HashMap;
use series_info::{SeriesID, SeriesInfo, SeriesInfoSimplified};
use crate::{direct_io, FileSync};
use num_enum::{IntoPrimitive, TryFromPrimitive};
use crate::file_manager::FileManager;
use crc32fast;
use num_traits::cast::ToPrimitive;
use libc::munlock;

const VERSION: u8 = 1;
const V1_HEAD_LEN: usize = 2 + 1 + 1 + 4;
const MAX_SLICES_SIZE: usize = 1024 * 1024 * 512; // 512MB

pub struct ForwardIndex {
    series_info_set: HashMap<SeriesID, SeriesInfoSimplified>,
    file_path: PathBuf,
    file: direct_io::File,
}

impl ForwardIndex {
    pub fn new(path: &str) -> Self {
        let file = FileManager::get_instance().open_file_with(
            path, &fs::OpenOptions::new().read(true).write(true).create(true)).unwrap();

        let mut fi = ForwardIndex {
            file_path: PathBuf::from(path),
            series_info_set: HashMap::new(),
            file,
        };
        fi.load_cache_file();

        fi
    }

    pub fn load_cache_file(&mut self) {
        let mut p: usize = 0;
        loop {
            p = self.load_slices(p);
            if p == self.file.len().try_into().unwrap() {
                println!("{}", p);
                println!("{}", self.file.len());
                break;
            }
        }
    }

    fn load_slices(&mut self, offset: usize) -> usize {
        // read file slices
        let len = self.file.len() as usize;
        let mut read_len = len - offset;
        if read_len > MAX_SLICES_SIZE {
            read_len = MAX_SLICES_SIZE;
        }
        let mut buf = Vec::<u8>::new();
        buf.resize(read_len, 0);
        self.file.read_at(offset as u64, buf.as_mut_slice());

        // encode data
        let mut p: usize = 0;
        loop {
            if buf.len() - p < 2 {
                break;
            }
            let data_len = u16::from_be_bytes(buf[p..p + 2].try_into().unwrap());
            let info_offset = p;
            p = p + V1_HEAD_LEN + data_len as usize;
            if p > buf.len() {
                break;
            }

            //read version
            let version = u8::from_be_bytes(
                buf[info_offset + 2..info_offset + 2 + 1].try_into().unwrap());
            if version != VERSION {
                continue;
            }

            //read type
            let elem_type = u8::from_be_bytes(
                buf[info_offset + 2 + 1..info_offset + 2 + 2].try_into().unwrap());
            let elem_type = ElemType::try_from(elem_type).unwrap();

            //check crc
            let data_crc_number = u32::from_be_bytes(
                buf[info_offset + 2 + 2..info_offset + 2 + 6].try_into().unwrap());
            let calculate_crc = crc32fast::hash(
                buf[info_offset + 2 + 6..info_offset + 2 + 6 + data_len as usize].borrow());
            if data_crc_number != calculate_crc {
                panic!("crc check err");
            }

            //match elem_type
            match ElemType::try_from(elem_type).unwrap() {
                ElemType::Add => {
                    let info = SeriesInfo::decoded(
                        &buf[info_offset + 2 + 6..info_offset + 2 + 6 + data_len as usize].to_vec());
                    let mut simplified_info = info.simplified();
                    simplified_info.offset = info_offset;
                    self.series_info_set.insert(info.id, info.simplified());
                }
                ElemType::Del => {
                    todo!()
                }
            }
        }

        p
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
        let len = data.len().to_u16().unwrap();
        buffer.append(&mut len.to_be_bytes().to_vec());
        //version
        buffer.append(&mut VERSION.to_be_bytes().to_vec());
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
        self.encode(ElemType::Del, id.to_be_bytes().to_vec())
    }

    pub fn add_series(&mut self, info: SeriesInfo) {
        let size = self.file.len();

        let mut simplified_info = info.simplified();
        simplified_info.set_offset(size.try_into().unwrap());

        self.file.write_at(size, &self.add_encode(&info)).unwrap();

        self.series_info_set.insert(info.id, simplified_info);
    }

    pub fn del_series(&mut self, id: SeriesID) {
        self.file.write_at(self.file.len(), &self.del_encode(id)).unwrap();

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

    pub fn close(&self) {
        self.file.sync_all(FileSync::Hard);
    }
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, TryFromPrimitive, IntoPrimitive)]
#[repr(u8)]
enum ElemType {
    Add = 1,
    Del,
}

impl ElemType {
    pub fn u8_number(&self) -> u8 {
        *self as u8
    }
}