use std::time::{SystemTime, UNIX_EPOCH};

use bytes::BufMut;

use models::FieldId;

use crate::byte_utils;

use super::*;

const LOW_40BIT_MASK: u64 = (0x01 << 40) - 1;
const HIGH_24BIT_MASK: u64 = ((0x01 << 24) - 1) << 40;

pub fn split_id(id: u64) -> (u32, u64) {
    (((id & HIGH_24BIT_MASK) >> 40) as u32, id & LOW_40BIT_MASK)
}

pub fn unite_id(hash_id: u64, incr_id: u64) -> u64 {
    hash_id << 40 | (incr_id & LOW_40BIT_MASK)
}

pub fn encode_inverted_index_key(tab: &String, tag_key: &[u8], tag_val: &[u8]) -> Vec<u8> {
    let mut buf: Vec<u8> = Vec::with_capacity(tab.len() + 1 + tag_key.len() + 1 + tag_val.len());

    buf.put_slice(tab.as_bytes());
    buf.push(b'.');
    buf.put_slice(tag_key);
    buf.push(b'=');
    buf.put_slice(tag_val);

    return buf;
}

pub fn decode_series_id_list(data: &[u8]) -> IndexResult<Vec<u64>> {
    if data.len() % 8 != 0 {
        return Err(IndexError::DecodeSeriesIDList);
    }

    let count = data.len() / 8;
    let mut list: Vec<u64> = Vec::with_capacity(count);
    for i in 0..count {
        let id = byte_utils::decode_be_u64(&data[i * 8..]);
        list.push(id);
    }

    Ok(list)
}

pub fn encode_series_id_list(list: &Vec<u64>) -> Vec<u8> {
    let mut data: Vec<u8> = Vec::with_capacity(list.len() * 8);
    for i in 0..list.len() {
        data.put_u64(list[i]);
    }

    return data;
}

pub fn and_u64(arr1: &[u64], arr2: &[u64]) -> Vec<u64> {
    let mut len = arr1.len();
    if len > arr2.len() {
        len = arr2.len();
    }

    let mut i = 0;
    let mut j = 0;
    let mut result = Vec::with_capacity(len);

    loop {
        if i >= arr1.len() || j >= arr2.len() {
            break;
        }

        if (arr1[i] & LOW_40BIT_MASK) < (arr2[j] & LOW_40BIT_MASK) {
            i = i + 1;
        } else if (arr1[i] & LOW_40BIT_MASK) > (arr2[j] & LOW_40BIT_MASK) {
            j = j + 1;
        } else {
            result.push(arr1[i]);
            i = i + 1;
            j = j + 1;
        }
    }

    return result;
}

pub fn or_u64(arr1: &[u64], arr2: &[u64]) -> Vec<u64> {
    let mut i = 0;
    let mut j = 0;
    let mut result = Vec::with_capacity(arr1.len() + arr2.len());

    loop {
        if i >= arr1.len() || j >= arr2.len() {
            break;
        }

        if (arr1[i] & LOW_40BIT_MASK) < (arr2[j] & LOW_40BIT_MASK) {
            result.push(arr1[i]);
            i = i + 1;
        } else if (arr1[i] & LOW_40BIT_MASK) > (arr2[j] & LOW_40BIT_MASK) {
            result.push(arr2[j]);
            j = j + 1;
        } else {
            result.push(arr1[i]);
            i = i + 1;
            j = j + 1;
        }
    }

    if i < arr1.len() {
        result.extend_from_slice(&arr1[i..]);
    }

    if j < arr2.len() {
        result.extend_from_slice(&arr2[j..]);
    }

    return result;
}

pub fn now_timestamp() -> u64 {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(n) => n.as_millis() as u64,
        Err(_) => panic!("SystemTime before UNIX EPOCH!"),
    }
}

pub fn to_str(arr: &[u8]) -> String {
    String::from_utf8(arr.to_vec()).unwrap()
}
