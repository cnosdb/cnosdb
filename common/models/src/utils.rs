use std::time::{SystemTime, UNIX_EPOCH};

const LOW_40BIT_MASK: u64 = (0x01 << 40) - 1;
const HIGH_24BIT_MASK: u64 = ((0x01 << 24) - 1) << 40;

const LOW_4BIT_MASK: u8 = (0x01 << 4) - 1;
const HIGH_4BIT_MASK: u8 = ((0x01 << 4) - 1) << 4;

pub fn split_id(id: u64) -> (u32, u64) {
    (((id & HIGH_24BIT_MASK) >> 40) as u32, id & LOW_40BIT_MASK)
}

pub fn split_code_type_id(id: u8) -> (u8, u8) {
    ((id & HIGH_4BIT_MASK) >> 4, id & LOW_4BIT_MASK)
}

pub fn combine_code_type_id(ts: u8, val: u8) -> u8 {
    (ts & LOW_4BIT_MASK) << 4 | val & LOW_4BIT_MASK
}

pub fn unite_id(hash_id: u64, incr_id: u64) -> u64 {
    hash_id << 40 | (incr_id & LOW_40BIT_MASK)
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
            i += 1;
        } else if (arr1[i] & LOW_40BIT_MASK) > (arr2[j] & LOW_40BIT_MASK) {
            j += 1;
        } else {
            result.push(arr1[i]);
            i += 1;
            j += 1;
        }
    }

    result
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
            i += 1;
        } else if (arr1[i] & LOW_40BIT_MASK) > (arr2[j] & LOW_40BIT_MASK) {
            result.push(arr2[j]);
            j += 1;
        } else {
            result.push(arr1[i]);
            i += 1;
            j += 1;
        }
    }

    if i < arr1.len() {
        result.extend_from_slice(&arr1[i..]);
    }

    if j < arr2.len() {
        result.extend_from_slice(&arr2[j..]);
    }

    result
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

pub fn min_num<T: std::cmp::PartialOrd>(a: T, b: T) -> T {
    if a < b {
        return a;
    } else {
        return b;
    }
}

pub fn max_num<T: std::cmp::PartialOrd>(a: T, b: T) -> T {
    if a > b {
        return a;
    } else {
        return b;
    }
}

#[cfg(test)]
mod test {
    use crate::utils::{combine_code_type_id, split_code_type_id};

    #[test]
    fn test_split_code_type_id() {
        let id_0 = 0b00010011;
        let (code0, code1) = split_code_type_id(id_0);
        assert_eq!(code0, 1);
        assert_eq!(code1, 3);

        let id_1 = 0b01011111;
        let (code2, code3) = split_code_type_id(id_1);
        assert_eq!(code2, 5);
        assert_eq!(code3, 15)
    }

    #[test]
    fn test_combine_code_type_id() {
        let id_0 = 0b00000001;
        let id_1 = 0b00000001;

        let code = combine_code_type_id(id_0, id_1);
        assert_eq!(code, 0b00010001);
    }
}
