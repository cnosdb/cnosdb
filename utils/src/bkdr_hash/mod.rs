const SEED: u64 = 1313;

pub fn hash_with_bytes(data: &Vec<u8>) -> u64 {
    let mut hash: u64 = 0;
    for c in data.iter() {
        hash = hash.wrapping_mul(SEED).wrapping_add(*c as u64);
    }

    hash
}

#[test]
fn test_hash() {
    let data = Vec::<u8>::from("1234");
    let hash = hash_with_bytes(&data);
    println!("{}", hash);
}
