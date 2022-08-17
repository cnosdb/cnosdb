use std::fs;
use std::io;
use std::path;

use sled;

#[derive(Debug)]
pub struct IndexEngine {
    db: sled::Db,
    dir: path::PathBuf,
}

impl IndexEngine {
    pub fn new(path: &String) -> IndexEngine {
        let dir = path::Path::new(&path).to_path_buf();
        fs::create_dir_all(&dir);

        let config = sled::Config::new()
            .path(path)
            .cache_capacity(128 * 1024 * 1024)
            .mode(sled::Mode::HighThroughput);

        let db = config.open().expect(&format!("open db {} failed!", &path));
        db.set_merge_operator(concatenate_merge);

        Self { db, dir }
    }

    pub fn set(&self, key: &[u8], value: &[u8]) -> Result<(), sled::Error> {
        self.db.insert(key, value)?;

        Ok(())
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, sled::Error> {
        let val = self.db.get(key)?;

        match val {
            None => Ok(None),
            Some(v) => Ok(Some(v.to_vec())),
        }
    }

    pub fn push(&self, key: &[u8], item: &[u8]) -> Result<(), sled::Error> {
        self.db.merge(key, item)?;

        Ok(())
    }

    pub fn delete(&self, key: &[u8]) -> Result<(), sled::Error> {
        self.db.remove(key)?;

        Ok(())
    }

    pub fn prefix(&self, key: &[u8]) -> sled::Iter {
        return self.db.scan_prefix(key);
    }

    pub fn exist(&self, key: &[u8]) -> Result<bool, sled::Error> {
        let res = self.db.contains_key(key)?;

        Ok(res)
    }

    pub fn batch(&self, batch: sled::Batch) -> Result<(), sled::Error> {
        let res = self.db.apply_batch(batch)?;

        Ok(())
    }

    pub fn incr_id(&self) -> Result<u64, sled::Error> {
        let id = self.db.generate_id()?;

        Ok(id)
    }

    pub fn flush(&self) {
        self.db.flush().unwrap();
    }
}

fn concatenate_merge(
    _key: &[u8],              // the key being merged
    old_value: Option<&[u8]>, // the previous value, if one existed
    merged_bytes: &[u8],      // the new bytes being merged in
) -> Option<Vec<u8>> {
    // set the new value, return None to delete
    let mut ret = old_value.map(|ov| ov.to_vec()).unwrap_or_else(|| vec![]);

    ret.extend_from_slice(merged_bytes);

    Some(ret)
}
