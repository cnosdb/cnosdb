use std::{collections::HashMap, sync::Arc};

use crate::{option::{TseriesFamDesc, MAX_MEMCACHE_SIZE}, tseries_family, MemCache, TseriesFamily, Version};

pub struct VersionSet {
    ts_families: HashMap<u32, TseriesFamily>,
    ts_families_names: HashMap<String, u32>,
}

impl VersionSet {
    pub fn new(desc: &[TseriesFamDesc], vers_set: HashMap<u32, Arc<Version>>) -> Self {
        let mut ts_families = HashMap::new();
        let mut ts_families_names = HashMap::new();
        for (id, ver) in vers_set {
            let name = ver.get_name().to_string();
            let seq = ver.seq_no;
            for item in desc.iter() {
                if item.name == name {
                    let tf = TseriesFamily::new(
                        id,
                        name.clone(),
                        MemCache::new(id, 100 * 1024 * 1024, seq),
                        ver.clone(),
                        item.opt.clone(),
                    );
                    ts_families.insert(id, tf);
                    ts_families_names.insert(name.clone(), id);
                }
            }
        }

        Self {
            ts_families,
            ts_families_names,
        }
    }

    pub fn switch_memcache(&mut self, tf_id:u32, seq: u64){
        let tf = self.ts_families.get_mut(&tf_id).unwrap();
        let mem = Arc::new(MemCache::new(tf_id, MAX_MEMCACHE_SIZE, seq));
        tf.switch_memcache(mem.clone());
    }
}
