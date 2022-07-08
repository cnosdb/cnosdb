use std::{
    cell::RefCell,
    collections::HashMap,
    sync::{Arc, Mutex},
};

use config::GLOBAL_CONFIG;
use tokio::sync::RwLock;

use crate::{
    kv_option::TseriesFamDesc,
    memcache::MemCache,
    tseries_family::{TseriesFamily, Version},
};

pub struct VersionSet {
    ts_families: HashMap<u32, TseriesFamily>,
    ts_families_names: HashMap<String, u32>,
}

impl VersionSet {
    pub async fn new(desc: &[TseriesFamDesc],
                     vers_set: HashMap<u32, Arc<RwLock<Version>>>)
                     -> Self {
        let mut ts_families = HashMap::new();
        let mut ts_families_names = HashMap::new();
        for (id, ver) in vers_set {
            let name = ver.read().await.get_name().to_string();
            let seq = ver.read().await.log_no;
            for item in desc.iter() {
                if item.name == name {
                    let tf = TseriesFamily::new(id,
                                                name.clone(),
                                                MemCache::new(id,
                                                              GLOBAL_CONFIG.max_memcache_size,
                                                              seq,
                                                              false),
                                                ver.clone(),
                                                item.opt.clone()).await;
                    ts_families.insert(id, tf);
                    ts_families_names.insert(name.clone(), id);
                }
            }
        }

        Self { ts_families, ts_families_names }
    }

    pub fn new_default() -> Self {
        Self { ts_families: Default::default(), ts_families_names: Default::default() }
    }

    pub async fn switch_memcache(&mut self, tf_id: u32, seq: u64) {
        let tf = self.ts_families.get_mut(&tf_id).unwrap();
        let mem = Arc::new(RwLock::new(MemCache::new(tf_id,
                                                     GLOBAL_CONFIG.max_memcache_size,
                                                     seq,
                                                     false)));
        tf.switch_memcache(mem).await;
    }

    // todo: deal with add tsf and del tsf
    pub fn get_tsfamily_immut(&self, sid: u64) -> Option<&TseriesFamily> {
        if self.ts_families.is_empty() {
            return None;
        }
        let partid = sid as u32 % self.ts_families.len() as u32;
        self.ts_families.get(&partid)
    }

    // todo: deal with add tsf and del tsf
    pub fn get_tsfamily(&mut self, sid: u64) -> Option<&mut TseriesFamily> {
        if self.ts_families.is_empty() {
            return None;
        }
        let partid = sid as u32 % self.ts_families.len() as u32;
        self.ts_families.get_mut(&partid)
    }
}
