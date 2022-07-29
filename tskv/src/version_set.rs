use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use config::GLOBAL_CONFIG;
use parking_lot::RwLock;
use tokio::sync::{mpsc::UnboundedSender, oneshot};
use trace::error;

use crate::{
    kv_option::{TseriesFamDesc, TseriesFamOpt},
    memcache::MemCache,
    summary::{SummaryTask, VersionEdit},
    tseries_family::{TseriesFamily, Version},
};

pub struct VersionSet {
    ts_families: HashMap<u32, TseriesFamily>,
    ts_families_names: HashMap<String, u32>,
}

impl VersionSet {
    pub async fn new(desc: &[Arc<TseriesFamDesc>],
                     vers_set: HashMap<u32, Arc<RwLock<Version>>>)
                     -> Self {
        let mut ts_families = HashMap::new();
        let mut ts_families_names = HashMap::new();
        for (id, ver) in vers_set {
            let name = ver.read().get_name().to_string();
            let seq = ver.read().last_seq;
            for item in desc.iter() {
                if item.name == name {
                    let tf = TseriesFamily::new(id,
                                                name.clone(),
                                                MemCache::new(id,
                                                              GLOBAL_CONFIG.max_memcache_size,
                                                              seq,
                                                              false),
                                                ver.clone(),
                                                item.opt.clone());
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
        match self.ts_families.get_mut(&tf_id) {
            Some(tf) => {
                let mem = Arc::new(RwLock::new(MemCache::new(tf_id,
                                                             GLOBAL_CONFIG.max_memcache_size,
                                                             seq,
                                                             false)));
                tf.switch_memcache(mem).await;
            },
            None => {
                error!("ts_family with tsf id {} not found.", tf_id);
            },
        }
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

    pub fn get_tsfamily_by_tf_id(&self, tf_id: u32) -> Option<&TseriesFamily> {
        if self.ts_families.is_empty() {
            return None;
        }
        self.ts_families.get(&tf_id)
    }

    pub async fn add_tsfamily(&mut self,
                              tf_id: u32,
                              name: String,
                              seq_no: u64,
                              file_id: u64,
                              opt: Arc<TseriesFamOpt>,
                              summary_task_sender: UnboundedSender<SummaryTask>) {
        let tf = TseriesFamily::new(tf_id,
                                    name.clone(),
                                    MemCache::new(tf_id,
                                                  GLOBAL_CONFIG.max_memcache_size,
                                                  seq_no,
                                                  false),
                                    Arc::new(RwLock::new(Version::new(tf_id,
                                                                      file_id,
                                                                      name.clone(),
                                                                      vec![],
                                                                      i64::MIN))),
                                    opt.clone());
        self.ts_families.insert(tf_id, tf);
        self.ts_families_names.insert(name.clone(), tf_id);
        let mut edits = vec![];
        let mut edit = VersionEdit::new();
        edit.add_tsfamily(tf_id, name);
        edits.push(edit);
        let (task_state_sender, task_state_receiver) = oneshot::channel();
        let task = SummaryTask { edits, cb: task_state_sender };
        if let Err(e) = summary_task_sender.send(task) {
            error!("failed to send Summary task, {:?}", e);
        }
    }

    pub fn del_tsfamily(&mut self,
                        tf_id: u32,
                        name: String,
                        summary_task_sender: UnboundedSender<SummaryTask>) {
        match self.ts_families_names.get(&name) {
            None => {
                error!("failed get tf id with tf name {}", &name);
            },
            Some(v) => {
                if tf_id != *v {
                    error!("tf_id and name can`t match");
                    return;
                }
            },
        }
        self.ts_families.remove(&tf_id);
        self.ts_families_names.remove(&name);
        let mut edits = vec![];
        let mut edit = VersionEdit::new();
        edit.del_tsfamily(tf_id);
        edits.push(edit);
        let (task_state_sender, task_state_receiver) = oneshot::channel();
        let task = SummaryTask { edits, cb: task_state_sender };
        if let Err(e) = summary_task_sender.send(task) {
            error!("failed to send Summary task, {:?}", e);
        }
    }

    pub fn tsf_num(&self) -> usize {
        self.ts_families.len()
    }
}
