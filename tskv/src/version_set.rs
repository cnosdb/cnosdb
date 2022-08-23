use std::{
    collections::HashMap,
    sync::{atomic::AtomicU32, atomic::Ordering, Arc, Mutex},
};

use parking_lot::RwLock;
use tokio::sync::{mpsc::UnboundedSender, oneshot};
use trace::error;

use crate::tseries_family::LevelInfo;
use crate::{
    kv_option::{TseriesFamDesc, TseriesFamOpt},
    memcache::MemCache,
    summary::{SummaryTask, VersionEdit},
    tseries_family::{TseriesFamily, Version},
};

#[derive(Debug)]
pub struct VersionSet {
    tf_incr_id: AtomicU32,
    ts_families: HashMap<u32, TseriesFamily>,
    ts_families_names: HashMap<String, u32>,
}

impl VersionSet {
    pub async fn new(desc: &[Arc<TseriesFamDesc>], vers_set: HashMap<u32, Arc<Version>>) -> Self {
        let mut ts_families = HashMap::new();
        let mut ts_families_names = HashMap::new();
        let mut max_id = 0_u32;
        for (id, ver) in vers_set {
            let name = ver.tf_name().to_string();
            let seq = ver.last_seq;
            for item in desc.iter() {
                if item.name == name {
                    let tf = TseriesFamily::new(
                        id,
                        name.clone(),
                        MemCache::new(id, item.tsf_opt.max_memcache_size, seq, false),
                        ver.clone(),
                        item.tsf_opt.clone(),
                    );
                    ts_families.insert(id, tf);
                    ts_families_names.insert(name.clone(), id);

                    if max_id < id {
                        max_id = id;
                    }
                }
            }
        }

        Self {
            tf_incr_id: AtomicU32::new(max_id + 1),
            ts_families,
            ts_families_names,
        }
    }

    pub fn new_default() -> Self {
        Self {
            tf_incr_id: AtomicU32::new(1),
            ts_families: Default::default(),
            ts_families_names: Default::default(),
        }
    }

    pub async fn switch_memcache(&mut self, tf_id: u32, seq: u64) {
        match self.ts_families.get_mut(&tf_id) {
            Some(tf) => {
                let mem = Arc::new(RwLock::new(MemCache::new(
                    tf_id,
                    tf.options().max_memcache_size,
                    seq,
                    false,
                )));
                tf.switch_memcache(mem).await;
            }
            None => {
                error!("ts_family with tsf id {} not found.", tf_id);
            }
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

    pub fn get_mutable_tsfamily_by_tf_id(&mut self, tf_id: u32) -> Option<&mut TseriesFamily> {
        if self.ts_families.is_empty() {
            return None;
        }
        self.ts_families.get_mut(&tf_id)
    }

    pub fn get_tsfamily_by_name(&self, name: &String) -> Option<&TseriesFamily> {
        if let Some(v) = self.ts_families_names.get(name) {
            return self.ts_families.get(v);
        }

        None
    }

    pub fn get_mutable_tsfamily_by_name(&mut self, name: &String) -> Option<&mut TseriesFamily> {
        if let Some(v) = self.ts_families_names.get(name) {
            return self.ts_families.get_mut(v);
        }

        None
    }

    // todo: Maybe TseriesFamily::new() should be refactored.
    #[allow(clippy::too_many_arguments)]
    pub fn add_tsfamily(
        &mut self,
        tsf_name: String,
        seq_no: u64,
        file_id: u64,
        opt: Arc<TseriesFamOpt>,
        summary_task_sender: UnboundedSender<SummaryTask>,
    ) -> &mut TseriesFamily {
        let tsf_id = self.tf_incr_id.fetch_add(1, Ordering::SeqCst);

        let tf = TseriesFamily::new(
            tsf_id,
            tsf_name.clone(),
            MemCache::new(tsf_id, opt.max_memcache_size, seq_no, false),
            Arc::new(Version::new(
                tsf_id,
                tsf_name.clone(),
                opt.clone(),
                file_id,
                LevelInfo::init_levels(opt.clone()),
                i64::MIN,
            )),
            opt,
        );
        self.ts_families.insert(tsf_id, tf);
        self.ts_families_names.insert(tsf_name.clone(), tsf_id);

        let mut edit = VersionEdit::new();
        edit.add_tsfamily(tsf_id, tsf_name);
        let edits = vec![edit];
        let (task_state_sender, task_state_receiver) = oneshot::channel();
        let task = SummaryTask {
            edits,
            cb: task_state_sender,
        };
        if let Err(e) = summary_task_sender.send(task) {
            error!("failed to send Summary task, {:?}", e);
        }

        self.ts_families.get_mut(&tsf_id).unwrap()
    }

    pub fn del_tsfamily(
        &mut self,
        tf_id: u32,
        name: String,
        summary_task_sender: UnboundedSender<SummaryTask>,
    ) {
        match self.ts_families_names.get(&name) {
            None => {
                error!("failed get tf id with tf name {}", &name);
            }
            Some(v) => {
                if tf_id != *v {
                    error!("tf_id and name can`t match");
                    return;
                }
            }
        }
        self.ts_families.remove(&tf_id);
        self.ts_families_names.remove(&name);
        let mut edits = vec![];
        let mut edit = VersionEdit::new();
        edit.del_tsfamily(tf_id);
        edits.push(edit);
        let (task_state_sender, task_state_receiver) = oneshot::channel();
        let task = SummaryTask {
            edits,
            cb: task_state_sender,
        };
        if let Err(e) = summary_task_sender.send(task) {
            error!("failed to send Summary task, {:?}", e);
        }
    }

    pub fn tsf_num(&self) -> usize {
        self.ts_families.len()
    }

    pub fn ts_families(&self) -> &HashMap<u32, TseriesFamily> {
        &self.ts_families
    }

    pub fn ts_families_names(&self) -> &HashMap<String, u32> {
        &self.ts_families_names
    }
}
