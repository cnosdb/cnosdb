use std::{rc::Rc, sync::Arc};

use crate::{option::TseriesFamOpt, MemCache};

pub struct Version {}

pub struct SuperVersion {
    pub id: u32,
    pub mut_cache: Arc<MemCache>,
    pub immut_cache: Vec<Arc<MemCache>>,
    pub cur_version: Arc<Version>,
    pub opt: Arc<TseriesFamOpt>,
    pub version_id: u32,
}

pub struct Summary {}
pub struct TseriesFamily {
    mut_cache: Rc<MemCache>,
    immut_cache: Vec<Rc<MemCache>>,

    seq_no: u64,
    opts: TseriesFamOpt,
}
