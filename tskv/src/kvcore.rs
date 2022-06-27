use std::{borrow::BorrowMut, cell::RefCell, ops::DerefMut, sync::Arc, thread::JoinHandle};
use std::collections::HashMap;

use ::models::{InMemPoint, FieldInfo, SeriesInfo, Tag, ValueType};
use once_cell::sync::OnceCell;
use parking_lot::Mutex;
use protos::{
    kv_service::{WritePointsRpcRequest, WritePointsRpcResponse, WriteRowsRpcRequest},
    models as fb_models,
};
use snafu::ResultExt;
use tokio::{
    runtime::Builder,
    sync::{
        mpsc::{self, UnboundedReceiver, UnboundedSender},
        oneshot, RwLock,
    },
};
use models::{FieldID, SeriesID};

use crate::{
    context::GlobalContext,
    error::{self, Result},
    file_manager::{self, FileManager},
    file_utils,
    forward_index::ForwardIndex,
    kv_option::{DBOptions, Options, QueryOption, TseriesFamDesc, TseriesFamOpt, WalConfig},
    memcache::{MemCache, DataType},
    record_file::Reader,
    runtime::WorkerQueue,
    summary::{Summary, VersionEdit},
    tseries_family::Version,
    version_set,
    version_set::VersionSet,
    wal::{self, WalEntryType, WalManager, WalTask},
    Error, Task,
};
use crate::tseries_family::TimeRange;
use crate::tsm::{BlockReader, TsmBlockReader, TsmIndexReader};

pub struct Entry {
    pub series_id: u64,
}

pub struct TsKv {
    options: Arc<Options>,
    kvctx: Arc<KvContext>,
    version_set: Arc<RwLock<VersionSet>>,

    wal_sender: UnboundedSender<WalTask>,
    forward_index: Arc<RwLock<ForwardIndex>>,
}

impl TsKv {
    pub async fn open(opt: Options) -> Result<Self> {
        let shared_options = Arc::new(opt);
        let kvctx = Arc::new(KvContext::new(shared_options.clone()));
        let version_set = Self::recover(shared_options.clone()).await;
        let mut fidx = ForwardIndex::new(&shared_options.forward_index_conf.path);
        fidx.load_cache_file().await.map_err(|err| Error::LogRecordErr { source: err })?;
        let (wal_sender, wal_receiver) = mpsc::unbounded_channel();
        let core = Self { options: shared_options,
                          kvctx,
                          forward_index: Arc::new(RwLock::new(fidx)),
                          version_set,
                          wal_sender };
        core.run_wal_job(wal_receiver);

        Ok(core)
    }

    async fn recover(opt: Arc<Options>) -> Arc<RwLock<VersionSet>> {
        if !file_manager::try_exists(&opt.db.db_path) {
            std::fs::create_dir_all(&opt.db.db_path).context(error::IOSnafu).unwrap();
        }
        let summary_file = file_utils::make_summary_file(&opt.db.db_path, 0);
        let summary = if file_manager::try_exists(&summary_file) {
            Summary::recover(&[TseriesFamDesc { name: "default".to_string(),
                                                opt: TseriesFamOpt::default() }],
                             &opt.db).await
                                     .unwrap()
        } else {
            Summary::new(&[TseriesFamDesc { name: "default".to_string(),
                                            opt: TseriesFamOpt::default() }],
                         &opt.db).await
                                 .unwrap()
        };
        let version_set = summary.version_set();
        let wal_manager = WalManager::new(opt.wal.clone());
        wal_manager.recover(version_set.clone(), summary.global_context()).await.unwrap();

        version_set.clone()
    }

    pub async fn write(&self,
                       write_batch: WritePointsRpcRequest)
                       -> Result<WritePointsRpcResponse> {
        let shared_write_batch = Arc::new(write_batch.points);
        let fb_points = flatbuffers::root::<fb_models::Points>(&shared_write_batch)
            .context(error::InvalidFlatbufferSnafu)?;

        // get or create forward index
        for point in fb_points.points().unwrap() {
            let info = SeriesInfo::from_flatbuffers(&point).context(error::InvalidModelSnafu)?;
            self.forward_index
                .write()
                .await
                .add_series_info_if_not_exists(info)
                .await
                .context(error::ForwardIndexErrSnafu)?;
        }

        // write wal
        let (cb, rx) = oneshot::channel();
        self.wal_sender
            .send(WalTask::Write { points: shared_write_batch.clone(), cb })
            .map_err(|err| Error::Send)?;
        let (seq, _) = rx.await.context(error::ReceiveSnafu)??;

        // write memcache
        if let Some(points) = fb_points.points() {
            let mut version_set = self.version_set.write().await;
            for point in points.iter() {
                let p = InMemPoint::from(point);
                let sid = p.series_id();
                if let Some(tsf) = version_set.get_tsfamily(sid) {
                    for f in p.fields().iter() {
                        tsf.put_mutcache(f.field_id(),
                                         &f.value,
                                         f.value_type,
                                         seq,
                                         point.timestamp() as i64)
                           .await
                    }
                } else {
                    println!("[WARN] [tskv] ts_family for sid {} not found.", sid);
                }
            }
        }

        // let _ = self.kvctx.shard_write(0, write_batch).await;
        Ok(WritePointsRpcResponse { version: 1, points: vec![] })
    }

    pub async fn read(&self, sids: Vec<SeriesID>, time_range: TimeRange, fields: Vec<FieldID>) {
        let mut version_set = self.version_set.write().await;
        let mut output = HashMap::<&FieldID,Vec<DataType>>::new();
        for sid in sids {
            if let Some(tsf) = version_set.get_tsfamily(sid) {
                for field_id in fields.iter() {
                    //get data from memcache
                    if let Some(mem_entry) = tsf.cache().read().await.data_cache.get(field_id) {
                        let mut out_put_raw = vec![];
                        for i in mem_entry.cells.iter() {
                            if i.timestamp() > time_range.min_ts && i.timestamp() < time_range.max_ts {
                                out_put_raw.push(i.clone());
                            }
                        }
                                output.insert(field_id, out_put_raw);
                    }
                    //get data from im_memcache
                    for mem_cache in tsf.im_cache().iter() {
                        if let Some(mem_entry) = mem_cache.read().await.data_cache.get(field_id) {
                            let mut out_put_raw = vec![];
                            for i in mem_entry.cells.iter() {
                                if i.timestamp() > time_range.min_ts && i.timestamp() < time_range.max_ts {
                                    out_put_raw.push(i.clone());
                                }
                            }
                            output.get_mut(field_id).unwrap().append(&mut out_put_raw);
                        }
                    }
                    //get data from levelinfo
                   for level_info in tsf.version().levels_info.iter() {
                       for file in level_info.files.iter() {
                           let fs = FileManager::new();
                           let ts_cf = TseriesFamOpt::default();
                           let p = format!("/_{:06}.tsm",file.file_id());
                           // println!("{}",ts_cf.wsm_dir+ &*tsf.tf_id().to_string()+ &*p);
                           let fs = fs.open_file(ts_cf.wsm_dir.clone()+ &*tsf.tf_id().to_string()+ &*p).unwrap();
                           let len = fs.len();
                           let mut fs_cursor = fs.into_cursor();
                           let index = TsmIndexReader::try_new(&mut fs_cursor, len as usize);
                           let mut blocks = Vec::new();
                           for res in &mut index.unwrap() {
                               let entry = res.unwrap();
                               let key = entry.filed_id();

                               blocks.push(entry.block);
                           }

                           let mut block_reader = TsmBlockReader::new(&mut fs_cursor);
                           for block in blocks {
                               let data = block_reader.decode(&block).expect("error decoding block data");
                               println!("{:?}",data);
                           }
                       }
                   }
                }
            }
        }
        for data in output.iter() {
            println!("{}::{:?}",data.0,data.1);
        }
    }

    pub async fn insert_cache(&self, seq: u64, buf: &[u8]) -> Result<()> {
        let ps =
            flatbuffers::root::<fb_models::Points>(buf).context(error::InvalidFlatbufferSnafu)?;
        if let Some(points) = ps.points() {
            let mut version_set = self.version_set.write().await;
            for point in points.iter() {
                let p = InMemPoint::from(point);
                // use sid to dispatch to tsfamily
                // so if you change the colume name
                // please keep the series id
                let sid = p.series_id();
                if let Some(tsf) = version_set.get_tsfamily(sid) {
                    for f in p.fields().iter() {
                        tsf.put_mutcache(f.field_id(),
                                         &f.value,
                                         f.value_type,
                                         seq,
                                         point.timestamp() as i64)
                           .await
                    }
                }
            }
        }

        Ok(())
    }

    fn run_wal_job(&self, mut receiver: UnboundedReceiver<WalTask>) {
        println!("[WARN] [tskv] job 'WAL' starting.");
        let wal_opt = self.options.wal.clone();
        let mut wal_manager = WalManager::new(wal_opt);
        let f = async move {
            while let Some(x) = receiver.recv().await {
                match x {
                    WalTask::Write { points, cb } => {
                        // write wal
                        let ret = wal_manager.write(wal::WalEntryType::Write, &points).await;
                        let send_ret = cb.send(ret);
                        match send_ret {
                            Ok(wal_result) => {},
                            Err(err) => {
                                println!("[WARN] send WAL write result failed.")
                            },
                        }
                    },
                }
            }
        };
        tokio::spawn(f);
        println!("[WARN] [tskv] job 'WAL' started.");
    }

    pub fn start(tskv: TsKv, mut req_rx: UnboundedReceiver<Task>) {
        println!("[WARN] [tskv] job 'main' starting.");
        let f = async move {
            while let Some(command) = req_rx.recv().await {
                match command {
                    Task::WritePoints { req, tx } => {
                        println!("[WARN] [tskv] writing points.");
                        match tskv.write(req).await {
                            Ok(resp) => {
                                let _ret = tx.send(Ok(resp));
                            },
                            Err(err) => {
                                let _ret = tx.send(Err(err));
                            },
                        }
                        println!("[WARN] [tskv] write points completed.");
                    },
                    _ => panic!("unimplented."),
                }
            }
        };

        tokio::spawn(f);
        println!("[WARN] [tskv] job 'main' started.");
    }
    pub async fn query(&self, _opt: QueryOption) -> Result<Option<Entry>> {
        Ok(None)
    }
}

#[allow(dead_code)]
enum KvStatus {
    Init,
    Recover,
    Runing,
    Closed,
}

#[allow(dead_code)]
pub(crate) struct KvContext {
    front_handler: Arc<WorkerQueue>,
    handler: Vec<JoinHandle<()>>,
    status: KvStatus,
}

#[allow(dead_code)]
impl KvContext {
    pub fn new(opt: Arc<Options>) -> Self {
        let front_work_queue = Arc::new(WorkerQueue::new(opt.db.front_cpu));
        let worker_handle = Vec::with_capacity(opt.db.back_cpu);
        Self { front_handler: front_work_queue, handler: worker_handle, status: KvStatus::Init }
    }

    pub fn recover(&mut self) -> Result<()> {
        // todo: index wal summary shardinfo recover here.
        self.status = KvStatus::Recover;
        Ok(())
    }

    pub async fn shard_write(&self,
                             partion_id: usize,
                             mem: Arc<RwLock<MemCache>>,
                             entry: WritePointsRpcRequest)
                             -> Result<()> {
        let (tx, rx) = oneshot::channel();
        self.front_handler.add_task(partion_id, async move {
                               let ps =
                                   flatbuffers::root::<fb_models::Points>(&entry.points).unwrap();
                               let err = 0;
                               // todo
                               let _ = tx.send(err);
                           })?;
        rx.await.unwrap();
        Ok(())
    }
    pub async fn shard_query(&self, _opt: QueryOption) -> Result<Option<Entry>> {
        Ok(None)
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;
    use chrono::Local;

    use futures::{channel::oneshot, future::join_all, SinkExt};
    use snafu::ResultExt;
    use tokio::sync::mpsc;
    use tokio::sync::oneshot::channel;
    use models::{FieldInfo, SeriesInfo, Tag, ValueType};
    use protos::{kv_service, models_helper,models as fb_models,};
    use protos::kv_service::WritePointsRpcResponse;

    use crate::{kv_option::WalConfig, TsKv, Task, error, Error};
    use crate::tseries_family::TimeRange;

    async fn get_tskv() -> TsKv {
        let opt = crate::kv_option::Options { wal: WalConfig { dir: String::from("/tmp/test/wal"),
                                                               ..Default::default() },
                                              ..Default::default() };

        TsKv::open(opt).await.unwrap()
    }

    #[tokio::test]
    async fn test_init() {
        let tskv = get_tskv().await;

        dbg!("Ok");
    }

    #[tokio::test]
    async fn test_write() {
        let tskv = get_tskv().await;

        let database = "db".to_string();
        let mut fbb = flatbuffers::FlatBufferBuilder::new();
        let points = models_helper::create_random_points(&mut fbb, 1);
        fbb.finish(points, None);
        let points = fbb.finished_data().to_vec();
        let request = kv_service::WritePointsRpcRequest { version: 1, database, points };

        tskv.write(request).await.unwrap();
    }

    #[tokio::test]
    async fn test_read() -> Result<(),Error>{
        let tskv = get_tskv().await;

        let database = "db".to_string();
        let mut fbb = flatbuffers::FlatBufferBuilder::new();
        let points = models_helper::create_random_points(&mut fbb, 20);
        fbb.finish(points, None);
        let points = fbb.finished_data().to_vec();
        let request = kv_service::WritePointsRpcRequest { version: 1, database, points };

        tskv.write(request.clone()).await.unwrap();

        let shared_write_batch = Arc::new(request.points);
        let fb_points = flatbuffers::root::<fb_models::Points>(&shared_write_batch).context(error::InvalidFlatbufferSnafu)?;
        let mut sids = vec![];
        let mut fields_id = vec![];
        for point in fb_points.points().unwrap() {
            let mut info = SeriesInfo::from_flatbuffers(&point).context(error::InvalidModelSnafu)?;
            info.finish();
            sids.push(info.series_id().clone());
            for field in info.field_infos().iter() {
                fields_id.push(field.filed_id().clone());
            }
        }
        Ok(tskv.read(sids, TimeRange::new( Local::now().timestamp_millis()+100,0), fields_id).await)
    }

    #[tokio::test]
    async fn test_big_write() {
        let tskv = get_tskv().await;

        let database = "db".to_string();
        let mut fbb = flatbuffers::FlatBufferBuilder::new();
        let points = models_helper::create_big_random_points(&mut fbb, 1);
        fbb.finish(points, None);
        let points = fbb.finished_data().to_vec();

        let request = kv_service::WritePointsRpcRequest { version: 1, database, points };

        tskv.write(request).await.unwrap();
    }

    #[tokio::test]
    async fn test_insert_cache() {
        let tskv = get_tskv().await;

        let database = "db".to_string();
        let mut fbb = flatbuffers::FlatBufferBuilder::new();
        let points = models_helper::create_random_points(&mut fbb, 1);
        fbb.finish(points, None);
        let points = fbb.finished_data();

        tskv.insert_cache(1, points).await.unwrap();
    }

    #[tokio::test]
    async fn test_start() {
        let tskv = get_tskv().await;

        let (wal_sender, wal_receiver) = mpsc::unbounded_channel();
        let (tx, rx) = channel();

        let database = "db".to_string();
        let mut fbb = flatbuffers::FlatBufferBuilder::new();
        let points = models_helper::create_random_points(&mut fbb, 1);
        fbb.finish(points, None);
        let points = fbb.finished_data().to_vec();
        let req = kv_service::WritePointsRpcRequest { version: 1, database, points };

        wal_sender.send(Task::WritePoints { req, tx }).unwrap();

        TsKv::start(tskv, wal_receiver);

        match rx.await {
            Ok(Ok(resp)) => println!("successful"),
            _ => println!("wrong"),
        };
    }
}
