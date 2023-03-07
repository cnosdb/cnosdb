// use tokio::fs::File;
// use tokio::fs::OpenOptions;
// use tokio::io::{self, AsyncReadExt, AsyncWriteExt};
//
// use std::io::Cursor;
// use std::io::Error;
// use std::io::ErrorKind;
// use walkdir::WalkDir;
//
// use crate::store::state_machine::StateMachineContent;
// use crate::store::Store;
// use crate::{ClusterNode, ClusterNodeId, TypeConfig};
// use openraft::storage::Snapshot;
// use openraft::SnapshotMeta;
// use crate::error::StorageResult;

// type SnapshotType = Snapshot<ClusterNodeId, ClusterNode, Cursor<Vec<u8>>>;
// impl Store {
//     #[tracing::instrument(level = "debug", skip(self))]
//     pub async fn write_snapshot(&self) -> io::Result<()> {
//         tracing::debug!("write_snapshot: start");
//
//         match &*self.current_snapshot.read().await {
//             Some(snapshot) => {
//                 let file_name = format!(
//                     "{}/{}+{}.bin",
//                     self.config.snapshot_path,
//                     self.node_id,
//                     snapshot.meta.snapshot_id
//                 );
//                 tracing::debug!("write_snapshot: [{:?}, +oo)", file_name);
//                 let file = OpenOptions::new()
//                     .write(true)
//                     .create_new(true)
//                     .open(file_name)
//                     .await;
//                 match file {
//                     Ok(mut file) => file.write_all(snapshot.data.as_slice()).await?,
//                     Err(_e) => (), // TODO: we need to change index and write to another file.
//                 }
//             }
//             None => (),
//         }
//         Ok(())
//     }
//
//     #[tracing::instrument(level = "debug", skip(self))]
//     pub async fn read_snapshot_file(&self) -> io::Result<Vec<u8>> {
//         let latest_file = match self.latest_snapshot_file().await {
//             Ok(file) => file,
//             _ => return Err(Error::new(ErrorKind::NotFound, "No snapshot files")),
//         };
//         tracing::debug!("read_file: {}", latest_file);
//
//         let file = File::open(&latest_file).await;
//         let mut file = match file {
//             Ok(file) => file,
//             Err(e) => return Err(e),
//         };
//         let mut data = Vec::new();
//         file.read_to_end(&mut data).await?;
//
//         Ok(data)
//     }
//
//     #[tracing::instrument(level = "debug", skip(self))]
//     pub async fn latest_snapshot_file(&self) -> Result<String, ()> {
//         let mut max_index: u64 = 0;
//         let mut latest_snapshot_file: String = String::from("");
//
//         for entry in WalkDir::new(&self.config.snapshot_path)
//             .into_iter()
//             .filter_map(Result::ok)
//             .filter(|e| !e.file_type().is_dir())
//         {
//             let f_name = String::from(entry.file_name().to_string_lossy());
//             let mut s1 = f_name.split('.');
//             let file = s1.next();
//             let ext = s1.next();
//             if ext.unwrap() == "bin" {
//                 tracing::debug!("file: {:?}", file);
//                 let mut s3 = file.unwrap().split('+');
//                 let node_id = s3.next().unwrap();
//                 if node_id != self.node_id.to_string() {
//                     continue;
//                 };
//                 let snapshot_id = s3.next().unwrap();
//
//                 let mut s2 = snapshot_id.split('-');
//                 //TODO:
//                 let _term_id = s2.next();
//                 let _node_id = s2.next();
//                 let index = s2.next();
//                 let _snapshot_id = s2.next();
//
//                 let index = index.unwrap().parse::<u64>().unwrap();
//                 if index > max_index {
//                     max_index = index;
//                     latest_snapshot_file = f_name;
//                 }
//             }
//         }
//         if !latest_snapshot_file.is_empty() {
//             Ok(format!(
//                 "{}/{}",
//                 self.config.snapshot_path, latest_snapshot_file
//             ))
//         } else {
//             Err(())
//         }
//     }
//
//     #[tracing::instrument(level = "trace", skip(self))]
//     pub async fn load_latest_snapshot(&self) -> StorageResult<Option<SnapshotType>> {
//         tracing::debug!("load_latest_snapshot: start");
//
//         match &*self.current_snapshot.read().await {
//             Some(snapshot) => {
//                 let data = snapshot.data.clone();
//                 Ok(Some(Snapshot {
//                     meta: snapshot.meta.clone(),
//                     snapshot: Box::new(Cursor::new(data)),
//                 }))
//             }
//             None => {
//                 let data = self.read_snapshot_file().await;
//                 let data = match data {
//                     Ok(c) => c,
//                     Err(_e) => return Ok(None),
//                 };
//
//                 let content: StateMachineContent = serde_json::from_slice(&data).unwrap();
//
//                 let last_applied_log = content.last_applied_log.unwrap();
//                 tracing::debug!(
//                     "load_latest_snapshot: last_applied_log = {:?}",
//                     last_applied_log
//                 );
//
//                 let snapshot_idx = {
//                     let mut l = self.snapshot_idx.lock().unwrap();
//                     *l += 1;
//                     *l
//                 };
//
//                 let snapshot_id = format!(
//                     "{}-{}-{}",
//                     last_applied_log.leader_id, last_applied_log.index, snapshot_idx
//                 );
//
//                 let meta = SnapshotMeta {
//                     last_log_id: last_applied_log,
//                     snapshot_id,
//                 };
//
//                 tracing::debug!("load_latest_snapshot: meta {:?}", meta);
//
//                 Ok(Some(Snapshot {
//                     meta,
//                     snapshot: Box::new(Cursor::new(data)),
//                 }))
//             }
//         }
//     }
// }
