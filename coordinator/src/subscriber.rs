use std::collections::HashMap;
use std::sync::Arc;

use config::sub_config::SubConfig;
use meta::model::MetaRef;
use models::meta_data::*;
use protos::kv_service::tskv_service_client::TskvServiceClient;
use tokio::sync::{mpsc, RwLock};
use tokio::time::Duration;
use tonic::transport::Channel;
use tower::timeout::Timeout;
use trace::info;

use crate::WriteRequest;

struct SubEntry {
    pub tenant: String,
    pub db_name: String,
    pub _table_name: String,

    pub sender: async_channel::Sender<WriteRequest>,
}

// #[derive(Debug)]
// struct DataBlock {
//     pub ts: i64,
//     pub tenant_len: u32,
//     pub db_name_len: u32,
//     pub data_len: u32,

//     pub tenant: String,
//     pub db_name: String,
//     pub data: Vec<u8>,
// }

pub struct SubService {
    meta: MetaRef,
    config: SubConfig,
    sender: mpsc::Sender<WriteRequest>,
    sub_map: RwLock<HashMap<String, SubEntry>>, //tenant.db.name -> SubEntry
}

impl SubService {
    pub async fn new(config: SubConfig, meta: MetaRef) -> Arc<Self> {
        let (sender, recver) = mpsc::channel(config.buf_size);
        let sub = Arc::new(Self {
            meta,
            config,
            sender,
            sub_map: RwLock::new(HashMap::new()),
        });

        tokio::spawn(SubService::update_subscription(sub.clone()));
        tokio::spawn(SubService::dispatch_service(sub.clone(), recver));

        sub
    }

    pub async fn publish_request(&self, request: WriteRequest) {
        let _ = self.sender.send(request).await;
    }

    async fn update_subscription(service: Arc<SubService>) {
        let mut receiver = service.meta.subscribe_sub_change().await;

        while let Ok(sub) = receiver.recv().await {
            info!("get subscription info: {:?}", sub);
            let mut sub_map = service.sub_map.write().await;

            let key = format!("{}.{}.{}", sub.tenant, sub.db_name, sub.info.name);
            if sub.opt_type == OPERATION_TYPE_ADD || sub.opt_type == OPERATION_TYPE_UPDATE {
                let (sender, receiver) = async_channel::bounded(service.config.buf_size);
                let entry = SubEntry {
                    sender,
                    tenant: sub.tenant,
                    db_name: sub.db_name,
                    _table_name: sub.info._table_name,
                };

                if sub.info.destinations.is_empty() {
                    continue;
                }

                for _ in 0..service.config.concurrency {
                    tokio::spawn(SubService::send_to_destination(
                        receiver.clone(),
                        sub.info.destinations.clone(),
                        service.config.timeout,
                    ));
                }
                sub_map.insert(key, entry);
            } else if sub.opt_type == OPERATION_TYPE_DELETE {
                sub_map.remove(&key);
            }
        }
    }

    async fn dispatch_service(
        service: Arc<SubService>,
        mut receiver: mpsc::Receiver<WriteRequest>,
    ) {
        while let Some(request) = receiver.recv().await {
            let sub_map = service.sub_map.read().await;
            for (_, entry) in sub_map.iter() {
                if request.tenant == entry.tenant && request.db_name == entry.db_name {
                    let _ = entry.sender.send(request.clone()).await;
                }
            }
        }
    }

    async fn send_to_destination(
        receiver: async_channel::Receiver<WriteRequest>,
        dests: Vec<String>,
        timeout: usize,
    ) {
        info!("create subscription sender: {:?}", dests);
        let endpoints = dests
            .iter()
            .map(|a| Channel::from_shared(format!("http://{}", a)).unwrap());
        let channel = Channel::balance_list(endpoints);
        let timeout_channel = Timeout::new(channel, Duration::from_secs(timeout as u64));
        let mut client = TskvServiceClient::<Timeout<Channel>>::new(timeout_channel);

        while let Ok(request) = receiver.recv().await {
            SubService::write_until_success(&mut client, request).await;
        }

        info!("finish subscription sender: {:?}", dests);
    }

    async fn write_until_success(
        client: &mut TskvServiceClient<Timeout<Channel>>,
        request: WriteRequest,
    ) {
        loop {
            let stream = tokio_stream::iter(vec![request.request.clone()]);
            if let Err(err) = client.write_points(stream).await {
                info!("subscriber call remote failed: {}", err);
                tokio::time::sleep(tokio::time::Duration::from_secs(3)).await;
            } else {
                break;
            }
        }
    }
}

impl std::fmt::Debug for SubService {
    fn fmt(&self, _f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Ok(())
    }
}
