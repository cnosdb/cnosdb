use std::collections::HashSet;
use std::sync::Arc;

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use tracing::info;

use crate::error::{MetaError, MetaResult};
use crate::limiter::local_request_limiter::{LocalBucketRequest, LocalBucketResponse};
use crate::store::command::*;

#[derive(Debug, Clone)]
pub struct MetaHttpClient {
    inner: Arc<reqwest::Client>,
    addrs: Vec<String>,
    pub leader: Arc<RwLock<String>>,
}

impl MetaHttpClient {
    /// Create new MetaHttpClient. Param `attrs` is meta server addresses split by character ';'.
    pub fn new(addrs: &str) -> Self {
        let mut addrs: Vec<String> = addrs.split(';').map(|s| s.to_string()).collect();
        addrs.sort();
        let leader_addr = addrs[0].clone();

        Self {
            addrs,
            inner: Arc::new(reqwest::Client::new()),
            leader: Arc::new(RwLock::new(leader_addr)),
        }
    }

    pub async fn read<T>(&self, req: &ReadCommand) -> MetaResult<T>
    where
        T: for<'a> Deserialize<'a>,
    {
        let rsp = self.try_send_to_leader("read", req).await?;

        serde_json::from_str::<MetaResult<T>>(&rsp).map_err(|err| MetaError::SerdeMsgInvalid {
            err: err.to_string(),
        })?
    }

    pub async fn write<T>(&self, req: &WriteCommand) -> MetaResult<T>
    where
        T: for<'a> Deserialize<'a>,
    {
        let rsp = self.try_send_to_leader("write", req).await?;

        serde_json::from_str::<MetaResult<T>>(&rsp).map_err(|err| MetaError::SerdeMsgInvalid {
            err: err.to_string(),
        })?
    }

    pub async fn watch<T>(&self, req: &(String, String, HashSet<String>, u64)) -> MetaResult<T>
    where
        T: for<'a> Deserialize<'a>,
    {
        let rsp = self.try_send_to_leader("watch", req).await?;

        serde_json::from_str::<MetaResult<T>>(&rsp).map_err(|err| MetaError::SerdeMsgInvalid {
            err: err.to_string(),
        })?
    }

    //////////////////////////////////////////////////

    async fn switch_leader(&self) {
        let mut t = self.leader.write();

        if let Ok(index) = self.addrs.binary_search(&t) {
            let index = (index + 1) % self.addrs.len();
            *t = self.addrs[index].clone();
        } else {
            *t = self.addrs[0].clone();
        }
    }

    async fn try_send_to_leader<Req>(&self, uri: &str, req: &Req) -> MetaResult<String>
    where
        Req: Serialize + 'static,
    {
        let mut n_retry = 3;
        loop {
            let res = self.send_rpc_to_leader(uri, req).await;
            if let Ok(_) = res {
                return res;
            }

            n_retry -= 1;
            if n_retry > 0 {
                continue;
            }

            return res;
        }
    }

    async fn send_rpc_to_leader<Req>(&self, uri: &str, req: &Req) -> MetaResult<String>
    where
        Req: Serialize + 'static,
    {
        let ttl = tokio::time::Duration::from_secs(60);
        match tokio::time::timeout(ttl, self.do_send_rpc_to_leader(uri, req)).await {
            Ok(res) => match res {
                Ok(data) => return Ok(data),
                Err(err) => {
                    if let MetaError::ChangeLeader { new_leader } = &err {
                        let mut t = self.leader.write();
                        *t = new_leader.to_string();
                    } else {
                        self.switch_leader().await;
                    }

                    return Err(err);
                }
            },

            Err(_) => {
                self.switch_leader().await;

                return Err(MetaError::MetaClientErr {
                    msg: "Request timeout...".to_string(),
                });
            }
        }
    }

    async fn do_send_rpc_to_leader<Req>(&self, uri: &str, req: &Req) -> MetaResult<String>
    where
        Req: Serialize + 'static,
    {
        let url = format!("http://{}/{}", self.leader.read(), uri);

        let resp = self
            .inner
            .post(url.clone())
            .json(req)
            .send()
            .await
            .map_err(|e| MetaError::MetaClientErr { msg: e.to_string() })?;

        let resp_code = resp.status();
        let data = resp.text().await.map_err(|err| MetaError::MetaClientErr {
            msg: err.to_string(),
        })?;

        if resp_code == http::StatusCode::OK {
            return Ok(data);
        } else if resp_code == http::StatusCode::PERMANENT_REDIRECT {
            return Err(MetaError::ChangeLeader { new_leader: data });
        } else {
            return Err(MetaError::MetaClientErr {
                msg: format!("httpcode: {}, response:{}", resp_code, data),
            });
        }
    }

    pub async fn limiter_request(
        &self,
        cluster: &str,
        tenant: &str,
        request: LocalBucketRequest,
    ) -> MetaResult<LocalBucketResponse> {
        let req = WriteCommand::LimiterRequest {
            cluster: cluster.to_string(),
            tenant: tenant.to_string(),
            request,
        };
        self.write::<LocalBucketResponse>(&req).await
    }
}

#[cfg(test)]
mod test {
    use std::collections::HashSet;
    use std::{thread, time};

    use models::meta_data::{NodeAttribute, NodeInfo};

    use crate::client::MetaHttpClient;
    use crate::store::command;

    #[tokio::test]
    #[ignore]
    async fn test_client() {
        let read_url = "http://127.0.0.1:8901/read";
        let write_url = "http://127.0.0.1:8901/write";
        let cluster = "cluster_xxx".to_string();
        let node = NodeInfo {
            id: 111,
            attribute: NodeAttribute::Hot,
            grpc_addr: "".to_string(),
            http_addr: "127.0.0.1:8888".to_string(),
        };

        let client = reqwest::Client::new();
        let req = command::WriteCommand::AddDataNode(cluster.clone(), node);
        let resp = client.post(write_url).json(&req).send().await.unwrap();

        let data = resp.text().await.unwrap();
        println!("{}", data);

        let req = command::ReadCommand::DataNodes(cluster.clone());
        let resp = client.post(read_url).json(&req).send().await.unwrap();
        let data = resp.text().await.unwrap();
        println!("{}", data);
    }

    #[tokio::test]
    #[ignore]
    async fn test_meta_client() {
        let cluster = "cluster_xxx".to_string();

        //let hand = tokio::spawn(watch_tenant("cluster_xxx", "tenant_test"));

        let client = MetaHttpClient::new("127.0.0.1:8911");

        let node = NodeInfo {
            id: 111,
            attribute: NodeAttribute::Hot,
            grpc_addr: "".to_string(),
            http_addr: "127.0.0.1:8888".to_string(),
        };

        let req = command::WriteCommand::AddDataNode(cluster.clone(), node);
        let rsp = client.write::<()>(&req).await;
        println!("=== add data: {:?}", rsp);
        thread::sleep(time::Duration::from_secs(3));

        let req = command::ReadCommand::DataNodes(cluster.clone());
        let rsp = client.read::<(Vec<NodeInfo>, u64)>(&req).await.unwrap();
        println!("read data nodes: {}", serde_json::to_string(&rsp).unwrap());

        thread::sleep(time::Duration::from_secs(300000));
    }

    #[tokio::test]
    #[ignore]
    async fn test_meta_watch() {
        let mut request = (
            "client_123".to_string(),
            "cluster_xx".to_string(),
            HashSet::from(["tenant_xx".to_string()]),
            0,
        );

        let client = MetaHttpClient::new("127.0.0.1:8901");
        loop {
            let watch_data = client.watch::<command::WatchData>(&request).await.unwrap();
            println!("{:?}", watch_data);

            if !watch_data.entry_logs.is_empty() {
                println!("{}", watch_data.entry_logs[0].val)
            }

            request.3 = watch_data.max_ver;
        }
    }
}
