use std::fmt::Debug;
use std::ops::Add;

use lazy_static::lazy_static;
use serde::Serialize;
use tokio::task::JoinHandle;
use trace::debug;

use crate::spi::service::Service;
use crate::{server, VERSION};

async fn get_country_code_and_regin_name() -> (String, String) {
    let client = reqwest::Client::new();

    let request = match client.post("http://ip-api.com/json/").build() {
        Ok(request) => request,
        Err(_) => return (String::new(), String::new()),
    };

    let resp = match client.execute(request).await {
        Ok(resp) => resp,
        Err(_) => return (String::new(), String::new()),
    };

    let json = match resp.json::<serde_json::Value>().await {
        Ok(text) => text,
        Err(_) => return (String::new(), String::new()),
    };

    let country_code = json
        .get("countryCode")
        .map(|v| v.as_str())
        .unwrap_or_default()
        .unwrap_or_default()
        .to_string();

    let regin_name = json
        .get("regionName")
        .map(|v| v.as_str())
        .unwrap_or_default()
        .unwrap_or_default()
        .to_string();

    (country_code, regin_name)
}

fn get_os_type() -> String {
    let info = os_info::get();
    let mut os_type = info.os_type().to_string();
    os_type.push(' ');
    os_type.push_str(info.version().to_string().as_str());
    os_type
}

fn get_client() -> reqwest::Client {
    reqwest::ClientBuilder::new()
        // .use_native_tls()
        .build()
        .unwrap_or_default()
}

const fn seconds_of_days(day: u64) -> u64 {
    60 * 60 * 24 * day
}

lazy_static! {
    static ref OS_TYPE: String = get_os_type();
    static ref CLIENT: reqwest::Client = get_client();
}

const USAGE_SERVER_URL: &str = "http://usage.cnosdb.com";

#[derive(Debug, Clone, Serialize)]
struct ReportMessage<'a> {
    time: std::time::SystemTime,
    start_time: std::time::SystemTime,
    os_type: &'static str,
    arch: &'static str,
    version: &'static str,
    country_code: &'a str,
    regin_name: &'a str,
}

impl<'a> ReportMessage<'a> {
    pub fn new(
        start_time: std::time::SystemTime,
        country_code: &'a str,
        regin_name: &'a str,
    ) -> ReportMessage<'a> {
        Self {
            time: std::time::SystemTime::now(),
            start_time,
            os_type: &OS_TYPE,
            arch: std::env::consts::ARCH,
            version: VERSION.as_ref(),
            country_code,
            regin_name,
        }
    }
}

pub struct ReportService {
    service_handle: Option<JoinHandle<()>>,
}

impl ReportService {
    pub fn new() -> Self {
        Self {
            service_handle: None,
        }
    }
    async fn send_report(message: ReportMessage<'_>) {
        let request = match CLIENT.post(USAGE_SERVER_URL).json(&message).build() {
            Ok(request) => request,
            Err(e) => {
                debug!("usage request construct fail: {}", e);
                return;
            }
        };
        debug!("message:{:?}", message);
        match CLIENT.execute(request).await {
            Ok(resp) => {
                if !resp.status().is_success() {
                    debug!("resp: {:?}", resp);
                }
            }
            Err(e) => {
                debug!("message:{:?} send fail, e: {}", &message, &e);
            }
        }
    }
}

#[async_trait::async_trait]
impl Service for ReportService {
    fn start(&mut self) -> Result<(), server::Error> {
        self.service_handle = Some(tokio::spawn(async move {
            let start_time = tokio::time::Instant::now();
            let start_time_timestamp = std::time::SystemTime::now();

            let (country_code, regin_name) = get_country_code_and_regin_name().await;

            let interval = tokio::time::Duration::from_secs(seconds_of_days(1));
            let mut timer = tokio::time::interval_at(
                start_time.add(tokio::time::Duration::from_secs(5)),
                interval,
            );

            loop {
                timer.tick().await;
                let message = ReportMessage::new(start_time_timestamp, &country_code, &regin_name);
                ReportService::send_report(message).await;
            }
        }));

        Ok(())
    }

    async fn stop(&mut self, _force: bool) {
        if let Some(stop) = self.service_handle.take() {
            stop.abort();
        };
    }
}
