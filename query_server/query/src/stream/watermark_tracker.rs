use std::path::PathBuf;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;

use spi::service::protocol::QueryId;
use spi::QueryError;
use tokio::fs;

pub type WatermarkTrackerRef = Arc<WatermarkTracker>;

const WATERMARK_FILE_NAME: &str = "watermark";

/// Real-time tracking of watermark during query running, which can be recovered after system restart.
/// Information is logged to a local file.
#[derive(Default, Debug)]
pub struct WatermarkTracker {
    global_watermark_ns: AtomicI64,
    file_path: PathBuf,
}

impl WatermarkTracker {
    pub fn try_new(query_id: QueryId, path: impl Into<PathBuf>) -> Result<Self, QueryError> {
        let mut path: PathBuf = path.into();
        path.push(&format!("{}", query_id));
        path.push(WATERMARK_FILE_NAME);

        let watermark_ns = if std::path::Path::new(path.as_path()).exists() {
            let mut buf: [u8; 8] = [0; 8];
            let bytes = std::fs::read(path.as_path())?;
            if bytes.len() != 8 {
                return Err(QueryError::Internal {
                    reason: format!(
                        "Invalid watermark file: {:?}, content: {:?}",
                        path.as_path(),
                        bytes
                    ),
                });
            }
            buf.copy_from_slice(&bytes[..8]);

            i64::from_be_bytes(buf)
        } else {
            if let Some(parent) = path.parent() {
                std::fs::create_dir_all(parent)?;
            }
            i64::MIN
        };

        Ok(Self {
            global_watermark_ns: AtomicI64::new(watermark_ns),
            file_path: path,
        })
    }

    pub fn current_watermark_ns(&self) -> i64 {
        self.global_watermark_ns.load(Ordering::Relaxed)
    }

    pub fn update_watermark(&self, event_time: i64, _delay: i64) {
        // TODO _delay needs to be processed after kv supports offset
        // self.global_watermark_ns
        //     .store(event_time - delay, Ordering::Relaxed);
        self.global_watermark_ns
            .store(event_time, Ordering::Relaxed);
    }

    /// Persist watermark to local file.
    /// The purpose is to not output duplicate data after the system restarts.
    pub async fn commit(&self, _batch_id: i64) -> Result<(), QueryError> {
        let contents = self
            .global_watermark_ns
            .load(Ordering::Relaxed)
            .to_be_bytes();
        fs::write(&self.file_path, contents).await.map_err(|err| {
            trace::error!("Commit streaming query watermark, error: {:?}", err);
            err
        })?;

        Ok(())
    }
}
