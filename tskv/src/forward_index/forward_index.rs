use super::*;
use crate::{Record, RecordFileError, RecordFileResult};
use std::collections::HashMap;
use std::path::PathBuf;
use num_traits::ToPrimitive;

const VERSION: u8 = 1;

pub struct ForwardIndex {
    series_info_set: HashMap<models::SeriesID, AbstractSeriesInfo>,
    record_writer: record_file::Writer,
    record_reader: record_file::Reader,
    file_path: PathBuf,
}

#[repr(u8)]
#[derive(Copy, Clone)]
enum ForwardIndexAction {
    Unknown = 0_u8,
    AddSeriesInfo = 1_u8,
    DelSeriesInfo = 2_u8,
}

impl ForwardIndexAction {
    fn u8_number(&self) -> u8 {
        *self as u8
    }
}

impl From<u8> for ForwardIndexAction {
    fn from(act: u8) -> Self {
        match act {
            0_u8 => ForwardIndexAction::Unknown,
            1_u8 => ForwardIndexAction::AddSeriesInfo,
            2_u8 => ForwardIndexAction::DelSeriesInfo,
            _ => ForwardIndexAction::Unknown,
        }
    }
}

impl ForwardIndex {
    pub fn new(path: &PathBuf) -> ForwardIndex {
        ForwardIndex {
            series_info_set: HashMap::new(),
            record_writer: record_file::Writer::new(path),
            record_reader: record_file::Reader::new(path),
            file_path: path.clone(),
        }
    }

    pub async fn add_series_info_if_not_exists(&mut self, mut series_info: SeriesInfo) -> ForwardIndexResult<()> {
        series_info.update_id();
        match self.series_info_set.entry(series_info.series_id()) {
            std::collections::hash_map::Entry::Occupied(mut entry) => {
                let mut abs_series_info = entry.get_mut();
                for field_info in series_info.field_infos {
                    let mut flag = false;
                    for abs_field_info in &abs_series_info.abstract_field_infos {
                        if field_info.id == abs_field_info.id {
                            if field_info.value_type == abs_field_info.value_type {
                                flag = true;
                                break;
                            } else {
                                return Err(ForwardIndexError::FieldType);
                            }
                        }
                    }

                    if !flag {
                        let record = self.record_reader.read_one(
                            abs_series_info.pos.to_usize().unwrap()).await.map_err(|err| {
                            ForwardIndexError::ReadFile { source: err }
                        })?;
                        if record.data_type != ForwardIndexAction::AddSeriesInfo.u8_number() {
                            return Err(ForwardIndexError::Action);
                        }
                        if record.data_version != VERSION {
                            return Err(ForwardIndexError::Version);
                        }
                        let abs_field_info = field_info.to_abstract();
                        let mut origin_series_info = SeriesInfo::decoded(&record.data);
                        origin_series_info.field_infos.push(field_info);
                        let pos = self.record_writer.write_record(
                            VERSION,
                            ForwardIndexAction::AddSeriesInfo.u8_number(),
                            &origin_series_info.encode(),
                        ).await.map_err(|err| {
                            ForwardIndexError::WriteFile { source: err }
                        })?;
                        abs_series_info.pos = pos;
                        abs_series_info.abstract_field_infos.push(abs_field_info);
                    }
                }
            }
            std::collections::hash_map::Entry::Vacant(entry) => {
                let data = series_info.encode();
                let pos = self.record_writer.write_record(
                    VERSION,
                    ForwardIndexAction::AddSeriesInfo.u8_number(),
                    &data,
                ).await.map_err(|err| {
                    ForwardIndexError::WriteFile { source: err }
                })?;
                entry.insert(series_info.to_abstract(pos));
            }
        }

        Ok(())
    }

    pub async fn close(&mut self) -> ForwardIndexResult<()> {
        self.record_writer.close().await.map_err(|err| {
            ForwardIndexError::CloseFile { source: err }
        })
    }

    pub async fn del_series_info(&mut self, series_id: SeriesID) -> ForwardIndexResult<()> {
        match self.series_info_set.entry(series_id) {
            std::collections::hash_map::Entry::Occupied(entry) => {
                self.record_writer.write_record(
                    VERSION,
                    ForwardIndexAction::DelSeriesInfo.u8_number(),
                    &series_id.to_le_bytes().to_vec(),
                ).await.map_err(|err| {
                    ForwardIndexError::WriteFile { source: err }
                })?;
                entry.remove();
                Ok(())
            }
            std::collections::hash_map::Entry::Vacant(entry) => {
                // TODO Err()?
                Ok(())
            }
        }
    }

    pub async fn load_cache_file(&mut self) -> RecordFileResult<()> {
        let mut record_reader = record_file::Reader::new(&self.file_path);
        loop {
            match record_reader.read_record().await {
                Ok(record) => {
                    self.handle_record(record).await;
                }
                Err(_) => {
                    break;
                }
            }
        }
        Ok(())
    }

    async fn handle_record(&mut self, record: Record) {
        if record.data_version != VERSION {
            // TODO warning log
            return;
        }

        match ForwardIndexAction::from(record.data_type) {
            ForwardIndexAction::AddSeriesInfo => {
                let series_info = SeriesInfo::decoded(&record.data);
                let abs_series_info = series_info.to_abstract(record.pos);
                self.series_info_set.insert(abs_series_info.id, abs_series_info);
            }

            ForwardIndexAction::DelSeriesInfo => {
                if record.data.len() != 8 {
                    //TODO warning log
                    return;
                }
                let series_id = u64::from_le_bytes(record.data.try_into().unwrap());
                self.series_info_set.remove(&series_id);
            }
            ForwardIndexAction::Unknown => {
                //TODO warning log
            }
        };
    }
}

impl From<&str> for ForwardIndex {
    fn from(path: &str) -> Self {
        ForwardIndex::new(&PathBuf::from(path))
    }
}

#[derive(Clone)]
pub struct ForwardIndexConfig {
    pub path: PathBuf,
}

impl Default for ForwardIndexConfig {
    fn default() -> Self {
        ForwardIndexConfig {
            path: PathBuf::from("/tmp/test/tskv.fidx")
        }
    }
}

