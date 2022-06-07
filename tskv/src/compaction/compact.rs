use std::{collections::HashMap, sync::Arc};

use crate::{
    compaction::CompactReq, context::GlobalContext, error::Result, summary::VersionEdit,
    tseries_family::ColumnFile,
};

pub async fn run_compaction_job(request: CompactReq, kernel: Arc<GlobalContext>) -> Result<()> {
    Ok(())
}
