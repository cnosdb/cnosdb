use std::{fs::File, time::SystemTime};

use pprof::protos::Message;

pub async fn gernate_pprof() -> Result<String, String> {
    let guard = pprof::ProfilerGuardBuilder::default()
        .frequency(1000)
        .blocklist(&["libc", "libgcc", "pthread", "vdso"])
        .build()
        .map_err(|e| e.to_string())?;

    tokio::time::sleep(tokio::time::Duration::from_secs(10)).await;

    let now_time = SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("get current time")
        .as_millis();
    let profile_name = format!("/tmp/profile_{}.pb", now_time);
    let flamegraph_name = format!("/tmp/flamegraph_{}.svg", now_time);
    if let Ok(report) = guard.report().build() {
        let profile = report.pprof().map_err(|e| e.to_string())?;
        let mut content = Vec::new();
        profile
            .write_to_vec(&mut content)
            .map_err(|e| e.to_string())?;
        std::fs::write(profile_name.clone(), &content).map_err(|e| e.to_string())?;

        let file = File::create(flamegraph_name.clone()).map_err(|e| e.to_string())?;
        report.flamegraph(file).map_err(|e| e.to_string())?;

        Ok(format!(
            "gernate report in {} {}",
            profile_name, flamegraph_name
        ))
    } else {
        Err("build report failed".to_string())
    }
}
