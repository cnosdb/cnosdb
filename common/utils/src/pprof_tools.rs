use std::fs::File;

use pprof::protos::Message;
use tikv_jemalloc_ctl::{Access, AsName};

#[cfg(all(any(target_arch = "x86_64", target_arch = "aarch64")))]
pub async fn gernate_pprof() -> Result<String, String> {
    let guard = pprof::ProfilerGuardBuilder::default()
        .frequency(1000)
        .blocklist(&["libc", "libgcc", "pthread", "vdso"])
        .build()
        .map_err(|e| e.to_string())?;

    tokio::time::sleep(tokio::time::Duration::from_secs(30)).await;

    let profile_name = "/tmp/cpu_profile.pb".to_string();
    let flamegraph_name = "/tmp/flamegraph.svg".to_string();
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

#[cfg(not(all(any(target_arch = "x86_64", target_arch = "aarch64"))))]
pub async fn gernate_pprof() -> Result<String, String> {
    Err("not support".to_string())
}

// MALLOC_CONF=prof:true
// CARGO_FEATURE_PROFILING=true
const PROF_ACTIVE: &[u8] = b"prof.active\0";
const PROF_DUMP: &[u8] = b"prof.dump\0";
const PROFILE_OUTPUT_FILE: &[u8] = b"/tmp/mem_profile.out\0";
const PROFILE_OUTPUT_FILE_STR: &str = "/tmp/mem_profile.out";

#[cfg(all(any(target_arch = "x86_64", target_arch = "aarch64")))]
pub async fn gernate_jeprof() -> Result<String, String> {
    set_prof_active(true)?;
    tokio::time::sleep(tokio::time::Duration::from_secs(30)).await;
    let _ = tokio::fs::remove_file(PROFILE_OUTPUT_FILE_STR).await;
    dump_profile()?;
    set_prof_active(false)?;

    Ok(format!(
        "gernate memory profile in: {}",
        PROFILE_OUTPUT_FILE_STR
    ))
}

#[cfg(not(all(any(target_arch = "x86_64", target_arch = "aarch64"))))]
pub async fn gernate_jeprof() -> Result<String, String> {
    Err("not support".to_string())
}

fn set_prof_active(active: bool) -> Result<(), String> {
    let name = PROF_ACTIVE.name();
    name.write(active).map_err(|e| e.to_string())?;
    Ok(())
}

fn dump_profile() -> Result<(), String> {
    let name = PROF_DUMP.name();
    name.write(PROFILE_OUTPUT_FILE).map_err(|e| e.to_string())?;
    Ok(())
}
