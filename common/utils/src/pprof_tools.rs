use std::ffi::{c_char, CString};
use std::fs::File;

use pprof::protos::Message;

#[cfg(not(all(any(target_arch = "x86_64", target_arch = "aarch64"))))]
pub async fn gernate_pprof() -> Result<String, String> {
    Err("not support".to_string())
}

#[cfg(not(all(any(target_arch = "x86_64", target_arch = "aarch64"))))]
pub async fn gernate_jeprof() -> Result<String, String> {
    Err("not support".to_string())
}

#[cfg(any(target_arch = "x86_64", target_arch = "aarch64"))]
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

// MALLOC_CONF=prof:true
const PROF_DUMP: &[u8] = b"prof.dump\0";
const OPT_PROF: &[u8] = b"opt.prof\0";
const PROFILE_OUTPUT_FILE_STR: &str = "/tmp/mem_profile.out";

#[cfg(any(target_arch = "x86_64", target_arch = "aarch64"))]
pub async fn gernate_jeprof() -> Result<String, String> {
    let _ = tokio::fs::remove_file(PROFILE_OUTPUT_FILE_STR).await;

    dump_mem_profile()?;

    Ok(format!(
        "gernate memory profile in: {}",
        PROFILE_OUTPUT_FILE_STR
    ))
}

fn dump_mem_profile() -> Result<(), String> {
    if !is_prof_enabled()? {
        return Err("prof not enabled, please enable first!".to_string());
    }

    let mut bytes = CString::new(PROFILE_OUTPUT_FILE_STR)
        .map_err(|e| e.to_string())?
        .into_bytes_with_nul();

    // #safety: we always expect a valid temp file path to write profiling data to.
    let ptr = bytes.as_mut_ptr() as *mut c_char;
    unsafe {
        tikv_jemalloc_ctl::raw::write(PROF_DUMP, ptr).map_err(|e| e.to_string())?;
    }

    Ok(())
}

fn is_prof_enabled() -> Result<bool, String> {
    // safety: OPT_PROF variable, if present, is always a boolean value.
    Ok(unsafe { tikv_jemalloc_ctl::raw::read::<bool>(OPT_PROF).map_err(|e| e.to_string())? })
}
