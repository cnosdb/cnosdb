use std::{fs::File, time::SystemTime};

use pprof::protos::Message;

use libc::c_char;

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

const PROF_DUMP: &[u8] = b"prof.dump\0";
const OPT_PROF: &[u8] = b"opt.prof\0";

pub fn gernate_jeprof() -> Result<String, String> {
    // precheck
    if !is_prof_enabled()? {
        return Err("opt.prof is not ON. Start server e.g. MALLOC_CONF=prof:true ".into());
    }

    let now_time = SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("get current time")
        .as_millis();
    let profile_name = format!("/tmp/mem_profile_{}.prof", now_time);
    let mut name_bytes = std::ffi::CString::new(profile_name.clone())
        .map_err(|e| format!("filename to std::ffi::CString failed {}", e.to_string()))?
        .into_bytes_with_nul();
    let name_ptr = name_bytes.as_mut_ptr() as *mut c_char;

    unsafe {
        tikv_jemalloc_ctl::raw::write(PROF_DUMP, name_ptr).map_err(|e| {
            format!(
                "dump Jemalloc prof to path {}: failed: {}",
                profile_name,
                e.to_string()
            )
        })?;
    }

    Ok(format!("gernate memory profile in {}", profile_name))
}

fn is_prof_enabled() -> Result<bool, String> {
    Ok(unsafe {
        tikv_jemalloc_ctl::raw::read::<bool>(OPT_PROF)
            .map_err(|e| format!("read opt.prof failure: {}", e))?
    })
}
