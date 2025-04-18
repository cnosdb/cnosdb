pub const GIT_HASH: Option<&str> = option_env!("GIT_HASH");
pub const BUILD_TIME: Option<&str> = option_env!("BUILD_TIME");

/// Get the version of crate calling this macro.
#[macro_export]
macro_rules! crate_version {
    () => {
        format!(
            "{}, revision: {}, build_time: {}",
            env!("CARGO_PKG_VERSION"),
            $crate::GIT_HASH.unwrap_or("UNKNOWN"),
            $crate::BUILD_TIME.unwrap_or("UNKNOWN")
        )
    };
}

/// Get the workspace version.
/// 'package.version' in manifest of this crate is set to 'version.workspace = true'.
pub fn workspace_version() -> &'static str {
    use std::sync::LazyLock;

    // Use LazyCell to ensure the version string is only leaked once.
    static VERSION: LazyLock<&'static str> = LazyLock::new(|| {
        // use crate_version!() to get version of this lib.
        Box::leak(crate_version!().into_boxed_str())
    });

    *VERSION
}
