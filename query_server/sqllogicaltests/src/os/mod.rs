#[cfg(not(target_family = "windows"))]
mod unix;
#[cfg(not(target_family = "windows"))]
pub use unix::*;

#[cfg(target_family = "windows")]
mod windows;
#[cfg(target_family = "windows")]
pub use windows::*;
