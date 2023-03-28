#[cfg(unix)]
pub use unix::*;
#[cfg(windows)]
pub use windows::*;

#[cfg(unix)]
mod unix;

#[cfg(windows)]
mod windows;
