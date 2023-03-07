extern crate core;
use core::alloc::{GlobalAlloc, Layout};

use libc::{c_int, c_void};
use tikv_jemalloc_sys as ffi;

#[cfg(all(any(
    target_arch = "arm",
    target_arch = "mips",
    target_arch = "mipsel",
    target_arch = "powerpc"
)))]
const ALIGNOF_MAX_ALIGN_T: usize = 8;
#[cfg(all(any(
    target_arch = "x86",
    target_arch = "x86_64",
    target_arch = "aarch64",
    target_arch = "powerpc64",
    target_arch = "powerpc64le",
    target_arch = "mips64",
    target_arch = "riscv64",
    target_arch = "s390x",
    target_arch = "sparc64"
)))]
const ALIGNOF_MAX_ALIGN_T: usize = 16;

fn layout_to_flags(align: usize, size: usize) -> c_int {
    if align <= ALIGNOF_MAX_ALIGN_T && align <= size {
        0
    } else {
        ffi::MALLOCX_ALIGN(align)
    }
}

// Assumes a condition that always must hold.
macro_rules! assume {
    ($e:expr) => {
        debug_assert!($e);
        if !($e) {
            core::hint::unreachable_unchecked();
        }
    };
}

/// Handle to the jemalloc allocator
///
/// This type implements the `GlobalAllocAlloc` trait, allowing usage a global allocator.
///
/// When the `alloc_trait` feature of this crate is enabled, it also implements the `Alloc` trait,
/// allowing usage in collections.
#[derive(Copy, Clone, Default, Debug)]
pub struct Jemalloc;

unsafe impl GlobalAlloc for Jemalloc {
    #[inline]
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        assume!(layout.size() != 0);
        let flags = layout_to_flags(layout.align(), layout.size());
        let ptr = if flags == 0 {
            ffi::malloc(layout.size())
        } else {
            ffi::mallocx(layout.size(), flags)
        };
        ptr as *mut u8
    }

    #[inline]
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        assume!(!ptr.is_null());
        assume!(layout.size() != 0);
        let flags = layout_to_flags(layout.align(), layout.size());
        ffi::sdallocx(ptr as *mut c_void, layout.size(), flags)
    }

    #[inline]
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        assume!(layout.size() != 0);
        let flags = layout_to_flags(layout.align(), layout.size());
        let ptr = if flags == 0 {
            ffi::calloc(1, layout.size())
        } else {
            ffi::mallocx(layout.size(), flags | ffi::MALLOCX_ZERO)
        };
        ptr as *mut u8
    }

    #[inline]
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        assume!(layout.size() != 0);
        assume!(new_size != 0);
        let flags = layout_to_flags(layout.align(), new_size);
        let ptr = if flags == 0 {
            ffi::realloc(ptr as *mut c_void, new_size)
        } else {
            ffi::rallocx(ptr as *mut c_void, new_size, flags)
        };
        ptr as *mut u8
    }
}

#[cfg(test)]
mod tests {
    use chrono::Utc;

    use crate::Jemalloc;
    #[global_allocator]
    static A: Jemalloc = Jemalloc;
    #[test]
    #[ignore]
    fn it_works() {
        for _ in 0..10 {
            let start = Utc::now();
            for i in 0..100000 {
                // let size = rand() as usize;
                let mut a = Box::new(vec![0_u64; i]);
                a.insert(0, 199);
            }
            let end = Utc::now();
            println!("run time: {:?} ", end - start);
        }
    }
}
