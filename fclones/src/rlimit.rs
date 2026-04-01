// The non-unix cfg does not use all imports
#![allow(unused_imports)]

use crate::semaphore::Semaphore;
use std::sync::Arc;
use std::sync::LazyLock;

use nix::libc;

#[cfg(unix)]
// Get the maximum number of open file descriptors for this process, and if
// the hard limit is larger than the soft limit increase it.
fn rlimit_nofile() -> libc::rlim_t {
    let mut file_limit = libc::rlimit {
        rlim_cur: 0,
        rlim_max: 0,
    };
    unsafe {
        if libc::getrlimit(libc::RLIMIT_NOFILE, &raw mut file_limit) != 0 {
            return 200;
        }
    }

    if file_limit.rlim_max > file_limit.rlim_cur {
        let prev = file_limit.rlim_cur;
        file_limit.rlim_cur = file_limit.rlim_max;
        unsafe {
            if libc::setrlimit(libc::RLIMIT_NOFILE, &raw const file_limit) == 0 {
                file_limit.rlim_max
            } else {
                prev
            }
        }
    } else {
        file_limit.rlim_cur
    }
}

#[cfg(unix)]
// stdin, stdout, stderr, plus two as a buffer
const OTHER_OPEN_FILES: isize = 3 + 2;

// Globally track the number of opened files so many parallel operations do not raise
// "Too many open files (os error 24)".
#[cfg(unix)]
pub static RLIMIT_OPEN_FILES: LazyLock<Arc<Semaphore>> = LazyLock::new(|| {
    Arc::new(Semaphore::new(std::cmp::max(
        (rlimit_nofile() as isize) - OTHER_OPEN_FILES,
        64, // fallback value
    )))
});

#[cfg(not(unix))]
pub mod not_unix {
    #[derive(Clone, Copy)]
    pub struct NoRlimit;
    impl NoRlimit {
        pub fn new() -> Self {
            Self {}
        }
        pub fn clone(self) -> Self {
            self
        }
        pub fn access_owned(self) -> () {}
    }
}
#[cfg(not(unix))]
pub static RLIMIT_OPEN_FILES: LazyLock<not_unix::NoRlimit> =
    LazyLock::new(|| not_unix::NoRlimit::new());
