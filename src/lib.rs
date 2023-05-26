pub mod config;
pub mod file;
pub mod log;
pub mod path;
pub mod progress;
pub mod report;

mod arg;
mod cache;
mod dedupe;
mod device;
mod error;
mod group;
mod hasher;
mod lock;
mod pattern;
mod phase;
mod reflink;
mod regex;
mod rlimit;
mod selector;
mod semaphore;
mod transform;
mod util;
mod walk;

pub use dedupe::{dedupe, log_script, run_script, DedupeOp, DedupeResult};
pub use error::Error;
pub use group::{group_files, write_report};

const TIMESTAMP_FMT: &str = "%Y-%m-%d %H:%M:%S.%3f %z";
