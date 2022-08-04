pub mod config;
pub mod log;
pub mod progress;
pub mod report;

mod arg;
mod cache;
mod dedupe;
mod device;
mod error;
mod file;
mod group;
mod hasher;
mod lock;
mod path;
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

pub use config::{DedupeConfig, GroupConfig, Priority};
pub use dedupe::{
    dedupe, log_script, run_script, sort_by_priority, DedupeOp, DedupeResult, PartitionedFileGroup,
    PathAndMetadata,
};
pub use device::DiskDevices;
pub use error::Error;
pub use file::{FileHash, FileId, FileInfo, FileLen};
pub use group::{group_files, write_report, FileGroup, FileSubGroup};
pub use path::Path;

const TIMESTAMP_FMT: &str = "%Y-%m-%d %H:%M:%S.%3f %z";
