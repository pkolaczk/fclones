use core::fmt;
use std::cell::RefCell;
use std::cmp::{max, Reverse};
use std::collections::HashMap;
use std::env::{args, current_dir};
use std::ffi::{OsStr, OsString};
use std::fmt::{Display, Formatter};
use std::fs::File;
use std::io;
use std::io::BufWriter;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::mpsc::{channel, Receiver, Sender};
use std::sync::Arc;

use chrono::{DateTime, Local};
use console::Term;
use crossbeam_utils::thread;
use itertools::Itertools;
use rayon::prelude::*;
use serde::*;
use sysinfo::DiskType;
use thread_local::ThreadLocal;

pub use dedupe::{dedupe, log_script, run_script, DedupeOp, DedupeResult};

use crate::config::*;
use crate::device::{DiskDevice, DiskDevices};
use crate::files::FileInfo;
use crate::files::*;
use crate::group::*;
use crate::log::Log;
use crate::path::Path;
use crate::report::{FileStats, ReportHeader, ReportWriter};
use crate::selector::PathSelector;
use crate::semaphore::Semaphore;
use crate::transform::Transform;
use crate::walk::Walk;

pub mod config;
pub mod files;
pub mod log;
pub mod path;
pub mod progress;
pub mod report;

mod dedupe;
mod device;
mod group;
mod lock;
mod pattern;
mod regex;
mod selector;
mod semaphore;
mod transform;
mod util;
mod walk;

const TIMESTAMP_FMT: &str = "%Y-%m-%d %H:%M:%S.%3f %z";

/// Error reported by top-level fclones functions
#[derive(Debug)]
pub struct Error {
    pub message: String,
}

impl Error {
    pub fn new(msg: String) -> Error {
        Error { message: msg }
    }
}

impl std::error::Error for Error {}

impl Display for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl From<String> for Error {
    fn from(s: String) -> Self {
        Error::new(s)
    }
}

impl From<&str> for Error {
    fn from(s: &str) -> Self {
        Error::new(s.to_owned())
    }
}

/// Holds stuff needed globally by the whole application
struct AppCtx<'a> {
    pub config: &'a GroupConfig,
    pub log: &'a Log,
    devices: DiskDevices,
    transform: Option<Transform>,
    path_selector: PathSelector,
}

impl<'a> AppCtx<'a> {
    pub fn new(config: &'a GroupConfig, log: &'a Log) -> Result<AppCtx<'a>, Error> {
        let thread_pool_sizes = config.thread_pool_sizes();
        let devices = DiskDevices::new(&thread_pool_sizes);
        let transform = match config.transform() {
            None => None,
            Some(Ok(transform)) => Some(transform),
            Some(Err(e)) => return Err(Error::new(format!("Invalid transform: {}", e))),
        };
        let base_dir = Path::from(current_dir().unwrap_or_default());
        let selector = config
            .path_selector(&base_dir)
            .map_err(|e| format!("Invalid pattern: {}", e))?;

        Self::check_pool_config(thread_pool_sizes, &devices)?;

        Ok(AppCtx {
            config,
            log,
            devices,
            transform,
            path_selector: selector,
        })
    }

    /// Checks if all thread pool names refer to existing pools or devices
    fn check_pool_config(
        thread_pool_sizes: HashMap<OsString, Parallelism>,
        devices: &DiskDevices,
    ) -> Result<(), Error> {
        let mut allowed_pool_names = DiskDevices::device_types();
        allowed_pool_names.push("main");
        allowed_pool_names.push("default");
        for (name, _) in thread_pool_sizes.iter() {
            let name = name.to_string_lossy();
            match name.strip_prefix("dev:") {
                Some(name) if devices.get_by_name(OsStr::new(name)).is_none() => {
                    return Err(Error::new(format!("Unknown device: {}", name)));
                }
                None if !allowed_pool_names.contains(&name.as_ref()) => {
                    return Err(Error::new(format!(
                        "Unknown thread pool or device type: {}",
                        name
                    )));
                }
                _ => {}
            }
        }
        Ok(())
    }
}

/// A group of files that have something in common, e.g. same size or same hash
#[derive(Serialize, Debug, PartialEq, Eq, Clone)]
pub struct FileGroup<F> {
    /// Length of each file
    pub file_len: FileLen,
    /// Hash of a part or the whole of the file
    pub file_hash: FileHash,
    /// Group of files with the same length and hash
    pub files: Vec<F>,
}

/// Computes summaries of the results obtained from each grouping stage.
pub trait StageMetrics {
    /// Returns the total count of the files
    fn total_count(self) -> usize;

    /// Returns the sum of file lengths
    fn total_size(self) -> FileLen;

    /// Returns the total count of redundant files
    fn selected_count(self, rf_over: usize, rf_under: usize) -> usize;

    /// Returns the amount of data in redundant files
    fn selected_size(self, rf_over: usize, rf_under: usize) -> FileLen;
}

impl<'a, I, F> StageMetrics for I
where
    I: IntoIterator<Item = &'a FileGroup<F>> + 'a,
    F: 'a,
{
    fn total_count(self) -> usize {
        self.into_iter().map(|g| g.files.len()).sum()
    }

    fn total_size(self) -> FileLen {
        self.into_iter()
            .map(|g| g.file_len * g.files.len() as u64)
            .sum()
    }

    fn selected_count(self, rf_over: usize, rf_under: usize) -> usize {
        self.into_iter()
            .filter(|&g| g.files.len() < rf_under)
            .map(|g| g.files.len().saturating_sub(rf_over))
            .sum()
    }

    fn selected_size(self, rf_over: usize, rf_under: usize) -> FileLen {
        self.into_iter()
            .filter(|&g| g.files.len() < rf_under)
            .map(|g| g.file_len * g.files.len().saturating_sub(rf_over) as u64)
            .sum()
    }
}

/// Helper struct to preserve the original file hash and keep it together with file information
/// Sometimes the old hash must be taken into account, e.g. when combining the prefix hash with
/// the suffix hash.
struct HashedFileInfo {
    file_hash: FileHash,
    file_info: FileInfo,
}

/// Partitions files into separate vectors, where each vector holds files persisted
/// on the same disk device. The vectors are returned in the same order as devices.
fn partition_by_devices(
    files: Vec<FileGroup<FileInfo>>,
    devices: &DiskDevices,
) -> Vec<Vec<HashedFileInfo>> {
    let mut result: Vec<Vec<HashedFileInfo>> = Vec::with_capacity(devices.len());
    for _ in 0..devices.len() {
        result.push(Vec::new());
    }
    for g in files {
        for f in g.files {
            let device = &devices[f.get_device_index()];
            result[device.index].push(HashedFileInfo {
                file_hash: g.file_hash,
                file_info: f,
            });
        }
    }
    result
}

/// Iterates over grouped files, in parallel
fn flat_iter(files: &[FileGroup<FileInfo>]) -> impl ParallelIterator<Item = &FileInfo> {
    files.par_iter().flat_map(|g| &g.files)
}

#[derive(Copy, Clone, Debug)]
enum AccessType {
    Sequential,
    Random,
}

/// Groups files by length and hash computed by given `hash_fn`.
/// Runs in parallel on dedicated thread pools.
/// Files on different devices are hashed separately from each other.
/// File hashes within a single device are computed in the order given by
/// their `location` field to minimize seek latency.
///
/// Caveats: the original grouping is lost. It is possible for two files that
/// were in the different groups to end up in the same group if they have the same length
/// and they hash to the same value. If you don't want this, you need to combine the old
/// hash with the new hash in the provided `hash_fn`.
fn rehash<'a, F1, F2, H>(
    groups: Vec<FileGroup<FileInfo>>,
    group_pre_filter: F1,
    group_post_filter: F2,
    devices: &DiskDevices,
    access_type: AccessType,
    hash_fn: H,
) -> Vec<FileGroup<FileInfo>>
where
    F1: Fn(&FileGroup<FileInfo>) -> bool,
    F2: Fn(&FileGroup<FileInfo>) -> bool,
    H: Fn((&mut FileInfo, FileHash)) -> Option<FileHash> + Sync + Send + 'a,
{
    // Allow sharing the hash function between threads:
    type HashFn<'a> = dyn Fn((&mut FileInfo, FileHash)) -> Option<FileHash> + Sync + Send + 'a;
    let hash_fn: &HashFn<'a> = &hash_fn;

    let (tx, rx): (Sender<HashedFileInfo>, Receiver<HashedFileInfo>) = channel();

    // There is no point in fclonesing groups containing a single file.
    // Normally when searching for duplicates such groups are filtered out automatically after
    // each stage, however they are possible when searching for unique files.
    let (groups_to_fclones, groups_to_pass): (Vec<_>, Vec<_>) =
        groups.into_iter().partition(group_pre_filter);

    // This way we can split fclonesing to separate thread-pools, one per device:
    let files = partition_by_devices(groups_to_fclones, &devices);
    let mut hash_map =
        GroupMap::new(|f: HashedFileInfo| ((f.file_info.len, f.file_hash), f.file_info));
    let hash_map_ref = &mut hash_map;

    // Scope needed so threads can access shared stuff like groups or shared functions.
    // The threads we launch are guaranteed to not live longer than this scope.
    thread::scope(move |s| {
        // Process all files in background
        for (mut files, device) in files.into_iter().zip(devices.iter()) {
            if files.is_empty() {
                continue;
            }

            let tx = tx.clone();

            // Launch a separate thread for each device, so we can fclones
            // files on each device independently
            s.spawn(move |_| {
                // Sort files by their physical location, to reduce disk seek latency
                if device.disk_type != DiskType::SSD {
                    files.par_sort_unstable_by_key(|f| f.file_info.location);
                }

                // Some devices like HDDs may benefit from different amount of parallelism
                // depending on the access type. Therefore we chose a thread pool appropriate
                // for the access type
                let thread_pool = match access_type {
                    AccessType::Sequential => device.seq_thread_pool(),
                    AccessType::Random => device.rand_thread_pool(),
                };

                let thread_count = thread_pool.current_num_threads() as isize;

                // Limit the number of tasks spawned at once into the thread-pool.
                // Each task creates a heap allocation and reserves memory in the queue.
                // It is more memory efficient to keep these tasks as long as possible
                // in our vector. Without this limit we observed over 20% more memory use
                // when fclonesing 1M of files.
                let semaphore = Arc::new(Semaphore::new(8 * thread_count));

                // Run hashing on the thread-pool dedicated to the device
                for mut f in files {
                    let tx = tx.clone();
                    let guard = semaphore.clone().access_owned();

                    // Spawning a task into a thread-pool requires a static lifetime,
                    // because generally the task could outlive caller's stack frame.
                    // However, this is not the case for rehash function, because
                    // we don't exit before all tasks are fclonesed.
                    // In the perfect world we should use scopes for that. Unfortunately
                    // the current implementation of rayon scopes runs the scope body
                    // on one of the thread-pool worker threads, so it is not possible
                    // to safely block inside the scope, because that leads to deadlock
                    // when the pool has only one thread.
                    let hash_fn: &HashFn<'static> = unsafe { std::mem::transmute(hash_fn) };
                    thread_pool.spawn_fifo(move || {
                        if let Some(hash) = hash_fn((&mut f.file_info, f.file_hash)) {
                            f.file_hash = hash;
                            tx.send(f).unwrap();
                        }
                        // This forces moving the guard into this task and be released when
                        // the task is done
                        drop(guard);
                    });
                }
            });
        }
        // Drop the original tx, so all tx are closed when the threads finish and
        // the next while loop will eventually exit
        drop(tx);

        // Collect the results from all threads and group them.
        // Note that this will happen as soon as data are available
        while let Ok(hashed_file) = rx.recv() {
            hash_map_ref.add(hashed_file);
        }
    })
    .unwrap();

    // Convert the hashmap into vector, leaving only large-enough groups:
    hash_map
        .into_iter()
        .map(|((len, hash), files)| FileGroup {
            file_len: len,
            file_hash: hash,
            files: files.to_vec(),
        })
        .filter(group_post_filter)
        .chain(groups_to_pass.into_iter())
        .collect()
}

/// Walks the directory tree and collects matching files in parallel into a vector
fn scan_files(ctx: &AppCtx<'_>) -> Vec<Vec<FileInfo>> {
    let file_collector = ThreadLocal::new();
    let spinner = ctx.log.spinner("Scanning files");
    let spinner_tick = &|_: &Path| spinner.tick();

    let config = &ctx.config;
    let min_size = config.min_size;
    let max_size = config.max_size.unwrap_or(FileLen::MAX);

    let mut walk = Walk::new();
    walk.depth = config.depth.unwrap_or(usize::MAX);
    walk.skip_hidden = config.skip_hidden;
    walk.follow_links = config.follow_links;
    walk.path_selector = ctx.path_selector.clone();
    walk.log = Some(&ctx.log);
    walk.on_visit = spinner_tick;
    walk.run(ctx.config.input_paths(), |path| {
        file_info_or_log_err(path, &ctx.devices, &ctx.log)
            .into_iter()
            .filter(|info| {
                let l = info.len;
                l >= min_size && l <= max_size
            })
            .for_each(|info| {
                let vec = file_collector.get_or(|| RefCell::new(Vec::new()));
                vec.borrow_mut().push(info);
            });
    });

    ctx.log
        .info(format!("Scanned {} file entries", spinner.position()));

    let files: Vec<_> = file_collector.into_iter().map(|r| r.into_inner()).collect();

    let file_count: usize = files.iter().map(|v| v.len()).sum();
    let total_size: u64 = files.iter().flat_map(|v| v.iter().map(|i| i.len.0)).sum();
    ctx.log.info(format!(
        "Found {} ({}) files matching selection criteria",
        file_count,
        FileLen(total_size)
    ));
    files
}

fn group_by_size(ctx: &AppCtx<'_>, files: Vec<Vec<FileInfo>>) -> Vec<FileGroup<FileInfo>> {
    let file_count: usize = files.iter().map(|v| v.len()).sum();
    let progress = ctx.log.progress_bar("Grouping by size", file_count as u64);

    let mut groups = GroupMap::new(|info: FileInfo| (info.len, info));
    for files in files.into_iter() {
        for file in files.into_iter() {
            progress.tick();
            groups.add(file);
        }
    }
    let rf_over = ctx.config.rf_over();
    let rf_under = ctx.config.rf_under();

    let groups: Vec<_> = groups
        .into_iter()
        .filter(|(_, files)| files.len() > rf_over)
        .map(|(l, files)| FileGroup {
            file_len: l,
            file_hash: FileHash(0),
            files: files.into_vec(),
        })
        .collect();

    let count: usize = groups.selected_count(rf_over, rf_under);
    let bytes: FileLen = groups.selected_size(rf_over, rf_under);
    ctx.log.info(format!(
        "Found {} ({}) candidates after grouping by size",
        count, bytes
    ));
    groups
}

/// Removes duplicate files matching by full-path or by inode-id.
/// Deduplication by inode-id is not performed if the flag to preserve hard-links (-H) is set.
fn deduplicate<F>(ctx: &AppCtx<'_>, files: &mut Vec<FileInfo>, progress: F)
where
    F: Fn(&Path) + Sync + Send,
{
    let mut groups = GroupMap::new(|fi: FileInfo| (fi.location, fi));
    for f in files.drain(..) {
        groups.add(f)
    }

    for (_, file_group) in groups.into_iter() {
        if file_group.len() == 1 {
            files.extend(file_group.into_iter().inspect(|p| progress(&p.path)));
        } else if ctx.config.hard_links {
            files.extend(
                file_group
                    .into_iter()
                    .inspect(|p| progress(&p.path))
                    .unique_by(|p| p.path.hash128()),
            )
        } else {
            files.extend(
                file_group
                    .into_iter()
                    .inspect(|p| progress(&p.path))
                    .unique_by(|p| file_id_or_log_err(&p.path, &ctx.log)),
            )
        }
    }
}

fn remove_same_files(
    ctx: &AppCtx<'_>,
    groups: Vec<FileGroup<FileInfo>>,
) -> Vec<FileGroup<FileInfo>> {
    let file_count: usize = groups.total_count();
    let progress = ctx
        .log
        .progress_bar("Removing same files", file_count as u64);

    let rf_over = ctx.config.rf_over();
    let rf_under = ctx.config.rf_under();

    let groups: Vec<_> = groups
        .into_par_iter()
        .update(|g| deduplicate(ctx, &mut g.files, |_| progress.tick()))
        .filter(|g| g.files.len() > rf_over)
        .collect();

    let count: usize = groups.selected_count(rf_over, rf_under);
    let bytes: FileLen = groups.selected_size(rf_over, rf_under);
    ctx.log.info(format!(
        "Found {} ({}) candidates after grouping by paths {}",
        count,
        bytes,
        if ctx.config.hard_links {
            ""
        } else {
            "and file identifiers"
        }
    ));
    groups
}

fn atomic_counter_vec(len: usize) -> Vec<AtomicU32> {
    let mut v = Vec::with_capacity(len);
    for _ in 0..len {
        v.push(AtomicU32::new(0));
    }
    v
}

fn update_file_locations(ctx: &AppCtx<'_>, groups: &mut Vec<FileGroup<FileInfo>>) {
    #[cfg(target_os = "linux")]
    {
        let count = groups.total_count();
        let progress = ctx.log.progress_bar("Fetching extents", count as u64);

        let err_counters = atomic_counter_vec(ctx.devices.len());
        const MAX_ERR_COUNT_TO_LOG: u32 = 10;

        groups
            .par_iter_mut()
            .flat_map(|g| &mut g.files)
            .update(|fi| {
                let device: &DiskDevice = &ctx.devices[fi.get_device_index()];
                if device.disk_type != DiskType::SSD {
                    if let Err(e) = fi.fetch_physical_location() {
                        let counter = &err_counters[device.index];
                        if counter.load(Ordering::Relaxed) < MAX_ERR_COUNT_TO_LOG {
                            let err_count = counter.fetch_add(1, Ordering::Relaxed);
                            ctx.log.warn(format!(
                                "Failed to fetch extents for file {}: {}",
                                fi.path, e
                            ));
                            if err_count == MAX_ERR_COUNT_TO_LOG {
                                ctx.log.warn(
                                    "Too many fetch extents errors. More errors will be ignored. \
                                 Random access performance might be affected on spinning drives.",
                                )
                            }
                        }
                    }
                }
            })
            .for_each(|_| progress.tick());
    }
}

/// Transforms files by piping them to an external program and groups them by their hashes
fn group_transformed(
    ctx: &AppCtx<'_>,
    transform: &Transform,
    groups: Vec<FileGroup<FileInfo>>,
) -> Vec<FileGroup<FileInfo>> {
    let file_count: usize = groups.total_count();
    let progress = ctx
        .log
        .progress_bar("Transforming & grouping", file_count as u64);

    let rf_over = ctx.config.rf_over();
    let rf_under = ctx.config.rf_under();

    let groups = rehash(
        groups,
        |_| true,
        |g| g.files.len() > rf_over,
        &ctx.devices,
        AccessType::Sequential,
        |(fi, _)| {
            let result = transform
                .run_or_log_err(&fi.path, &ctx.log)
                .map(|(len, hash)| {
                    fi.len = len;
                    hash
                });
            progress.tick();
            result
        },
    );

    let count = groups.selected_count(rf_over, rf_under);
    let bytes = groups.selected_size(rf_over, rf_under);
    ctx.log.info(format!(
        "Found {} ({}) {} files",
        count,
        bytes,
        ctx.config.search_type()
    ));
    groups
}

/// Returns the maximum value of the given property of the device,
/// among the devices actually used to store any of the given files
fn max_device_property<'a>(
    devices: &DiskDevices,
    files: impl ParallelIterator<Item = &'a FileInfo>,
    property_fn: impl Fn(&DiskDevice) -> FileLen + Sync,
) -> FileLen {
    files
        .into_par_iter()
        .map(|f| property_fn(&devices[f.get_device_index()]))
        .max()
        .unwrap_or_else(|| property_fn(devices.get_default()))
}

/// Returns the desired prefix length for a group of files.
/// The return value depends on the capabilities of the devices the files are stored on.
/// Higher values are desired if any of the files resides on an HDD.
fn prefix_len<'a>(
    partitions: &DiskDevices,
    files: impl ParallelIterator<Item = &'a FileInfo>,
) -> FileLen {
    max_device_property(partitions, files, |dd| dd.max_prefix_len())
}

/// Groups files by a hash of their first few thousand bytes.
fn group_by_prefix(
    ctx: &AppCtx<'_>,
    prefix_len: FileLen,
    groups: Vec<FileGroup<FileInfo>>,
) -> Vec<FileGroup<FileInfo>> {
    let pre_filter = |g: &FileGroup<FileInfo>| g.files.len() > 1;
    let remaining_files = groups.iter().filter(|&g| pre_filter(g)).total_count();
    let progress = ctx
        .log
        .progress_bar("Grouping by prefix", remaining_files as u64);

    let rf_over = ctx.config.rf_over();
    let rf_under = ctx.config.rf_under();

    let groups = rehash(
        groups,
        pre_filter,
        |g| g.files.len() > rf_over,
        &ctx.devices,
        AccessType::Random,
        |(fi, _)| {
            progress.tick();
            let device = &ctx.devices[fi.get_device_index()];
            let buf_len = device.buf_len();
            let (caching, prefix_len) = if fi.len <= prefix_len {
                (Caching::Default, prefix_len)
            } else {
                (Caching::Random, device.min_prefix_len())
            };

            file_hash_or_log_err(
                &fi.path,
                FilePos(0),
                prefix_len,
                buf_len,
                caching,
                |_| {},
                &ctx.log,
            )
        },
    );

    let count = groups.selected_count(rf_over, rf_under);
    let bytes = groups.selected_size(rf_over, rf_under);
    ctx.log.info(format!(
        "Found {} ({}) candidates after grouping by prefix",
        count, bytes
    ));
    groups
}

/// Returns the desired suffix length for a group of files.
/// The return value depends on the capabilities of the devices the files are stored on.
/// Higher values are desired if any of the files resides on an HDD.
fn suffix_len<'a>(
    partitions: &DiskDevices,
    files: impl ParallelIterator<Item = &'a FileInfo>,
) -> FileLen {
    max_device_property(partitions, files, |dd| dd.suffix_len())
}

fn suffix_threshold<'a>(
    partitions: &DiskDevices,
    files: impl ParallelIterator<Item = &'a FileInfo>,
) -> FileLen {
    max_device_property(partitions, files, |dd| dd.suffix_threshold())
}

fn group_by_suffix(ctx: &AppCtx<'_>, groups: Vec<FileGroup<FileInfo>>) -> Vec<FileGroup<FileInfo>> {
    let suffix_len = suffix_len(&ctx.devices, flat_iter(&groups));
    let suffix_threshold = suffix_threshold(&ctx.devices, flat_iter(&groups));
    let pre_filter = |g: &FileGroup<FileInfo>| g.file_len >= suffix_threshold && g.files.len() > 1;
    let remaining_files = groups.iter().filter(|&g| pre_filter(g)).total_count();
    let progress = ctx
        .log
        .progress_bar("Grouping by suffix", remaining_files as u64);

    let rf_over = ctx.config.rf_over();
    let rf_under = ctx.config.rf_under();

    let groups = rehash(
        groups,
        pre_filter,
        |g| g.files.len() > rf_over,
        &ctx.devices,
        AccessType::Random,
        |(fi, old_hash)| {
            progress.tick();
            let device = &ctx.devices[fi.get_device_index()];
            let buf_len = device.buf_len();
            file_hash_or_log_err(
                &fi.path,
                fi.len.as_pos() - suffix_len,
                suffix_len,
                buf_len,
                Caching::Default,
                |_| {},
                &ctx.log,
            )
            .map(|new_hash| old_hash ^ new_hash)
        },
    );

    let count = groups.selected_count(rf_over, rf_under);
    let bytes = groups.selected_size(rf_over, rf_under);
    ctx.log.info(format!(
        "Found {} ({}) candidates after grouping by suffix",
        count, bytes
    ));
    groups
}

fn group_by_contents(
    ctx: &AppCtx<'_>,
    min_file_len: FileLen,
    groups: Vec<FileGroup<FileInfo>>,
) -> Vec<FileGroup<FileInfo>> {
    let pre_filter = |g: &FileGroup<FileInfo>| g.files.len() > 1 && g.file_len >= min_file_len;
    let bytes_to_scan = groups.iter().filter(|&g| pre_filter(g)).total_size();
    let progress = &ctx
        .log
        .bytes_progress_bar("Grouping by contents", bytes_to_scan.0);

    let rf_over = ctx.config.rf_over();
    let rf_under = ctx.config.rf_under();

    let groups = rehash(
        groups,
        pre_filter,
        |g| g.files.len() > rf_over,
        &ctx.devices,
        AccessType::Sequential,
        |(fi, _)| {
            let device = &ctx.devices[fi.get_device_index()];
            let buf_len = device.buf_len();
            file_hash_or_log_err(
                &fi.path,
                FilePos(0),
                fi.len,
                buf_len,
                Caching::Sequential,
                |delta| progress.inc(delta),
                &ctx.log,
            )
        },
    );

    let count = groups.selected_count(rf_over, rf_under);
    let bytes = groups.selected_size(rf_over, rf_under);
    ctx.log.info(format!(
        "Found {} ({}) {} files",
        count,
        bytes,
        ctx.config.search_type()
    ));
    groups
}

/// Groups identical files together by 128-bit hash of their contents.
/// Depending on filtering settings, can find unique, duplicate, over- or under-replicated files.
///
/// # Input
/// The input set of files or paths to scan should be given in the `config.paths` property.
/// When `config.recursive` is set to true, the search descends into
/// subdirectories recursively (default is false).
///
/// # Output
/// Returns a vector of groups of absolute paths.
/// Each group of files has a common hash and length.
/// Groups are sorted descending by file size.
///
/// # Errors
/// An error is returned immediately if the configuration is invalid.
/// I/O errors during processing are logged as warnings and unreadable files are skipped.
/// If panics happen they are likely a result of a bug and should be reported.
///
/// # Performance characteristics
/// The worst-case running time to is roughly proportional to the time required to
/// open and read all files. Depending on the number of matching files and parameters of the
/// query, that time can be lower because some files can be skipped from some stages of processing.
/// The expected memory utilisation is roughly proportional the number of files and
/// their path lengths.
///
/// # Threading
/// This function blocks caller's thread until all files are processed.
/// To speed up processing, it spawns multiple threads internally.
/// Some processing is performed on the default Rayon thread pool, therefore this function
/// must not be called on Rayon thread pool to avoid a deadlock.
/// The parallelism level is automatically set based on the type of storage and can be overridden
/// in the configuration.
///
/// # Algorithm
/// Files are grouped in multiple stages and filtered after each stage.
/// Files that turn out to be unique at some point are skipped from further stages.
/// Stages are ordered by increasing I/O cost. On rotational drives,
/// an attempt is made to sort files by physical data location before each grouping stage
/// to reduce disk seek times.
///
/// 1. Create a list of files to process by walking directory tree if recursive mode selected.
/// 2. Get length and identifier of each file.
/// 3. Group files by length.
/// 4. In each group, remove duplicate files with the same identifier.
/// 5. Group files by hash of the prefix.
/// 6. Group files by hash of the suffix.
/// 7. Group files by hash of their full contents.
///
/// # Example
/// ```
/// use fclones::log::Log;
/// use fclones::config::GroupConfig;
/// use fclones::{group_files, write_report};
/// use std::path::PathBuf;
///
/// let log = Log::new();
/// let mut config = GroupConfig::default();
/// config.paths = vec![PathBuf::from("/path/to/a/dir")];
///
/// let groups = group_files(&config, &log).unwrap();
/// println!("Found {} groups: ", groups.len());
///
/// // print standard fclones report to stdout:
/// write_report(&config, &log, &groups).unwrap();
/// ```
pub fn group_files(config: &GroupConfig, log: &Log) -> Result<Vec<FileGroup<Path>>, Error> {
    let spinner = log.spinner("Initializing");
    let ctx = AppCtx::new(config, log)?;

    drop(spinner);
    let matching_files = scan_files(&ctx);
    let size_groups = group_by_size(&ctx, matching_files);
    let mut size_groups_pruned = remove_same_files(&ctx, size_groups);
    update_file_locations(&ctx, &mut size_groups_pruned);

    let groups = match &ctx.transform {
        Some(transform) => group_transformed(&ctx, transform, size_groups_pruned),
        _ => {
            let prefix_len = prefix_len(&ctx.devices, flat_iter(&size_groups_pruned));
            let prefix_groups = group_by_prefix(&ctx, prefix_len, size_groups_pruned);
            let suffix_groups = group_by_suffix(&ctx, prefix_groups);
            group_by_contents(&ctx, prefix_len, suffix_groups)
        }
    };
    let mut groups: Vec<_> = groups
        .into_par_iter()
        .map(|g| FileGroup {
            file_len: g.file_len,
            file_hash: g.file_hash,
            files: g.files.into_iter().map(|fi| fi.path).collect(),
        })
        .collect();
    groups.retain(|g| g.files.len() < ctx.config.rf_under());
    groups.par_sort_by_key(|g| Reverse((g.file_len, g.file_hash)));
    groups.par_iter_mut().for_each(|g| g.files.sort());
    Ok(groups)
}

/// Writes the list of groups to a file or the standard output.
///
/// # Parameters
/// - `config.output`: a path to the output file, `None` for standard output
/// - `config.format`: selects the format of the output, see [`config::OutputFormat`]
/// - `log`: used for drawing a progress bar to standard error
/// - `groups`: list of groups of files to print, e.g. obtained from [`group_files`]
///
/// # Errors
/// Returns [`io::Error`] on I/O write error or if the output file cannot be created.
pub fn write_report(config: &GroupConfig, log: &Log, groups: &[FileGroup<Path>]) -> io::Result<()> {
    let now = Local::now();
    let rf_over = max(1, config.rf_over());
    let header = ReportHeader {
        timestamp: DateTime::from_utc(now.naive_utc(), *now.offset()),
        version: env!("CARGO_PKG_VERSION").to_owned(),
        command: args().collect(),
        stats: Some(FileStats {
            group_count: groups.len(),
            redundant_file_count: groups.selected_count(rf_over, usize::MAX),
            redundant_file_size: groups.selected_size(rf_over, usize::MAX),
        }),
    };

    match &config.output {
        Some(path) => {
            let progress = log.progress_bar("Writing report", groups.len() as u64);
            let iter = groups.iter().inspect(|_g| progress.tick());
            let file = BufWriter::new(File::create(path)?);
            let mut reporter = ReportWriter::new(file, false);
            reporter.write(&config.format, &header, iter)
        }
        None => {
            let term = Term::stdout();
            let color = term.is_term();
            let mut reporter = ReportWriter::new(BufWriter::new(term), color);
            reporter.write(&config.format, &header, groups.iter())
        }
    }
}

#[cfg(test)]
mod test {
    use std::fs::{hard_link, File, OpenOptions};
    use std::io::{Read, Write};
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Mutex;

    use rand::seq::SliceRandom;

    use crate::path::Path;
    use crate::util::test::*;

    use super::*;

    const MAX_PREFIX_LEN: usize = 256 * 1024;
    const MAX_SUFFIX_LEN: usize = 256 * 1024;

    /// Files hashing to different values should be placed into different groups
    #[test]
    fn test_rehash_puts_files_with_different_hashes_to_different_groups() {
        let devices = DiskDevices::default();
        let input = vec![FileGroup {
            file_len: FileLen(200),
            file_hash: FileHash(0),
            files: vec![
                FileInfo {
                    len: FileLen(200),
                    location: 0,
                    path: Path::from("file1"),
                },
                FileInfo {
                    len: FileLen(200),
                    location: 35847587,
                    path: Path::from("file2"),
                },
            ],
        }];

        let result = rehash(
            input,
            |_| true,
            |_| true,
            &devices,
            AccessType::Random,
            |(fi, _)| Some(FileHash(fi.location as u128)),
        );

        assert_eq!(result.len(), 2);
        assert_eq!(result[0].files.len(), 1);
        assert_eq!(result[1].files.len(), 1);
        assert_ne!(result[0].files[0].path, result[1].files[0].path);
    }

    /// Files hashing to same values should be placed into the same groups
    #[test]
    fn test_rehash_puts_files_with_same_hashes_to_same_groups() {
        let devices = DiskDevices::default();
        let input = vec![
            FileGroup {
                file_len: FileLen(200),
                file_hash: FileHash(0),
                files: vec![FileInfo {
                    len: FileLen(200),
                    location: 0,
                    path: Path::from("file1"),
                }],
            },
            FileGroup {
                file_len: FileLen(500),
                file_hash: FileHash(0),
                files: vec![FileInfo {
                    len: FileLen(200),
                    location: 35847587,
                    path: Path::from("file2"),
                }],
            },
        ];

        let result = rehash(
            input,
            |_| true,
            |_| true,
            &devices,
            AccessType::Random,
            |(_, _)| Some(FileHash(123456)),
        );

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].files.len(), 2);
    }

    #[test]
    fn test_rehash_can_skip_processing_files() {
        let devices = DiskDevices::default();
        let input = vec![FileGroup {
            file_len: FileLen(200),
            file_hash: FileHash(0),
            files: vec![FileInfo {
                len: FileLen(200),
                location: 0,
                path: Path::from("file1"),
            }],
        }];

        let called = AtomicBool::new(false);
        let result = rehash(
            input,
            |_| false,
            |_| true,
            &devices,
            AccessType::Random,
            |(fi, _)| {
                called.store(true, Ordering::Release);
                Some(FileHash(fi.location as u128))
            },
        );

        assert_eq!(result.len(), 1);
        assert!(!called.load(Ordering::Acquire));
    }

    #[test]
    fn test_rehash_post_filter_removes_groups() {
        let devices = DiskDevices::default();
        let input = vec![FileGroup {
            file_len: FileLen(200),
            file_hash: FileHash(0),
            files: vec![
                FileInfo {
                    len: FileLen(200),
                    location: 0,
                    path: Path::from("file1"),
                },
                FileInfo {
                    len: FileLen(200),
                    location: 35847587,
                    path: Path::from("file2"),
                },
            ],
        }];

        let result = rehash(
            input,
            |_| true,
            |g| g.files.len() >= 2,
            &devices,
            AccessType::Random,
            |(fi, _)| Some(FileHash(fi.location as u128)),
        );

        assert!(result.is_empty())
    }

    #[test]
    fn test_rehash_processes_files_in_location_order_on_hdd() {
        let thread_count = 2;
        let devices = DiskDevices::single(DiskType::HDD, thread_count);
        let count = 1000;
        let mut input = Vec::with_capacity(count);
        for i in 0..count {
            input.push(FileGroup {
                file_len: FileLen(0),
                file_hash: FileHash(0),
                files: vec![FileInfo {
                    len: FileLen(0),
                    location: i as u64,
                    path: Path::from(format!("file{}", i)),
                }],
            })
        }
        input.shuffle(&mut rand::thread_rng());

        let processing_order = Mutex::new(Vec::new());
        rehash(
            input,
            |_| true,
            |_| true,
            &devices,
            AccessType::Random,
            |(fi, _)| {
                processing_order.lock().unwrap().push(fi.location as i32);
                Some(FileHash(fi.location as u128))
            },
        );
        let processing_order = processing_order.into_inner().unwrap();

        // Because we're processing files in parallel, we have no strict guarantee they
        // will be processed in the exact same order as in the input.
        // However, we expect some locality so the total distance between subsequent accesses
        // is low.
        let mut distance = 0;
        for i in 0..processing_order.len() - 1 {
            distance += i32::abs(processing_order[i] - processing_order[i + 1])
        }
        assert!(distance < (thread_count * count) as i32)
    }

    #[test]
    fn identical_small_files() {
        with_dir("main/identical_small_files", |root| {
            let file1 = root.join("file1");
            let file2 = root.join("file2");
            write_test_file(&file1, b"aaa", b"", b"");
            write_test_file(&file2, b"aaa", b"", b"");

            let log = test_log();
            let mut config = GroupConfig::default();
            config.paths = vec![file1, file2];
            let results = group_files(&config, &log).unwrap();
            assert_eq!(results.len(), 1);
            assert_eq!(results[0].file_len, FileLen(3));
            assert_eq!(results[0].files.len(), 2);
        });
    }

    #[test]
    fn identical_large_files() {
        with_dir("main/identical_large_files", |root| {
            let file1 = root.join("file1");
            let file2 = root.join("file2");
            write_test_file(&file1, &[0; MAX_PREFIX_LEN], &[1; 4096], &[2; 4096]);
            write_test_file(&file2, &[0; MAX_PREFIX_LEN], &[1; 4096], &[2; 4096]);

            let log = test_log();
            let mut config = GroupConfig::default();
            config.paths = vec![file1, file2];

            let results = group_files(&config, &log).unwrap();
            assert_eq!(results.len(), 1);
            assert_eq!(results[0].files.len(), 2);
        });
    }

    #[test]
    fn files_differing_by_size() {
        with_dir("main/files_differing_by_size", |root| {
            let file1 = root.join("file1");
            let file2 = root.join("file2");
            write_test_file(&file1, b"aaaa", b"", b"");
            write_test_file(&file2, b"aaa", b"", b"");

            let log = test_log();
            let mut config = GroupConfig::default();
            config.paths = vec![file1.clone(), file2.clone()];
            config.rf_over = Some(0);

            let results = group_files(&config, &log).unwrap();
            assert_eq!(results.len(), 2);
            assert_eq!(
                results[0].files,
                vec![Path::from(file1.canonicalize().unwrap())]
            );
            assert_eq!(
                results[1].files,
                vec![Path::from(file2.canonicalize().unwrap())]
            );
        });
    }

    #[test]
    fn files_differing_by_prefix() {
        with_dir("main/files_differing_by_prefix", |root| {
            let file1 = root.join("file1");
            let file2 = root.join("file2");
            write_test_file(&file1, b"aaa", b"", b"");
            write_test_file(&file2, b"bbb", b"", b"");

            let log = test_log();
            let mut config = GroupConfig::default();
            config.paths = vec![file1.clone(), file2.clone()];
            config.unique = true;

            let results = group_files(&config, &log).unwrap();
            assert_eq!(results.len(), 2);
            assert_eq!(results[0].files.len(), 1);
            assert_eq!(results[1].files.len(), 1);
        });
    }

    #[test]
    fn files_differing_by_suffix() {
        with_dir("main/files_differing_by_suffix", |root| {
            let file1 = root.join("file1");
            let file2 = root.join("file2");
            let prefix = [0; MAX_PREFIX_LEN];
            let mid = [1; MAX_PREFIX_LEN + MAX_SUFFIX_LEN];
            write_test_file(&file1, &prefix, &mid, b"suffix1");
            write_test_file(&file2, &prefix, &mid, b"suffix2");

            let log = test_log();
            let mut config = GroupConfig::default();
            config.paths = vec![file1.clone(), file2.clone()];
            config.unique = true;

            let results = group_files(&config, &log).unwrap();
            assert_eq!(results.len(), 2);
            assert_eq!(results[0].files.len(), 1);
            assert_eq!(results[1].files.len(), 1);
        });
    }

    #[test]
    fn files_differing_by_middle() {
        with_dir("main/files_differing_by_middle", |root| {
            let file1 = root.join("file1");
            let file2 = root.join("file2");
            let prefix = [0; MAX_PREFIX_LEN];
            let suffix = [1; MAX_SUFFIX_LEN];
            write_test_file(&file1, &prefix, b"middle1", &suffix);
            write_test_file(&file2, &prefix, b"middle2", &suffix);

            let log = test_log();
            let mut config = GroupConfig::default();
            config.paths = vec![file1.clone(), file2.clone()];
            config.unique = true;

            let results = group_files(&config, &log).unwrap();
            assert_eq!(results.len(), 2);
            assert_eq!(results[0].files.len(), 1);
            assert_eq!(results[1].files.len(), 1);
        });
    }

    #[test]
    fn hard_links() {
        with_dir("main/hard_links", |root| {
            let file1 = root.join("file1");
            let file2 = root.join("file2");
            write_test_file(&file1, b"aaa", b"", b"");
            hard_link(&file1, &file2).unwrap();

            let log = test_log();
            let mut config = GroupConfig::default();
            config.paths = vec![file1.clone(), file2.clone()];
            config.unique = true;

            let results = group_files(&config, &log).unwrap();
            assert_eq!(results.len(), 1);
            assert_eq!(results[0].files.len(), 1);
        });
    }

    #[test]
    fn duplicate_input_files() {
        with_dir("main/duplicate_input_files", |root| {
            let file1 = root.join("file1");
            write_test_file(&file1, b"foo", b"", b"");
            let log = test_log();
            let mut config = GroupConfig::default();
            config.paths = vec![file1.clone(), file1.clone(), file1.clone()];
            config.unique = true;
            config.hard_links = true;

            let results = group_files(&config, &log).unwrap();
            assert_eq!(results.len(), 1);
            assert_eq!(results[0].files.len(), 1);
        });
    }

    #[test]
    #[cfg(unix)]
    fn duplicate_input_files_non_canonical() {
        use std::os::unix::fs::symlink;

        with_dir("main/duplicate_input_files_non_canonical", |root| {
            let dir = root.join("dir");
            symlink(&root, &dir).unwrap();

            let file1 = root.join("file1");
            let file2 = root.join("dir/file1");
            write_test_file(&file1, b"foo", b"", b"");

            let log = test_log();
            let mut config = GroupConfig::default();
            config.paths = vec![file1.clone(), file2.clone()];
            config.unique = true;
            config.hard_links = true;

            let results = group_files(&config, &log).unwrap();
            assert_eq!(results.len(), 1);
            assert_eq!(results[0].files.len(), 1);
        });
    }

    #[test]
    fn report() {
        with_dir("main/report", |root| {
            let file = root.join("file1");
            write_test_file(&file, b"foo", b"", b"");

            let report_file = root.join("report.txt");
            let log = test_log();
            let mut config = GroupConfig::default();
            config.paths = vec![file.clone()];
            config.unique = true;
            config.output = Some(report_file.clone());

            let results = group_files(&config, &log).unwrap();
            write_report(&config, &log, &results).unwrap();

            assert!(report_file.exists());
            let mut report = String::new();
            File::open(report_file)
                .unwrap()
                .read_to_string(&mut report)
                .unwrap();
            assert!(report.contains("file1"))
        });
    }

    fn write_test_file(path: &PathBuf, prefix: &[u8], mid: &[u8], suffix: &[u8]) {
        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .open(&path)
            .unwrap();
        file.write(prefix).unwrap();
        file.write(mid).unwrap();
        file.write(suffix).unwrap();
    }

    fn test_log() -> Log {
        let mut log = Log::new();
        log.no_progress = true;
        log
    }
}
