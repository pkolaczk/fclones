//! Removing redundant files

use std::cmp::{max, min, Reverse};
use std::fmt::{Display, Formatter};
use std::fs::Metadata;
use std::io::{BufWriter, ErrorKind, Write};
use std::ops::{Add, AddAssign};
use std::sync::{Arc, Mutex};
use std::{fmt, fs, io};

use chrono::{DateTime, FixedOffset, Local};
use crossbeam_utils::atomic::AtomicCell;
use rand::distributions::Alphanumeric;
use rand::Rng;
use rayon::iter::IntoParallelIterator;
use rayon::iter::ParallelIterator;

use crate::config::{DedupeConfig, Priority};
use crate::device::DiskDevices;
use crate::files::FileLen;
use crate::lock::FileLock;
use crate::log::Log;
use crate::path::Path;
use crate::util::fallible_sort_by_key;
use crate::{Error, FileGroup, TIMESTAMP_FMT};
use std::collections::HashMap;

/// Defines what to do with redundant files
#[derive(Clone)]
pub enum DedupeOp {
    /// Removes redundant files.
    Remove,
    /// Moves redundant files to a different dir
    Move(Arc<Path>),
    /// Replaces redundant files with soft-links (ln -s on Unix).
    SoftLink,
    /// Replaces redundant files with hard-links (ln on Unix).
    HardLink,
}

/// Convenience struct for holding a path to a file and its metadata together
#[derive(Debug)]
pub struct FileMetadata {
    pub path: Path,
    pub metadata: Metadata,
}

impl FileMetadata {
    pub fn new(path: Path) -> io::Result<FileMetadata> {
        let path_buf = path.to_path_buf();
        let metadata = fs::symlink_metadata(&path_buf).map_err(|e| {
            io::Error::new(
                e.kind(),
                format!("Failed to read metadata of {}: {}", path, e),
            )
        })?;
        Ok(FileMetadata { path, metadata })
    }

    #[cfg(unix)]
    pub fn device_id(&self) -> Option<u64> {
        use std::os::unix::fs::MetadataExt;
        Some(self.metadata.dev())
    }

    #[cfg(windows)]
    pub fn device_id(&self) -> Option<u64> {
        use crate::files::FileId;
        FileId::new(&self.path).ok().map(|f| f.device)
    }
}

impl Display for FileMetadata {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.pad(format!("{}", self.path).as_str())
    }
}

/// Portable abstraction for commands used to remove duplicates
pub enum FsCommand {
    Remove {
        file: FileMetadata,
    },
    Move {
        source: FileMetadata,
        target: Path,
        use_rename: bool, // try to move the file directly by issuing fs rename command
    },
    SoftLink {
        target: Arc<FileMetadata>,
        link: FileMetadata,
    },
    HardLink {
        target: Arc<FileMetadata>,
        link: FileMetadata,
    },
}

impl FsCommand {
    fn remove(path: &Path) -> io::Result<()> {
        let _ = FileLock::new(path)?;
        fs::remove_file(path.to_path_buf())
            .map_err(|e| io::Error::new(e.kind(), format!("Failed to remove file {}: {}", path, e)))
    }

    #[cfg(unix)]
    fn symlink_internal(target: &std::path::Path, link: &std::path::Path) -> io::Result<()> {
        std::os::unix::fs::symlink(target, link)
    }

    #[cfg(windows)]
    fn symlink_internal(target: &std::path::Path, link: &std::path::Path) -> io::Result<()> {
        std::os::windows::fs::symlink_file(target, link)
    }

    fn symlink(target: &Path, link: &Path) -> io::Result<()> {
        Self::symlink_internal(&target.to_path_buf(), &link.to_path_buf()).map_err(|e| {
            io::Error::new(
                e.kind(),
                format!(
                    "Failed to create symbolic link {} -> {}: {}",
                    link, target, e
                ),
            )
        })
    }

    fn hardlink(target: &Path, link: &Path) -> io::Result<()> {
        fs::hard_link(&target.to_path_buf(), &link.to_path_buf()).map_err(|e| {
            io::Error::new(
                e.kind(),
                format!("Failed to create hard link {} -> {}: {}", link, target, e),
            )
        })
    }

    fn check_can_rename(source: &Path, target: &Path) -> io::Result<()> {
        if target.to_path_buf().exists() {
            return Err(io::Error::new(
                ErrorKind::AlreadyExists,
                format!(
                    "Cannot move {} to {}: Target already exists",
                    source.display(),
                    target.display()
                ),
            ));
        }
        Ok(())
    }

    fn mkdirs(path: &Path) -> io::Result<()> {
        fs::create_dir_all(path.to_path_buf()).map_err(|e| {
            io::Error::new(
                e.kind(),
                format!("Failed to create directory {}: {}", path, e),
            )
        })
    }

    /// Renames/moves a file from one location to another.
    /// If the target exists, it would be overwritten.
    fn unsafe_rename(source: &Path, target: &Path) -> io::Result<()> {
        fs::rename(&source.to_path_buf(), &target.to_path_buf()).map_err(|e| {
            io::Error::new(
                e.kind(),
                format!(
                    "Failed to rename file from {} to {}: {}",
                    source,
                    target.display(),
                    e
                ),
            )
        })
    }

    /// Copies a file from one location to another.
    /// If the target exists, it would be overwritten.
    fn unsafe_copy(source: &Path, target: &Path) -> io::Result<()> {
        fs::copy(&source.to_path_buf(), &target.to_path_buf()).map_err(|e| {
            io::Error::new(
                e.kind(),
                format!(
                    "Failed to copy file from {} to {}: {}",
                    source,
                    target.display(),
                    e
                ),
            )
        })?;
        Ok(())
    }

    /// Moves the file from one location to another by single `fs::rename` command.
    /// Fails if target exists.
    fn move_rename(source: &Path, target: &Path) -> io::Result<()> {
        Self::check_can_rename(source, target)?;
        Self::mkdirs(target.parent().unwrap())?;
        Self::unsafe_rename(source, target)?;
        Ok(())
    }

    /// Moves the file by copying it first to another location and then removing the original.
    /// Fails if target exists.
    fn move_copy(source: &Path, target: &Path) -> io::Result<()> {
        Self::check_can_rename(source, target)?;
        Self::mkdirs(target.parent().unwrap())?;
        Self::unsafe_copy(source, target)?;
        Self::remove(source)?;
        Ok(())
    }

    /// Returns a random temporary file name in the same directory, guaranteed to not collide with
    /// any other file in the same directory
    fn temp_file(path: &Path) -> Path {
        let mut name = path
            .file_name()
            .expect("must be a regular file with a name");
        name.push(".");
        name.push(
            rand::thread_rng()
                .sample_iter(&Alphanumeric)
                .take(24)
                .map(char::from)
                .collect::<String>(),
        );
        match path.parent() {
            Some(parent) => parent.join(Path::from(name)),
            None => Path::from(name),
        }
    }

    /// Safely moves the file to a different location and invokes the function.
    /// If the function fails, moves the file back to the original location.
    /// If the function succeeds, removes the file permanently.
    fn safe_remove<R>(path: &Path, f: impl Fn(&Path) -> io::Result<R>, log: &Log) -> io::Result<R> {
        let _ = FileLock::new(path)?; // don't remove a locked file
        let tmp = Self::temp_file(path);
        Self::unsafe_rename(path, &tmp)?;
        let result = match f(path) {
            Ok(result) => result,
            Err(e) => {
                // Try to undo the move if possible
                if let Err(remove_err) = Self::unsafe_rename(&tmp, path) {
                    log.warn(format!(
                        "Failed to undo move from {} to {}: {}",
                        &path, &tmp, remove_err
                    ))
                }
                return Err(e);
            }
        };
        // Cleanup the temp file.
        if let Err(e) = Self::remove(&tmp) {
            log.warn(format!("Failed to remove temporary {}: {}", &tmp, e))
        }
        Ok(result)
    }

    /// Executes the command and returns the number of bytes reclaimed
    pub fn execute(&self, log: &Log) -> io::Result<FileLen> {
        match self {
            FsCommand::Remove { file } => {
                Self::remove(&file.path)?;
                Ok(FileLen(file.metadata.len()))
            }
            FsCommand::SoftLink { target, link } => {
                Self::safe_remove(&link.path, |link| Self::symlink(&target.path, link), log)?;
                Ok(FileLen(link.metadata.len()))
            }
            FsCommand::HardLink { target, link } => {
                Self::safe_remove(&link.path, |link| Self::hardlink(&target.path, link), log)?;
                Ok(FileLen(link.metadata.len()))
            }
            FsCommand::Move {
                source,
                target,
                use_rename,
            } => {
                let len = FileLen(source.metadata.len());
                if *use_rename && Self::move_rename(&source.path, target).is_ok() {
                    return Ok(len);
                }
                Self::move_copy(&source.path, target)?;
                Ok(len)
            }
        }
    }

    /// Returns how much disk space running this command would reclaim
    pub fn space_to_reclaim(&self) -> FileLen {
        match self {
            FsCommand::Remove { file, .. }
            | FsCommand::SoftLink { link: file, .. }
            | FsCommand::HardLink { link: file, .. }
            | FsCommand::Move { source: file, .. } => FileLen(file.metadata.len()),
        }
    }

    /// Formats the command as a string that can be pasted to a Unix shell (e.g. bash)
    #[cfg(unix)]
    pub fn to_shell_str(&self) -> Vec<String> {
        let mut result = Vec::new();
        match self {
            FsCommand::Remove { file, .. } => {
                let path = file.path.shell_quote();
                result.push(format!("rm {}", path));
            }
            FsCommand::SoftLink { target, link, .. } => {
                let tmp = Self::temp_file(&link.path);
                let target = target.path.shell_quote();
                let link = link.path.shell_quote();
                result.push(format!("mv {} {}", link, tmp));
                result.push(format!("ln -s {} {}", target, link));
                result.push(format!("rm {}", tmp));
            }
            FsCommand::HardLink { target, link, .. } => {
                let tmp = Self::temp_file(&link.path);
                let target = target.path.shell_quote();
                let link = link.path.shell_quote();
                result.push(format!("mv {} {}", link, tmp));
                result.push(format!("ln {} {}", target, link));
                result.push(format!("rm {}", tmp));
            }
            FsCommand::Move {
                source,
                target,
                use_rename,
            } => {
                let source = source.path.shell_quote();
                let target = target.shell_quote();
                if *use_rename {
                    result.push(format!("mv {} {}", &source, &target));
                } else {
                    result.push(format!("cp {} {}", &source, &target));
                    result.push(format!("rm {}", &source));
                }
            }
        }
        result
    }

    #[cfg(windows)]
    pub fn to_shell_str(&self) -> Vec<String> {
        let mut result = Vec::new();
        match self {
            FsCommand::Remove { file, .. } => {
                let path = file.path.shell_quote();
                result.push(format!("del {}", path));
            }
            FsCommand::SoftLink { target, link, .. } => {
                let tmp = Self::temp_file(&link.path);
                let target = target.path.shell_quote();
                let link = link.path.shell_quote();
                result.push(format!("move {} {}", link, tmp));
                result.push(format!("mklink {} {}", target, link));
                result.push(format!("del {}", tmp));
            }
            FsCommand::HardLink { target, link, .. } => {
                let tmp = Self::temp_file(&link.path);
                let target = target.path.shell_quote();
                let link = link.path.shell_quote();
                result.push(format!("move {} {}", link, tmp));
                result.push(format!("mklink /H {} {}", target, link));
                result.push(format!("del {}", tmp));
            }
            FsCommand::Move {
                source,
                target,
                use_rename,
            } => {
                let source = source.path.shell_quote();
                let target = target.shell_quote();
                if *use_rename {
                    result.push(format!("move {} {}", &source, &target));
                } else {
                    result.push(format!("copy {} {}", &source, &target));
                    result.push(format!("del {}", &source));
                }
            }
        }
        result
    }
}

/// Provides information about the number of deduplicated files and reclaimed disk space
#[derive(Default)]
pub struct DedupeResult {
    pub processed_count: u64,
    pub reclaimed_space: FileLen,
}

impl Add<DedupeResult> for DedupeResult {
    type Output = DedupeResult;

    fn add(self, rhs: Self) -> Self::Output {
        DedupeResult {
            processed_count: self.processed_count + rhs.processed_count,
            reclaimed_space: self.reclaimed_space + rhs.reclaimed_space,
        }
    }
}

impl AddAssign for DedupeResult {
    fn add_assign(&mut self, rhs: Self) {
        self.processed_count += rhs.processed_count;
        self.reclaimed_space += rhs.reclaimed_space;
    }
}

/// Returns true if any of the files has been modified after given timestamp
/// Also returns true if file timestamp could
fn was_modified(files: &[FileMetadata], after: DateTime<FixedOffset>, log: &Log) -> bool {
    let mut result = false;
    let after: DateTime<Local> = after.into();
    for FileMetadata {
        path: p,
        metadata: m,
        ..
    } in files.iter()
    {
        match m.modified() {
            Ok(file_timestamp) => {
                let file_timestamp: DateTime<Local> = file_timestamp.into();
                if file_timestamp > after {
                    log.warn(format!(
                        "File {} was updated after {} (at {})",
                        p,
                        after.format(TIMESTAMP_FMT),
                        file_timestamp.format(TIMESTAMP_FMT)
                    ));
                    result = true;
                }
            }
            Err(e) => {
                log.warn(format!(
                    "Failed to read modification time of file {}: {}",
                    p, e
                ));
                result = true;
            }
        }
    }
    result
}

/// Sort files so that files with highest priority (newest, most recently updated,
/// recently accessed, etc) are sorted last.
/// In cases when metadata of a file cannot be accessed, an error message is pushed
/// in the result vector and such file is placed at the beginning of the list.
fn sort_by_priority(files: &mut Vec<FileMetadata>, priority: &Priority) -> Vec<Error> {
    let errors = match priority {
        Priority::Newest => fallible_sort_by_key(files, |m| {
            m.metadata
                .created()
                .map_err(|e| format!("Failed to read creation time of file {}: {}", m.path, e))
        }),
        Priority::Oldest => fallible_sort_by_key(files, |m| {
            m.metadata
                .created()
                .map(Reverse)
                .map_err(|e| format!("Failed to read creation time of file {}: {}", m.path, e))
        }),
        Priority::MostRecentlyModified => fallible_sort_by_key(files, |m| {
            m.metadata
                .modified()
                .map_err(|e| format!("Failed to read modification time of file {}: {}", m.path, e))
        }),
        Priority::LeastRecentlyModified => fallible_sort_by_key(files, |m| {
            m.metadata
                .modified()
                .map(Reverse)
                .map_err(|e| format!("Failed to read modification time of file {}: {}", m.path, e))
        }),
        Priority::MostRecentlyAccessed => fallible_sort_by_key(files, |m| {
            m.metadata
                .accessed()
                .map_err(|e| format!("Failed to read access time of file {}: {}", m.path, e))
        }),
        Priority::LeastRecentlyAccessed => fallible_sort_by_key(files, |m| {
            m.metadata
                .accessed()
                .map(Reverse)
                .map_err(|e| format!("Failed to read access time of file {}: {}", m.path, e))
        }),
        Priority::MostNested => {
            files.sort_by_key(|m| m.path.component_count());
            vec![]
        }
        Priority::LeastNested => {
            files.sort_by_key(|m| Reverse(m.path.component_count()));
            vec![]
        }
    };
    errors.into_iter().map(Error::from).collect()
}

/// Returns true if given path matches any of the `keep` patterns
fn should_keep(path: &Path, config: &DedupeConfig) -> bool {
    let matches_any_name = config
        .keep_name_patterns
        .iter()
        .any(|p| match path.file_name_cstr() {
            Some(name) => p.matches(name.to_string_lossy().as_ref()),
            None => false,
        });
    let matches_any_path = || {
        config
            .keep_path_patterns
            .iter()
            .any(|p| p.matches_path(&path.to_path_buf()))
    };

    matches_any_name || matches_any_path()
}

/// Returns true if given path matches all of the `drop` patterns.
/// If there are no `drop` patterns, returns true.
fn may_drop(path: &Path, config: &DedupeConfig) -> bool {
    let matches_any_name = || {
        config
            .name_patterns
            .iter()
            .any(|p| match path.file_name_cstr() {
                Some(name) => p.matches(name.to_string_lossy().as_ref()),
                None => false,
            })
    };
    let matches_any_path = || {
        config
            .path_patterns
            .iter()
            .any(|p| p.matches_path(&path.to_path_buf()))
    };

    (config.name_patterns.is_empty() && config.path_patterns.is_empty())
        || matches_any_name()
        || matches_any_path()
}

struct PartitionedFileGroup {
    to_keep: Vec<FileMetadata>,
    to_drop: Vec<FileMetadata>,
}

impl PartitionedFileGroup {
    /// Returns the destination path where the file should be moved when the
    /// dedupe mode was selected to move
    fn move_target(target_dir: &Arc<Path>, source_path: &Path) -> Path {
        let root = source_path
            .root()
            .to_string()
            .replace("/", "")
            .replace(":", "");
        let suffix = source_path.strip_root();
        if root.is_empty() {
            target_dir.join(suffix)
        } else {
            Arc::new(target_dir.join(Path::from(root))).join(suffix)
        }
    }

    fn are_on_same_mount(devices: &DiskDevices, file1: &Path, file2: &Path) -> bool {
        let mount1 = devices.get_mount_point(file1);
        let mount2 = devices.get_mount_point(file2);
        mount1 == mount2
    }

    /// Returns a list of commands that would remove redundant files in this group when executed.
    fn dedupe_script(mut self, strategy: &DedupeOp, devices: &DiskDevices) -> Vec<FsCommand> {
        if self.to_drop.is_empty() {
            return vec![];
        }
        assert!(
            !self.to_keep.is_empty(),
            "No files would be left after deduplicating"
        );
        let mut commands = Vec::new();
        let retained_file = Arc::new(self.to_keep.swap_remove(0));
        for dropped_file in self.to_drop {
            let devices_differ = retained_file.device_id() != dropped_file.device_id();
            match strategy {
                DedupeOp::SoftLink => commands.push(FsCommand::SoftLink {
                    target: retained_file.clone(),
                    link: dropped_file,
                }),
                // hard links are not supported between files on different file systems
                DedupeOp::HardLink if devices_differ => commands.push(FsCommand::SoftLink {
                    target: retained_file.clone(),
                    link: dropped_file,
                }),
                DedupeOp::HardLink => commands.push(FsCommand::HardLink {
                    target: retained_file.clone(),
                    link: dropped_file,
                }),
                DedupeOp::Remove => commands.push(FsCommand::Remove { file: dropped_file }),
                DedupeOp::Move(target_dir) => {
                    let source = dropped_file;
                    let source_path = &source.path;
                    let use_rename = Self::are_on_same_mount(devices, source_path, target_dir);
                    let target = Self::move_target(target_dir, source_path);
                    commands.push(FsCommand::Move {
                        source,
                        target,
                        use_rename,
                    })
                }
            }
        }
        commands
    }
}

/// Partitions a group of files into files to keep and files that can be safely dropped
/// (or linked).
fn partition(
    group: FileGroup<Path>,
    config: &DedupeConfig,
    log: &Log,
) -> Result<PartitionedFileGroup, Error> {
    let file_len = group.file_len;
    let file_hash = group.file_hash;
    let error = |msg: &str| {
        Err(Error::from(format!(
            "Could not determine files to drop in group with hash {} and len {}: {}",
            file_hash, file_len.0, msg
        )))
    };

    // Fetch metadata of the files
    let mut metadata_err = false;
    let mut files: Vec<_> = group
        .files
        .into_iter()
        .map(|p| {
            FileMetadata::new(p)
                .map_err(|e| {
                    log.warn(e);
                    metadata_err = true;
                })
                .ok()
        })
        .flatten()
        .collect();

    // On metadata errors, we're just ignoring the group.
    // We must not attempt removing any files from the group
    // because we can't reliably determine which files to remove.
    // What if the files we couldn't access should be removed and the ones we access
    // should remain untouched?
    if metadata_err {
        return error("Metadata of some files could not be obtained");
    }

    // We don't want to remove dirs or symlinks
    files.retain(|m| {
        let is_file = m.metadata.is_file();
        if !is_file {
            log.warn(format!("Skipping file {}: Not a regular file", m.path));
        }
        is_file
    });

    // If file has a different length, then we really know it has been modified.
    // Therefore, it does not belong to the group and we can safely skip it.
    files.retain(|m| {
        let len_ok = FileLen(m.metadata.len()) == file_len;
        if !len_ok {
            log.warn(format!(
                "Skipping file {} with length {} different than the group length {}",
                m.path,
                m.metadata.len(),
                file_len.0,
            ));
        }
        len_ok
    });

    // Bail out as well if any file has been modified after `config.modified_before`.
    // We need to skip the whole group, because we don't know if these files are really different.
    if let Some(max_timestamp) = config.modified_before {
        if was_modified(&files, max_timestamp, log) {
            return error("Some files could be updated since the previous run of fclones");
        }
    }

    // Sort files to remove in user selected order.
    // The priorities at the beginning of the argument list have precedence over
    // the priorities given at the end of the argument list, therefore we're applying
    // them in reversed order.
    let mut sort_errors = Vec::new();
    for priority in config.priority.iter().rev() {
        sort_errors.extend(sort_by_priority(&mut files, priority));
    }

    if !sort_errors.is_empty() {
        for e in sort_errors {
            log.warn(e);
        }
        return error("Metadata of some files could not be read.");
    }

    // Split the set of files into two sets - a set that we want to keep intact and a set
    // that we can remove or replace with links:
    let (mut to_retain, mut to_drop): (Vec<_>, Vec<_>) = files
        .into_iter()
        .partition(|m| should_keep(&m.path, config) || !may_drop(&m.path, config));

    // If the set to retain is smaller than the number of files we must keep (rf), then
    // move some higher priority files from `to_drop` and append them to `to_retain`.
    let n = max(1, config.rf_over.unwrap_or(1));
    let missing_count = min(to_drop.len(), n.saturating_sub(to_retain.len()));
    to_retain.extend(to_drop.drain(0..missing_count));

    assert!(to_retain.len() >= n || to_drop.is_empty());
    Ok(PartitionedFileGroup {
        to_keep: to_retain,
        to_drop,
    })
}

/// Generates a list of commands that will remove the redundant files in the groups provided
/// by the `groups` iterator.
///
/// Calling this is perfectly safe - the function does not perform any disk changes.
///
/// This function performs extensive checks if files can be removed.
/// It rejects a group of files if:
/// - metadata of any files in the group cannot be read,
/// - any file in the group was modified after the `modified_before` configuration property
///
/// Additionally it will never emit commands to remove a file which:
/// - has length that does not match the file length recorded in the group metadata
/// - was matched by any of the `retain_path` or `retain_name` patterns
/// - was not matched by all `drop_path` and `drop_name` patterns
///
/// # Parameters
/// - `groups`: iterator over groups of identical files
/// - `op`: what to do with duplicates
/// - `config`: controls which files from each group to remove / link
/// - `log`: logging target
pub fn dedupe<'a, I>(
    groups: I,
    op: DedupeOp,
    config: &'a DedupeConfig,
    log: &'a Log,
) -> impl ParallelIterator<Item = FsCommand> + 'a
where
    I: IntoParallelIterator<Item = FileGroup<Path>> + 'a,
{
    let devices = DiskDevices::new(&HashMap::new());
    groups
        .into_par_iter()
        .flat_map(move |group| match partition(group, config, log) {
            Ok(group) => group.dedupe_script(&op, &devices),
            Err(e) => {
                log.warn(e);
                Vec::new()
            }
        })
}

/// Runs a deduplication script generated by [`dedupe`].
///
/// Calling this function is going to change the contents of the file-system.
/// No safety checks are performed.
/// Commands are executed in parallel, on the default Rayon thread-pool.
/// On command execution failure, a warning is logged and the execution of remaining commands
/// continues.
/// Returns the number of files processed and the amount of disk space reclaimed.
pub fn run_script(script: impl IntoParallelIterator<Item = FsCommand>, log: &Log) -> DedupeResult {
    script
        .into_par_iter()
        .map(|cmd| cmd.execute(log))
        .inspect(|res| {
            if let Err(e) = res {
                log.warn(e);
            }
        })
        .filter_map(|res| res.ok())
        .map(|len| DedupeResult {
            processed_count: 1,
            reclaimed_space: len,
        })
        .reduce(DedupeResult::default, |a, b| a + b)
}

/// Prints a script generated by [`dedupe`] to stdout.
///
/// Does not perform any filesystem changes.
/// Returns the number of files processed and the amount of disk space that would be
/// reclaimed if all commands of the script were executed with no error.
pub fn log_script(
    script: impl IntoParallelIterator<Item = FsCommand>,
    out: impl Write + Send,
) -> io::Result<DedupeResult> {
    let writer = Mutex::new(BufWriter::new(out));
    let err = AtomicCell::new(None);
    let result = script
        .into_par_iter()
        .map(|cmd| {
            let mut w = writer.lock().unwrap();
            for line in cmd.to_shell_str() {
                if let Err(e) = writeln!(w, "{}", line) {
                    err.store(Some(e));
                    return None;
                }
            }
            Some(DedupeResult {
                processed_count: 1,
                reclaimed_space: cmd.space_to_reclaim(),
            })
        })
        .while_some()
        .reduce(DedupeResult::default, |a, b| a + b);

    match err.take() {
        None => Ok(result),
        Some(e) => Err(e),
    }
}

#[cfg(test)]
mod test {
    use std::collections::HashSet;
    use std::path::PathBuf;
    use std::{thread, time};

    use chrono::Duration;

    use crate::files::FileHash;
    use crate::pattern::Pattern;
    use crate::util::test::{create_file, create_file_newer_than, read_file, with_dir, write_file};

    use super::*;

    #[test]
    fn test_temp_file_name_generation() {
        let path = Path::from("/foo/bar");
        let temp = FsCommand::temp_file(&path);
        assert_ne!(path, temp);
        assert_ne!(
            path.file_name().unwrap().len(),
            temp.file_name().unwrap().len()
        );
        assert_eq!(path.parent(), temp.parent());
    }

    #[test]
    fn test_remove_command_removes_file() {
        with_dir("dedupe/remove_cmd", |root| {
            let log = Log::new();
            let file_path = root.join("file");
            create_file(&file_path);
            let file = FileMetadata::new(Path::from(&file_path)).unwrap();
            let cmd = FsCommand::Remove { file };
            cmd.execute(&log).unwrap();
            assert!(!file_path.exists())
        })
    }

    #[test]
    fn test_move_command_moves_file_by_rename() {
        with_dir("dedupe/move_rename_cmd", |root| {
            let log = Log::new();
            let file_path = root.join("file");
            let target = Path::from(root.join("target"));
            create_file(&file_path);
            let file = FileMetadata::new(Path::from(&file_path)).unwrap();
            let cmd = FsCommand::Move {
                source: file,
                target: target.clone(),
                use_rename: true,
            };
            cmd.execute(&log).unwrap();
            assert!(!file_path.exists());
            assert!(target.to_path_buf().exists());
        })
    }

    #[test]
    fn test_move_command_moves_file_by_copy() {
        with_dir("dedupe/move_copy_cmd", |root| {
            let log = Log::new();
            let file_path = root.join("file");
            let target = Path::from(root.join("target"));
            create_file(&file_path);
            let file = FileMetadata::new(Path::from(&file_path)).unwrap();
            let cmd = FsCommand::Move {
                source: file,
                target: target.clone(),
                use_rename: false,
            };
            cmd.execute(&log).unwrap();
            assert!(!file_path.exists());
            assert!(target.to_path_buf().exists());
        })
    }

    #[test]
    fn test_move_fails_if_target_exists() {
        with_dir("dedupe/move_target_exists", |root| {
            let log = Log::new();
            let file_path = root.join("file");
            let target = root.join("target");
            create_file(&file_path);
            create_file(&target);
            let file = FileMetadata::new(Path::from(&file_path)).unwrap();
            let cmd = FsCommand::Move {
                source: file,
                target: Path::from(&target),
                use_rename: false,
            };
            assert!(cmd.execute(&log).is_err());
        })
    }

    #[test]
    fn test_soft_link_command_replaces_file_with_a_link() {
        with_dir("dedupe/soft_link_cmd", |root| {
            let log = Log::new();
            let file_path_1 = root.join("file_1");
            let file_path_2 = root.join("file_2");
            write_file(&file_path_1, "foo");
            write_file(&file_path_2, "");

            let file_1 = FileMetadata::new(Path::from(&file_path_1)).unwrap();
            let file_2 = FileMetadata::new(Path::from(&file_path_2)).unwrap();
            let cmd = FsCommand::SoftLink {
                target: Arc::new(file_1),
                link: file_2,
            };
            cmd.execute(&log).unwrap();

            assert!(file_path_1.exists());
            assert!(file_path_2.exists());
            assert!(fs::symlink_metadata(&file_path_2)
                .unwrap()
                .file_type()
                .is_symlink());
            assert_eq!(read_file(&file_path_2), "foo");
        })
    }

    #[test]
    fn test_hard_link_command_replaces_file_with_a_link() {
        with_dir("dedupe/hard_link_cmd", |root| {
            let log = Log::new();
            let file_path_1 = root.join("file_1");
            let file_path_2 = root.join("file_2");

            write_file(&file_path_1, "foo");
            write_file(&file_path_2, "");

            let file_1 = FileMetadata::new(Path::from(&file_path_1)).unwrap();
            let file_2 = FileMetadata::new(Path::from(&file_path_2)).unwrap();
            let cmd = FsCommand::HardLink {
                target: Arc::new(file_1),
                link: file_2,
            };
            cmd.execute(&log).unwrap();

            assert!(file_path_1.exists());
            assert!(file_path_2.exists());
            assert_eq!(read_file(&file_path_2), "foo");
        })
    }

    /// Creates 3 empty files with different creation time and returns a FileGroup describing them
    fn make_group(root: &PathBuf) -> FileGroup<Path> {
        let file_1 = root.join("file_1");
        let file_2 = root.join("file_2");
        let file_3 = root.join("file_3");
        create_file(&file_1);
        let ctime_1 = fs::metadata(&file_1).unwrap().modified().unwrap();
        let ctime_2 = create_file_newer_than(&file_2, ctime_1);
        create_file_newer_than(&file_3, ctime_2);

        let group = FileGroup {
            file_len: FileLen(0),
            file_hash: FileHash(0),
            files: vec![
                Path::from(&file_1),
                Path::from(&file_2),
                Path::from(&file_3),
            ],
        };
        group
    }

    #[test]
    fn test_partition_selects_files_for_removal() {
        with_dir("dedupe/partition/basic", |root| {
            let group = make_group(root);
            let config = DedupeConfig::default();
            let partitioned = partition(group, &config, &Log::new()).unwrap();
            assert_eq!(partitioned.to_keep.len(), 1);
            assert_eq!(partitioned.to_drop.len(), 2);
        })
    }

    #[test]
    fn test_partition_bails_out_if_file_modified_too_late() {
        with_dir("dedupe/partition/modification", |root| {
            let group = make_group(root);
            let mut config = DedupeConfig::default();
            config.modified_before = Some(DateTime::from(Local::now() - Duration::days(1)));
            let partitioned = partition(group, &config, &Log::new());
            assert!(partitioned.is_err());
        })
    }

    #[test]
    fn test_partition_skips_file_with_different_len() {
        with_dir("dedupe/partition/file_len", |root| {
            let group = make_group(root);
            let path = group.files[0].clone();
            write_file(&path.to_path_buf(), "foo");

            let mut config = DedupeConfig::default();
            config.priority = vec![Priority::MostRecentlyModified];
            let partitioned = partition(group, &config, &Log::new()).unwrap();
            assert!(partitioned
                .to_drop
                .iter()
                .find(|m| m.path == path)
                .is_none());
            assert!(partitioned
                .to_keep
                .iter()
                .find(|m| m.path == path)
                .is_none());
        })
    }

    fn path_set(v: &Vec<FileMetadata>) -> HashSet<&Path> {
        v.iter().map(|f| &f.path).collect()
    }

    #[test]
    fn test_partition_respects_creation_time_priority() {
        with_dir("dedupe/partition/ctime_priority", |root| {
            if fs::metadata(root).unwrap().created().is_err() {
                // can't run the test because the filesystem doesn't support fetching
                // file creation time
                return;
            }
            let group = make_group(root);
            let mut config = DedupeConfig::default();
            config.priority = vec![Priority::Newest];
            let partitioned_1 = partition(group.clone(), &config, &Log::new()).unwrap();
            config.priority = vec![Priority::Oldest];
            let partitioned_2 = partition(group.clone(), &config, &Log::new()).unwrap();

            assert_ne!(
                path_set(&partitioned_1.to_keep),
                path_set(&partitioned_2.to_keep)
            );
            assert_ne!(
                path_set(&partitioned_1.to_drop),
                path_set(&partitioned_2.to_drop)
            );
        });
    }

    #[test]
    fn test_partition_respects_modification_time_priority() {
        with_dir("dedupe/partition/mtime_priority", |root| {
            let group = make_group(root);

            thread::sleep(time::Duration::from_millis(10));
            let path = group.files[0].clone();
            write_file(&path.to_path_buf(), "foo");

            let mut config = DedupeConfig::default();
            config.priority = vec![Priority::MostRecentlyModified];
            let partitioned_1 = partition(group.clone(), &config, &Log::new()).unwrap();
            config.priority = vec![Priority::LeastRecentlyModified];
            let partitioned_2 = partition(group.clone(), &config, &Log::new()).unwrap();

            assert_ne!(
                path_set(&partitioned_1.to_keep),
                path_set(&partitioned_2.to_keep)
            );
            assert_ne!(
                path_set(&partitioned_1.to_drop),
                path_set(&partitioned_2.to_drop)
            );
        });
    }

    #[test]
    fn test_partition_respects_keep_patterns() {
        with_dir("dedupe/partition/keep", |root| {
            let group = make_group(root);
            let mut config = DedupeConfig::default();
            config.priority = vec![Priority::LeastRecentlyModified];
            config.keep_name_patterns = vec![Pattern::glob("*_1").unwrap()];
            let p = partition(group.clone(), &config, &Log::new()).unwrap();
            assert_eq!(p.to_keep.len(), 1);
            assert_eq!(&p.to_keep[0].path, &group.files[0]);

            config.keep_name_patterns = vec![];
            config.keep_path_patterns = vec![Pattern::glob("**/file_1").unwrap()];
            let p = partition(group.clone(), &config, &Log::new()).unwrap();
            assert_eq!(p.to_keep.len(), 1);
            assert_eq!(&p.to_keep[0].path, &group.files[0]);
        })
    }

    #[test]
    fn test_partition_respects_drop_patterns() {
        with_dir("dedupe/partition/drop", |root| {
            let group = make_group(root);
            let mut config = DedupeConfig::default();
            config.priority = vec![Priority::LeastRecentlyModified];
            config.name_patterns = vec![Pattern::glob("*_3").unwrap()];
            let p = partition(group.clone(), &config, &Log::new()).unwrap();
            assert_eq!(p.to_drop.len(), 1);
            assert_eq!(&p.to_drop[0].path, &group.files[2]);

            config.name_patterns = vec![];
            config.path_patterns = vec![Pattern::glob("**/file_3").unwrap()];
            let p = partition(group.clone(), &config, &Log::new()).unwrap();
            assert_eq!(p.to_drop.len(), 1);
            assert_eq!(&p.to_drop[0].path, &group.files[2]);
        })
    }

    #[test]
    fn test_run_dedupe_script() {
        with_dir("dedupe/partition/dedupe_script", |root| {
            let group = make_group(root);
            let mut config = DedupeConfig::default();
            config.priority = vec![Priority::LeastRecentlyModified];
            let log = Log::new();
            let script = dedupe(vec![group], DedupeOp::Remove, &config, &log);
            let dedupe_result = run_script(script, &log);
            assert_eq!(dedupe_result.processed_count, 2);
            assert!(!root.join("file_1").exists());
            assert!(!root.join("file_2").exists());
            assert!(root.join("file_3").exists());
        });
    }
}
