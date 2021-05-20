use crate::config::{DedupeConfig, Priority};
use crate::files::{FileHash, FileLen};
use crate::lock::FileLock;
use crate::log::Log;
use crate::path::Path;
use crate::util::fallible_sort_by_key;
use crate::{Error, FileGroup};

use chrono::{DateTime, FixedOffset, Local};
use fallible_iterator::FallibleIterator;

use rayon::prelude::IntoParallelRefIterator;
use std::cmp::{min, Reverse};
use std::fmt::Display;
use std::fs::Metadata;
use std::ops::AddAssign;
use std::{fs, io};

/// Defines what to do with redundant files
#[derive(Copy, Clone)]
pub enum DedupeOp {
    /// Remove redundant files
    Remove,
    /// Replace redundant files with soft-links (ln -s on Unix)
    SoftLink,
    /// Replace redundant files with hard-links (ln on Unix)
    HardLink,
}

/// Portable abstraction for commands used to remove duplicates
pub enum FsCommand {
    Remove {
        path: Path,
        len: FileLen,
    },
    SoftLink {
        target: Path,
        link: Path,
        len: FileLen,
    },
    HardLink {
        target: Path,
        link: Path,
        len: FileLen,
    },
}

impl FsCommand {
    /// Executes the command and returns the number of bytes reclaimed
    pub fn execute(&self) -> io::Result<FileLen> {
        match self {
            FsCommand::Remove { path, len } => {
                fs::remove_file(path.to_path_buf())?;
                Ok(*len)
            }
            FsCommand::SoftLink { target, link, len } => {
                let link = link.to_path_buf();
                fs::remove_file(&link)?;
                #[cfg(unix)]
                std::os::unix::fs::symlink(&target.to_path_buf(), &link);
                #[cfg(windows)]
                std::os::windows::fs::symlink_file(&target.to_path_buf(), &link);
                Ok(*len)
            }
            FsCommand::HardLink { target, link, len } => {
                let link = link.to_path_buf();
                fs::remove_file(&link)?;
                fs::hard_link(&target.to_path_buf(), &link)?;
                Ok(*len)
            }
        }
    }

    /// Formats the command as a string that can be pasted to a Unix shell (e.g. bash)
    #[cfg(unix)]
    pub fn to_shell_str(&self) -> String {
        match self {
            FsCommand::Remove { path, .. } => {
                let path = path.shell_quote();
                format!("rm {}", path)
            }
            FsCommand::SoftLink { target, link, .. } => {
                let target = target.shell_quote();
                let link = link.shell_quote();
                format!("ln -s {} {}", target, link)
            }
            FsCommand::HardLink { target, link, .. } => {
                let target = target.shell_quote();
                let link = link.shell_quote();
                format!("ln {} {}", target, link)
            }
        }
    }

    #[cfg(windows)]
    pub fn to_shell_str(&self) -> String {
        match self {
            FsCommand::Remove { path, .. } => {
                let path = path.shell_quote();
                format!("del {}", path)
            }
            FsCommand::SoftLink { target, link, .. } => {
                let target = target.shell_quote();
                let link = link.shell_quote();
                format!("mklink {} {}", target, link)
            }
            FsCommand::HardLink { target, link, .. } => {
                let target = target.shell_quote();
                let link = link.shell_quote();
                format!("mklink /H {} {}", target, link)
            }
        }
    }
}

/// Provides information about the number of deduplicated files and reclaimed disk space
#[derive(Default)]
pub struct DedupeResult {
    pub processed_count: u64,
    pub reclaimed_space: FileLen,
}

impl AddAssign for DedupeResult {
    fn add_assign(&mut self, rhs: Self) {
        self.processed_count += rhs.processed_count;
        self.reclaimed_space += rhs.reclaimed_space;
    }
}

/// The purpose of this structure is to keep the file locked as long as we wish
/// to rely on its metadata. File is locked before reading the metadata.
struct FileMetadata {
    path: Path,
    metadata: Metadata,
    lock: FileLock,
}

impl FileMetadata {
    fn new(path: Path) -> io::Result<FileMetadata> {
        let lock = FileLock::new(&path).map_err(|e| {
            io::Error::new(e.kind(), format!("Failed to lock file {}: {}", path, e))
        })?;
        let metadata = path.to_path_buf().metadata().map_err(|e| {
            io::Error::new(
                e.kind(),
                format!("Failed to obtain metadata of file {}: {}", path, e),
            )
        })?;
        Ok(FileMetadata {
            path,
            metadata,
            lock,
        })
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
                        p, after, file_timestamp
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

/// Returns true if given path matches any of the `retain` patterns
fn should_retain(path: &Path, config: &DedupeConfig) -> bool {
    let matches_any_name = config
        .retain_name_patterns
        .iter()
        .any(|p| match path.file_name() {
            Some(name) => p.matches(name.to_string_lossy().as_ref()),
            None => false,
        });
    let matches_any_path = || {
        config
            .retain_path_patterns
            .iter()
            .any(|p| p.matches_path(&path.to_path_buf()))
    };

    matches_any_name || matches_any_path()
}

/// Returns true if given path matches all of the `drop` patterns.
/// If there are no `drop` patterns, returns true.
fn may_drop(path: &Path, config: &DedupeConfig) -> bool {
    let matches_all_names = config
        .retain_name_patterns
        .iter()
        .all(|p| match path.file_name() {
            Some(name) => p.matches(name.to_string_lossy().as_ref()),
            None => false,
        });

    let matches_all_paths = || {
        config
            .retain_path_patterns
            .iter()
            .all(|p| p.matches_path(&path.to_path_buf()))
    };

    matches_all_names || matches_all_paths()
}

struct PartitionedFileGroup {
    file_hash: FileHash,
    file_len: FileLen,
    to_retain: Vec<FileMetadata>,
    to_drop: Vec<FileMetadata>,
}

impl PartitionedFileGroup {
    /// Returns how much space would be reclaimed if this group of files was deduplicated.
    fn space_to_reclaim(&self) -> FileLen {
        self.file_len * self.to_drop.len() as u64
    }

    /// Returns a list of commands that would remove redundant files in this group when executed.
    fn dedupe_script(&self, strategy: DedupeOp) -> Vec<FsCommand> {
        assert!(
            self.to_retain.len() > 0 || self.to_drop.is_empty(),
            "No files would be left after deduplicating"
        );
        let mut commands = Vec::new();
        for f in self.to_drop.iter() {
            let retained_path = &self.to_retain[0].path;
            let dropped_path = &f.path;
            commands.push(FsCommand::Remove {
                path: dropped_path.clone(),
                len: self.file_len,
            });
            match strategy {
                DedupeOp::Remove => {}
                DedupeOp::SoftLink => commands.push(FsCommand::SoftLink {
                    target: retained_path.clone(),
                    link: dropped_path.clone(),
                    len: self.file_len,
                }),
                DedupeOp::HardLink => commands.push(FsCommand::HardLink {
                    target: retained_path.clone(),
                    link: dropped_path.clone(),
                    len: self.file_len,
                }),
            }
        }
        commands
    }
}

/// Partitions a group of files into files to retain and files that can be safely dropped (or linked).
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
    for priority in config.drop_priority.iter().rev() {
        sort_errors.extend(sort_by_priority(&mut files, priority));
    }
    files.reverse(); // after this, files with the highest priority to drop are at the beginning
    for priority in config.retain_priority.iter().rev() {
        sort_errors.extend(sort_by_priority(&mut files, priority));
    }
    files.reverse(); // after this, files with the highers priority to are at the end

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
        .partition(|m| should_retain(&m.path, config) || !may_drop(&m.path, config));

    // If the set to retain is smaller than the number of files we must keep (rf), then
    // move some higher priority files from `to_drop` and append them to `to_retain`.
    let n = config.rf_over.unwrap_or(1);
    let missing_count = min(to_drop.len(), n.saturating_sub(to_retain.len()));
    to_retain.extend(to_drop.drain(0..missing_count));

    assert!(to_retain.len() >= n || to_drop.is_empty());
    Ok(PartitionedFileGroup {
        file_hash,
        file_len,
        to_retain,
        to_drop,
    })
}

// pub struct DedupeScript<'a, I>
// where
//     I: Iterator<Item = FileGroup<Path>>,
// {
//     groups: I,
//     op: DedupeOp,
//     config: &'a DedupeConfig,
//     log: &'a Log,
//     buffer: Vec<FsCommand>
// }
//
// impl<'a, I> DedupeScript<'a, I>
// where
//     I: Iterator<Item = FileGroup<Path>>,
// {
//     fn new<I2>(
//         groups: I,
//         op: DedupeOp,
//         config: &'a DedupeConfig,
//         log: &'a Log,
//     ) -> DedupeScript<'a, I>
//     where
//         I2: IntoIterator<Item = FileGroup<Path>>,
//     {
//         DedupeScript {
//             groups: groups.into_iter(),
//             op,
//             config,
//             log,
//             buffer: Vec::new()
//         }
//     }
// }
//
// impl<'a, I> Iterator for DedupeScript<'a, I>
// where
//     I: Iterator<Item = FileGroup<Path>>,
// {
//     type Item = FsCommand;
//
//     fn next(&mut self) -> Option<Self::Item> {
//
//
//         match self.groups.next() {
//             Some(group) => {},
//             None => None
//         }
//     }
// }

/// Generates a list of commands that will remove duplicates.
/// # Parameters
/// - `groups`: iterator over group of same files
/// - `op`: what to do with duplicates
/// - `config`: controls which files from each group to remove / link
/// - `log`: logging target
pub fn dedupe_script<'a, I>(
    groups: I,
    op: DedupeOp,
    config: &'a DedupeConfig,
    log: &'a Log,
) -> impl Iterator<Item = FsCommand> + 'a
where
    I: IntoIterator<Item = FileGroup<Path>> + 'a,
{
    groups
        .into_iter()
        .flat_map(move |group| match partition(group, &config, log) {
            Ok(group) => group.dedupe_script(op),
            Err(e) => {
                log.warn(e);
                Vec::new()
            }
        })
}
