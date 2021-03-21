use std::cell::RefCell;
use std::cmp::Reverse;
use std::collections::HashMap;
use std::env::current_dir;
use std::ffi::{OsStr, OsString};
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::PathBuf;
use std::process::exit;
use std::sync::Arc;

use console::{style, Term};
use indoc::indoc;
use itertools::Itertools;
use rayon::prelude::*;
use regex::Regex;
use structopt::StructOpt;
use thread_local::ThreadLocal;

use fclones::config::*;
use fclones::device::{DiskDevice, DiskDevices, Parallelism};
use fclones::files::*;
use fclones::group::*;
use fclones::log::Log;
use fclones::path::Path;
use fclones::pattern::ESCAPE_CHAR;
use fclones::progress::FastProgressBar;
use fclones::report::Reporter;
use fclones::transform::Transform;
use fclones::walk::Walk;

struct AppCtx {
    config: Config,
    log: Log,
    devices: DiskDevices,
}

/// Configures global thread pool to use desired number of threads
fn configure_main_thread_pool(pool_sizes: &mut HashMap<OsString, Parallelism>) {
    let parallelism = pool_sizes.remove(OsStr::new("main")).unwrap_or_else(|| {
        *pool_sizes
            .get(OsStr::new("default"))
            .unwrap_or(&Parallelism {
                sequential: 0,
                random: 0,
            })
    });

    rayon::ThreadPoolBuilder::new()
        .num_threads(parallelism.random)
        .build_global()
        .unwrap();
}

/// Walks the directory tree and collects matching files in parallel into a vector
fn scan_files(ctx: &mut AppCtx) -> Vec<Vec<FileInfo>> {
    let base_dir = Path::from(current_dir().unwrap_or_default());
    let path_selector = ctx.config.path_selector(&ctx.log, &base_dir);
    let file_collector = ThreadLocal::new();
    let spinner = ctx.log.spinner("Scanning files");
    let spinner_tick = &|_: &Path| spinner.tick();

    let config = &ctx.config;
    let min_size = config.min_size;
    let max_size = config.max_size.unwrap_or(FileLen::MAX);

    let mut walk = Walk::new();
    walk.recursive = config.recursive;
    walk.depth = config.depth.unwrap_or(usize::MAX);
    walk.skip_hidden = config.skip_hidden;
    walk.follow_links = config.follow_links;
    walk.path_selector = path_selector;
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

fn group_by_size(ctx: &mut AppCtx, files: Vec<Vec<FileInfo>>) -> Vec<FileGroup<FileInfo>> {
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
fn deduplicate<F>(ctx: &AppCtx, files: &mut Vec<FileInfo>, progress: F)
where
    F: Fn(&Path) + Sync + Send,
{
    let count = files.len();
    let mut groups = GroupMap::with_capacity(count, |fi: FileInfo| (fi.location, fi));
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
    ctx: &mut AppCtx,
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

/// Transforms files by piping them to an external program and groups them by their hashes
fn group_transformed(
    ctx: &mut AppCtx,
    transform: &Transform,
    groups: Vec<FileGroup<FileInfo>>,
) -> Vec<FileGroup<FileInfo>> {
    let file_count: usize = groups.total_count();
    let progress = ctx
        .log
        .progress_bar("Transforming & grouping by hash", file_count as u64);

    let rf_over = ctx.config.rf_over();
    let rf_under = ctx.config.rf_under();

    let groups = rehash(
        groups,
        rf_over + 1,
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
    ctx: &mut AppCtx,
    prefix_len: FileLen,
    groups: Vec<FileGroup<FileInfo>>,
) -> Vec<FileGroup<FileInfo>> {
    let remaining_files = groups.iter().filter(|g| g.files.len() > 1).total_count();
    let progress = ctx
        .log
        .progress_bar("Computing prefix hashes", remaining_files as u64);

    let rf_over = ctx.config.rf_over();
    let rf_under = ctx.config.rf_under();
    let groups = rehash(
        groups,
        rf_over + 1,
        &ctx.devices,
        AccessType::Random,
        |(fi, _)| {
            progress.tick();
            let buf_len = ctx.devices.get_by_path(&fi.path).buf_len();
            let caching = if fi.len <= prefix_len {
                Caching::Default
            } else {
                Caching::Random
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

fn group_by_suffix(ctx: &mut AppCtx, groups: Vec<FileGroup<FileInfo>>) -> Vec<FileGroup<FileInfo>> {
    let suffix_len = suffix_len(&ctx.devices, flat_iter(&groups));
    let suffix_threshold = suffix_threshold(&ctx.devices, flat_iter(&groups));
    let remaining_files = groups
        .iter()
        .filter(|&g| g.file_len >= suffix_threshold)
        .total_count();
    let progress = ctx
        .log
        .progress_bar("Grouping by suffix", remaining_files as u64);

    let rf_over = ctx.config.rf_over();
    let rf_under = ctx.config.rf_under();

    let groups = rehash(
        groups,
        rf_over + 1,
        &ctx.devices,
        AccessType::Random,
        |(fi, old_hash)| {
            if fi.len >= suffix_threshold {
                progress.tick();
                let buf_len = ctx.devices.get_by_path(&fi.path).buf_len();
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
            } else {
                Some(old_hash)
            }
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
    ctx: &mut AppCtx,
    min_file_len: FileLen,
    groups: Vec<FileGroup<FileInfo>>,
) -> Vec<FileGroup<FileInfo>> {
    let bytes_to_scan = groups
        .iter()
        .filter(|&g| g.file_len >= min_file_len)
        .total_size();
    let progress = &ctx
        .log
        .bytes_progress_bar("Grouping by contents", bytes_to_scan.0);

    let rf_over = ctx.config.rf_over();
    let rf_under = ctx.config.rf_under();

    let groups = rehash(
        groups,
        rf_over + 1,
        &ctx.devices,
        AccessType::Sequential,
        |(fi, old_hash)| {
            if fi.len >= min_file_len {
                let buf_len = ctx.devices.get_by_path(&fi.path).buf_len();
                file_hash_or_log_err(
                    &fi.path,
                    FilePos(0),
                    fi.len,
                    buf_len,
                    Caching::Sequential,
                    |delta| progress.inc(delta),
                    &ctx.log,
                )
            } else {
                Some(old_hash)
            }
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

fn write_report(ctx: &mut AppCtx, groups: &[FileGroup<Path>]) {
    let remaining_files = groups.total_count();
    let progress = ctx
        .log
        .progress_bar("Writing report", remaining_files as u64);

    // No progress bar when we write to terminal
    let mut reporter = match &ctx.config.output {
        Some(path) => file_reporter(ctx, path, progress),
        None => stdout_reporter(progress),
    };

    let result = match &ctx.config.format {
        OutputFormat::Text => reporter.write_as_text(groups),
        OutputFormat::Fdupes => reporter.write_as_fdupes(groups),
        OutputFormat::Csv => reporter.write_as_csv(groups),
        OutputFormat::Json => reporter.write_as_json(groups),
    };
    match result {
        Ok(()) => (),
        Err(e) => ctx.log.err(format!("Failed to write report: {}", e)),
    }
}

/// Creates a reporter that writes to the standard output
fn stdout_reporter(progress: Arc<FastProgressBar>) -> Reporter<Box<dyn Write>> {
    let stdout = Term::stdout();
    let is_term = stdout.is_term();
    if is_term {
        progress.finish_and_clear()
    }
    let out: Box<dyn Write> = Box::new(BufWriter::new(stdout));
    Reporter::new(out, is_term, progress)
}

/// Creates a reporter that writes to the given file.
/// Writes and error message and exists the application if the file cannot be open.
fn file_reporter(
    ctx: &AppCtx,
    path: &PathBuf,
    progress: Arc<FastProgressBar>,
) -> Reporter<Box<dyn Write>> {
    match File::create(path) {
        Ok(file) => {
            let out: Box<dyn Write> = Box::new(BufWriter::new(file));
            Reporter::new(out, false, progress)
        }
        Err(e) => {
            ctx.log
                .err(format!("Could not create {}: {}", path.display(), e));
            exit(1)
        }
    }
}

fn paint_help(s: &str) -> String {
    let escape_regex = "{escape}";
    let code_regex = Regex::new(r"`([^`]+)`").unwrap();
    let title_regex = Regex::new(r"(?m)^# *(.*?)$").unwrap();
    let s = s.replace(escape_regex, ESCAPE_CHAR);
    let s = code_regex.replace_all(s.as_ref(), style("$1").green().to_string().as_str());
    title_regex
        .replace_all(s.as_ref(), style("$1").yellow().to_string().as_str())
        .to_string()
}

fn main() {
    let mut log = Log::new();

    let after_help = &paint_help(indoc!(
        "
    # PATTERN SYNTAX:
        Options `-n` `-p` and `-e` accept extended glob patterns.
        The following wildcards can be used:
        `?`         matches any character except the directory separator
        `[a-z]`     matches one of the characters or character ranges given in the square brackets
        `[!a-z]`    matches any character that is not given in the square brackets
        `*`         matches any sequence of characters except the directory separator
        `**`        matches any sequence of characters
        `{a,b}`     matches exactly one pattern from the comma-separated patterns given
                  inside the curly brackets
        `@(a|b)`    same as `{a,b}`
        `?(a|b)`    matches at most one occurrence of the pattern inside the brackets
        `+(a|b)`    matches at least occurrence of the patterns given inside the brackets
        `*(a|b)`    matches any number of occurrences of the patterns given inside the brackets
        `!(a|b)`    matches anything that doesn't match any of the patterns given inside the brackets
        `{escape}`         escapes wildcards, e.g. `{escape}?` would match `?` literally
    "
    ));

    let clap = Config::clap().after_help(after_help.as_str());
    let matches = clap.get_matches_safe().unwrap_or_else(|e| {
        // a hack to remove "error: " from the message,
        // until we switch to Clap 3.x, which will have the `cause` field
        let r = Regex::new("[^e]*error:[^ ]* ").unwrap();
        let message = e.message.as_str();
        if r.is_match(message) {
            log.err(r.replace(e.message.as_str(), ""));
        } else {
            println!("{}", e.message);
        }
        exit(1);
    });
    let config: Config = Config::from_clap(&matches);
    if config.quiet {
        log.no_progress = true;
    }

    let mut thread_pool_sizes = config.thread_pool_sizes();
    configure_main_thread_pool(&mut thread_pool_sizes);

    let mut ctx = AppCtx {
        log,
        config,
        devices: DiskDevices::new(&mut thread_pool_sizes),
    };
    ctx.config.check_transform(&ctx.log);
    ctx.config
        .check_thread_pools(&ctx.log, &mut thread_pool_sizes);

    if let Some(output) = &ctx.config.output {
        // Try to create the output file now and fail early so that
        // the user doesn't waste time to only find that the report cannot be written at the end:
        if let Err(e) = File::create(output) {
            ctx.log.err(format!(
                "Cannot create output file {}: {}",
                output.display(),
                e
            ));
            exit(1);
        }
    }

    let results = process(&mut ctx);
    write_report(&mut ctx, &results)
}

// Extracted for testing
fn process(mut ctx: &mut AppCtx) -> Vec<FileGroup<Path>> {
    ctx.log.info("Started");
    let matching_files = scan_files(&mut ctx);
    let size_groups = group_by_size(&mut ctx, matching_files);
    let size_groups_pruned = remove_same_files(&mut ctx, size_groups);
    let groups = match ctx.config.take_transform(&ctx.log) {
        Some(transform) => group_transformed(ctx, &transform, size_groups_pruned),
        None => {
            let prefix_len = prefix_len(&ctx.devices, flat_iter(&size_groups_pruned));
            let prefix_groups = group_by_prefix(&mut ctx, prefix_len, size_groups_pruned);
            let suffix_groups = group_by_suffix(&mut ctx, prefix_groups);
            group_by_contents(&mut ctx, prefix_len, suffix_groups)
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
    groups
}

#[cfg(test)]
mod test {
    use std::fs::{hard_link, OpenOptions};
    use std::io::{Read, Write};
    use std::path::PathBuf;

    use fclones::path::Path;
    use fclones::util::test::*;

    use super::*;

    const MAX_PREFIX_LEN: usize = 256 * 1024;
    const MAX_SUFFIX_LEN: usize = 256 * 1024;

    #[test]
    fn identical_small_files() {
        with_dir("target/test/main/identical_small_files", |root| {
            let file1 = root.join("file1");
            let file2 = root.join("file2");
            write_test_file(&file1, b"aaa", b"", b"");
            write_test_file(&file2, b"aaa", b"", b"");

            let mut ctx = test_ctx();
            ctx.config.paths = vec![file1, file2];
            let results = process(&mut ctx);
            assert_eq!(results.len(), 1);
            assert_eq!(results[0].file_len, FileLen(3));
            assert_eq!(results[0].files.len(), 2);
        });
    }

    #[test]
    fn identical_large_files() {
        with_dir("target/test/main/identical_large_files", |root| {
            let file1 = root.join("file1");
            let file2 = root.join("file2");
            write_test_file(&file1, &[0; MAX_PREFIX_LEN], &[1; 4096], &[2; 4096]);
            write_test_file(&file2, &[0; MAX_PREFIX_LEN], &[1; 4096], &[2; 4096]);

            let mut ctx = test_ctx();
            ctx.config.paths = vec![file1, file2];

            let results = process(&mut ctx);
            assert_eq!(results.len(), 1);
            assert_eq!(results[0].files.len(), 2);
        });
    }

    #[test]
    fn files_differing_by_size() {
        with_dir("target/test/main/files_differing_by_size", |root| {
            let file1 = root.join("file1");
            let file2 = root.join("file2");
            write_test_file(&file1, b"aaaa", b"", b"");
            write_test_file(&file2, b"aaa", b"", b"");

            let mut ctx = test_ctx();
            ctx.config.paths = vec![file1.clone(), file2.clone()];
            ctx.config.rf_over = Some(0);

            let results = process(&mut ctx);
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
        with_dir("target/test/main/files_differing_by_prefix", |root| {
            let file1 = root.join("file1");
            let file2 = root.join("file2");
            write_test_file(&file1, b"aaa", b"", b"");
            write_test_file(&file2, b"bbb", b"", b"");

            let mut ctx = test_ctx();
            ctx.config.paths = vec![file1.clone(), file2.clone()];
            ctx.config.unique = true;

            let results = process(&mut ctx);
            assert_eq!(results.len(), 2);
            assert_eq!(results[0].files.len(), 1);
            assert_eq!(results[1].files.len(), 1);
        });
    }

    #[test]
    fn files_differing_by_suffix() {
        with_dir("target/test/main/files_differing_by_suffix", |root| {
            let file1 = root.join("file1");
            let file2 = root.join("file2");
            let prefix = [0; MAX_PREFIX_LEN];
            let mid = [1; MAX_PREFIX_LEN + MAX_SUFFIX_LEN];
            write_test_file(&file1, &prefix, &mid, b"suffix1");
            write_test_file(&file2, &prefix, &mid, b"suffix2");

            let mut ctx = test_ctx();
            ctx.config.paths = vec![file1.clone(), file2.clone()];
            ctx.config.unique = true;

            let results = process(&mut ctx);
            assert_eq!(results.len(), 2);
            assert_eq!(results[0].files.len(), 1);
            assert_eq!(results[1].files.len(), 1);
        });
    }

    #[test]
    fn files_differing_by_middle() {
        with_dir("target/test/main/files_differing_by_middle", |root| {
            let file1 = root.join("file1");
            let file2 = root.join("file2");
            let prefix = [0; MAX_PREFIX_LEN];
            let suffix = [1; MAX_SUFFIX_LEN];
            write_test_file(&file1, &prefix, b"middle1", &suffix);
            write_test_file(&file2, &prefix, b"middle2", &suffix);

            let mut ctx = test_ctx();
            ctx.config.paths = vec![file1.clone(), file2.clone()];
            ctx.config.unique = true;

            let results = process(&mut ctx);
            assert_eq!(results.len(), 2);
            assert_eq!(results[0].files.len(), 1);
            assert_eq!(results[1].files.len(), 1);
        });
    }

    #[test]
    fn hard_links() {
        with_dir("target/test/main/hard_links", |root| {
            let file1 = root.join("file1");
            let file2 = root.join("file2");
            write_test_file(&file1, b"aaa", b"", b"");
            hard_link(&file1, &file2).unwrap();

            let mut ctx = test_ctx();
            ctx.config.paths = vec![file1.clone(), file2.clone()];
            ctx.config.unique = true;

            let results = process(&mut ctx);
            assert_eq!(results.len(), 1);
            assert_eq!(results[0].files.len(), 1);
        });
    }

    #[test]
    fn duplicate_input_files() {
        with_dir("target/test/main/duplicate_input_files", |root| {
            let file1 = root.join("file1");
            write_test_file(&file1, b"foo", b"", b"");
            let mut ctx = test_ctx();
            ctx.config.paths = vec![file1.clone(), file1.clone(), file1.clone()];
            ctx.config.unique = true;
            ctx.config.hard_links = true;

            let results = process(&mut ctx);
            assert_eq!(results.len(), 1);
            assert_eq!(results[0].files.len(), 1);
        });
    }

    #[test]
    #[cfg(unix)]
    fn duplicate_input_files_non_canonical() {
        use std::os::unix::fs::symlink;

        with_dir(
            "target/test/main/duplicate_input_files_non_canonical",
            |root| {
                let dir = root.join("dir");
                symlink(&root, &dir).unwrap();

                let file1 = root.join("file1");
                let file2 = root.join("dir/file1");
                write_test_file(&file1, b"foo", b"", b"");

                let mut ctx = test_ctx();
                ctx.config.paths = vec![file1.clone(), file2.clone()];
                ctx.config.unique = true;
                ctx.config.hard_links = true;

                let results = process(&mut ctx);
                assert_eq!(results.len(), 1);
                assert_eq!(results[0].files.len(), 1);
            },
        );
    }

    #[test]
    fn report() {
        with_dir("target/test/main/report", |root| {
            let file = root.join("file1");
            write_test_file(&file, b"foo", b"", b"");

            let report_file = root.join("report.txt");
            let mut ctx = test_ctx();
            ctx.config.paths = vec![file.clone()];
            ctx.config.unique = true;
            ctx.config.output = Some(report_file.clone());

            let results = process(&mut ctx);
            write_report(&mut ctx, &results);

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

    fn test_ctx() -> AppCtx {
        let mut log = Log::new();
        log.no_progress = true;
        AppCtx {
            log,
            config: Default::default(),
            devices: Default::default(),
        }
    }
}
