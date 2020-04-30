use std::cmp::Reverse;
use std::path::PathBuf;

use itertools::Itertools;
use rayon::iter::ParallelIterator;
use structopt::StructOpt;

use dff::files::*;
use dff::group::*;
use dff::progress::FastProgressBar;
use dff::report::Report;

const MIN_PREFIX_LEN: FileLen = FileLen(4096);
const MAX_PREFIX_LEN: FileLen = FileLen(2 * MIN_PREFIX_LEN.0);
const SUFFIX_LEN: FileLen = FileLen(4096);

#[derive(Debug, StructOpt)]
#[structopt(name = "dff", about = "Find duplicate files", author)]
struct Config {

    /// Skip hidden files
    #[structopt(short="A", long)]
    pub skip_hidden: bool,

    /// Follow symbolic links
    #[structopt(short="L", long)]
    pub follow_links: bool,

    /// Treat files reachable from multiple paths through
    /// symbolic or hard links as duplicates
    #[structopt(short="H", long)]
    pub duplicate_links: bool,

    /// Minimum file size. Inclusive.
    #[structopt(short="s", long, default_value="1")]
    pub min_size: FileLen,

    /// Maximum file size. Inclusive.
    #[structopt(short="x", long, default_value="18446744073709551615")]
    pub max_size: FileLen,

    /// Parallelism level.
    /// If set to 0, the number of CPU cores reported by the operating system is used.
    #[structopt(short, long, default_value="0")]
    pub threads: usize,

    /// Directory roots to scan
    #[structopt(parse(from_os_str), required = true)]
    pub paths: Vec<PathBuf>,
}

/// Configures global thread pool to use desired number of threads
fn configure_thread_pool(parallelism: usize) {
    rayon::ThreadPoolBuilder::new()
        .num_threads(parallelism)
        .build_global()
        .unwrap();
}

/// Unless `duplicate_links` is set to true,
/// remove duplicated `FileInfo` entries with the same inode and device id from the list.
fn remove_duplicate_links_if_needed(config: &Config, files: Vec<FileInfo>) -> Vec<FileInfo> {
    if config.duplicate_links {
        files
    } else {
        files
            .into_iter()
            .unique_by(|info| (info.dev_id, info.file_id))
            .collect()
    }
}


fn scan_files(report: &mut Report, config: &Config) -> Vec<(FileLen, Vec<PathBuf>)> {
    let spinner = FastProgressBar::new_spinner("[1/5] Grouping by size");
    let walk_opts = WalkOpts {
        skip_hidden: config.skip_hidden,
        follow_links: config.follow_links,
        parallelism: config.threads
    };
    let files = walk_dirs(config.paths.clone(), walk_opts);
    let groups = files
        .inspect(|_| spinner.tick())
        .filter_map(|path| file_info_or_log_err(path))
        .filter(|info| info.len >= config.min_size && info.len <= config.max_size)
        .group_by_key(|info| (info.len, info))
        .into_iter()
        .filter(|(_, files)| files.len() >= 2)
        .map(|(l, files)| (l, remove_duplicate_links_if_needed(&config, files)))
        .map(|(l, files)| (l, files.into_iter().map(|info| info.path).collect()))
        .collect();

    report.scanned_files(spinner.position());
    report.stage_finished("Group by size", &groups);
    groups
}

fn group_by_prefix(report: &mut Report, groups: Vec<(FileLen, Vec<PathBuf>)>)
                   -> Vec<((FileLen, FileHash), Vec<PathBuf>)>
{
    let remaining_files = count_values(&groups);
    let progress = FastProgressBar::new_progress_bar(
        "[2/5] Grouping by prefix", remaining_files as u64);
    let groups = split_groups(groups, 2, |&len, path| {
        progress.tick();
        let prefix_len = if len <= MAX_PREFIX_LEN { len } else { MIN_PREFIX_LEN };
        file_hash_or_log_err(path, FilePos(0), prefix_len).map(|h| (len, h))
    }).collect();

    report.stage_finished("Group by prefix", &groups);
    groups
}

fn group_by_suffix(report: &mut Report, groups: Vec<((FileLen, FileHash), Vec<PathBuf>)>)
                   -> Vec<((FileLen, FileHash), Vec<PathBuf>)>
{
    let remaining_files = count_values(&groups);
    let progress = FastProgressBar::new_progress_bar(
        "[3/5] Grouping by suffix", remaining_files as u64);
    let groups = split_groups(groups, 2, |&(len, hash), path| {
        progress.tick();
        if len >= MAX_PREFIX_LEN + SUFFIX_LEN {
            file_hash_or_log_err(path, (len - SUFFIX_LEN).as_pos(), SUFFIX_LEN)
                .map(|h| (len, h))
        } else {
            Some((len, hash))
        }
    }).collect();

    report.stage_finished("Group by suffix", &groups);
    groups
}

fn group_by_contents(report: &mut Report, groups: Vec<((FileLen, FileHash), Vec<PathBuf>)>)
                     -> Vec<((FileLen, FileHash), Vec<PathBuf>)>
{
    let remaining_files = count_values(&groups);
    let progress = FastProgressBar::new_progress_bar(
        "[4/5] Grouping by contents", remaining_files as u64);
    let groups = split_groups(groups, 2, |&(len, hash), path| {
        progress.tick();
        if len > MAX_PREFIX_LEN {
            file_hash_or_log_err(path, FilePos(0), len).map(|h| (len, h))
        } else {
            Some((len, hash))
        }
    }).collect();

    report.stage_finished("Group by contents", &groups);
    groups
}

fn write_report(report: &mut Report, groups: &mut Vec<((FileLen, FileHash), Vec<PathBuf>)>) {
    let remaining_files = count_values(&groups);
    let progress = FastProgressBar::new_progress_bar(
        "[5/5] Writing report", remaining_files as u64);
    groups.sort_by_key(|&((len, _), _)| Reverse(len));
    report.write(&mut std::io::stdout()).expect("Failed to write report");
    println!();
    for ((len, hash), files) in groups {
        println!("{} {}:", hash, len);
        for f in files {
            progress.tick();
            println!("    {}", f.display());
        }
    }
}

fn main() {
    let config: Config = Config::from_args();
    configure_thread_pool(config.threads);

    let mut report = Report::new();
    let size_groups = scan_files(&mut report, &config);
    let prefix_groups = group_by_prefix(&mut report, size_groups);
    let suffix_groups = group_by_suffix(&mut report, prefix_groups);
    let mut contents_groups= group_by_contents(&mut report, suffix_groups);
    write_report(&mut report, &mut contents_groups)
}