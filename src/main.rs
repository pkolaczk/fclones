use std::cmp::Reverse;
use std::path::PathBuf;

use rayon::iter::ParallelIterator;
use structopt::StructOpt;

use dff::files::*;
use dff::group::*;
use dff::progress::FastProgressBar;
use dff::report::Report;

const PREFIX_LEN: FileLen = FileLen(4096);

#[derive(Debug, StructOpt)]
#[structopt(name = "dff", about = "Find duplicate files")]
struct Config {
    /// Follow symlinks
    #[structopt(long)]
    pub follow_links: bool,

    /// Skip hidden files
    #[structopt(long)]
    pub skip_hidden: bool,

    /// Minimum file size. Inclusive.
    #[structopt(long, default_value="1")]
    pub min_size: FileLen,

    /// Maximum file size. Inclusive.
    #[structopt(long, default_value="18446744073709551615")]
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

fn scan_files(report: &mut Report, config: &Config) -> Vec<(FileLen, Vec<PathBuf>)> {
    let walk_opts = WalkOpts {
        skip_hidden: config.skip_hidden,
        follow_links: config.follow_links,
        parallelism: config.threads
    };
    let files = walk_dirs(config.paths.clone(), walk_opts);
    let spinner = FastProgressBar::new_spinner("[1/4] Scanning files");
    let groups = split_groups(
        vec![((), files)], 2, |_, path| {
            spinner.tick();
            file_len(path)
                .filter(|len| *len >= config.min_size && *len <= config.max_size)
        }
    ).collect();
    report.scanned_files(spinner.position());
    report.stage_finished("Group by paths", &groups);
    groups
}

fn group_by_prefix(report: &mut Report, groups: Vec<(FileLen, Vec<PathBuf>)>)
                   -> Vec<((FileLen, FileHash), Vec<PathBuf>)>
{
    let remaining_files = count_values(&groups);
    let progress = FastProgressBar::new_progress_bar(
        "[2/4] Hashing by prefix", remaining_files as u64);
    let groups = split_groups(groups, 2, |&len, path| {
        progress.tick();
        file_hash(path, PREFIX_LEN).map(|h| (len, h))
    }).collect();
    report.stage_finished("Group by prefix", &groups);
    groups
}

fn group_by_contents(report: &mut Report, groups: Vec<((FileLen, FileHash), Vec<PathBuf>)>)
                     -> Vec<((FileLen, FileHash), Vec<PathBuf>)>
{
    let remaining_files = count_values(&groups);
    let progress = FastProgressBar::new_progress_bar(
        "[3/4] Hashing by contents", remaining_files as u64);
    let groups = split_groups(groups, 2, |&(len, hash), path| {
        progress.tick();
        if len > PREFIX_LEN {
            file_hash(path, FileLen::MAX).map(|h| (len, h))
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
        "[4/4] Writing report", remaining_files as u64);
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
    let mut report = Report::new();

    configure_thread_pool(config.threads);

    let size_groups = scan_files(&mut report, &config);
    let prefix_groups = group_by_prefix(&mut report, size_groups);
    let mut contents_groups= group_by_contents(&mut report, prefix_groups);
    write_report(&mut report, &mut contents_groups)
}