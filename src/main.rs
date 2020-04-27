use std::cmp::Reverse;
use std::path::PathBuf;

use structopt::StructOpt;

use dff::files::*;
use dff::group::*;
use dff::progress::FastProgressBar;
use dff::report::Report;

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

fn main() {

    const PREFIX_LEN: FileLen = FileLen(4096);

    let config: Config = Config::from_args();
    configure_thread_pool(config.threads);

    let walk_opts = WalkOpts {
        skip_hidden: config.skip_hidden,
        follow_links: config.follow_links,
        parallelism: config.threads
    };

    let mut report = Report::new();

    let files= walk_dirs(config.paths.clone(), walk_opts);
    let file_scan_pb = FastProgressBar::new_spinner("[1/4] Scanning files");
    let size_groups = split_groups(
        vec![((), files)], 2,|_, path| {
            file_scan_pb.tick();
            file_len(path)
                .filter(|len| *len >= config.min_size && *len <= config.max_size)
    });
    report.scanned_files(file_scan_pb.position());
    let remaining_files = report.stage_finished("Group by paths", &size_groups);
    file_scan_pb.finish_and_clear();

    let prefix_hash_pb = FastProgressBar::new_progress_bar(
        "[2/4] Hashing by prefix", remaining_files as u64);
    let prefix_groups = split_groups(size_groups, 2, |&len, path | {
        prefix_hash_pb.tick();
        file_hash(path, PREFIX_LEN).map(|h| (len, h))
    });

    let remaining_files = report.stage_finished("Group by prefix", &prefix_groups);
    prefix_hash_pb.finish_and_clear();

    let contents_hash_pb = FastProgressBar::new_progress_bar(
        "[3/4] Hashing by contents", remaining_files as u64);
    let mut contents_groups = split_groups(prefix_groups, 2, |&(len, hash), path | {
        contents_hash_pb.tick();
        if len > PREFIX_LEN {
            file_hash(path, FileLen::MAX).map(|h| (len, h))
        } else {
            Some((len, hash))
        }
    });

    let remaining_files = report.stage_finished("Group by contents", &contents_groups);
    contents_hash_pb.finish_and_clear();

    let report_pb = FastProgressBar::new_progress_bar(
        "[4/4] Writing report", remaining_files as u64);
    contents_groups.sort_by_key(|&((len, _), _)| Reverse(len));
    report.write(&mut std::io::stdout()).expect("Failed to write report");
    println!();
    for ((len, hash), files) in contents_groups {
        println!("{} {}:", hash, len);
        for f in files {
            report_pb.tick();
            println!("    {}", f.display());
        }
    }
}