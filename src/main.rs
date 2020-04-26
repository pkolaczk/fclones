use std::cmp::Reverse;
use std::path::PathBuf;

use atomic_counter::{AtomicCounter, RelaxedCounter};
use bytesize::ByteSize;
use structopt::StructOpt;

use dff::files::*;
use dff::group::*;
use dff::progress::FastProgressBar;

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
    pub min_size: u64,

    /// Maximum file size. Inclusive.
    #[structopt(long, default_value="18446744073709551615")]
    pub max_size: u64,

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

    const PREFIX_LEN: u64 = 4096;

    let config: Config = Config::from_args();
    configure_thread_pool(config.threads);

    let walk_opts = WalkOpts {
        skip_hidden: config.skip_hidden,
        follow_links: config.follow_links,
        parallelism: config.threads
    };

    let matching_file_count = RelaxedCounter::new(0);
    let files= walk_dirs(config.paths.clone(), walk_opts);
    let file_scan_pb = FastProgressBar::new_spinner("[1/4] Scanning files");
    let size_groups = split_groups(
        vec![((), files)], 2,|_, path| {
            file_scan_pb.tick();
            file_len(path)
                .filter(|&len| len >= config.min_size && len <= config.max_size)
                .map(|l| { matching_file_count.inc(); l })
    });
    file_scan_pb.finish_and_clear();
    let file_count_0 = file_scan_pb.position();
    let file_count_1: usize = size_groups.iter().map(|(_, group)| group.len()).sum();
    let group_count_1 = size_groups.len();

    let prefix_hash_pb = FastProgressBar::new_progress_bar(
        "[2/4] Hashing by prefix", file_count_1 as u64);
    let prefix_groups = split_groups(size_groups, 2, |&len, path | {
        prefix_hash_pb.tick();
        file_hash(path, PREFIX_LEN).map(|h| (len, h))
    });

    prefix_hash_pb.finish_and_clear();
    let file_count_2: usize = prefix_groups.iter().map(|(_, group)| group.len()).sum();
    let group_count_2 = prefix_groups.len();

    let contents_hash_pb = FastProgressBar::new_progress_bar(
        "[3/4] Hashing by contents", file_count_2 as u64);
    let mut contents_groups = split_groups(prefix_groups, 2, |&(len, hash), path | {
        contents_hash_pb.tick();
        if len > PREFIX_LEN {
            file_hash(path, u64::MAX).map(|h| (len, h))
        } else {
            Some((len, hash))
        }
    });
    contents_groups.sort_by_key(|&((len, _), _)| Reverse(len));
    contents_hash_pb.finish_and_clear();

    let report_pb = FastProgressBar::new_progress_bar(
        "[4/4] Writing report", group_count_2 as u64);

    println!("# Total scanned files: {}", file_count_0);
    println!("# Selected files: {}", matching_file_count.get());
    println!("# Files matched by same size: {} in {} groups", file_count_1, group_count_1);
    println!("# Files matched by same prefix: {} in {} groups", file_count_2, group_count_2);
    println!();
    for ((len, hash), files) in contents_groups {
        report_pb.tick();
        println!("{:x} {}:", hash, ByteSize(len));
        for f in files {
           println!("    {}", f.display());
        }
    }
}