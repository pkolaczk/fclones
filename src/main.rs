use std::convert::identity;
use std::path::PathBuf;
use std::time::Duration;

use indicatif::{ProgressBar, ProgressStyle};
use rayon::prelude::*;
use structopt::StructOpt;

use dff::files::*;
use dff::group::*;
use dff::progress::FastProgressBar;
use atomic_counter::{RelaxedCounter, AtomicCounter};

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

fn file_scan_progress_bar() -> FastProgressBar {
    let file_scan_pb = ProgressBar::new_spinner();
    file_scan_pb.set_style(ProgressStyle::default_spinner().template("Scanned files: {pos}"));
    FastProgressBar::wrap(file_scan_pb, Duration::from_millis(125))
}

fn prefix_hash_progress_bar(len: u64) -> FastProgressBar {
    let pb = ProgressBar::new(len);
    FastProgressBar::wrap(pb, Duration::from_millis(125))
}

fn main() {
    let config: Config = Config::from_args();
    configure_thread_pool(config.threads);

    let walk_opts = WalkOpts {
        skip_hidden: config.skip_hidden,
        follow_links: config.follow_links,
        parallelism: config.threads
    };

    let matching_file_count = RelaxedCounter::new(0);
    let files= walk_dirs(config.paths.clone(), walk_opts);
    let file_scan_pb = file_scan_progress_bar();
    let size_groups: Vec<_> = files
        .inspect(|_| file_scan_pb.tick())
        .map(|path| (file_len(&path), path))
        .filter_map(|(size_opt, path)| size_opt.map(|size| (size, path)))  // remove entries with unknown size
        .filter(|(size, _)|
            *size >= config.min_size &&
            *size <= config.max_size)
        .inspect(|_| { matching_file_count.inc(); })
        .group_by_key(identity)
        .into_iter()
        .filter(|(_, files)| files.len() >= 2)
        .collect();

    file_scan_pb.finish_and_clear();
    let file_count_0 = file_scan_pb.position();
    let file_count_1: usize = size_groups.iter().map(|(_, group)| group.len()).sum();
    let group_count_1 = size_groups.len();

    let prefix_hash_pb = prefix_hash_progress_bar(file_count_1 as u64);
    let prefix_groups: Vec<_> = size_groups
        .par_iter()
        .map(|(_, group)| group)
        .flat_map(|files| files
            .par_iter()
            .map(|path| (file_hash(&path, 4096), path))
            .inspect(|_| prefix_hash_pb.inc(1))
            .filter_map(|(hash_opt, path)| hash_opt.map(|hash| (hash, path)))
            .group_by_key(identity)
            .into_iter()
            .collect::<Vec<_>>())
        .filter(|(_, files)| files.len() >= 2)
        .collect();

    prefix_hash_pb.finish_and_clear();
    let file_count_2: usize = prefix_groups.iter().map(|(_, group)| group.len()).sum();
    let group_count_2 = prefix_groups.len();


    println!("# Total scanned files: {}", file_count_0);
    println!("# Selected files: {}", matching_file_count.get());
    println!("# Files matched by same size: {} in {} groups", file_count_1, group_count_1);
    println!("# Files matched by same prefix: {} in {} groups", file_count_2, group_count_2);

    for (hash, files) in prefix_groups {
        println!("{:x}:", hash);
        for f in files {
           println!("    {}", f.display());
        }
    }
}