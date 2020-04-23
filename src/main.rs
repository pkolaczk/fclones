use std::convert::identity;
use std::path::PathBuf;
use std::thread;
use std::time::Duration;

use indicatif::{ProgressBar, ProgressDrawTarget, ProgressStyle};
use rayon::prelude::*;
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

fn file_scan_progress_bar() -> FastProgressBar {
    let file_scan_pb = ProgressBar::new_spinner();
    file_scan_pb.set_style(ProgressStyle::default_spinner().template("Scanned files: {pos}"));
    FastProgressBar::wrap(file_scan_pb, Duration::from_millis(125))
}

fn main() {
    let config: Config = Config::from_args();
    configure_thread_pool(config.threads);

    let walk_opts = WalkOpts {
        skip_hidden: config.skip_hidden,
        follow_links: config.follow_links,
        parallelism: config.threads
    };
    let files= walk_dirs(config.paths.clone(), walk_opts);
    let file_scan_pb = file_scan_progress_bar();
    let size_groups = files
        .inspect(move |path| file_scan_pb.tick())
        .map(|path| (file_len(&path), path))
        .filter_map(|(size_opt, path)| size_opt.map(|size| (size, path)))  // remove entries with unknown size
        .filter(|(size, _)|
            *size >= config.min_size &&
            *size <= config.max_size)
        .group_by_key(identity)
        .into_iter()
        .filter(|(_, files)| files.len() >= 2);

    for (size, files) in size_groups {
        println!("{}:", size);
        for f in files {
           println!("    {}", f.display());
        }
    }
}