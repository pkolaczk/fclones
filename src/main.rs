use std::convert::identity;
use std::path::PathBuf;

use rayon::prelude::*;
use structopt::StructOpt;

use dff::files::*;
use dff::group::*;

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

    /// Parallelism level
    #[structopt(short, long, default_value="8")]
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
    let config: Config = Config::from_args();
    configure_thread_pool(config.threads);

    let walk_opts = WalkOpts {
        skip_hidden: config.skip_hidden,
        follow_links: config.follow_links
    };
    let files= walk_dirs(&config.paths, &walk_opts);

    let size_groups = files
        .map(|path| (file_len(&path), path))
        .filter(|(size, _)|
            *size >= config.min_size &&
            *size <= config.max_size)
        .group_by_key(identity)
        .filter(|(_, files)| files.len() >= 2);

    for (size, files) in size_groups {
        println!("{}:", size);
        for f in files {
           println!("    {}", f.display());
        }
    }
}