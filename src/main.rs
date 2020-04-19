use std::fs::{File, Metadata};
use std::hash::Hasher;
use std::io::{BufRead, BufReader, Read};
use std::path::PathBuf;
use std::sync::mpsc::{channel, Receiver};

use jwalk::{DirEntry, Parallelism, WalkDir};
use rayon::iter::ParallelBridge;
use rayon::prelude::*;
use structopt::StructOpt;

use dff::group::*;
use dff::files::*;

#[derive(Debug, StructOpt)]
#[structopt(name = "dff", about = "Find duplicate files")]
struct Config {
    /// Follow symlinks
    #[structopt(long)]
    pub follow: bool,

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


fn walk_dirs(config: &Config) -> Receiver<PathBuf> {
    let (tx, rx) = channel();
    config.paths.par_iter().for_each_with(tx, |tx, path| {
        let walk = WalkDir::new(&path)
            .skip_hidden(config.skip_hidden)
            .follow_links(config.follow)
            .parallelism(Parallelism::RayonDefaultPool);
        for entry in walk {
            match entry {
                Ok(e) =>
                    if e.file_type.is_file() || e.file_type.is_symlink() {
                        tx.send(e.path()).unwrap();
                    },
                Err(e) =>
                    eprintln!("Cannot access path {}: {}", path.display(), e)
            }
        }
    });
    rx
}


fn main() {
    let config: Config = Config::from_args();
    rayon::ThreadPoolBuilder::new().num_threads(config.threads).build_global().unwrap();
    let files= walk_dirs(&config).into_iter().par_bridge();
    let size_groups = files
        .group_by_key(file_len)
        .into_iter()
        .filter(|(size, files)|
            *size >= config.min_size &&
                *size <= config.max_size &&
                files.len() >= 2);

    for (size, files) in size_groups {
        println!("{}:", size);
        for f in files {
           println!("    {}", f.display());
        }
    }
}