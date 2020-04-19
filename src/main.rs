
use std::fs::{File, Metadata};
use std::path::PathBuf;

use jwalk::{DirEntry, WalkDir, Parallelism};
use rayon::iter::ParallelBridge;
use rayon::prelude::*;
use structopt::StructOpt;
use std::sync::mpsc::{channel, Receiver};

#[derive(Debug, StructOpt)]
#[structopt(name = "dff", about = "Find duplicate files")]
struct Config {
    /// Follow symlinks
    #[structopt(long)]
    pub follow: bool,

    /// Skip hidden files
    #[structopt(long)]
    pub skip_hidden: bool,

    /// Parallelism level
    #[structopt(short, long, default_value = "8")]
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
                    if !e.file_type.is_dir() {
                        tx.send(e.path()).unwrap();
                    },
                Err(e) =>
                    eprintln!("Cannot access path {}: {}", path.display(), e)
            }
        }
    });
    rx
}

fn file_len(file: &PathBuf) -> u64 {
    match std::fs::metadata(file) {
        Ok(metadata) => metadata.len(),
        Err(e) => { eprintln!("Failed to read size of {}: {}", file.display(), e); 0 }
    }
}

fn main() {
    let config: Config = Config::from_args();
    rayon::ThreadPoolBuilder::new().num_threads(config.threads).build_global().unwrap();
    let files= walk_dirs(&config).into_iter().par_bridge();
    let size_map = chashmap::CHashMap::<u64, PathBuf>::with_capacity(65536);
    files.for_each(|path| {
        size_map.insert(file_len(&path), path); () }
    );
    println!("Map size = {:?}", size_map.len());
}