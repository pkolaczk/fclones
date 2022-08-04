use clap::Parser;
use rand::{thread_rng, Rng, RngCore};
use std::fs::File;
use std::io::Write;
use std::path::PathBuf;

#[derive(clap::Parser)]
struct CmdOptions {
    #[clap(short = 'n', long, default_value = "100")]
    count: usize,

    #[clap(short = 'c', long, default_value = "5")]
    max_group_size: usize,

    #[clap(default_value = ".")]
    target: PathBuf,
}

fn main() {
    let options = CmdOptions::parse();
    let mut buf = [0u8; 65538];
    for i in 0..options.count {
        let group_size: usize = thread_rng().gen_range(1..options.max_group_size);
        thread_rng().fill_bytes(&mut buf);
        for j in 0..group_size {
            let file_name = options.target.join(format!("file_{i}_{j}.txt"));
            let mut file = File::create(file_name).unwrap();
            file.write_all(&buf).unwrap();
        }
    }
}
