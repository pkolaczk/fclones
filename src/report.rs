use std::io::Write;
use std::sync::Arc;

use console::style;

use crate::group::FileGroup;
use crate::progress::FastProgressBar;

pub struct Reporter<W: Write> {
    out: W,
    progress: Arc<FastProgressBar>
}

impl<W: Write> Reporter<W> {

    pub fn new(out: W, progress: Arc<FastProgressBar>) -> Reporter<W> {
        Reporter { out, progress }
    }

    pub fn write_as_text(&mut self, results: &Vec<FileGroup>) {
        for g in results {
            let len = style(format!("{:8}", g.len)).yellow().bold();
            let hash =
                match g.hash {
                    None => style("-".repeat(32)).white().dim(),
                    Some(hash) => style(format!("{}", hash)).blue().bold().bright()
                };
            writeln!(self.out, "{} {}:", len, hash.for_stdout()).unwrap();
            for f in g.files.iter() {
                self.progress.tick();
                writeln!(self.out, "    {}", f.display()).unwrap();
            }
        }
    }



}

