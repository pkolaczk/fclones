use std::io;
use std::io::Write;
use std::sync::Arc;

use console::style;
use serde::{Serialize, Serializer};

use crate::group::FileGroup;
use crate::path::Path;
use crate::progress::FastProgressBar;

pub struct Reporter<W: Write> {
    out: W,
    color: bool,
    progress: Arc<FastProgressBar>,
}

impl<W: Write> Reporter<W> {
    pub fn new(out: W, color: bool, progress: Arc<FastProgressBar>) -> Reporter<W> {
        Reporter {
            out,
            color,
            progress,
        }
    }

    /// Writes the report in human-readable text format.
    /// A group of identical files starts with a group header at column 0,
    /// containing the size and hash of each file in the group.
    /// Then file paths are printed in separate, indented lines.
    ///
    /// ```text
    /// <size> <hash>:
    ///     <path 1>
    ///     <path 2>
    ///     ...
    ///     <path N>
    /// ```
    pub fn write_as_text(&mut self, results: &[FileGroup<Path>]) -> io::Result<()> {
        for g in results {
            let len = style(format!("{:8}", g.file_len)).yellow().bold();
            let hash = style(format!("{}", g.hash)).blue().bold().bright();
            writeln!(
                self.out,
                "{} {}:",
                len.force_styling(self.color),
                hash.force_styling(self.color)
            )?;
            for f in g.files.iter() {
                self.progress.tick();
                writeln!(self.out, "    {}", f.display())?;
            }
        }
        Ok(())
    }

    /// Writes the report in `fdupes` compatible format.
    /// This is very similar to the TEXT format, but there are no headers
    /// for each group, and groups are separated with empty lines.
    pub fn write_as_fdupes(&mut self, results: &[FileGroup<Path>]) -> io::Result<()> {
        for g in results {
            for f in g.files.iter() {
                self.progress.tick();
                writeln!(self.out, "{}", f.display())?;
            }
            writeln!(self.out)?;
        }
        Ok(())
    }

    /// Writes results in CSV format.
    /// Each file group is written as one line.
    /// The number of columns is dynamic.
    /// Columns:
    /// - file size in bytes
    /// - file hash (may be empty)
    /// - number of files in the group
    /// - file paths - each file in a separate column
    pub fn write_as_csv(&mut self, results: &[FileGroup<Path>]) -> io::Result<()> {
        let mut wtr = csv::WriterBuilder::new()
            .delimiter(b',')
            .quote_style(csv::QuoteStyle::Necessary)
            .flexible(true)
            .from_writer(&mut self.out);

        wtr.write_record(&["size", "hash", "count", "files"])?;
        for g in results {
            let mut record = csv::StringRecord::new();
            record.push_field(g.file_len.0.to_string().as_str());
            record.push_field(g.hash.to_string().as_str());
            record.push_field(g.files.len().to_string().as_str());
            for f in g.files.iter() {
                record.push_field(f.to_string_lossy().as_ref());
            }
            wtr.write_record(&record)?;
        }
        wtr.flush()
    }

    /// Writes results as JSON.
    /// # Example output
    /// ```json
    ///[
    ///   {
    ///     "len": 412233264,
    ///     "hash": "78b924aa8a1750a11b9327b286ef0403",
    ///     "files": [
    ///       "/home/example/x.log",
    ///       "/home/example/backup/x.log"
    ///     ]
    ///   },
    ///   {
    ///     "len": 113340416,
    ///     "hash": "c2bbd97c620815253705a58bd26b1f9d",
    ///     "files": [
    ///       "/home/example/.wine/drive_c/{152C8096-A1DC-4844-A294-C90EBEDD13C4}/1b00.msi",
    ///       "/home/example/.wine/drive_c/windows/Installer/1b00.msi"
    ///     ]
    ///   }
    /// ]
    /// ```
    pub fn write_as_json(&mut self, results: &[FileGroup<Path>]) -> io::Result<()> {
        let wrapper = SliceWrapper {
            vec: results,
            progress: self.progress.as_ref(),
        };
        serde_json::to_writer_pretty(&mut self.out, &wrapper)?;
        Ok(())
    }
}

/// This wrapper allows us to track progress while we're serializing
struct SliceWrapper<'a, T> {
    vec: &'a [T],
    progress: &'a FastProgressBar,
}

impl Serialize for SliceWrapper<'_, FileGroup<Path>> {
    fn serialize<S>(&self, serializer: S) -> Result<<S as Serializer>::Ok, <S as Serializer>::Error>
    where
        S: Serializer,
    {
        let i = self
            .vec
            .iter()
            .inspect(|&g| self.progress.inc(g.files.len()));
        serializer.collect_seq(i)
    }
}
