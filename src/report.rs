use std::io::Write;
use std::path::PathBuf;

use bytesize::ByteSize;

use crate::files::AsFileLen;
use crate::group::GroupedFileSetMetrics;

struct FileGroupInfo {
    count: usize,
    bytes: u64
}

struct Stage {
    name: String,
    group_count: usize,
    output_files: FileGroupInfo,
    redundant_files: FileGroupInfo,
}

/// Keeps track of total file counts and sizes at different stages of processing
pub struct Report {
    scanned_files_count: usize,
    stages: Vec<Stage>
}

impl Report {

    /// Create a new empty report
    pub fn new() -> Report {
        Report {
            scanned_files_count: 0,
            stages: vec![] }
    }

    /// Record the number and total size of all files that were scanned
    pub fn scanned_files(&mut self, count: usize) {
        self.scanned_files_count = count;
    }

    /// Record the results of a grouping stage and returns the count of files in that stage
    pub fn stage_finished<L: AsFileLen>(&mut self, name: &str, groups: &Vec<(L, Vec<PathBuf>)>)
        -> usize
    {
        let num_files = groups.total_count();
        let s = Stage {
            name: name.to_owned(),
            group_count: groups.len(),
            output_files: FileGroupInfo {
                count: num_files,
                bytes: groups.total_size()
            },
            redundant_files: FileGroupInfo {
                count: groups.redundant_count(1),
                bytes: groups.redundant_size(1)
            }
        };

        self.stages.push(s);
        num_files
    }

    /// Write report to the output stream
    pub fn write(&self, out: &mut impl Write) -> std::io::Result<()> {
        writeln!(out, "# Scanned files: {}", self.scanned_files_count)?;
        writeln!(out, "# Redundant files: {} ({})",
                 self.stages.last().unwrap().redundant_files.count,
                 ByteSize(self.stages.last().unwrap().redundant_files.bytes))?;
        writeln!(out, "# Stage info:")?;
        writeln!(out, "# stage              |     groups |                files |            redundant")?;
        writeln!(out, "# -----------------------------------------------------------------------------")?;
        for s in &self.stages {
            writeln!(out,
                     "# {:18} | {:>10} | {:>10} {:>9} | {:>10} {:>9}",
                     s.name,
                     s.group_count,
                     s.output_files.count, format!("{}", ByteSize(s.output_files.bytes)),
                     s.redundant_files.count, format!("{}", ByteSize(s.redundant_files.bytes))
            )?;
        }
        writeln!(out, "# -----------------------------------------------------------------------------")?;
        Ok(())
    }
}