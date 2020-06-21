use std::io;
use std::io::{stdin, BufRead, BufReader};
use std::path::PathBuf;
use std::process::exit;

use clap::arg_enum;
use clap::AppSettings;
use structopt::StructOpt;

use crate::files::FileLen;
use crate::log::Log;
use crate::path::Path;
use crate::pattern::{Pattern, PatternOpts};
use crate::selector::PathSelector;
use crate::transform::Transform;

arg_enum! {
    #[derive(Debug, StructOpt)]
    pub enum OutputFormat {
        Text, Csv, Json
    }
}

impl Default for OutputFormat {
    fn default() -> OutputFormat {
        OutputFormat::Text
    }
}

/// Finds duplicate, unique, under- or over-replicated files
#[derive(Debug, StructOpt, Default)]
#[structopt(
    name = "File Clones",
    setting(AppSettings::ColoredHelp),
    setting(AppSettings::DeriveDisplayOrder)
)]
pub struct Config {
    /// Writes the report to a file instead of the standard output
    #[structopt(short = "o", long, value_name("path"))]
    pub output: Option<PathBuf>,

    /// Sets output file format
    #[structopt(short = "f", long, possible_values = &OutputFormat::variants(),
    case_insensitive = true, default_value="Text")]
    pub format: OutputFormat,

    /// Reads the list of input paths from the standard input instead of the arguments.
    /// This flag is mostly useful together with `find` utility.
    #[structopt(short = "I", long)]
    pub stdin: bool,

    /// Descends into directories recursively
    #[structopt(short = "R", long)]
    pub recursive: bool,

    /// Limits recursion depth
    #[structopt(short = "d", long)]
    pub depth: Option<usize>,

    /// Skips hidden files
    #[structopt(short = "A", long)]
    pub skip_hidden: bool,

    /// Follows symbolic links
    #[structopt(short = "L", long)]
    pub follow_links: bool,

    /// Treats files reachable from multiple paths through
    /// hard links as duplicates
    #[structopt(short = "H", long)]
    pub hard_links: bool,

    /// Before matching, transform each file by the specified program.
    /// The value of this parameter should contain a command: the path to the program
    /// and optionally its space-separated arguments.
    /// By default, the file to process will be piped to the standard input of the program and the
    /// processed data will be read from the standard output.
    /// If the program does not support piping, but requires its input and/or output file path
    /// to be specified in the argument list, denote these paths by $IN and $OUT special variables.
    /// If $IN is specified in the command string, the file will not be piped to the standard input.
    /// If $OUT is specified in the command string, the result will not be read from
    /// the standard output, but fclones will set up a named pipe $OUT and read from
    /// that pipe instead.
    #[structopt(long, value_name("command"))]
    pub transform: Option<String>,

    /// Searches for over-replicated files with replication factor above the specified value.
    /// Specifying neither `--rf-over` nor `--rf-under` is equivalent to `--rf-over 1` which would
    /// report duplicate files.
    #[structopt(long, conflicts_with("rf-under"), value_name("count"))]
    pub rf_over: Option<usize>,

    /// Searches for under-replicated files with replication factor below the specified value.
    /// Specifying `--rf-under 2` will report unique files.
    #[structopt(long, conflicts_with("rf-over"), value_name("count"))]
    pub rf_under: Option<usize>,

    /// Instead of searching for duplicates, searches for unique files.
    #[structopt(short="U", long, conflicts_with_all(&["rf-over", "rf-under"]))]
    pub unique: bool,

    /// Minimum file size in bytes. Units like KB, KiB, MB, MiB, GB, GiB are supported. Inclusive.
    #[structopt(short = "s", long, default_value = "1", value_name("bytes"))]
    pub min_size: FileLen,

    /// Maximum file size in bytes. Units like KB, KiB, MB, MiB, GB, GiB are supported. Inclusive.
    #[structopt(long, value_name("bytes"))]
    pub max_size: Option<FileLen>,

    /// Includes only file names matched fully by any of the given patterns.
    #[structopt(short = "n", long = "names", value_name("pattern"))]
    pub name_patterns: Vec<String>,

    /// Includes only paths matched fully by any of the given patterns.
    #[structopt(short = "p", long = "paths", value_name("pattern"))]
    pub path_patterns: Vec<String>,

    /// Excludes paths matched fully by any of the given patterns.
    #[structopt(short = "e", long = "exclude", value_name("pattern"))]
    pub exclude_patterns: Vec<String>,

    /// Makes pattern matching case-insensitive
    #[structopt(short = "i", long)]
    pub caseless: bool,

    /// Expects patterns as Perl compatible regular expressions instead of Unix globs
    #[structopt(short = "g", long)]
    pub regex: bool,

    /// Parallelism level.
    /// If set to 0, the number of CPU cores reported by the operating system is used.
    #[structopt(short, long, default_value = "0", value_name("count"))]
    pub threads: usize,

    /// A list of input paths. Accepts files and directories.
    #[structopt(parse(from_os_str), required_unless("stdin"))]
    pub paths: Vec<PathBuf>,
}

impl Config {
    pub fn path_selector(&self, log: &Log, base_dir: &Path) -> PathSelector {
        let pattern_opts = if self.caseless {
            PatternOpts::case_insensitive()
        } else {
            PatternOpts::default()
        };
        let pattern = |s: &String| {
            let pattern = if self.regex {
                Pattern::regex_with(s.as_str(), &pattern_opts)
            } else {
                Pattern::glob_with(s.as_str(), &pattern_opts)
            };

            pattern.unwrap_or_else(|e| {
                log.err(e);
                exit(1);
            })
        };

        PathSelector::new(base_dir.clone())
            .include_names(self.name_patterns.iter().map(pattern).collect())
            .include_paths(self.path_patterns.iter().map(pattern).collect())
            .exclude_paths(self.exclude_patterns.iter().map(pattern).collect())
    }

    pub fn rf_over(&self) -> usize {
        // don't prune small groups if:
        // - there is transformation defined
        //   (distinct files can become identical after the transform)
        // - or we're looking for under-replicated files
        // - or we're looking for unique files
        if self.transform.is_some() || self.rf_under.is_some() || self.unique {
            0
        } else {
            self.rf_over.unwrap_or(1)
        }
    }

    pub fn rf_under(&self) -> usize {
        if self.unique {
            2
        } else {
            self.rf_under.unwrap_or(usize::MAX)
        }
    }

    pub fn search_type(&self) -> &'static str {
        if self.unique {
            "unique"
        } else if self.rf_over() == 1 {
            "duplicate"
        } else if self.rf_under.is_some() {
            "under-replicated"
        } else {
            "over-replicated"
        }
    }

    /// Returns an iterator over the input paths.
    /// Input paths may be provided as arguments or from standard input.
    pub fn input_paths(&self) -> Box<dyn Iterator<Item = Path> + Send> {
        if self.stdin {
            Box::new(
                BufReader::new(stdin())
                    .lines()
                    .map(|s| Path::from(s.unwrap().as_str())),
            )
        } else {
            Box::new(
                self.paths
                    .iter()
                    .map(Path::from)
                    .collect::<Vec<_>>()
                    .into_iter(),
            )
        }
    }

    /// Validates the transform command if defined; logs error and exits if broken
    pub fn check_transform(&self, log: &Log) {
        self.transform
            .as_ref()
            .map(|command| self.build_transform(command, log));
    }

    /// Constructs the transform object and clears the transform command from this object.
    pub fn take_transform(&mut self, log: &Log) -> Option<Transform> {
        self.transform
            .take()
            .and_then(|command| self.build_transform(&command, log).ok())
    }

    fn build_transform(&self, command: &str, log: &Log) -> io::Result<Transform> {
        Transform::new(command.to_string()).map_err(|e| {
            log.err(e);
            exit(1);
        })
    }
}
