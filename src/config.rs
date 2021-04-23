//! Main program configuration.

use std::collections::HashMap;
use std::ffi::OsString;
use std::io::{stdin, BufRead, BufReader};
use std::path::PathBuf;

use clap::arg_enum;
use clap::AppSettings;
use structopt::StructOpt;

use crate::files::FileLen;
use crate::path::Path;
use crate::pattern::{Pattern, PatternError, PatternOpts};
use crate::selector::PathSelector;
use crate::transform::Transform;
use std::io;

arg_enum! {
    #[derive(Debug, StructOpt)]
    pub enum OutputFormat {
        Text, Fdupes, Csv, Json
    }
}

impl Default for OutputFormat {
    fn default() -> OutputFormat {
        OutputFormat::Text
    }
}

/// Parses string with format: `<device>:<seq parallelism>[,<rand parallelism>]`
fn parse_thread_count_option(str: &str) -> Result<(OsString, Parallelism), String> {
    let (key, value) = if str.contains(':') {
        let index = str.rfind(':').unwrap();
        (&str[0..index], &str[(index + 1)..])
    } else {
        ("default", str)
    };
    let key = OsString::from(key);
    let value = value.to_string();
    let mut pool_sizes = value
        .split(',')
        .map(|v| v.parse::<usize>().map_err(|e| format!("{}: {}", e, v)));

    let random = match pool_sizes.next() {
        Some(v) => v?,
        None => return Err(String::from("Missing pool size specification")),
    };
    let sequential = match pool_sizes.next() {
        Some(v) => v?,
        None => random,
    };
    Ok((key, Parallelism { sequential, random }))
}

#[derive(Clone, Copy, Debug)]
pub struct Parallelism {
    pub random: usize,
    pub sequential: usize,
}

// Configuration of the `find` subcommand
#[derive(Debug, StructOpt, Default)]
#[structopt(
    setting(AppSettings::ColoredHelp),
    setting(AppSettings::DeriveDisplayOrder),
    setting(AppSettings::DisableVersion),
)]
pub struct FindConfig {
    /// Writes the report to a file instead of the standard output
    #[structopt(short = "o", long, value_name("path"))]
    pub output: Option<PathBuf>,

    /// Sets output file format
    #[structopt(short = "f", long, possible_values = &OutputFormat::variants(),
    case_insensitive = true, default_value="Text")]
    pub format: OutputFormat,

    /// Reads the list of input paths from the standard input instead of the arguments.
    /// This flag is mostly useful together with Unix `find` utility.
    #[structopt(short = "I", long)]
    pub stdin: bool,

    /// Limits recursion depth.
    ///
    /// 0 disables descending into directories.
    /// 1 descends into directories specified explicitly as input paths,
    /// but does not descend into subdirectories.
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

    /// Before matching, transforms each file by the specified program.
    /// The value of this parameter should contain a command: the path to the program
    /// and optionally a list of space-separated arguments.
    ///
    /// By default, the file to process will be piped to the standard input of the program and the
    /// processed data will be read from the standard output.
    /// If the program does not support piping, but requires its input and/or output file path(s)
    /// to be specified in the argument list, denote these paths by $IN and $OUT special variables.
    /// If $IN is specified in the command string, the file will not be piped to the standard input,
    /// but copied first to a temporary location and that temporary location will be substituted
    /// as the value of $IN when launching the transform command.
    /// Similarly, if $OUT is specified in the command string, the result will not be read from
    /// the standard output, but fclones will expect the program to write to a named pipe
    /// specified by $OUT and will read output from there.
    /// If the program modifies the original file in-place without writing to the standard output
    /// nor a distinct file, use --in-place flag.
    #[structopt(long, value_name("command"))]
    pub transform: Option<String>,

    /// Set this flag if the command given to --transform transforms the file in-place,
    /// i.e. it modifies the original input file instead of writing to the standard output
    /// or to a new file. This flag tells fclones to read output from the original file
    /// after the transform command exited.
    #[structopt(long)]
    pub in_place: bool,

    /// Doesn't copy the file to a temporary location before transforming,
    /// when `$IN` parameter is specified in the `--transform` command.
    /// If this flag is present, `$IN` will point to the original file.
    /// Caution:
    /// this option may speed up processing, but it may cause loss of data because it lets
    /// the transform command to work directly on the original file.
    #[structopt(long)]
    pub no_copy: bool,

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

    /// Sets the sizes of thread-pools
    ///
    /// The spec has the following format: `[<name>:]<r>[,<s>]`.
    /// The name can be one of:
    /// (1) a physical block device when prefixed with `dev:` e.g. `dev:/dev/sda`;
    /// (2) a type of device - `ssd`, `hdd`, `removable` or `unknown`;
    /// (3) a thread pool or thread pool group - `main`, `default`.
    /// If the name is not given, this option sets the size of the main thread pool
    /// and thread pools dedicated to SSD devices.
    ///
    /// The values `r` and `s` are integers denoting the sizes of the
    /// thread-pools used respectively for random access I/O and sequential I/O.
    /// If `s` is not given, it is assumed to be the same as `r`.
    ///
    /// This parameter can be used multiple times to configure multiple thread pools.
    #[structopt(
      short,
      long,
      value_name = "spec",
      parse(try_from_str = parse_thread_count_option),
      verbatim_doc_comment)]
    pub threads: Vec<(OsString, Parallelism)>,

    /// A list of input paths.
    ///
    /// Accepts files and directories.
    /// By default descends into directories recursively, unless a recursion depth
    /// limit is specified with `--depth`.
    #[structopt(parse(from_os_str), required_unless("stdin"))]
    pub paths: Vec<PathBuf>,
}

impl FindConfig {
    fn compile_pattern(&self, s: &str) -> Result<Pattern, PatternError> {
        let pattern_opts = if self.caseless {
            PatternOpts::case_insensitive()
        } else {
            PatternOpts::default()
        };
        if self.regex {
            Pattern::regex_with(s, &pattern_opts)
        } else {
            Pattern::glob_with(s, &pattern_opts)
        }
    }

    pub fn path_selector(&self, base_dir: &Path) -> Result<PathSelector, PatternError> {
        let include_names: Result<Vec<Pattern>, PatternError> = self
            .name_patterns
            .iter()
            .map(|p| self.compile_pattern(p))
            .collect();
        let include_paths: Result<Vec<Pattern>, PatternError> = self
            .path_patterns
            .iter()
            .map(|p| self.compile_pattern(p))
            .collect();
        let exclude_paths: Result<Vec<Pattern>, PatternError> = self
            .exclude_patterns
            .iter()
            .map(|p| self.compile_pattern(p))
            .collect();

        Ok(PathSelector::new(base_dir.clone())
            .include_names(include_names?)
            .include_paths(include_paths?)
            .exclude_paths(exclude_paths?))
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

    fn build_transform(&self, command: &str) -> io::Result<Transform> {
        let mut tr = Transform::new(command.to_string(), self.in_place)?;
        if self.no_copy {
            tr.copy = false
        };
        Ok(tr)
    }

    /// Constructs the transform object.
    /// Returns None if the transform was not set
    pub fn transform(&self) -> Option<io::Result<Transform>> {
        self.transform
            .as_ref()
            .map(|command| self.build_transform(command))
    }

    pub fn thread_pool_sizes(&self) -> HashMap<OsString, Parallelism> {
        let mut map = HashMap::new();
        for (k, v) in self.threads.iter() {
            map.insert(k.clone(), *v);
        }
        map
    }
}

#[derive(Debug, StructOpt)]
#[structopt(
    setting(AppSettings::DisableVersion),
    setting(AppSettings::ColoredHelp)
)]
pub struct DedupeConfig {
    /// Don't perform any changes on the file-system; only log what would be done
    #[structopt(long)]
    dry_run: bool,

    /// Path to the file containing the output of earlier `fclones find ...` execution
    #[structopt(parse(from_os_str))]
    file: PathBuf
}

#[derive(Debug, StructOpt)]
pub enum Command {
    /// Finds duplicate, unique, under- or over-replicated files
    Find(FindConfig),
    /// Removes redundant files earlier found by `find`
    Remove(DedupeConfig),
    /// Replaces redundant files earlier found by `find` with soft links
    SoftLink(DedupeConfig),
    /// Replaces redundant files earlier found by `find` with hard links
    HardLink(DedupeConfig),
}


/// Finds and cleans up redundant files
#[derive(Debug, StructOpt)]
#[structopt(
    name = "fclones",
    setting(AppSettings::ColoredHelp),
    setting(AppSettings::DeriveDisplayOrder)
)]
pub struct Config {
    /// Suppresses progress reporting
    #[structopt(short = "Q", long)]
    pub quiet: bool,

    /// Finds files
    #[structopt(subcommand)]
    pub command: Command
}
