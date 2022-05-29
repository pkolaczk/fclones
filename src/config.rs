//! Main program configuration.

use std::collections::HashMap;
use std::ffi::OsString;
use std::fmt::{Display, Formatter};
use std::io;
use std::io::{stdin, BufRead, BufReader, ErrorKind};
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;

use chrono::{DateTime, FixedOffset, Local};
use clap::AppSettings;
use structopt::StructOpt;

use crate::file::FileLen;
use crate::group::FileGroupFilter;
use crate::group::Replication::{Overreplicated, Underreplicated};
use crate::path::Path;
use crate::pattern::{Pattern, PatternError, PatternOpts};
use crate::selector::PathSelector;
use crate::transform::Transform;

#[derive(Debug, Clone, Copy)]
pub enum OutputFormat {
    Default,
    Fdupes,
    Csv,
    Json,
}

impl OutputFormat {
    pub fn variants() -> Vec<&'static str> {
        vec!["default", "fdupes", "csv", "json"]
    }
}

impl Display for OutputFormat {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            OutputFormat::Default => f.pad("default"),
            OutputFormat::Fdupes => f.pad("fdupes"),
            OutputFormat::Csv => f.pad("csv"),
            OutputFormat::Json => f.pad("json"),
        }
    }
}

impl FromStr for OutputFormat {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "default" => Ok(OutputFormat::Default),
            "fdupes" => Ok(OutputFormat::Fdupes),
            "csv" => Ok(OutputFormat::Csv),
            "json" => Ok(OutputFormat::Json),
            s => Err(format!("Unrecognized output format: {}", s)),
        }
    }
}

impl Default for OutputFormat {
    fn default() -> OutputFormat {
        OutputFormat::Default
    }
}

/// Parses date time string, accepts wide range of human-readable formats
fn parse_date_time(s: &str) -> Result<DateTime<FixedOffset>, String> {
    match dtparse::parse(s) {
        Ok((dt, Some(offset))) => Ok(DateTime::from_utc(dt, offset)),
        Ok((dt, None)) => {
            let local_offset = *Local::now().offset();
            Ok(DateTime::from_utc(dt, local_offset))
        }
        Err(e) => Err(format!("Failed to parse {} as date: {}", s, e)),
    }
}

/// Parses string with format: `<device>:<seq parallelism>[,<rand parallelism>]`
fn parse_thread_count_option(s: &str) -> Result<(OsString, Parallelism), String> {
    let (key, value) = if s.contains(':') {
        let index = s.rfind(':').unwrap();
        (&s[0..index], &s[(index + 1)..])
    } else {
        ("default", s)
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
    Ok((key, Parallelism { random, sequential }))
}

fn is_positive_int(v: String) -> Result<(), String> {
    if let Ok(f) = v.parse::<u64>() {
        if f > 0 {
            return Ok(());
        }
    }
    return Err(format!("Not a positive integer: {}", &*v));
}

#[derive(Clone, Copy, Debug)]
pub struct Parallelism {
    pub random: usize,
    pub sequential: usize,
}

// Configuration of the `group` subcommand
#[derive(Debug, StructOpt, Default)]
#[structopt(
    setting(AppSettings::ColoredHelp),
    setting(AppSettings::DeriveDisplayOrder),
    setting(AppSettings::DisableVersion)
)]
pub struct GroupConfig {
    /// Writes the report to a file instead of the standard output
    #[structopt(short = "o", long, value_name("path"))]
    pub output: Option<PathBuf>,

    /// Sets output file format
    #[structopt(short = "f", long, possible_values = &OutputFormat::variants(),
    case_insensitive = true, default_value="default")]
    pub format: OutputFormat,

    /// Reads the list of input paths from the standard input instead of the arguments.
    /// This flag is mostly useful together with Unix `find` utility.
    #[structopt(long)]
    pub stdin: bool,

    /// Limits recursion depth.
    ///
    /// 0 disables descending into directories.
    /// 1 descends into directories specified explicitly as input paths,
    /// but does not descend into subdirectories.
    #[structopt(short = "d", long)]
    pub depth: Option<usize>,

    /// Includes hidden files.
    #[structopt(short = ".", long)]
    pub hidden: bool,

    /// Does not ignore files matching patterns listed in .gitignore and .fdignore.
    #[structopt(short = "A", long)]
    pub no_ignore: bool,

    /// Follows symbolic links.
    ///
    /// If this flag is set together with `--symbolic-links`, only links to
    /// directories are followed.
    #[structopt(short = "L", long)]
    pub follow_links: bool,

    /// Treats files reachable from multiple paths through links as duplicates.
    ///
    /// If `--symbolic-links` is not set, only hard links are matched.
    /// If `--symbolic-links` is set, both hard and symbolic links are matched.
    #[structopt(short = "H", long)]
    pub match_links: bool,

    /// Doesn't ignore symbolic links to files.
    ///
    /// Reports symbolic links, not their targets.
    /// Symbolic links are not treated as duplicates of their targets,
    /// unless `--match-links` is set.
    #[structopt(short = "S", long)]
    pub symbolic_links: bool,

    /// Don't count matching files found within the same directory argument as duplicates.
    #[structopt(short("I"), long, conflicts_with("follow-links"))]
    pub isolate: bool,

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
    #[structopt(short("n"), long, conflicts_with("rf-under"), value_name("count"))]
    pub rf_over: Option<usize>,

    /// Searches for under-replicated files with replication factor below the specified value.
    /// Specifying `--rf-under 2` will report unique files.
    #[structopt(long, conflicts_with("rf-over"), value_name("count"))]
    pub rf_under: Option<usize>,

    /// Instead of searching for duplicates, searches for unique files.
    #[structopt(long, conflicts_with_all(&["rf-over", "rf-under"]))]
    pub unique: bool,

    /// Minimum file size in bytes. Units like KB, KiB, MB, MiB, GB, GiB are supported. Inclusive.
    #[structopt(short = "s", long("min"), default_value = "1", value_name("bytes"))]
    pub min_size: FileLen,

    /// Maximum file size in bytes. Units like KB, KiB, MB, MiB, GB, GiB are supported. Inclusive.
    #[structopt(long("max"), value_name("bytes"))]
    pub max_size: Option<FileLen>,

    /// Includes only file names matched fully by any of the given patterns.
    #[structopt(long = "name", value_name("pattern"))]
    pub name_patterns: Vec<String>,

    /// Includes only paths matched fully by any of the given patterns.
    #[structopt(long = "path", value_name("pattern"))]
    pub path_patterns: Vec<String>,

    /// Ignores paths matched fully by any of the given patterns.
    #[structopt(long = "exclude", value_name("pattern"))]
    pub exclude_patterns: Vec<String>,

    /// Makes pattern matching case-insensitive.
    #[structopt(short = "i", long)]
    pub ignore_case: bool,

    /// Expects patterns as Perl compatible regular expressions instead of Unix globs.
    #[structopt(short = "x", long)]
    pub regex: bool,

    /// Enables caching of file hashes.
    ///
    /// Caching can significantly speed up subsequent runs of `fclones group` by avoiding
    /// recomputations of hashes of the files that haven't changed since the last scan.
    /// Beware though, that this option relies on file modification times recorded by the
    /// file system. It also increases memory and storage space consumption.
    #[structopt(long)]
    pub cache: bool,

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

    /// Base directory to use when resolving relative input paths.
    #[structopt(long, parse(from_os_str), default_value("."))]
    pub base_dir: Path,

    /// A list of input paths.
    ///
    /// Accepts files and directories.
    /// By default descends into directories recursively, unless a recursion depth
    /// limit is specified with `--depth`.
    #[structopt(parse(from_os_str), required_unless("stdin"))]
    pub paths: Vec<Path>,
}

impl GroupConfig {
    fn validate(&self) -> Result<(), String> {
        if self.isolate && self.paths.len() <= self.rf_over() {
            return Err(format!(
                "The --isolate flag requires that the number of input paths ({}) \
                 is at least as large as the replication factor lower bound ({}). \
                 No files would be considered duplicate, regardless of their contents.",
                self.paths.len(),
                self.rf_over() + 1,
            ));
        }
        if self.isolate
            && (self.rf_under.is_some() || self.unique)
            && self.paths.len() < self.rf_under()
        {
            return Err(format!(
                "The --isolate flag requires that the number of input paths ({}) \
                 is larger than the replication factor upper bound ({}). \
                 All files would be considered unique or under-replicated, \
                 regardless of their contents.",
                self.paths.len(),
                self.rf_under() - 1,
            ));
        }

        Ok(())
    }

    fn compile_pattern(&self, s: &str) -> Result<Pattern, PatternError> {
        let pattern_opts = if self.ignore_case {
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

    pub fn group_filter(&self) -> FileGroupFilter {
        FileGroupFilter {
            replication: if self.unique {
                Underreplicated(2)
            } else if let Some(rf) = self.rf_under {
                Underreplicated(rf)
            } else {
                Overreplicated(self.rf_over())
            },
            root_paths: if self.isolate {
                self.input_paths().collect()
            } else {
                vec![]
            },
            group_by_id: !self.match_links,
        }
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
        } else if self.rf_under.is_some() {
            "under-replicated"
        } else {
            "redundant"
        }
    }

    /// Makes the base directory absolute.
    /// Returns error if the base directory does not exist.
    pub fn resolve_base_dir(&mut self) -> io::Result<&Path> {
        if self.base_dir.is_relative() {
            let curr_dir = Arc::from(Path::from(std::env::current_dir()?));
            self.base_dir = curr_dir.join(&self.base_dir)
        }
        if !self.base_dir.to_path_buf().is_dir() {
            return Err(io::Error::new(
                ErrorKind::NotFound,
                format!("Directory not found: {}", self.base_dir.to_escaped_string()),
            ));
        }
        self.base_dir = self.base_dir.canonicalize();
        Ok(&self.base_dir)
    }

    /// Returns an iterator over the absolute input paths.
    /// Input paths may be provided as arguments or from standard input.
    pub fn input_paths(&self) -> Box<dyn Iterator<Item = Path> + Send> {
        let base_dir = Arc::new(self.base_dir.clone());
        if self.stdin {
            Box::new(
                BufReader::new(stdin())
                    .lines()
                    .map(move |s| base_dir.resolve(Path::from(s.unwrap().as_str()))),
            )
        } else {
            Box::new(
                self.paths
                    .clone()
                    .into_iter()
                    .map(move |p| base_dir.resolve(p)),
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

/// Controls which files in a group should be removed / moved / replaced by links.
#[derive(Clone, Debug)]
pub enum Priority {
    /// Gives higher priority to the files with the most recent creation time.
    Newest,
    /// Gives higher priority to the files with the least recent creation time.
    Oldest,
    /// Gives higher priority to the files with the most recent modification time.
    MostRecentlyModified,
    /// Gives higher priority to the files with the least recent modification time.
    LeastRecentlyModified,
    /// Gives higher priority to the files with the most recent access time.
    MostRecentlyAccessed,
    /// Gives higher priority to the files with the least recent access time.
    LeastRecentlyAccessed,
    /// Gives higher priority to the files nested deeper in the directory tree.
    MostNested,
    /// Gives higher priority to the files nested shallower in the directory tree.
    LeastNested,
}

impl Priority {
    pub fn variants() -> Vec<&'static str> {
        vec![
            "newest",
            "oldest",
            "most-recently-modified",
            "least-recently-modified",
            "most-recently-accessed",
            "least-recently-accessed",
            "most-nested",
            "least-nested",
        ]
    }
}

impl FromStr for Priority {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "newest" => Ok(Priority::Newest),
            "oldest" => Ok(Priority::Oldest),
            "most-recently-modified" | "mrm" => Ok(Priority::MostRecentlyModified),
            "least-recently-modified" | "lrm" => Ok(Priority::MostRecentlyModified),
            "most-recently-accessed" | "mra" => Ok(Priority::MostRecentlyAccessed),
            "least-recently-accessed" | "lra" => Ok(Priority::LeastRecentlyAccessed),
            "most-nested" => Ok(Priority::MostNested),
            "least-nested" => Ok(Priority::LeastNested),
            _ => Err(format!("Unrecognized priority: {}", s)),
        }
    }
}

/// Configures which files should be removed
#[derive(Debug, Default, StructOpt)]
#[structopt(
    setting(AppSettings::DeriveDisplayOrder),
    setting(AppSettings::DisableVersion),
    setting(AppSettings::ColoredHelp)
)]
pub struct DedupeConfig {
    /// Doesn't perform any changes on the file-system, but writes a log of file operations
    /// to the standard output.
    #[structopt(long)]
    pub dry_run: bool,

    /// Writes the `dry_run` report to a file instead of the standard output.
    #[structopt(short = "o", long, value_name = "path")]
    pub output: Option<PathBuf>,

    /// Deduplicates only the files that were modified before the given time.
    ///
    /// If any of the files in a group was modified later, the whole group is skipped.
    #[structopt(long, short = "m", value_name = "timestamp", parse(try_from_str = parse_date_time))]
    pub modified_before: Option<DateTime<FixedOffset>>,

    /// Keeps at least n replicas untouched.
    ///
    /// If not given, it is assumed to be the same as the
    /// `--rf-over` value in the earlier `fclones group` run.
    #[structopt(short = "n", long, value_name = "count", validator(is_positive_int))]
    pub rf_over: Option<usize>,

    /// Restricts the set of files that can be removed or replaced by links to files
    /// with the name matching any given patterns.
    #[structopt(long = "name", value_name = "pattern")]
    pub name_patterns: Vec<Pattern>,

    /// Restricts the set of files that can be removed or replaced by links to files
    /// with the path matching any given patterns.
    #[structopt(long = "path", value_name = "pattern")]
    pub path_patterns: Vec<Pattern>,

    /// Sets the priority for files to be removed or replaced by links.
    #[structopt(long, value_name = "priority", possible_values = &Priority::variants())]
    pub priority: Vec<Priority>,

    /// Keeps files with names matching any given patterns untouched.
    #[structopt(long = "keep-name", value_name = "pattern")]
    pub keep_name_patterns: Vec<Pattern>,

    /// Keeps files with paths matching any given patterns untouched.
    #[structopt(long = "keep-path", value_name = "pattern")]
    pub keep_path_patterns: Vec<Pattern>,

    /// Specifies a list of path prefixes.
    /// If non-empty, all duplicates having the same path prefix (root) are treated as one.
    /// This also means that the files sharing the same root can be either all
    /// dropped or all retained.
    ///
    /// By default, it is set to the input paths given as arguments to the earlier
    /// `fclones group` command, if `--isolate` option was present.
    #[structopt(long = "isolate", value_name = "path", parse(from_os_str))]
    pub isolated_roots: Vec<Path>,

    /// Treats files reachable from multiple paths through links as duplicates.
    #[structopt(short = "H", long)]
    pub match_links: bool,

    /// Doesn't lock files before performing an action on them.
    #[structopt(long)]
    pub no_lock: bool,

    /// Allows the size of a file to be different than the size recorded during grouping.
    ///
    /// By default, files are checked for size to prevent accidentally removing a file
    /// that was modified since grouping.
    /// However, if `--transform` was used when grouping, the data sizes recorded in the `fclones group`
    /// report likely don't match the on-disk sizes of the files. Therefore,
    /// this flag is set automatically if `--transform` was used.
    #[structopt(long)]
    pub no_check_size: bool,
}

#[derive(Debug, StructOpt)]
pub enum Command {
    /// Produces a list of groups of identical files.
    ///
    /// Scans the given directories recursively, computes hashes of files and groups
    /// files with the same hash together.
    /// Writes the list of groups of files to the standard output, unless the target file
    /// is specified. This command is safe and does not modify the filesystem.
    Group(GroupConfig),

    /// Replaces redundant files with links.
    ///
    /// The list of groups earlier produced by `fclones group` should be submitted
    /// on the standard input.
    ///
    /// Unless `--soft` is specified, hard links are created for links within
    /// the same file system. Soft links are always created to link files between
    /// different file systems.
    Link {
        #[structopt(flatten)]
        config: DedupeConfig,

        /// Creates soft links.
        #[structopt(short, long)]
        soft: bool,
    },

    /// Deduplicates file data using native filesystem deduplication capabilities.
    ///
    /// The list of groups earlier produced by `fclones group` should be submitted
    /// on the standard input.
    ///
    /// After successful deduplication, all file clones would still be visible as distinct files,
    /// but the data would be stored only once, hence taking up possibly less space than before.
    /// Unlike with hard links, modifying a file does not modify any of its clones.
    /// The result is not visible to userland applications, so repeated runs
    /// will find the same files again. This also applies to `fclones dedupe` itself:
    /// The options `--priority` and `--rf-over` do not detect earlier deduplications.
    ///
    /// This command cannot cross file system boundaries.
    /// Not all file systems support deduplication.
    /// Not all metadata is preserved on macOS.
    /// Unsupported on Windows.
    Dedupe {
        #[structopt(flatten)]
        config: DedupeConfig,
    },

    /// Removes redundant files.
    ///
    /// The list of groups earlier produced by `fclones group` should be submitted
    /// on the standard input.
    Remove(DedupeConfig),

    /// Moves redundant files to the given directory.
    ///
    /// The list of groups earlier produced by `fclones group` should be submitted
    /// on the standard input.
    Move {
        #[structopt(flatten)]
        config: DedupeConfig,

        /// Target directory where the redundant files should be moved to.
        #[structopt(parse(from_os_str))]
        target: PathBuf,
    },
}

impl Command {
    pub fn validate(&self) -> Result<(), String> {
        match self {
            Command::Group(c) => c.validate(),
            _ => Ok(()),
        }
    }
}

/// Finds and cleans up redundant files
#[derive(Debug, StructOpt)]
#[structopt(
    name = "fclones",
    setting(AppSettings::ColoredHelp),
    setting(AppSettings::DeriveDisplayOrder),
    after_help = r#"EXAMPLES:

First call "fclones group -o duplicates.txt directory/" to find all duplicates
in a directory. See "fclones group --help" on how to modify this search.

Then you may call "fclones SUBCOMMAND < duplicates.txt", where SUBCOMMAND is one
of link, dedupe, remove or move. See the --help output of each subcommand
for more information. All have a --dry-run flag to preview changes.

It is also possible to combine the first and second step, e.g.:
"fclones group dir1/ dir2/ | fclones SUBCOMMAND""#
)]
pub struct Config {
    /// Suppresses progress reporting
    #[structopt(short("-q"), long)]
    pub quiet: bool,

    /// Finds files
    #[structopt(subcommand)]
    pub command: Command,
}
