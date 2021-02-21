use std::collections::HashMap;
use std::ffi::{OsStr, OsString};
use std::io::{BufRead, BufReader, stdin};
use std::path::PathBuf;
use std::process::exit;

use clap::AppSettings;
use clap::arg_enum;
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
        Text, Fdupes, Csv, Json
    }
}

impl Default for OutputFormat {
    fn default() -> OutputFormat {
        OutputFormat::Text
    }
}

fn parse_thread_count_option(str: &str) -> Result<(OsString, usize), String> {
    let (key, value) = if str.contains(':') {
        let index = str.rfind(':').unwrap();
        (&str[0..index], &str[(index + 1)..])
    } else {
        ("default", str)
    };
    let key = OsString::from(key);
    let value = value.to_string();
    match value.parse() {
        Ok(v) => Ok((key, v)),
        Err(e) => Err(format!("{}: {}", e, value)),
    }
}

/// Finds duplicate, unique, under- or over-replicated files
#[derive(Debug, StructOpt, Default)]
#[structopt(
    name = "fclones",
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

    /// Before matching, transforms each file by the specified program.
    /// The value of this parameter should contain a command: the path to the program
    /// and optionally a list of space-separated arguments.
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
    /// when $IN parameter is specified in the --transform command.
    /// If this flag is present, $IN will point to the original file.
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

    /// Sets the size of a thread-pool with the given name.
    /// The name can be one of:
    /// (1) a physical block device when prefixed with `dev:` e.g. `dev:/dev/sda`;
    /// (2) a type of device - `ssd`, `hdd` or `unknown`;
    /// (3) a thread pool or thread pool group - `main`, `default`.
    /// If the name is not given, this option sets the size of the main thread pool
    /// and thread pools dedicated to SSD devices.
    /// This parameter can be used multiple times to configure multiple thread pools.
    #[structopt(short, long, value_name = "[name:]value", parse(try_from_str = parse_thread_count_option))]
    pub threads: Vec<(OsString, usize)>,

    /// Suppresses progress reporting
    #[structopt(short = "Q", long)]
    pub quiet: bool,

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
            .map(|command| self.build_transform(&command, log))
    }

    fn build_transform(&self, command: &str, log: &Log) -> Transform {
        match Transform::new(command.to_string(), self.in_place) {
            Ok(mut tr) => {
                if self.no_copy {
                    tr.copy = false
                };
                tr
            }
            Err(e) => {
                log.err(e);
                exit(1);
            }
        }
    }

    pub fn thread_pool_sizes(&self) -> HashMap<OsString, usize> {
        let mut map = HashMap::new();
        map.insert(OsString::from("hdd"), 1);
        map.insert(OsString::from("unknown"), 1);
        for (k, v) in self.threads.iter() {
            map.insert(k.clone(), *v);
        }
        map
    }

    /// Checks if `thread_pool_sizes` map contains unknown thread pool names.
    /// If unrecognized keys are found, prints an error message and terminates the program.
    /// The only allowed key name is: "default".
    /// This method should be called after thread pools have been configured and their entries
    /// removed from the map.
    pub fn check_thread_pools(&self, log: &Log, thread_pool_sizes: &mut HashMap<OsString, usize>) {
        thread_pool_sizes.remove(OsStr::new("default"));
        if !thread_pool_sizes.is_empty() {
            for (name, _) in thread_pool_sizes.iter() {
                let name = name.to_string_lossy();
                match name.strip_prefix("dev:") {
                    Some(name) => log.err(format!("Unknown device: {}", name)),
                    None => log.err(format!("Unknown thread pool or device type: {}", name)),
                }
            }
            exit(1);
        }
    }
}
