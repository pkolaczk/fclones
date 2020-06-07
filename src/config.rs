use std::io::{BufRead, BufReader, stdin};
use std::process::exit;

use clap::AppSettings;
use clap::arg_enum;
use structopt::StructOpt;

use crate::files::FileLen;
use crate::log::Log;
use crate::pattern::{Pattern, PatternOpts};
use crate::selector::PathSelector;
use crate::path::Path;
use std::path::PathBuf;

arg_enum! {
    #[derive(Debug, StructOpt)]
    pub enum OutputFormat {
        Text, Csv, Json
    }
}


/// Finds duplicate, unique, under- or over-replicated files
#[derive(Debug, StructOpt)]
#[structopt(
name = "File Clones",
setting(AppSettings::ColoredHelp),
setting(AppSettings::DeriveDisplayOrder),
)]
pub struct Config {

    /// Sets output file format
    #[structopt(short="f", long, possible_values = &OutputFormat::variants(),
    case_insensitive = true, default_value="Text")]
    pub format: OutputFormat,

    /// Reads the list of input paths from the standard input instead of the arguments.
    /// This flag is mostly useful together with `find` utility.
    #[structopt(short="I", long)]
    pub stdin: bool,

    /// Descends into directories recursively
    #[structopt(short="R", long)]
    pub recursive: bool,

    /// Limits recursion depth
    #[structopt(short="d", long)]
    pub depth: Option<usize>,

    /// Skips hidden files
    #[structopt(short="A", long)]
    pub skip_hidden: bool,

    /// Follows symbolic links
    #[structopt(short="L", long)]
    pub follow_links: bool,

    /// Treats files reachable from multiple paths through
    /// hard links as duplicates
    #[structopt(short="H", long)]
    pub no_prune_links: bool,

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

    /// Minimum file size. Inclusive.
    #[structopt(short="s", long, default_value="1")]
    pub min_size: FileLen,

    /// Maximum file size. Inclusive.
    #[structopt(long)]
    pub max_size: Option<FileLen>,

    /// Includes only file names matched fully by any of the given patterns.
    #[structopt(short="n", long="names")]
    pub name_patterns: Vec<String>,

    /// Includes only paths matched fully by any of the given patterns.
    #[structopt(short="p", long="paths")]
    pub path_patterns: Vec<String>,

    /// Excludes paths matched fully by any of the given patterns.
    #[structopt(short="e", long="exclude")]
    pub exclude_patterns: Vec<String>,

    /// Makes pattern matching case-insensitive
    #[structopt(short="i", long)]
    pub caseless: bool,

    /// Expects patterns as Perl compatible regular expressions instead of Unix globs
    #[structopt(short="g", long)]
    pub regex: bool,

    /// Parallelism level.
    /// If set to 0, the number of CPU cores reported by the operating system is used.
    #[structopt(short, long, default_value="0")]
    pub threads: usize,

    /// A list of input paths. Accepts files and directories.
    #[structopt(parse(from_os_str), required_unless("stdin"))]
    pub paths: Vec<PathBuf>,
}

impl Config {
    pub fn path_selector(&self, log: &Log, base_dir: &Path) -> PathSelector {
        let pattern_opts =
            if self.caseless { PatternOpts::case_insensitive() }
            else { PatternOpts::default() };
        let pattern = |s: &String| {
            let pattern =
                if self.regex {
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
        self.rf_over.unwrap_or_else(|| if self.rf_under.is_some() || self.unique { 0 } else { 1 })
    }

    pub fn rf_under(&self) -> usize {
        if self.unique { 2 }
        else { self.rf_under.unwrap_or(usize::MAX) }
    }

    pub fn search_type(&self) -> &'static str {
        if self.rf_under.is_some() { "under-replicated" } else { "over-replicated" }
    }

    /// Returns an iterator over the input paths.
    /// Input paths may be provided as arguments or from standard input.
    pub fn input_paths(&self) -> Box<dyn Iterator<Item=Path> + Send> {
        if self.stdin {
            Box::new(BufReader::new(stdin())
                .lines()
                .into_iter()
                .map(|s| Path::from(s.unwrap().as_str())))
        } else {
            Box::new(
                self.paths.iter()
                    .map(|p| Path::from(p))
                    .collect::<Vec<_>>().into_iter())
        }
    }
}

