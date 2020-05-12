use std::cell::RefCell;
use std::cmp::Reverse;
use std::io::{BufWriter, Write};
use std::io::stdout;
use std::path::PathBuf;

use clap::AppSettings;
use itertools::Itertools;
use rayon::iter::ParallelIterator;
use structopt::StructOpt;
use thread_local::ThreadLocal;

use dff::files::*;
use dff::group::*;
use dff::progress::FastProgressBar;
use dff::report::Report;
use dff::selector::PathSelector;
use dff::walk::Walk;
use dff::pattern::{Pattern, PatternOpts};
use ansi_term::Colour;
use regex::Regex;
use std::process::exit;

const MIN_PREFIX_LEN: FileLen = FileLen(4096);
const MAX_PREFIX_LEN: FileLen = FileLen(2 * MIN_PREFIX_LEN.0);
const SUFFIX_LEN: FileLen = FileLen(4096);

/// Searches filesystem(s) and reports redundant files
///
#[derive(Debug, StructOpt)]
#[structopt(
    name = "Duplicate File Finder",
    setting(AppSettings::ColoredHelp),
    setting(AppSettings::DeriveDisplayOrder),
)]
struct Config {

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
    pub duplicate_links: bool,

    /// Minimum file size. Inclusive.
    #[structopt(short="s", long, default_value="1")]
    pub min_size: FileLen,

    /// Maximum file size. Inclusive.
    #[structopt(short="x", long)]
    pub max_size: Option<FileLen>,

    /// Includes only file names matched fully by any of the given patterns.
    #[structopt(short="n", long="names")]
    pub name_patterns: Vec<String>,

    /// Includes only paths matched fully by any of the given patterns.
    #[structopt(short="p", long="paths")]
    pub path_patterns: Vec<String>,

    /// Excludes paths matched fully by any of the given patterns.
    /// Accepts the same pattern syntax as -p.
    #[structopt(short="e", long="exclude")]
    pub exclude_patterns: Vec<String>,

    /// Makes pattern matching case-insensitive
    #[structopt(long)]
    pub caseless: bool,

    /// Expects patterns as Perl compatible regular expressions instead of Unix globs
    #[structopt(short="g", long)]
    pub regex: bool,

    /// Parallelism level.
    /// If set to 0, the number of CPU cores reported by the operating system is used.
    #[structopt(short, long, default_value="0")]
    pub threads: usize,

    /// A list of input paths
    #[structopt(parse(from_os_str), required = true)]
    pub paths: Vec<PathBuf>,
}

impl Config {
    fn path_selector(&self, base_dir: &PathBuf) -> PathSelector {
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
                eprintln!("{}: {}", Colour::Red.paint("error"), e);
                exit(1);
            })
        };

        PathSelector::new(base_dir.clone())
            .include_names(self.name_patterns.iter().map(pattern).collect())
            .include_paths(self.path_patterns.iter().map(pattern).collect())
            .exclude_paths(self.exclude_patterns.iter().map(pattern).collect())
    }
}


/// Configures global thread pool to use desired number of threads
fn configure_thread_pool(parallelism: usize) {
    rayon::ThreadPoolBuilder::new()
        .num_threads(parallelism)
        .build_global()
        .unwrap();
}

/// Unless `duplicate_links` is set to true,
/// remove duplicated `FileInfo` entries with the same inode and device id from the list.
fn remove_duplicate_links_if_needed(config: &Config, files: Vec<FileInfo>) -> Vec<FileInfo> {
    if config.duplicate_links {
        files
    } else {
        files
            .into_iter()
            .unique_by(|info| (info.dev_id, info.file_id))
            .collect()
    }
}

fn scan_files(report: &mut Report, config: &Config) -> Vec<Vec<FileInfo>> {
    let spinner = FastProgressBar::new_spinner("[1/6] Scanning files");
    let logger = spinner.logger();

    // Walk the tree and collect matching files in parallel:
    let file_collector = ThreadLocal::new();
    let mut walk = Walk::new();
    walk.recursive = config.recursive;
    walk.depth = config.depth.unwrap_or(usize::MAX);
    walk.skip_hidden = config.skip_hidden;
    walk.follow_links =  config.follow_links;
    walk.path_selector = config.path_selector(&walk.base_dir);
    walk.logger = &logger;
    walk.run(config.paths.clone(), |path| {
        spinner.tick();
        file_info_or_log_err(path, &logger)
            .into_iter()
            .filter(|info|
                info.len >= config.min_size && info.len <= config.max_size.unwrap_or(FileLen::MAX))
            .for_each(|info| {
                let vec = file_collector.get_or(|| RefCell::new(Vec::new()));
                vec.borrow_mut().push(info);
            });
    });

    report.scanned_files(spinner.position());
    file_collector.into_iter().map(|r| r.into_inner()).collect()
}

fn group_by_size(report: &mut Report, config: &Config, files: Vec<Vec<FileInfo>>)
                 -> Vec<(FileLen, Vec<PathBuf>)>
{
    let file_count: usize = files.iter().map(|v| v.len()).sum();
    let progress = FastProgressBar::new_progress_bar(
        "[2/6] Grouping by size", file_count as u64);

    let groups = GroupMap::new(|info: FileInfo| (info.len, info));
    for files in files.into_iter() {
        for file in files.into_iter() {
            progress.tick();
            groups.add(file);
        }
    }
   let groups = groups
        .into_iter()
        .filter(|(_, files)| files.len() >= 2)
        .map(|(l, files)| (l, remove_duplicate_links_if_needed(&config, files)))
        .map(|(l, files)| (l, files.into_iter().map(|info| info.path).collect()))
        .collect();

    report.stage_finished("Group by size", &groups);
    groups
}

fn group_by_prefix(report: &mut Report, groups: Vec<(FileLen, Vec<PathBuf>)>)
                   -> Vec<((FileLen, FileHash), Vec<PathBuf>)>
{
    let remaining_files = count_values(&groups);
    let progress = FastProgressBar::new_progress_bar(
        "[3/6] Grouping by prefix", remaining_files as u64);
    let log = progress.logger();

    let groups = split_groups(groups, 2, |&len, path| {
        progress.tick();
        let prefix_len = if len <= MAX_PREFIX_LEN { len } else { MIN_PREFIX_LEN };
        file_hash_or_log_err(path, FilePos(0), prefix_len, &log)
            .map(|h| (len, h))
    }).collect();

    report.stage_finished("Group by prefix", &groups);
    groups
}

fn group_by_suffix(report: &mut Report, groups: Vec<((FileLen, FileHash), Vec<PathBuf>)>)
                   -> Vec<((FileLen, FileHash), Vec<PathBuf>)>
{
    let remaining_files = count_values(&groups);
    let progress = FastProgressBar::new_progress_bar(
        "[4/6] Grouping by suffix", remaining_files as u64);
    let log = progress.logger();

    let groups = split_groups(groups, 2, |&(len, hash), path| {
        progress.tick();
        if len >= MAX_PREFIX_LEN + SUFFIX_LEN {
            file_hash_or_log_err(path, (len - SUFFIX_LEN).as_pos(), SUFFIX_LEN, &log)
                .map(|h| (len, h))
        } else {
            Some((len, hash))
        }
    }).collect();

    report.stage_finished("Group by suffix", &groups);
    groups
}

fn group_by_contents(report: &mut Report, groups: Vec<((FileLen, FileHash), Vec<PathBuf>)>)
                     -> Vec<((FileLen, FileHash), Vec<PathBuf>)>
{
    let remaining_files = count_values(&groups);
    let progress = FastProgressBar::new_progress_bar(
        "[5/6] Grouping by contents", remaining_files as u64);
    let log = progress.logger();

    let groups = split_groups(groups, 2, |&(len, hash), path| {
        progress.tick();
        if len > MAX_PREFIX_LEN {
            file_hash_or_log_err(path, FilePos(0), len, &log).map(|h| (len, h))
        } else {
            Some((len, hash))
        }
    }).collect();

    report.stage_finished("Group by contents", &groups);
    groups
}

fn write_report(report: &mut Report, groups: &mut Vec<((FileLen, FileHash), Vec<PathBuf>)>) {
    let remaining_files = count_values(&groups);
    let progress = FastProgressBar::new_progress_bar(
        "[6/6] Writing report", remaining_files as u64);
    groups.sort_by_key(|&((len, _), _)| Reverse(len));
    report.write(&mut std::io::stdout()).expect("Failed to write report");
    let mut out = BufWriter::new(stdout());
    writeln!(out).unwrap();
    for ((len, hash), files) in groups {
        writeln!(out, "{} {}:", hash, len).unwrap();
        for f in files {
            progress.tick();
            writeln!(out, "    {}", f.display()).unwrap();
        }
    }
}

fn paint_code(s: &str) -> String {
    let r = Regex::new(r"`([^`]+)`").unwrap();
    r.replace_all(s,  Colour::Green.paint("$1").to_string().as_str()).to_string()
}


fn main() {
    let after_help =
        Colour::Yellow.paint("PATTERN SYNTAX:").to_string() +
        &paint_code("
    Options `-n` `-p` and `-e` accept extended glob patterns.
    The following wildcards can be used:
      `?`      matches any character except the directory separator
      `[a-z]`  matches one of the characters or character ranges given in the square brackets
      `[!a-z]` matches any character that is not given in the square brackets
      `*`      matches any sequence of characters except the directory separator
      `**`     matches any sequence of characters
      `{a,b}`  matches exactly one pattern from the comma-separated patterns given
             inside the curly brackets
      `@(a|b)` same as `{a,b}`
      `?(a|b)` matches at most one occurrence of the pattern inside the brackets
      `+(a|b)` matches at least occurrence of the patterns given inside the brackets
      `*(a|b)` matches any number of occurrences of the patterns given inside the brackets
      `!(a|b)` matches anything that doesn't match any of the patterns given inside the brackets
    ");

    let clap = Config::clap()
        .after_help(after_help.as_str());
    let config: Config = Config::from_clap(&clap.get_matches());

    configure_thread_pool(config.threads);

    let mut report = Report::new();
    let matching_files = scan_files(&mut report, &config);
    let size_groups = group_by_size(&mut report, &config, matching_files);
    let prefix_groups = group_by_prefix(&mut report, size_groups);
    let suffix_groups = group_by_suffix(&mut report, prefix_groups);
    let mut contents_groups= group_by_contents(&mut report, suffix_groups);
    write_report(&mut report, &mut contents_groups)
}