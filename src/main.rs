use std::cell::RefCell;
use std::cmp::Reverse;
use std::env::current_dir;
use std::io::BufWriter;
use std::path::PathBuf;

use console::{style, Term};
use itertools::Itertools;
use regex::Regex;
use structopt::StructOpt;
use thread_local::ThreadLocal;

use fclones::config::*;
use fclones::files::*;
use fclones::group::*;
use fclones::log::Log;
use fclones::pattern::ESCAPE_CHAR;
use fclones::walk::Walk;
use indoc::indoc;
use fclones::report::Reporter;
use rayon::prelude::*;

const MIN_PREFIX_LEN: FileLen = FileLen(4096);
const MAX_PREFIX_LEN: FileLen = FileLen(2 * MIN_PREFIX_LEN.0);
const SUFFIX_LEN: FileLen = FileLen(4096);


struct AppCtx<'a> {
    config: &'a Config,
    log: &'a mut Log,
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

/// Walks the directory tree and collects matching files in parallel into a vector
fn scan_files(ctx: &mut AppCtx) -> Vec<Vec<FileInfo>> {
    let base_dir = current_dir().unwrap_or_default();
    let path_selector = ctx.config.path_selector(&ctx.log, &base_dir);
    let file_collector = ThreadLocal::new();
    let spinner = ctx.log.spinner("[1/6] Scanning files");
    let spinner_tick = &|_: &PathBuf| { spinner.tick() };

    let config = ctx.config;
    let min_size = config.min_size;
    let max_size = config.max_size.unwrap_or(FileLen::MAX);

    let mut walk = Walk::new();
    walk.recursive = config.recursive;
    walk.depth = config.depth.unwrap_or(usize::MAX);
    walk.skip_hidden = config.skip_hidden;
    walk.follow_links =  config.follow_links;
    walk.path_selector = path_selector;
    walk.log = Some(&ctx.log);
    walk.on_visit = spinner_tick;
    walk.run(ctx.config.input_paths(), |path| {
        file_info_or_log_err(path, &ctx.log)
            .into_iter()
            .filter(|info| info.len >= min_size && info.len <= max_size)
            .for_each(|info| {
                let vec = file_collector.get_or(|| RefCell::new(Vec::new()));
                vec.borrow_mut().push(info);
            });
    });

    ctx.log.info(format!("Scanned {} file entries", spinner.position()));

    let files: Vec<_> = file_collector.into_iter()
        .map(|r| r.into_inner()).collect();

    let file_count: usize = files.iter().map(|v| v.len()).sum();
    let total_size: u64 = files.iter().flat_map(|v| v.iter().map(|i| i.len.0)).sum();
    ctx.log.info(format!("Found {} ({}) files matching selection criteria",
                         file_count, FileLen(total_size)));
    files
}

fn group_by_size(ctx: &mut AppCtx, files: Vec<Vec<FileInfo>>) -> Vec<FileGroup> {
    let file_count: usize = files.iter().map(|v| v.len()).sum();
    let progress = ctx.log.progress_bar(
        "[2/6] Grouping by size", file_count as u64);

    let mut groups = GroupMap::new(|info: FileInfo| (info.len, info));
    for files in files.into_iter() {
        for file in files.into_iter() {
            progress.tick();
            groups.add(file);
        }
    }
    let rf_over = ctx.config.rf_over();
    let rf_under = ctx.config.rf_under();

    let groups: Vec<_> = groups
        .into_iter()
        .filter(|(_, files)| files.len() >= rf_over)
        .map(|(l, files)| (l, remove_duplicate_links_if_needed(&ctx.config, files)))
        .map(|(l, files)| FileGroup {
            len: l,
            hash: None,
            files: files.into_iter().map(|info| info.path).collect()
        })
        .collect();

    let count: usize = groups.selected_count(rf_over, rf_under);
    let bytes: FileLen = groups.selected_size(rf_over, rf_under);
    ctx.log.info(format!("Found {} ({}) candidates by grouping by size", count, bytes));
    groups
}

fn group_by_prefix(ctx: &mut AppCtx, groups: Vec<FileGroup>) -> Vec<FileGroup>
{
    let remaining_files = groups.iter()
        .filter(|g| g.files.len() > 1)
        .total_count();
    let progress = ctx.log.progress_bar(
        "[3/6] Grouping by prefix", remaining_files as u64);

    let rf_over = ctx.config.rf_over();
    let rf_under = ctx.config.rf_under();

    let groups: Vec<_> = groups.split(rf_over + 1, |len, _hash, path| {
        progress.tick();
        let prefix_len = if len <= MAX_PREFIX_LEN { len } else { MIN_PREFIX_LEN };
        file_hash_or_log_err(path, FilePos(0), prefix_len, |_| {}, &ctx.log)
    });

    let count = groups.selected_count(rf_over, rf_under);
    let bytes = groups.selected_size(rf_over, rf_under);
    ctx.log.info(format!("Found {} ({}) candidates by grouping by prefix", count, bytes));
    groups
}

fn group_by_suffix(ctx: &mut AppCtx, groups: Vec<FileGroup>) -> Vec<FileGroup>
{
    let needs_processing = |len: FileLen| len >= MAX_PREFIX_LEN + SUFFIX_LEN;
    let remaining_files = groups.iter()
        .filter(|&g| (needs_processing)(g.len) && g.files.len() > 1)
        .total_count();

    let progress = ctx.log.progress_bar(
        "[4/6] Grouping by suffix", remaining_files as u64);

    let rf_over = ctx.config.rf_over();
    let rf_under = ctx.config.rf_under();

    let groups: Vec<_> = groups.split(rf_over + 1, |len, hash, path| {
        if (needs_processing)(len) {
            progress.tick();
            file_hash_or_log_err(path, (len - SUFFIX_LEN).as_pos(), SUFFIX_LEN, |_|{}, &ctx.log)
        } else {
            hash
        }
    });

    let count = groups.selected_count(rf_over, rf_under);
    let bytes = groups.selected_size(rf_over, rf_under);
    ctx.log.info(format!("Found {} ({}) candidates by grouping by suffix", count, bytes));
    groups
}

fn group_by_contents(ctx: &mut AppCtx, groups: Vec<FileGroup>) -> Vec<FileGroup>
{
    let needs_processing = |len: FileLen| len >= MAX_PREFIX_LEN;
    let bytes_to_scan = groups.iter()
        .filter(|&g| (needs_processing)(g.len) && g.files.len() > 1)
        .total_size();
    let progress = ctx.log.bytes_progress_bar("[5/6] Grouping by contents", bytes_to_scan.0);
    let rf_over = ctx.config.rf_over();
    let rf_under = ctx.config.rf_under();

    let groups: Vec<_> = groups.split(rf_over + 1, |len, hash, path| {
        if (needs_processing)(len) {
            file_hash_or_log_err(path, FilePos(0), len, |delta| progress.inc(delta), &ctx.log)
        } else {
            hash
        }
    });

    let count = groups.selected_count(rf_over, rf_under);
    let bytes = groups.selected_size(rf_over, rf_under);
    ctx.log.info(format!("Found {} ({}) {} files", count, bytes, ctx.config.search_type()));
    groups
}

fn write_report(ctx: &mut AppCtx, groups: &mut Vec<FileGroup>) {
    let stdout = Term::stdout();
    let remaining_files = groups.total_count();
    let progress = ctx.log.progress_bar(
        "[6/6] Writing report", remaining_files as u64);

    groups.retain(|g| g.files.len() < ctx.config.rf_under());
    groups.par_sort_by_key(|g| Reverse(g.len));
    groups.par_iter_mut().for_each(|g| g.files.sort());

    // No progress bar when we write to terminal
    if stdout.is_term() {
        progress.finish_and_clear()
    }
    let out = BufWriter::new(stdout);
    let mut reporter = Reporter::new(out, progress);
    let result = match &ctx.config.format {
        OutputFormat::Text => reporter.write_as_text(groups),
        OutputFormat::Csv => reporter.write_as_csv(groups),
        OutputFormat::Json => reporter.write_as_json(groups),
    };
    match result {
        Ok(()) => (),
        Err(e) => ctx.log.err(format!("Failed to write report: {}", e))
    }
}

fn paint_help(s: &str) -> String {
    let escape_regex = Regex::new(r"\{escape}").unwrap();
    let code_regex = Regex::new(r"`([^`]+)`").unwrap();
    let title_regex = Regex::new(r"(?m)^# *(.*?)$").unwrap();
    let s = escape_regex.replace_all(s.as_ref(), ESCAPE_CHAR);
    let s = code_regex.replace_all(s.as_ref(), style("$1").green().to_string().as_str());
    title_regex.replace_all(s.as_ref(), style("$1").yellow().to_string().as_str()).to_string()
}

fn main() {
    let after_help = &paint_help(indoc!("
    # PATTERN SYNTAX:
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
        `{escape}`      escapes wildcards, e.g. `{escape}?` would match `?` literally
    "));

    let clap = Config::clap().after_help(after_help.as_str());
    let config: Config = Config::from_clap(&clap.get_matches());

    configure_thread_pool(config.threads);
    
    let mut log = Log::new();
    let mut ctx = AppCtx { log: &mut log, config: &config };

    let matching_files = scan_files(&mut ctx);
    let size_groups = group_by_size(&mut ctx, matching_files);
    let prefix_groups = group_by_prefix(&mut ctx, size_groups);
    let suffix_groups = group_by_suffix(&mut ctx, prefix_groups);
    let mut contents_groups= group_by_contents(&mut ctx, suffix_groups);
    write_report(&mut ctx, &mut contents_groups)
}