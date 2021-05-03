use std::fs::File;
use std::process::exit;

use console::style;
use indoc::indoc;
use regex::Regex;
use structopt::StructOpt;

use fclones::config::{Command, Config, DedupeConfig, FindConfig, Parallelism};
use fclones::log::Log;
use fclones::path::PATH_ESCAPE_CHAR;

use fclones::{group_files, write_report};

use fclones::report::TextReportReader;
use std::collections::HashMap;
use std::ffi::{OsStr, OsString};

fn paint_help(s: &str) -> String {
    let escape_regex = "{escape}";
    let code_regex = Regex::new(r"`([^`]+)`").unwrap();
    let title_regex = Regex::new(r"(?m)^# *(.*?)$").unwrap();
    let s = s.replace(escape_regex, PATH_ESCAPE_CHAR);
    let s = code_regex.replace_all(s.as_ref(), style("$1").green().to_string().as_str());
    title_regex
        .replace_all(s.as_ref(), style("$1").yellow().to_string().as_str())
        .to_string()
}

/// Configures global thread pool to use desired number of threads
fn configure_main_thread_pool(pool_sizes: &HashMap<OsString, Parallelism>) {
    let parallelism = pool_sizes.get(OsStr::new("main")).unwrap_or_else(|| {
        pool_sizes
            .get(OsStr::new("default"))
            .unwrap_or(&Parallelism {
                sequential: 0,
                random: 0,
            })
    });

    rayon::ThreadPoolBuilder::new()
        .num_threads(parallelism.random)
        .build_global()
        .unwrap();
}

fn find(config: &FindConfig, log: &mut Log) {
    let mut access_error = false;
    let mut has_input_files: bool = false;
    for path in config.paths.iter() {
        match std::fs::metadata(path) {
            Ok(metadata) if metadata.is_dir() && config.depth == Some(0) => log.warn(format!(
                "Skipping directory {} because recursive scan is disabled.",
                path.display()
            )),
            Err(e) => {
                log.err(format!("Can't access {}: {}", path.display(), e));
                access_error = true;
            }
            Ok(_) => has_input_files = true,
        }
    }

    if access_error {
        exit(1);
    }
    if !has_input_files {
        log.err("No input files");
        exit(1);
    }

    configure_main_thread_pool(&config.thread_pool_sizes());
    if let Some(output) = &config.output {
        // Try to create the output file now and fail early so that
        // the user doesn't waste time to only find that the report cannot be written at the end:
        if let Err(e) = File::create(output) {
            log.err(format!(
                "Cannot create output file {}: {}",
                output.display(),
                e
            ));
            exit(1);
        }
    }

    let results = match group_files(&config, &log) {
        Ok(groups) => groups,
        Err(e) => {
            log.err(e);
            exit(1)
        }
    };

    if let Err(e) = write_report(&config, &log, &results) {
        log.err(format!("Failed to write report: {}", e))
    }
}

enum DedupeOp {
    Remove,
    SoftLink,
    HardLink,
}

fn dedupe(config: &DedupeConfig, _op: DedupeOp, log: &mut Log) {
    let path = &config.file;
    let file = match File::open(&config.file) {
        Ok(file) => file,
        Err(e) => {
            log.err(format!("Cannot open {}: {}", path.display(), e));
            exit(1)
        }
    };
    let _reader = match TextReportReader::new(file) {
        Ok(reader) => reader,
        Err(e) => {
            log.err(format!("Error reading {}: {}", path.display(), e));
            exit(1)
        }
    };
}

fn main() {
    let mut log = Log::new();
    let _after_help = &paint_help(indoc!(
        "
    # PATTERN SYNTAX:
        Options `-n` `-p` and `-e` accept extended glob patterns.
        The following wildcards can be used:
        `?`         matches any character except the directory separator
        `[a-z]`     matches one of the characters or character ranges given in the square brackets
        `[!a-z]`    matches any character that is not given in the square brackets
        `*`         matches any sequence of characters except the directory separator
        `**`        matches any sequence of characters
        `{a,b}`     matches exactly one pattern from the comma-separated patterns given
                  inside the curly brackets
        `@(a|b)`    same as `{a,b}`
        `?(a|b)`    matches at most one occurrence of the pattern inside the brackets
        `+(a|b)`    matches at least occurrence of the patterns given inside the brackets
        `*(a|b)`    matches any number of occurrences of the patterns given inside the brackets
        `{escape}`         escapes wildcards, e.g. `{escape}?` would match `?` literally
    "
    ));

    let clap = Config::clap(); // .after_help(after_help.as_str());

    let matches = clap.get_matches_safe().unwrap_or_else(|e| {
        // a hack to remove "error: " from the message,
        // until we switch to Clap 3.x, which will have the `cause` field
        let r = Regex::new("[^e]*error:[^ ]* ").unwrap();
        let message = e.message.as_str();
        if r.is_match(message) {
            log.err(r.replace(e.message.as_str(), ""));
        } else {
            println!("{}", e.message);
        }
        exit(1);
    });
    let config: Config = Config::from_clap(&matches);
    if config.quiet {
        log.no_progress = true;
    }

    match config.command {
        Command::Find(config) => find(&config, &mut log),
        Command::Remove(config) => dedupe(&config, DedupeOp::Remove, &mut log),
        Command::SoftLink(config) => dedupe(&config, DedupeOp::SoftLink, &mut log),
        Command::HardLink(config) => dedupe(&config, DedupeOp::HardLink, &mut log),
    }
}
