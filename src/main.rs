use std::collections::HashMap;
use std::ffi::{OsStr, OsString};
use std::fs::File;
use std::io;
use std::process::exit;

use fallible_iterator::FallibleIterator;
use regex::Regex;
use structopt::StructOpt;

use fclones::config::{Command, Config, DedupeCommand, FindConfig, Parallelism};
use fclones::log::Log;
use fclones::report::TextReportReader;
use fclones::{dedupe, log_script, run_script, DedupeOp};
use fclones::{group_files, write_report, Error};

/// Strips a red "error:" prefix and usage information added by clap.
/// We don't want this, because we're using our own logging anyways.
fn extract_error_cause(message: &str) -> String {
    let r = Regex::new("[^e]*error:[^ ]* ").unwrap();
    let message = if r.is_match(message) {
        r.replace(message, "").to_string()
    } else {
        message.to_owned()
    };
    message
    //message.chars().take_while(|&c| c != '\n').collect()
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

fn run_find(config: &FindConfig, log: &mut Log) -> Result<(), Error> {
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
        return Err(Error::from(""));
    }
    if !has_input_files {
        return Err(Error::from("No input files"));
    }

    configure_main_thread_pool(&config.thread_pool_sizes());
    if let Some(output) = &config.output {
        // Try to create the output file now and fail early so that
        // the user doesn't waste time to only find that the report cannot be written at the end:
        if let Err(e) = File::create(output) {
            return Err(Error::new(format!(
                "Cannot create output file {}: {}",
                output.display(),
                e
            )));
        }
    }

    let results = group_files(&config, &log).map_err(|e| Error::new(e.message))?;

    write_report(&config, &log, &results)
        .map_err(|e| Error::new(format!("Failed to write report: {}", e)))
}

pub fn run_dedupe(op: DedupeOp, command: DedupeCommand, log: &mut Log) -> Result<(), Error> {
    let path = command.file;
    let file = File::open(&path).map_err(|e| format!("Cannot open {}: {}", path.display(), e))?;
    let reader = TextReportReader::new(file)
        .map_err(|e| format!("Error reading {}: {}", path.display(), e))?;

    let find_config: Config = Config::from_iter_safe(&reader.header.command).map_err(|e| {
        let message: String = extract_error_cause(&e.message);
        format!("Unrecognized earlier fclones configuration: {}", message)
    })?;

    let mut dedupe_config = command.config;
    let rf_over = match find_config.command {
        Command::Find(c) => c.rf_over(),
        _ if dedupe_config.rf_over.is_some() => dedupe_config.rf_over.unwrap(),
        _ => {
            return Err(Error::from(
                "Could not extract --rf-over setting from the earlier fclones configuration.",
            ))
        }
    };

    dedupe_config.rf_over = Some(rf_over);
    if dedupe_config.modified_before.is_none() {
        dedupe_config.modified_before = Some(reader.header.timestamp);
    }
    let mut result: Result<(), io::Error> = Ok(());
    let groups = reader
        .groups
        .iterator()
        .map(|g| match g {
            Ok(g) => Some(g),
            Err(e) => {
                result = Err(e);
                None
            }
        })
        .fuse()
        .flatten();

    let script = dedupe(groups, op, &dedupe_config, log);
    if dedupe_config.dry_run {
        let result = log_script(script);
        log.info(format!(
            "Would process {} files and reclaim {} space",
            result.processed_count, result.reclaimed_space
        ));
    } else {
        let result = run_script(script, log);
        log.info(format!(
            "Processed {} files and reclaimed {} space",
            result.processed_count, result.reclaimed_space
        ));
    };
    result.map_err(|e| Error::new(format!("Failed to read file list: {}", e)))
}

fn main() {
    let mut log = Log::new();

    let clap = Config::clap();
    let matches = clap.get_matches_safe().unwrap_or_else(|e| {
        if e.message.contains("error:") {
            let message = extract_error_cause(&e.message);
            log.err(message);
        } else {
            println!("{}", e.message)
        }
        exit(1);
    });
    let config: Config = Config::from_clap(&matches);
    if config.quiet {
        log.no_progress = true;
    }

    let result = match config.command {
        Command::Find(config) => run_find(&config, &mut log),
        Command::Remove(cmd) => run_dedupe(DedupeOp::Remove, cmd, &mut log),
        Command::SoftLink(cmd) => run_dedupe(DedupeOp::SoftLink, cmd, &mut log),
        Command::HardLink(cmd) => run_dedupe(DedupeOp::HardLink, cmd, &mut log),
    };

    if let Err(e) = result {
        if !e.message.is_empty() {
            log.err(e);
        }
        exit(1);
    }
}
