use std::collections::HashMap;
use std::ffi::{OsStr, OsString};
use std::fs::File;
use std::io;
use std::io::{stdin, Write};
use std::process::exit;

use fallible_iterator::FallibleIterator;
use itertools::Itertools;
use rayon::iter::ParallelBridge;
use regex::Regex;
use structopt::StructOpt;

use fclones::config::{Command, Config, DedupeConfig, GroupConfig, Parallelism};
use fclones::log::Log;
use fclones::report::TextReportReader;
use fclones::{dedupe, log_script, run_script, DedupeOp};
use fclones::{group_files, write_report, Error};

/// Strips a red "error:" prefix and usage information added by clap.
/// Removes ansi formatting.
/// Joins all lines into a single line.
fn extract_error_cause(message: &str) -> String {
    let drop_ansi = Regex::new(r"\x1b\[[0-9;]*m").unwrap();
    let drop_error = Regex::new("error:[^ ]* ").unwrap();
    let message = drop_ansi.replace_all(message, "");
    let message = drop_error.replace(&message, "");
    message
        .split('\n')
        .take_while(|l| !l.starts_with("USAGE:"))
        .map(|l| l.trim())
        .filter(|l| !l.is_empty())
        .join(" ")
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

fn run_group(config: &GroupConfig, log: &mut Log) -> Result<(), Error> {
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

    log.info("Started grouping");
    let results = group_files(&config, &log).map_err(|e| Error::new(e.message))?;

    write_report(&config, &log, &results)
        .map_err(|e| Error::new(format!("Failed to write report: {}", e)))
}

/// Depending on the `output` configuration field, returns either a reference to the standard
/// output or a file opened for writing.
/// Reports error if the output file cannot be created.
fn get_output_writer(config: &DedupeConfig) -> Result<Box<dyn Write + Send>, Error> {
    match &config.output {
        Some(path) => {
            let f = File::create(path)
                .map_err(|e| format!("Failed to create output file {}: {}", path.display(), e))?;
            Ok(Box::new(f))
        }
        None => Ok(Box::new(io::stdout())),
    }
}

pub fn run_dedupe(op: DedupeOp, config: DedupeConfig, log: &mut Log) -> Result<(), Error> {
    let mut dedupe_config = config;
    let reader = TextReportReader::new(stdin()).map_err(|e| format!("Input error: {}", e))?;

    let find_config: Config = Config::from_iter_safe(&reader.header.command).map_err(|e| {
        let message: String = extract_error_cause(&e.message);
        format!("Unrecognized earlier fclones configuration: {}", message)
    })?;

    let rf_over = match find_config.command {
        Command::Group(c) => c.rf_over(),
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

    if dedupe_config.dry_run {
        log.info("Started deduplicating (dry run)");
    } else {
        log.info("Started deduplicating");
    }

    let mut result: Result<(), io::Error> = Ok(());
    let group_count = reader.header.stats.map(|s| s.group_count as u64);
    let progress = match group_count {
        _ if dedupe_config.dry_run && dedupe_config.output.is_none() => log.hidden(),
        Some(group_count) => log.progress_bar("Deduplicating", group_count),
        None => log.spinner("Deduplicating"),
    };

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
        .inspect(|_| progress.tick())
        .fuse()
        .flatten()
        .par_bridge();

    let script = dedupe(groups, op, &dedupe_config, log);
    if dedupe_config.dry_run {
        let out = get_output_writer(&dedupe_config)?;
        let result = log_script(script, out).map_err(|e| format!("Output error: {}", e))?;
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
    let config = Config::from_args();
    let mut log = Log::new();
    if config.quiet {
        log.no_progress = true;
    }

    let result = match config.command {
        Command::Group(config) => run_group(&config, &mut log),
        Command::Remove(config) => run_dedupe(DedupeOp::Remove, config, &mut log),
        Command::Link { config, soft: true } => run_dedupe(DedupeOp::SoftLink, config, &mut log),
        Command::Link {
            config,
            soft: false,
        } => run_dedupe(DedupeOp::HardLink, config, &mut log),
    };

    if let Err(e) = result {
        if !e.message.is_empty() {
            log.err(e);
        }
        exit(1);
    }
}

#[cfg(test)]
mod test {

    #[test]
    fn test_extract_error_cause_strips_error_prefix() {
        assert_eq!(super::extract_error_cause("error: foo"), "foo");
    }

    #[test]
    fn test_extract_error_cause_joins_lines() {
        assert_eq!(
            super::extract_error_cause("line1:\n    line2"),
            "line1: line2"
        );
    }

    #[test]
    fn test_extract_error_cause_strips_usage() {
        assert_eq!(
            super::extract_error_cause("error message\n\nUSAGE:\n blah blah blah"),
            "error message"
        );
    }
}
