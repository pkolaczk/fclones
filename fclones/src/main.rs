use std::collections::HashMap;
use std::ffi::{OsStr, OsString};
use std::fs::File;
use std::io::{stdin, Write};
use std::process::exit;
use std::sync::Arc;
use std::{fs, io};

use clap::Parser;
use console::style;
use fallible_iterator::FallibleIterator;
use itertools::Itertools;
use regex::Regex;

use fclones::config::{Command, Config, DedupeConfig, GroupConfig, Parallelism};
use fclones::log::{Log, LogExt, ProgressBarLength, StdLog};
use fclones::progress::{NoProgressBar, ProgressTracker};
use fclones::report::{open_report, ReportHeader};
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

/// Returns error if any of the input paths doesn't exist or if input paths list is empty.
fn check_input_paths_exist(config: &GroupConfig, log: &dyn Log) -> Result<(), Error> {
    // Unfortunately we can't fail fast here when the list of files
    // is streamed from the standard input, because we'd have to collect all paths into a vector
    // list first, but we don't want to do this because there may be many.
    // In that case, we just let the lower layers handle eventual
    // problems and report as warnings.
    if config.stdin {
        return Ok(());
    }

    // If files aren't streamed on stdin, we can inspect all of them now
    // and exit early on any access error. If depth is set to 0 (recursive scan disabled)
    // we also want to filter out directories and terminate with an error if there are
    // no files in the input.
    let mut access_error = false;
    let depth = config.depth;
    let input_paths = config
        .input_paths()
        .filter(|p| match fs::metadata(p.to_path_buf()) {
            Ok(m) if m.is_dir() && depth == Some(0) => {
                log.warn(format!(
                    "Skipping directory {} because recursive scan is disabled.",
                    p.display()
                ));
                false
            }
            Err(e) => {
                log.err(format!("Can't access {}: {}", p.display(), e));
                access_error = true;
                false
            }
            Ok(_) => true,
        })
        .collect_vec();
    if access_error {
        return Err(Error::from("Some input paths could not be accessed."));
    }
    if input_paths.is_empty() {
        return Err(Error::from("No input files."));
    }
    Ok(())
}

/// Attempts to create the output file and returns an error if it fails.
fn check_can_create_output_file(config: &GroupConfig) -> Result<(), Error> {
    if let Some(output) = &config.output {
        if let Err(e) = File::create(output) {
            return Err(Error::new(format!(
                "Cannot create output file {}: {}",
                output.display(),
                e
            )));
        }
    }
    Ok(())
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

fn run_group(mut config: GroupConfig, log: &dyn Log) -> Result<(), Error> {
    config.resolve_base_dir().map_err(|e| e.to_string())?;
    check_input_paths_exist(&config, log)?;
    check_can_create_output_file(&config)?;
    configure_main_thread_pool(&config.thread_pool_sizes());
    log.info("Started grouping");
    let results = group_files(&config, log).map_err(|e| Error::new(e.message))?;

    write_report(&config, log, &results)
        .map_err(|e| Error::new(format!("Failed to write report: {e}")))
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

/// Returns the configuration of a previously executed fclones command,
/// stored in the report header.
fn get_command_config(header: &ReportHeader) -> Result<Config, Error> {
    let mut command: Config = Config::try_parse_from(&header.command).map_err(|e| {
        let message: String = extract_error_cause(&e.to_string());
        format!("Unrecognized earlier fclones configuration: {message}")
    })?;

    // Configure the same base directory as set when running the previous command.
    // This is important to get the correct input paths.
    if let Command::Group(ref mut group_config) = command.command {
        group_config.base_dir = header.base_dir.clone();
    }
    Ok(command)
}

pub fn run_dedupe(op: DedupeOp, config: DedupeConfig, log: &dyn Log) -> Result<(), Error> {
    let input_error = |e: io::Error| format!("Input error: {e}");
    let mut dedupe_config = config;
    let mut reader = open_report(stdin()).map_err(input_error)?;
    let header = reader.read_header().map_err(input_error)?;
    let prev_command_config = get_command_config(&header)?;

    if let Command::Group(c) = &prev_command_config.command {
        // we cannot check size if a transformation was applied, because the transformation
        // may change the size of the data and the recorded data size
        // would not match the physical size of the file
        dedupe_config.no_check_size |= c.transform.is_some();
        dedupe_config.match_links |= c.match_links;

        if dedupe_config.rf_over.is_none() {
            dedupe_config.rf_over = Some(c.rf_over())
        }
        if dedupe_config.isolated_roots.is_empty() && c.isolate {
            dedupe_config.isolated_roots = c.input_paths().collect();
        }
    }

    if dedupe_config.rf_over.is_none() {
        return Err(Error::from(
            "Could not extract --rf-over setting from the earlier fclones configuration. \
            Please set --rf-over explicitly.",
        ));
    };

    if dedupe_config.modified_before.is_none() {
        dedupe_config.modified_before = Some(header.timestamp);
    }

    if dedupe_config.dry_run {
        log.info("Started deduplicating (dry run)");
    } else {
        log.info("Started deduplicating");
    }

    let mut result: Result<(), io::Error> = Ok(());
    let group_count = header.stats.map(|s| s.group_count as u64);
    let progress: Arc<dyn ProgressTracker> = match group_count {
        _ if dedupe_config.dry_run && dedupe_config.output.is_none() => Arc::new(NoProgressBar),
        Some(group_count) => {
            log.progress_bar("Deduplicating", ProgressBarLength::Items(group_count))
        }
        None => log.progress_bar("Deduplicating", ProgressBarLength::Unknown),
    };

    let groups = reader.read_groups();

    let groups = groups
        .map_err(input_error)?
        .iterator()
        .map(|g| match g {
            Ok(g) => Some(g),
            Err(e) => {
                result = Err(e);
                None
            }
        })
        .take_while(|g| g.is_some())
        .map(|g| g.unwrap())
        .inspect(|_| progress.inc(1));

    let upto = if op == DedupeOp::RefLink {
        // Can't be sure because any previous deduplications are not
        // visible without calling fs-specific tooling.
        "up to "
    } else {
        ""
    };

    let script = dedupe(groups, op, &dedupe_config, log);
    if dedupe_config.dry_run {
        let out = get_output_writer(&dedupe_config)?;
        let result = log_script(script, out).map_err(|e| format!("Output error: {e}"))?;
        log.info(format!(
            "Would process {} files and reclaim {}{} space",
            result.processed_count, upto, result.reclaimed_space
        ));
    } else {
        let result = run_script(script, !dedupe_config.no_lock, log);
        log.info(format!(
            "Processed {} files and reclaimed {}{} space",
            result.processed_count, upto, result.reclaimed_space
        ));
    };
    result.map_err(|e| Error::new(format!("Failed to read file list: {e}")))
}

fn main() {
    let config: Config = Config::parse();
    if let Err(e) = config.command.validate() {
        eprintln!("{} {}", style("error:").for_stderr().bold().red(), e);
        exit(1);
    }

    let mut log = StdLog::new();
    if config.quiet {
        log.no_progress = true;
    }

    let cwd = match std::env::current_dir() {
        Ok(cwd) => cwd,
        Err(e) => {
            log.err(format!("Cannot determine current working directory: {e}"));
            exit(1);
        }
    };

    let result = match config.command {
        Command::Group(config) => run_group(config, &log),
        Command::Remove(config) => run_dedupe(DedupeOp::Remove, config, &log),
        Command::Link { config, soft: true } => run_dedupe(DedupeOp::SymbolicLink, config, &log),
        Command::Link {
            config,
            soft: false,
        } => run_dedupe(DedupeOp::HardLink, config, &log),
        Command::Dedupe { config, .. } => {
            if cfg!(windows) {
                log.err("Command \"dedupe\" is unsupported on Windows");
                exit(1);
            }
            run_dedupe(DedupeOp::RefLink, config, &log)
        }
        Command::Move { config, target } => {
            let target = fclones::Path::from(target);
            let target = Arc::new(fclones::Path::from(cwd)).resolve(target);
            run_dedupe(DedupeOp::Move(Arc::new(target)), config, &log)
        }
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
