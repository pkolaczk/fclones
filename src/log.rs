//! Logging and progress reporting.

use std::sync::{Arc, Mutex, Weak};

use console::style;
use nom::lib::std::fmt::Display;

use crate::progress::{FastProgressBar, ProgressTracker};
use chrono::Local;

/// Determines the size of the task tracked by ProgressTracker.
#[derive(Debug, Clone, Copy)]
pub enum ProgressBarLength {
    Items(u64),
    Bytes(u64),
    Unknown,
}

#[derive(Debug, Clone, Copy)]
pub enum LogLevel {
    Info,
    Warn,
    Error,
}

/// Common interface for logging diagnostics and progress.
pub trait Log: Sync + Send {
    /// Clears any previous progress bar or spinner and installs a new progress bar.
    fn progress_bar(&self, msg: &str, len: ProgressBarLength) -> Arc<dyn ProgressTracker>;

    /// Logs a message.
    fn log(&self, level: LogLevel, msg: String);
}

/// Additional convenience methods for logging.
pub trait LogExt {
    /// Logs an info message.
    fn info(&self, msg: impl Display);
    /// Logs an warning.
    fn warn(&self, msg: impl Display);
    /// Logs an error.
    fn err(&self, msg: impl Display);
}

/// Additional convenience methods for logging.
impl<L: Log + ?Sized> LogExt for L {
    /// Logs an info message.
    fn info(&self, msg: impl Display) {
        self.log(LogLevel::Info, msg.to_string())
    }

    /// Logs an warning.
    fn warn(&self, msg: impl Display) {
        self.log(LogLevel::Warn, msg.to_string())
    }

    /// Logs an error.
    fn err(&self, msg: impl Display) {
        self.log(LogLevel::Error, msg.to_string())
    }
}

/// A logger that uses standard error stream to communicate with the user.
pub struct StdLog {
    program_name: String,
    progress_bar: Mutex<Weak<FastProgressBar>>,
    pub log_stderr_to_stdout: bool,
    pub no_progress: bool,
}

impl StdLog {
    pub fn new() -> StdLog {
        StdLog {
            progress_bar: Mutex::new(Weak::default()),
            program_name: std::env::current_exe()
                .unwrap()
                .file_name()
                .unwrap()
                .to_string_lossy()
                .to_string(),
            log_stderr_to_stdout: false,
            no_progress: false,
        }
    }

    /// Clears any previous progress bar or spinner and installs a new spinner.
    pub fn spinner(&self, msg: &str) -> Arc<FastProgressBar> {
        if self.no_progress {
            return Arc::new(FastProgressBar::new_hidden());
        }
        self.progress_bar
            .lock()
            .unwrap()
            .upgrade()
            .iter()
            .for_each(|pb| pb.finish_and_clear());
        let result = Arc::new(FastProgressBar::new_spinner(msg));
        *self.progress_bar.lock().unwrap() = Arc::downgrade(&result);
        result
    }

    /// Clears any previous progress bar or spinner and installs a new progress bar.
    pub fn progress_bar(&self, msg: &str, len: u64) -> Arc<FastProgressBar> {
        if self.no_progress {
            return Arc::new(FastProgressBar::new_hidden());
        }
        let result = Arc::new(FastProgressBar::new_progress_bar(msg, len));
        *self.progress_bar.lock().unwrap() = Arc::downgrade(&result);
        result
    }

    /// Creates a no-op progressbar that doesn't display itself.
    pub fn hidden(&self) -> Arc<FastProgressBar> {
        Arc::new(FastProgressBar::new_hidden())
    }

    /// Clears any previous progress bar or spinner and installs a new progress bar.
    pub fn bytes_progress_bar(&self, msg: &str, len: u64) -> Arc<FastProgressBar> {
        if self.no_progress {
            return Arc::new(FastProgressBar::new_hidden());
        }
        self.progress_bar
            .lock()
            .unwrap()
            .upgrade()
            .iter()
            .for_each(|pb| pb.finish_and_clear());
        let result = Arc::new(FastProgressBar::new_bytes_progress_bar(msg, len));
        *self.progress_bar.lock().unwrap() = Arc::downgrade(&result);
        result
    }

    /// Prints a message to stderr.
    /// Does not interfere with progress bar.
    fn eprintln<I: Display>(&self, msg: I) {
        match self.progress_bar.lock().unwrap().upgrade() {
            Some(pb) if pb.is_visible() => pb.println(format!("{}", msg)),
            _ if self.log_stderr_to_stdout => println!("{}", msg),
            _ => eprintln!("{}", msg),
        }
    }

    const TIMESTAMP_FMT: &'static str = "[%Y-%m-%d %H:%M:%S.%3f]";
}

impl Log for StdLog {
    fn progress_bar(&self, msg: &str, len: ProgressBarLength) -> Arc<dyn ProgressTracker> {
        match len {
            ProgressBarLength::Items(count) => self.progress_bar(msg, count),
            ProgressBarLength::Bytes(count) => self.bytes_progress_bar(msg, count),
            ProgressBarLength::Unknown => self.spinner(msg),
        }
    }

    fn log(&self, level: LogLevel, msg: String) {
        let timestamp = Local::now();
        let level = match level {
            LogLevel::Info => style(" info:").for_stderr().green(),
            LogLevel::Warn => style("warn:").for_stderr().yellow(),
            LogLevel::Error => style("error:").for_stderr().red(),
        };
        let msg = format!(
            "{} {}: {} {}",
            style(timestamp.format(Self::TIMESTAMP_FMT))
                .for_stderr()
                .dim()
                .white(),
            style(&self.program_name).for_stderr().yellow(),
            level,
            msg
        );
        self.eprintln(msg);
    }
}

impl Default for StdLog {
    fn default() -> Self {
        StdLog::new()
    }
}
