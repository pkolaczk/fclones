use std::sync::{Arc, Mutex, Weak};

use console::style;
use indicatif::ProgressDrawTarget;
use nom::lib::std::fmt::Display;

use crate::progress::FastProgressBar;
use chrono::Local;

pub struct Log {
    program_name: String,
    progress_bar: Mutex<Weak<FastProgressBar>>,
    pub log_stderr_to_stdout: bool,
    pub no_progress: bool,
}

impl Log {
    pub fn new() -> Log {
        Log {
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

    /// Prints a message to stdout.
    /// Does not interfere with progress bar.
    pub fn println<I: Display>(&self, msg: I) {
        match self.progress_bar.lock().unwrap().upgrade() {
            Some(pb) if pb.is_visible() => {
                pb.set_draw_target(ProgressDrawTarget::hidden());
                println!("{}", msg);
                pb.set_draw_target(ProgressDrawTarget::stderr());
            }
            _ => println!("{}", msg),
        }
    }

    /// Prints a message to stderr.
    /// Does not interfere with progress bar.
    pub fn eprintln<I: Display>(&self, msg: I) {
        match self.progress_bar.lock().unwrap().upgrade() {
            Some(pb) if pb.is_visible() => pb.println(format!("{}", msg)),
            _ if self.log_stderr_to_stdout => println!("{}", msg),
            _ => eprintln!("{}", msg),
        }
    }

    const TIMESTAMP_FMT: &'static str = "[%Y-%m-%d %H:%M:%S.%3f]";

    pub fn info<I: Display>(&self, msg: I) {
        let timestamp = Local::now();
        let msg = format!(
            "{} {}: {} {}",
            style(timestamp.format(Self::TIMESTAMP_FMT))
                .for_stderr()
                .green(),
            style(&self.program_name).for_stderr().yellow(),
            style(" info:").for_stderr().green(),
            msg
        );
        self.eprintln(msg);
    }

    pub fn warn<I: Display>(&self, msg: I) {
        let timestamp = Local::now();
        let msg = format!(
            "{} {}: {} {}",
            style(timestamp.format(Self::TIMESTAMP_FMT))
                .for_stderr()
                .green(),
            style(&self.program_name).for_stderr().yellow(),
            style(" warn:").for_stderr().yellow(),
            msg
        );
        self.eprintln(msg);
    }

    pub fn err<I: Display>(&self, msg: I) {
        let timestamp = Local::now();
        let msg = format!(
            "{} {}: {} {}",
            style(timestamp.format(Self::TIMESTAMP_FMT))
                .for_stderr()
                .green(),
            style(&self.program_name).for_stderr().yellow(),
            style("error:").for_stderr().red(),
            msg
        );
        self.eprintln(msg);
    }
}

impl Default for Log {
    fn default() -> Self {
        Log::new()
    }
}
