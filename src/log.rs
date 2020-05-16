use std::sync::{Arc, Weak};

use indicatif::ProgressDrawTarget;
use nom::lib::std::fmt::Display;

use crate::progress::FastProgressBar;
use console::{style, Term};

pub struct Log {
    program_name: String,
    progress_bar: Weak<FastProgressBar>
}

impl Log {

    pub fn new() -> Log {
        Log {
            progress_bar: Weak::default(),
            program_name: std::env::current_exe().unwrap()
                .file_name().unwrap()
                .to_string_lossy().to_string()
        }
    }

    /// Clears any previous progress bar or spinner and installs a new spinner.
    pub fn spinner(&mut self, msg: &str) -> Arc<FastProgressBar> {
        self.progress_bar.upgrade().iter().for_each(|pb| pb.finish_and_clear());
        let result = Arc::new(FastProgressBar::new_spinner(msg));
        self.progress_bar = Arc::downgrade(&result);
        result
    }

    /// Clears any previous progress bar or spinner and installs a new progress bar.
    pub fn progress_bar(&mut self, msg: &str, len: u64) -> Arc<FastProgressBar> {
        self.progress_bar.upgrade().iter().for_each(|pb| pb.finish_and_clear());
        let result = Arc::new(FastProgressBar::new_progress_bar(msg, len));
        self.progress_bar = Arc::downgrade(&result);
        result
    }

    /// Clears any previous progress bar or spinner and installs a new progress bar.
    pub fn bytes_progress_bar(&mut self, msg: &str, len: u64) -> Arc<FastProgressBar> {
        self.progress_bar.upgrade().iter().for_each(|pb| pb.finish_and_clear());
        let result = Arc::new(FastProgressBar::new_bytes_progress_bar(msg, len));
        self.progress_bar = Arc::downgrade(&result);
        result
    }


    /// Prints a message to stdout.
    /// Does not interfere with progress bar.
    pub fn println<I: Display>(&self, msg: I) {
        let term = Term::stdout();
        match self.progress_bar.upgrade() {
            Some(pb) if pb.is_visible() => {
                pb.set_draw_target(ProgressDrawTarget::hidden());
                term.write_line(format!("{}", msg).as_str()).unwrap();
                pb.set_draw_target(ProgressDrawTarget::stderr());
            },
            _ =>
                term.write_line(format!("{}", msg).as_str()).unwrap()

        }
    }

    /// Prints a message to stderr.
    /// Does not interfere with progress bar.
    pub fn eprintln<I: Display>(&self, msg: I) {
        let term = Term::stderr();
        match self.progress_bar.upgrade() {
            Some(pb) if pb.is_visible() =>
                pb.println(format!("{}", msg)),
            _ =>
                term.write_line(format!("{}", msg).as_str()).unwrap()
        }
    }

    pub fn info<I: Display>(&self, msg: I) {
        let msg = format!("{}: {} {}",
                          self.program_name, style(" info:").for_stderr().cyan(), msg);
        self.eprintln(msg);
    }

    pub fn warn<I: Display>(&self, msg: I) {
        let msg = format!("{}: {} {}",
                          self.program_name, style(" warn:").for_stderr().yellow(), msg);
        self.eprintln(msg);
    }

    pub fn err<I: Display>(&self, msg: I) {
        let msg = format!("{}: {} {}",
                          self.program_name, style("error:").for_stderr().red(), msg);
        self.eprintln(msg);
    }

}
