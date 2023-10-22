//! Fast, concurrent, lockless progress bars.

use crate::FileLen;
use console::style;
use status_line::{Options, StatusLine};
use std::fmt::{Display, Formatter};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

/// Common interface for components that can show progress of a task. E.g. progress bars.
pub trait ProgressTracker: Sync + Send {
    fn inc(&self, delta: u64);
}

/// A progress bar that doesn't display itself and does nothing.
/// This exists purely because sometimes there is an operation that needs to report progress,
/// but we don't want to show it to the user.
pub struct NoProgressBar;

impl ProgressTracker for NoProgressBar {
    fn inc(&self, _delta: u64) {}
}

#[derive(Debug, Default)]
enum ProgressUnit {
    #[default]
    Item,
    Bytes,
}

/// Keeps state of the progress bar and controls how it is rendered to a string
#[derive(Debug)]
struct Progress {
    msg: String,         // message shown before the progress bar
    value: AtomicU64,    // controls the length of the progress bar
    max: Option<u64>,    // maximum expected value, if not set an animated spinner is shown
    unit: ProgressUnit,  // how to format the numbers
    start_time: Instant, // needed for the animation
    color: bool,
}

impl Progress {
    fn fmt_value(&self, value: u64) -> String {
        match self.unit {
            ProgressUnit::Item => value.to_string(),
            ProgressUnit::Bytes => FileLen(value).to_string(),
        }
    }

    /// Draws the progress bar alone (without message and numbers)
    fn bar(&self, length: usize) -> String {
        let mut bar = "=".repeat(length);
        if !bar.is_empty() {
            bar.pop();
            bar.push('>');
        }
        bar.truncate(MAX_BAR_LEN);
        bar
    }

    fn animate_spinner(&self, frame: u64) -> String {
        let spaceship = "<===>";
        let max_pos = (MAX_BAR_LEN - spaceship.len()) as u64;
        let pos = ((frame + max_pos) % (max_pos * 2)).abs_diff(max_pos);
        assert!(pos < MAX_BAR_LEN as u64);
        " ".repeat(pos as usize) + spaceship
    }
}

impl Default for Progress {
    fn default() -> Self {
        Progress {
            msg: "".to_owned(),
            value: AtomicU64::default(),
            max: None,
            unit: ProgressUnit::default(),
            start_time: Instant::now(),
            color: true,
        }
    }
}

const MAX_BAR_LEN: usize = 50;

impl Display for Progress {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let value = self.value.load(Ordering::Relaxed);
        let value_str = self.fmt_value(value);
        let msg = if self.color {
            style(self.msg.clone()).for_stderr().cyan().bold()
        } else {
            style(self.msg.clone())
        };

        match self.max {
            Some(max) => {
                let max_str = self.fmt_value(max);
                let bar_len = (MAX_BAR_LEN as u64 * value / max.max(1)) as usize;
                let bar = self.bar(bar_len);
                write!(f, "{msg:32}[{bar:MAX_BAR_LEN$}]{value_str:>14} / {max_str}")
            }
            None => {
                let frame = (self.start_time.elapsed().as_millis() / 50) as u64;
                let bar = self.animate_spinner(frame);
                write!(f, "{msg:32}[{bar:MAX_BAR_LEN$}]{value_str:>14}")
            }
        }
    }
}

/// Console-based progress bar that renders to standard error.
pub struct ProgressBar {
    status_line: StatusLine<Progress>,
}

impl ProgressBar {
    /// Create a new preconfigured animated spinner with given message.
    pub fn new_spinner(msg: &str) -> ProgressBar {
        let progress = Progress {
            msg: msg.to_string(),
            ..Default::default()
        };
        ProgressBar {
            status_line: StatusLine::new(progress),
        }
    }

    /// Create a new preconfigured progress bar with given message.
    pub fn new_progress_bar(msg: &str, len: u64) -> ProgressBar {
        let progress = Progress {
            msg: msg.to_string(),
            max: Some(len),
            ..Default::default()
        };
        ProgressBar {
            status_line: StatusLine::new(progress),
        }
    }

    /// Create a new preconfigured progress bar with given message.
    /// Displays progress in bytes.
    pub fn new_bytes_progress_bar(msg: &str, len: u64) -> ProgressBar {
        let progress = Progress {
            msg: msg.to_string(),
            max: Some(len),
            unit: ProgressUnit::Bytes,
            ..Default::default()
        };
        ProgressBar {
            status_line: StatusLine::new(progress),
        }
    }

    /// Creates a new invisible progress bar.
    /// This is useful when you need to disable progress bar, but you need to pass an instance
    /// of a `ProgressBar` to something that expects it.
    pub fn new_hidden() -> ProgressBar {
        ProgressBar {
            status_line: StatusLine::with_options(
                Progress::default(),
                Options {
                    refresh_period: Default::default(),
                    initially_visible: false,
                    enable_ansi_escapes: false,
                },
            ),
        }
    }

    pub fn is_visible(&self) -> bool {
        self.status_line.is_visible()
    }

    pub fn eprintln<I: AsRef<str>>(&self, msg: I) {
        let was_visible = self.status_line.is_visible();
        self.status_line.set_visible(false);
        eprintln!("{}", msg.as_ref());
        self.status_line.set_visible(was_visible);
    }

    pub fn tick(&self) {
        self.status_line.value.fetch_add(1, Ordering::Relaxed);
    }

    pub fn finish_and_clear(&self) {
        self.status_line.set_visible(false);
    }
}

impl ProgressTracker for ProgressBar {
    fn inc(&self, delta: u64) {
        self.status_line.value.fetch_add(delta, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod test {
    use crate::progress::{Progress, ProgressUnit};
    use crate::regex::Regex;
    use std::sync::atomic::{AtomicU64, Ordering};

    #[test]
    fn draw_progress_bar() {
        let p = Progress {
            msg: "Message".to_string(),
            max: Some(100),
            color: false,
            ..Default::default()
        };

        assert_eq!(p.to_string(), "Message                         [                                                  ]             0 / 100");
        p.value.fetch_add(2, Ordering::Relaxed);
        assert_eq!(p.to_string(), "Message                         [>                                                 ]             2 / 100");
        p.value.fetch_add(50, Ordering::Relaxed);
        assert_eq!(p.to_string(), "Message                         [=========================>                        ]            52 / 100");
        p.value.fetch_add(48, Ordering::Relaxed);
        assert_eq!(p.to_string(), "Message                         [=================================================>]           100 / 100");
    }

    #[test]
    fn draw_progress_bar_bytes() {
        let p = Progress {
            msg: "Message".to_string(),
            max: Some(1000000000),
            value: AtomicU64::new(12000),
            unit: ProgressUnit::Bytes,
            color: false,
            ..Default::default()
        };

        assert_eq!(p.to_string(), "Message                         [                                                  ]       12.0 KB / 1000.0 MB");
    }

    #[test]
    fn animate_spinner() {
        let p = Progress {
            msg: "Message".to_string(),
            color: false,
            ..Default::default()
        };

        let pattern = Regex::new(
            "^Message                         \\[ *<===> *\\]             0$",
            false,
        )
        .unwrap();
        let s = p.to_string();
        assert!(
            pattern.is_match(s.as_str()),
            "Spinner doesn't match pattern: {}",
            s
        );

        assert_eq!(p.animate_spinner(0), "<===>");
        assert_eq!(p.animate_spinner(1), " <===>");
        assert_eq!(p.animate_spinner(2), "  <===>");
        assert_eq!(p.animate_spinner(3), "   <===>");

        assert_eq!(p.animate_spinner(85), "     <===>");
        assert_eq!(p.animate_spinner(86), "    <===>");
        assert_eq!(p.animate_spinner(87), "   <===>");
        assert_eq!(p.animate_spinner(88), "  <===>");
        assert_eq!(p.animate_spinner(89), " <===>");
        assert_eq!(p.animate_spinner(90), "<===>");
        assert_eq!(p.animate_spinner(91), " <===>");
        assert_eq!(p.animate_spinner(92), "  <===>");
    }
}
