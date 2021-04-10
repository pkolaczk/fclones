//! Fast, concurrent, lockless progress bars.

use atomic_counter::{AtomicCounter, RelaxedCounter};
use console::style;
use indicatif::{ProgressBar, ProgressDrawTarget, ProgressStyle};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

/// A wrapper over `indicatif::ProgressBar` that makes updating its progress lockless.
/// Unfortunately `indicatif::ProgressBar` wraps state in a `Mutex`, so updates are slow
/// and can become a bottleneck in multithreaded context.
/// This wrapper uses `atomic_counter::RelaxedCounter` to keep shared state without ever blocking
/// writers. That state is copied repeatedly by a background thread to an underlying
/// `ProgressBar` at a low rate.
pub struct FastProgressBar {
    counter: Arc<RelaxedCounter>,
    progress_bar: Arc<ProgressBar>,
}

impl FastProgressBar {
    /// Width of the progress bar in characters
    const WIDTH: usize = 50;
    /// Spinner animation looks like this (moves right and left):
    const SPACESHIP: &'static str = "<===>";
    /// Progress bar looks like this:
    const PROGRESS_CHARS: &'static str = "=> ";
    /// How much time to wait between refreshes, in milliseconds
    const REFRESH_PERIOD_MS: u64 = 50;

    /// Wrap an existing `ProgressBar` and start the background updater-thread.
    /// The thread periodically copies the `FastProgressBar` position into the wrapped
    /// `ProgressBar` instance.
    pub fn wrap(progress_bar: ProgressBar) -> FastProgressBar {
        let pb = Arc::new(progress_bar);
        let pb2 = pb.clone();
        let counter = Arc::new(RelaxedCounter::new(0));
        let counter2 = counter.clone();
        thread::spawn(move || {
            while Arc::strong_count(&counter2) > 1 && !pb2.is_finished() {
                pb2.set_position(counter2.get() as u64);
                thread::sleep(Duration::from_millis(Self::REFRESH_PERIOD_MS));
            }
        });
        FastProgressBar {
            counter,
            progress_bar: pb,
        }
    }

    /// Generate spinner animation strings.
    /// The spinner moves to the next string from the returned vector with every tick.
    /// The spinner is rendered as a SPACESHIP that bounces right and left from the
    /// ends of the spinner bar.
    fn gen_tick_strings() -> Vec<String> {
        let mut tick_strings = vec![];
        for i in 0..(Self::WIDTH - Self::SPACESHIP.len()) {
            let prefix_len = i;
            let suffix_len = Self::WIDTH - i - Self::SPACESHIP.len();
            let tick_str = " ".repeat(prefix_len) + Self::SPACESHIP + &" ".repeat(suffix_len);
            tick_strings.push(tick_str);
        }
        let mut tick_strings_2 = tick_strings.clone();
        tick_strings_2.reverse();
        tick_strings.extend(tick_strings_2);
        tick_strings
    }

    /// Create a new preconfigured animated spinner with given message.
    pub fn new_spinner(msg: &str) -> FastProgressBar {
        let inner = ProgressBar::new_spinner();
        let template =
            style("{msg:28}").cyan().bold().for_stderr().to_string() + "[{spinner}] {pos:>10}";
        let tick_strings = Self::gen_tick_strings();
        let tick_strings: Vec<&str> = tick_strings.iter().map(|s| s as &str).collect();
        inner.set_style(
            ProgressStyle::default_spinner()
                .template(template.as_str())
                .tick_strings(tick_strings.as_slice()),
        );
        inner.set_message(msg);
        Self::wrap(inner)
    }

    /// Create a new preconfigured progress bar with given message.
    pub fn new_progress_bar(msg: &str, len: u64) -> FastProgressBar {
        let inner = ProgressBar::new(len);
        let template = style("{msg:28}").cyan().bold().for_stderr().to_string()
            + &"[{bar:WIDTH}] {pos:>10}/{len}".replace("WIDTH", Self::WIDTH.to_string().as_str());

        inner.set_style(
            ProgressStyle::default_bar()
                .template(template.as_str())
                .progress_chars(Self::PROGRESS_CHARS),
        );
        inner.set_message(msg);
        FastProgressBar::wrap(inner)
    }

    /// Create a new preconfigured progress bar with given message.
    /// Displays progress in bytes.
    pub fn new_bytes_progress_bar(msg: &str, len: u64) -> FastProgressBar {
        let inner = ProgressBar::new(len);
        let template = style("{msg:28}").cyan().bold().for_stderr().to_string()
            + &"[{bar:WIDTH}] {bytes:>10}/{total_bytes}"
                .replace("WIDTH", Self::WIDTH.to_string().as_str());

        inner.set_style(
            ProgressStyle::default_bar()
                .template(template.as_str())
                .progress_chars(Self::PROGRESS_CHARS),
        );
        inner.set_message(msg);

        FastProgressBar::wrap(inner)
    }

    /// Creates a new invisible progress bar.
    /// This is useful when you need to disable progress bar, but you need to pass an instance
    /// of a `ProgressBar` to something that expects it.
    pub fn new_hidden() -> FastProgressBar {
        let inner = ProgressBar::new(u64::MAX);
        inner.set_draw_target(ProgressDrawTarget::hidden());
        FastProgressBar::wrap(inner)
    }

    fn update_progress(&self) {
        let value = self.counter.get() as u64;
        self.progress_bar.set_position(value);
    }

    pub fn set_draw_target(&self, target: ProgressDrawTarget) {
        self.progress_bar.set_draw_target(target)
    }

    pub fn is_visible(&self) -> bool {
        !self.progress_bar.is_hidden()
    }

    pub fn println<I: Into<String>>(&self, msg: I) {
        self.progress_bar.println(msg);
    }

    pub fn tick(&self) {
        self.counter.inc();
    }

    pub fn inc(&self, delta: usize) {
        self.counter.add(delta);
    }

    pub fn position(&self) -> usize {
        self.counter.get()
    }

    pub fn last_displayed_position(&self) -> u64 {
        self.progress_bar.position()
    }

    pub fn finish(&self) {
        self.update_progress();
        self.progress_bar.finish();
    }

    pub fn finish_and_clear(&self) {
        self.update_progress();
        self.progress_bar.finish_and_clear();
    }

    pub fn finish_with_msg(&self, message: &str) {
        self.update_progress();
        self.progress_bar.finish_with_message(message);
    }

    pub fn abandon(&self) {
        self.update_progress();
        self.progress_bar.abandon();
    }

    pub fn is_finished(&self) -> bool {
        self.progress_bar.is_finished()
    }
}

impl Drop for FastProgressBar {
    fn drop(&mut self) {
        if !self.is_finished() {
            self.finish_and_clear();
        }
    }
}

#[cfg(test)]
mod test {

    use super::*;
    use rayon::prelude::*;

    #[test]
    fn all_ticks_should_be_counted() {
        let collection = vec![0; 100000];
        let pb = ProgressBar::new(collection.len() as u64);
        pb.set_draw_target(ProgressDrawTarget::hidden());
        let pb = FastProgressBar::wrap(pb);
        collection
            .par_iter()
            .inspect(|_| pb.tick())
            .for_each(|_| ());
        pb.abandon();
        assert_eq!(pb.position(), 100000);
        assert_eq!(pb.last_displayed_position(), 100000);
    }
}
