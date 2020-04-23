use indicatif::ProgressBar;
use atomic_counter::{RelaxedCounter, AtomicCounter};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

/// A wrapper over `indicatif::ProgressBar` that makes updating its progress lockless.
/// Unfortunately `indicatif::ProgressBar` wraps state in a `Mutex`, so updates are slow
/// and can become a bottleneck in multithreaded context.
/// This wrapper uses `atomic_counter::RelaxedCounter` to keep shared state without ever blocking
/// writers. That state is copied repeatedly by a background thread to an underlying
/// `ProgressBar` at a low rate.
///
/// # Example
/// ```
/// use indicatif::{ProgressBar, ProgressDrawTarget};
/// use rayon::prelude::*;
/// use dff::progress::FastProgressBar;
/// use std::time::Duration;
///
/// let collection = vec![0; 100000];
/// let pb = ProgressBar::new(collection.len() as u64);
/// let pb = FastProgressBar::wrap(pb, Duration::from_millis(100));
/// collection.par_iter()
///     .inspect(|x| pb.tick())
///     .for_each(|x| ());
/// pb.abandon();
/// assert_eq!(pb.position(), 100000);
/// assert_eq!(pb.last_displayed_position(), 100000);
/// ```
pub struct FastProgressBar {
    counter: Arc<RelaxedCounter>,
    progress_bar: Arc<ProgressBar>
}

impl FastProgressBar {

    /// Wraps an existing `ProgressBar` and starts the background updater-thread.
    /// The thread periodically copies the `FastProgressBar` position into the wrapped
    /// `ProgressBar` instance.
    pub fn wrap(progress_bar: ProgressBar, refresh_period: Duration) -> FastProgressBar {
        let pb = Arc::new(progress_bar);
        let pb2 = pb.clone();
        let counter = Arc::new(RelaxedCounter::new(0));
        let counter2 = counter.clone();
        thread::spawn(move || {
            while Arc::strong_count(&counter2) > 1 && !pb2.is_finished() {
                pb2.set_position(counter2.get() as u64);
                thread::sleep(refresh_period);
            }
        });
        FastProgressBar { counter, progress_bar: pb }
    }

    fn update_progress(&self) {
        let value = self.counter.get() as u64;
        self.progress_bar.set_position(value);
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
            self.abandon();
        }
    }
}