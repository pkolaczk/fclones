// Copyright 2014 The Rust Project Developers. See the COPYRIGHT
// file at the top-level directory of this distribution and at
// http://rust-lang.org/COPYRIGHT.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.
//
// Owned guards added by Piotr Kołaczkowski

use std::ops::Drop;
use std::sync::{Arc, Condvar, Mutex};

/// A counting, blocking, semaphore.
///
/// Semaphores are a form of atomic counter where access is only granted if the
/// counter is a positive value. Each acquisition will block the calling thread
/// until the counter is positive, and each release will increment the counter
/// and unblock any threads if necessary.
///
pub struct Semaphore {
    lock: Mutex<isize>,
    cvar: Condvar,
}

/// An RAII guard which will release a resource acquired from a semaphore when
/// dropped.
pub struct SemaphoreGuard<'a> {
    sem: &'a Semaphore,
}

/// An RAII guard which will release a resource acquired from a semaphore when
/// dropped. This guard allows the semaphore to be acquired on a different thread than
/// released.
pub struct OwnedSemaphoreGuard {
    sem: Arc<Semaphore>,
}

#[allow(clippy::mutex_atomic, unused)]
impl Semaphore {
    /// Creates a new semaphore with the initial count specified.
    ///
    /// The count specified can be thought of as a number of resources, and a
    /// call to `acquire` or `access` will block until at least one resource is
    /// available. It is valid to initialize a semaphore with a negative count.
    pub fn new(count: isize) -> Semaphore {
        Semaphore {
            lock: Mutex::new(count),
            cvar: Condvar::new(),
        }
    }

    /// Acquires a resource of this semaphore, blocking the current thread until
    /// it can do so.
    ///
    /// This method will block until the internal count of the semaphore is at
    /// least 1.
    pub fn acquire(&self) {
        let mut count = self.lock.lock().unwrap();
        while *count <= 0 {
            count = self.cvar.wait(count).unwrap();
        }
        *count -= 1;
    }

    /// Release a resource from this semaphore.
    ///
    /// This will increment the number of resources in this semaphore by 1 and
    /// will notify any pending waiters in `acquire` or `access` if necessary.
    pub fn release(&self) {
        *self.lock.lock().unwrap() += 1;
        self.cvar.notify_one();
    }

    /// Acquires a resource of this semaphore, returning an RAII guard to
    /// release the semaphore when dropped.
    ///
    /// This function is semantically equivalent to an `acquire` followed by a
    /// `release` when the guard returned is dropped.
    pub fn access(&self) -> SemaphoreGuard<'_> {
        self.acquire();
        SemaphoreGuard { sem: self }
    }

    /// Acquires a resource of this semaphore, returning an RAII guard to
    /// release the semaphore when dropped. Allows moving the guard to a different
    /// thread.
    ///
    /// This function is semantically equivalent to an `acquire` followed by a
    /// `release` when the guard returned is dropped.
    pub fn access_owned(self: Arc<Self>) -> OwnedSemaphoreGuard {
        self.acquire();
        OwnedSemaphoreGuard { sem: self }
    }
}

impl<'a> Drop for SemaphoreGuard<'a> {
    fn drop(&mut self) {
        self.sem.release();
    }
}

impl Drop for OwnedSemaphoreGuard {
    fn drop(&mut self) {
        self.sem.release();
    }
}

#[cfg(test)]
mod tests {
    use std::prelude::v1::*;

    use super::Semaphore;
    use std::sync::mpsc::channel;
    use std::sync::Arc;
    use std::thread;

    #[test]
    fn test_sem_acquire_release() {
        let s = Semaphore::new(1);
        s.acquire();
        s.release();
        s.acquire();
    }

    #[test]
    fn test_sem_basic() {
        let s = Semaphore::new(1);
        let _g = s.access();
    }

    #[test]
    fn test_sem_as_mutex() {
        let s = Arc::new(Semaphore::new(1));
        let s2 = s.clone();
        let _t = thread::spawn(move || {
            let _g = s2.access();
        });
        let _g = s.access();
    }

    #[test]
    fn test_sem_as_mutex_owned_guard() {
        let s = Arc::new(Semaphore::new(1));
        let g = s.clone().access_owned();
        let _t = thread::spawn(move || {
            drop(g);
        });
        let _g = s.access();
    }

    #[test]
    fn test_sem_as_cvar() {
        // Child waits and parent signals
        let (tx, rx) = channel();
        let s = Arc::new(Semaphore::new(0));
        let s2 = s.clone();
        let _t = thread::spawn(move || {
            s2.acquire();
            tx.send(()).unwrap();
        });
        s.release();
        let _ = rx.recv();

        // Parent waits and child signals
        let (tx, rx) = channel();
        let s = Arc::new(Semaphore::new(0));
        let s2 = s.clone();
        let _t = thread::spawn(move || {
            s2.release();
            let _ = rx.recv();
        });
        s.acquire();
        tx.send(()).unwrap();
    }

    #[test]
    fn test_sem_multi_resource() {
        // Parent and child both get in the critical section at the same
        // time, and shake hands.
        let s = Arc::new(Semaphore::new(2));
        let s2 = s.clone();
        let (tx1, rx1) = channel();
        let (tx2, rx2) = channel();
        let _t = thread::spawn(move || {
            let _g = s2.access();
            let _ = rx2.recv();
            tx1.send(()).unwrap();
        });
        let _g = s.access();
        tx2.send(()).unwrap();
        rx1.recv().unwrap();
    }

    #[test]
    fn test_sem_runtime_friendly_blocking() {
        let s = Arc::new(Semaphore::new(1));
        let s2 = s.clone();
        let (tx, rx) = channel();
        {
            let _g = s.access();
            thread::spawn(move || {
                tx.send(()).unwrap();
                drop(s2.access());
                tx.send(()).unwrap();
            });
            rx.recv().unwrap(); // wait for child to come alive
        }
        rx.recv().unwrap(); // wait for child to be done
    }
}
