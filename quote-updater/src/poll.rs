//! What the crate's background readers share, and what an issuer's adapters are built with
//! so they behave like them: the schedule they read on ([`every`]), the wall clock a
//! source's reading is stamped with ([`wall_clock_ms`]), the lock they leave readings
//! under ([`lock`]), and how they log a source that keeps failing ([`Failures`]).

use std::{
    sync::{Mutex, MutexGuard, PoisonError},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use crate::eyre;
use tokio::time::{Instant, Interval, MissedTickBehavior};

/// A reading this process took, and when, by the monotonic clock: its age cannot jump when
/// the wall clock is stepped. A source's own timestamp (a price's) stays on the wall clock.
#[derive(Clone, Copy, Debug)]
pub struct Stamped<T> {
    /// When the reading was taken, by the monotonic clock.
    pub at: std::time::Instant,
    /// The reading.
    pub value: T,
}

impl<T> Stamped<T> {
    /// `value`, read now.
    pub fn now(value: T) -> Stamped<T> {
        Stamped {
            at: std::time::Instant::now(),
            value,
        }
    }
}

/// Milliseconds since the epoch now, by the wall clock: what a reading is stamped with.
pub fn wall_clock_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |since| since.as_millis() as i64)
}

/// A reader's schedule: every `period`, the first a whole period from now, since the build
/// that starts the reader has just read the same thing. A slow read delays the next tick
/// rather than bunching the missed ones up.
pub fn every(period: Duration) -> Interval {
    let mut ticks = tokio::time::interval_at(Instant::now() + period, period);
    ticks.set_missed_tick_behavior(MissedTickBehavior::Delay);
    ticks
}

/// `mutex`'s value, poisoned or not: a reader that panicked mid-write left a plain value,
/// whole, and the last one written is still the best there is.
pub fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

/// At most one log line per this long while a reader keeps failing.
const LOG_EVERY: Duration = Duration::from_secs(60);

/// Logs a reader's failures without a line per attempt, whatever its period: the first, at
/// most one a minute after it, and the recovery. Whoever reads the reading withdraws on its
/// age either way; this is for the human.
pub struct Failures {
    what: String,
    count: u64,
    last_logged: Option<Instant>,
}

impl Failures {
    /// For the reader of `what`, as its lines name it: `Ondo price for AAPLon`.
    pub fn new(what: String) -> Failures {
        Failures {
            what,
            count: 0,
            last_logged: None,
        }
    }

    /// A read that failed with `err`: logged on the first, then at most once a minute.
    pub fn failed(&mut self, err: &eyre::Report) {
        if let Some(in_a_row) = self.record(Instant::now()) {
            tracing::warn!("{}: read failed ({in_a_row} in a row): {err:#}", self.what);
        }
    }

    /// A read that succeeded: logged when it ends a run of failures.
    pub fn succeeded(&mut self) {
        if self.count > 0 {
            tracing::info!("{}: reading again after {} failures", self.what, self.count);
        }
        self.count = 0;
        self.last_logged = None;
    }

    /// Counts a failure at `now`, answering the count in a row when it is one to log.
    fn record(&mut self, now: Instant) -> Option<u64> {
        self.count += 1;
        let due = self
            .last_logged
            .is_none_or(|logged| now.saturating_duration_since(logged) >= LOG_EVERY);
        if due {
            self.last_logged = Some(now);
        }
        due.then_some(self.count)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_failing_reader_logs_its_first_failure_then_once_a_minute() {
        let start = Instant::now();
        let mut failures = Failures::new("test".to_owned());
        assert_eq!(failures.record(start), Some(1));
        // A reader polling every second fails 59 more times inside the minute: no line.
        for second in 1..60 {
            assert_eq!(
                failures.record(start + Duration::from_secs(second)),
                None,
                "{second}s"
            );
        }
        assert_eq!(failures.record(start + LOG_EVERY), Some(61));
        assert_eq!(
            failures.record(start + LOG_EVERY + Duration::from_secs(1)),
            None
        );
    }

    #[test]
    fn a_reader_that_recovers_logs_its_next_failure_at_once() {
        let start = Instant::now();
        let mut failures = Failures::new("test".to_owned());
        assert_eq!(failures.record(start), Some(1));
        failures.succeeded();
        assert_eq!(failures.record(start + Duration::from_secs(1)), Some(1));
    }

    #[tokio::test]
    async fn a_readers_first_tick_waits_a_whole_period() {
        let mut ticks = every(Duration::from_secs(60));
        let first = tokio::time::timeout(Duration::from_millis(50), ticks.tick()).await;
        assert!(
            first.is_err(),
            "the reader re-read what its build just read"
        );
    }
}
