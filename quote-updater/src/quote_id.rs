//! Mints the id a signed quote carries in its third slot: the signing instant in UNIX
//! nanoseconds. An update found on chain then says by itself when its price was computed,
//! with no local series to join against. Only used when the `quote_id` setting is on; the
//! contract reads the first two slots and ignores this one. The timing is public: anyone
//! reading the calldata sees it.

use std::{
    sync::atomic::{AtomicU64, Ordering},
    time::SystemTime,
};

/// The last id handed out, so two signs in the same nanosecond (two pairs on one tick)
/// still get distinct, strictly increasing ids.
static LAST: AtomicU64 = AtomicU64::new(0);

/// Now, in UNIX nanoseconds, bumped by one if the clock has not moved since the last id.
pub fn mint() -> u64 {
    let now = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map_or(1, |since| since.as_nanos() as u64);
    LAST.fetch_update(Ordering::SeqCst, Ordering::SeqCst, |last| {
        Some(now.max(last + 1))
    })
    .map_or(now, |last| now.max(last + 1))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ids_are_the_signing_instant_in_unix_nanoseconds() {
        let before = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64;
        let id = mint();
        let after = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64;
        assert!(
            before <= id && id <= after + 1,
            "{before} <= {id} <= {after}"
        );
    }

    #[test]
    fn ids_never_repeat_and_always_grow() {
        let ids: Vec<u64> = (0..1000).map(|_| mint()).collect();
        assert!(ids.windows(2).all(|w| w[1] > w[0]));
    }
}
