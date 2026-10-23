//! Ondo's [`MarketStatus`]: the market session and the asset events. Every `ondo` pair reads
//! the same two answers, so the process reads them once per API, key and period, whatever the
//! number of pairs, rather than each pair asking Ondo the same questions on a shared key; each
//! pair judges them for its own asset ([`OndoStatus`]).
//!
//! No task of this module's keeps them current. Each pair's build runs [`keep_fresh`] as one
//! of the pair's own tasks, which the library starts once the build has succeeded and stops
//! with the pair, and every pair's task asks the shared [`Status`] to [`Status::refresh`]:
//! the first to ask in each half period reads, the others find it read.

use std::{
    collections::HashMap,
    sync::{Arc, Mutex, MutexGuard, OnceLock, Weak},
    time::{Duration, Instant},
};

use crate::eyre;

use super::api::{self, AssetEvent};
use crate::{
    poll::{self, Failures, Stamped, lock},
    stocks::ports::{MarketStatus, StatusReading, Stoppage},
};

/// What the latest read of Ondo's status found.
#[derive(Debug, Default)]
pub struct Readings {
    /// `regular`, `premarket`, `closed`, ...
    pub session: Option<Stamped<String>>,
    /// Every event Ondo lists, for any asset: each pricer judges its own.
    pub events: Option<Stamped<Vec<AssetEvent>>>,
}

/// One reader's readings, held by every pricer that reads through its API and key, and by
/// each of their refresh tasks.
pub struct Status {
    api: String,
    key: String,
    every: Duration,
    readings: Mutex<Readings>,
    /// Held across a read, so pairs asking at once read one at a time, and the later ones
    /// find the reading the first just took.
    refresh: tokio::sync::Mutex<Refresh>,
}

/// Without the key, which a debug print must never show.
impl std::fmt::Debug for Status {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Status({} every {:?})", self.api, self.every)
    }
}

/// When the reader last read Ondo, and how its reads have been failing.
struct Refresh {
    last: Option<Instant>,
    failures: Failures,
}

impl Status {
    /// The status the pricers on `api` and `key` share, read every `every`: the live one if
    /// a pricer holds it; otherwise read now, so a wrong key or an answer that does not
    /// parse fails the build that asked. Starts nothing: see [`keep_fresh`].
    pub async fn shared(api: &str, key: &str, every: Duration) -> eyre::Result<Arc<Status>> {
        let id = (api.to_owned(), key.to_owned(), every);
        let held = {
            let mut live = lock(live());
            // Every lookup drops the entries of readers nobody holds: an entry carries the
            // key it was opened with, and a rotated key must not sit in a static map.
            live.retain(|_, held| held.strong_count() > 0);
            live.get(&id).and_then(Weak::upgrade)
        };
        if let Some(status) = held {
            return Ok(status);
        }
        // Not under the lock: one pricer's slow read would hold up every other's build.
        let (session, events) = read(api, key).await?;
        let mut live = lock(live());
        // Another build may have started one while this one read; it is as fresh.
        if let Some(status) = live.get(&id).and_then(Weak::upgrade) {
            return Ok(status);
        }
        let status = Arc::new(Status::new(api, key, every, stamped(session, events)));
        live.insert(id, Arc::downgrade(&status));
        Ok(status)
    }

    fn new(api: &str, key: &str, every: Duration, readings: Readings) -> Status {
        let last = readings.session.as_ref().map(|session| session.at);
        Status {
            api: api.to_owned(),
            key: key.to_owned(),
            every,
            readings: Mutex::new(readings),
            refresh: tokio::sync::Mutex::new(Refresh {
                last,
                failures: Failures::new(format!("Ondo status from {api}")),
            }),
        }
    }

    /// Readings no task refreshes: a test's.
    #[cfg(test)]
    pub fn of(readings: Readings) -> Arc<Status> {
        Arc::new(Status::new("", "", Duration::MAX, readings))
    }

    /// The readings the reader has left, under their lock.
    pub fn readings(&self) -> MutexGuard<'_, Readings> {
        lock(&self.readings)
    }

    /// Reads Ondo again, unless it was read (or tried) within half a period: what each
    /// pair's [`keep_fresh`] asks every period. However many pairs share the reader, Ondo
    /// is asked at most twice a period, and once when their ticks fall together.
    pub async fn refresh(&self) {
        let mut refresh = self.refresh.lock().await;
        if refresh
            .last
            .is_some_and(|last| last.elapsed() < self.every / 2)
        {
            return;
        }
        let read = read(&self.api, &self.key).await;
        refresh.last = Some(Instant::now());
        match read {
            Ok((session, events)) => {
                *self.readings() = stamped(session, events);
                refresh.failures.succeeded();
            }
            Err(err) => refresh.failures.failed(&err),
        }
    }
}

/// A pair's task keeping `status` current: asks it to refresh every period, for as long as
/// the pair runs. The library runs it as one of the pair's tasks (supervised, and stopped
/// with the pair), so the reader lives exactly as long as some pair uses it.
pub async fn keep_fresh(status: Arc<Status>) {
    let mut ticks = poll::every(status.every);
    loop {
        ticks.tick().await;
        status.refresh().await;
    }
}

/// One pair's view of the shared [`Status`]: Ondo's answers, judged for its asset once per
/// reading, since a tick runs far more often than Ondo is read.
pub struct OndoStatus {
    status: Arc<Status>,
    judged: Mutex<Option<Judged>>,
}

/// A reading, judged for one asset.
struct Judged {
    /// When the reading it judged was taken.
    at: Instant,
    names: Vec<String>,
    reading: StatusReading,
}

impl OndoStatus {
    /// The adapter over a shared reader of Ondo's status.
    pub fn new(status: Arc<Status>) -> OndoStatus {
        OndoStatus {
            status,
            judged: Mutex::new(None),
        }
    }
}

impl MarketStatus for OndoStatus {
    /// The session must be `regular` (in any case): every other (pre-market, overnight,
    /// weekend, paused) withdraws. The `us_market_hours` guard says the same from the
    /// calendar; the two are independent on purpose. The events are judged by
    /// [`api::events_for`], under the token's symbol or the stock's ticker.
    fn read(&self, names: &[String]) -> StatusReading {
        let readings = self.status.readings();
        let at = readings.session.as_ref().map(|session| session.at);
        let mut judged = lock(&self.judged);
        if let (Some(at), Some(held)) = (at, judged.as_ref())
            && held.at == at
            && held.names == names
        {
            return held.reading.clone();
        }
        let reading = StatusReading {
            session: readings.session.as_ref().map(|session| Stamped {
                at: session.at,
                value: if session.value.eq_ignore_ascii_case("regular") {
                    Ok(())
                } else {
                    Err(format!("Ondo reports the market {}", session.value))
                },
            }),
            asset: readings.events.as_ref().map(|events| Stamped {
                at: events.at,
                value: Arc::<[Stoppage]>::from(api::events_for(&events.value, names)),
            }),
        };
        if let Some(at) = at {
            *judged = Some(Judged {
                at,
                names: names.to_vec(),
                reading: reading.clone(),
            });
        }
        reading
    }
}

/// The live readers, weakly, so the registry keeps none alive.
type Live = HashMap<(String, String, Duration), Weak<Status>>;

fn live() -> &'static Mutex<Live> {
    static LIVE: OnceLock<Mutex<Live>> = OnceLock::new();
    LIVE.get_or_init(Mutex::default)
}

fn stamped(session: String, events: Vec<AssetEvent>) -> Readings {
    let at = Instant::now();
    Readings {
        session: Some(Stamped { at, value: session }),
        events: Some(Stamped { at, value: events }),
    }
}

/// Both answers, read at once.
async fn read(api: &str, key: &str) -> eyre::Result<(String, Vec<AssetEvent>)> {
    let http = api::client();
    let (market, assets) = tokio::try_join!(
        api::get(&http, api, "/v1/status/market", key),
        api::get(&http, api, "/v1/status/assets", key),
    )?;
    let session = api::parse_market_session(&market).map_err(|err| eyre::eyre!(err))?;
    let events = api::parse_asset_events(&assets).map_err(|err| eyre::eyre!(err))?;
    Ok((session, events))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing::MockApi;

    fn ondo() -> MockApi {
        MockApi::serve(vec![
            (
                "/v1/status/market",
                MockApi::json(r#"{"marketStatus":"regular"}"#),
            ),
            ("/v1/status/assets", MockApi::json("[]")),
        ])
    }

    #[tokio::test]
    async fn pairs_on_one_api_and_key_share_one_reader() {
        let api = ondo();
        let every = Duration::from_secs(60);
        let first = Status::shared(&api.url, "shared", every).await.unwrap();
        let second = Status::shared(&api.url, "shared", every).await.unwrap();
        assert!(Arc::ptr_eq(&first, &second));
        assert_eq!(api.hits("/v1/status/market"), 1);
        assert_eq!(api.hits("/v1/status/assets"), 1);
        assert_eq!(
            first.readings().session.as_ref().map(|s| s.value.as_str()),
            Some("regular")
        );
        // Another key is another reader.
        let other = Status::shared(&api.url, "other", every).await.unwrap();
        assert!(!Arc::ptr_eq(&first, &other));
        // Once no pricer holds a reader, the next build starts one of its own.
        drop((first, second));
        let again = Status::shared(&api.url, "shared", every).await.unwrap();
        assert_eq!(api.hits("/v1/status/market"), 3);
        drop((other, again));
    }

    #[tokio::test]
    async fn a_reader_nobody_holds_leaves_the_registry_on_the_next_lookup() {
        let api = ondo();
        let every = Duration::from_secs(60);
        let held = Status::shared(&api.url, "held", every).await.unwrap();
        let dropped = Status::shared(&api.url, "dropped", every).await.unwrap();
        let id = (api.url.clone(), "dropped".to_owned(), every);
        assert!(lock(live()).contains_key(&id));
        drop(dropped);
        // A lookup that finds its reader live inserts nothing: the dead entry, with the key
        // string in it, must go all the same.
        let again = Status::shared(&api.url, "held", every).await.unwrap();
        assert!(Arc::ptr_eq(&held, &again));
        assert!(
            !lock(live()).contains_key(&id),
            "a dropped reader's entry lingers"
        );
    }

    #[tokio::test]
    async fn building_a_reader_starts_no_task_of_its_own() {
        // The pairs' own tasks refresh it (`keep_fresh`, run by each pair's build once that
        // build has succeeded), so a build that fails after this leaves nothing running.
        let api = ondo();
        let status = Status::shared(&api.url, "quiet", Duration::from_millis(20))
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert_eq!(
            api.hits("/v1/status/market"),
            1,
            "something read Ondo behind the build's back"
        );
        drop(status);
    }

    #[tokio::test]
    async fn pairs_sharing_a_reader_read_ondo_at_most_once_a_half_period() {
        let api = ondo();
        let every = Duration::from_millis(400);
        let status = Status::shared(&api.url, "refreshed", every).await.unwrap();
        let first_at = status.readings().session.as_ref().unwrap().at;
        // The build just read it: a pair asking now reads nothing.
        status.refresh().await;
        assert_eq!(api.hits("/v1/status/market"), 1);
        tokio::time::sleep(every / 2).await;
        // Two pairs asking at once: one read between them.
        tokio::join!(status.refresh(), status.refresh());
        assert_eq!(api.hits("/v1/status/market"), 2);
        assert_eq!(api.hits("/v1/status/assets"), 2);
        assert!(status.readings().session.as_ref().unwrap().at > first_at);
    }

    #[test]
    fn a_pairs_view_of_ondos_answers_is_judged_once_per_reading() {
        let names = ["AAPLon".to_owned(), "AAPL".to_owned()];
        let halted = r#"[{"symbol":"AAPLon","status":"active","reason":{"code":"ASSET_PAUSED"}}]"#;
        let at = std::time::Instant::now();
        let status = Status::of(Readings {
            session: Some(Stamped {
                at,
                value: "regular".to_owned(),
            }),
            events: Some(Stamped {
                at,
                value: api::parse_asset_events(halted).unwrap(),
            }),
        });
        let view = OndoStatus::new(Arc::clone(&status));
        let events = |view: &OndoStatus| view.read(&names).asset.unwrap().value.len();
        assert_eq!(events(&view), 1);
        // A tick between two reads reuses the judgment, whatever it would find now.
        status.readings().events.as_mut().unwrap().value.clear();
        assert_eq!(events(&view), 1);
        // A new reading is judged anew.
        *status.readings() = stamped("regular".to_owned(), Vec::new());
        assert_eq!(events(&view), 0);
    }

    #[tokio::test]
    async fn a_wrong_key_or_an_answer_that_does_not_parse_fails_the_build_that_asked() {
        let api = MockApi::serve(vec![
            ("/v1/status/market", MockApi::json(r#"{"isOpen":true}"#)),
            ("/v1/status/assets", MockApi::json("[]")),
        ]);
        let err = Status::shared(&api.url, "unparsed", Duration::from_secs(60))
            .await
            .unwrap_err();
        assert!(format!("{err:#}").contains("market-status"), "{err:#}");
    }
}
