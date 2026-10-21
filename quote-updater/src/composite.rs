//! The composite: one pair's `(delta, mid)`, averaged by weight over the venues it reads,
//! recomputed whenever any of them delivers, and judged by the pair's circuit breaker and
//! recorded into its σ history here, because these must see the number the lane
//! publishes, not any one venue's.
//!
//! A venue whose latest sample is older than [`MAX_PRICE_AGE`] drops out of the average
//! and its weight is spread over the rest; the pair keeps publishing as long as at least
//! `min_sources` venues are fresh, and stops (by publishing nothing, so the last composite
//! ages out downstream exactly like a stalled feed did) when fewer are. A single venue
//! with weight 1 is exactly the feed the pusher had before venues were plural.

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime};

use ethrex_common::U256;
use eyre::{Result, eyre};
use tokio::sync::{mpsc, watch};

use crate::config::{Feeds, Source};
use crate::feed::{self, Adoption, Gated, PriceSample, scaled_to_f64};
use crate::guard::{BoundGuard, Cause, CompositeSample, Latch, SourceSample, TripReason, Verdict};
use crate::metrics::{Metrics, PairMetrics, VenueMetrics};
use crate::record::Recorder;
use crate::update::MAX_PRICE_AGE;
use crate::venue::{Endpoints, VenueId};
use crate::volatile::PriceHistory;

/// How often the composite re-reads its sources' freshness when none of them is ticking.
/// Well under MAX_PRICE_AGE, so the gauges say "down" before the alert on them can fire
/// for the wrong reason.
const FRESHNESS_AUDIT: Duration = Duration::from_secs(5);

/// How long the first composite sample may take to arrive. A feed that has not delivered
/// by then has the wrong symbol or a blocked endpoint, and the pair refuses to start on it
/// rather than quoting nothing indefinitely.
pub const FIRST_PRICE_TIMEOUT: Duration = Duration::from_secs(15);

/// One venue's live feed, as the composite reads it and as `--check` and the metric
/// seeding print it. Cloneable because `Live` is cloned per supervised restart and carries
/// these for the venue gauges.
#[derive(Clone)]
pub struct VenueFeed {
    pub venue: VenueId,
    pub symbol: String,
    pub rx: watch::Receiver<Option<PriceSample>>,
    pub metrics: VenueMetrics,
    /// Whether the venue is connected right now, as its feed task sets it. The `up` gauge
    /// reads it at scrape time once the pair takes over its series (`adopt_metrics`).
    pub connected: feed::Connected,
}

impl VenueFeed {
    /// `binance:ETHUSDC`, how a venue is named in every line about it.
    pub fn name(&self) -> String {
        format!("{}:{}", self.venue, self.symbol)
    }
}

/// Everything a composite needs besides its sources.
pub struct Params<'a> {
    pub endpoints: &'a Endpoints,
    pub decimals: u32,
    pub invert: bool,
    /// The lane's latch: read before any guard runs, tripped by a guard's verdict or its
    /// panic.
    pub latch: Latch,
    /// The market guards, the built-in deviation guard first, then the pair's stanzas in
    /// file order. Judged here, on every composite sample, whether or not the quote loop
    /// reads it: the loop stops ticking during an RPC outage or a restart backoff, and a
    /// reference that moved only when the loop asked would freeze there and then measure
    /// the first price after the outage against a minutes-old one, halting a healthy feed
    /// on ordinary drift.
    pub guards: Vec<BoundGuard>,
    /// The switch that lets this generation's feeds report; see [`Gated`].
    pub adopted: Adoption,
    pub metrics: &'a Metrics,
    pub pair_metrics: PairMetrics,
    /// The pair's label, which names its venue series and the composite's recorder key.
    pub label: String,
    pub recorder: Option<Recorder>,
    /// The market's σ history, for a volatile pair. Receives every composite market mid.
    pub history: Option<Arc<Mutex<PriceHistory>>>,
}

/// A composite that has been spawned: what the quote loop reads, and the venues under it.
pub struct Spawned {
    pub rx: watch::Receiver<Option<PriceSample>>,
    pub venues: Vec<VenueFeed>,
}

/// Dials every source and spawns the task that averages them. Returns at once; use
/// [`connect`] to also wait for the first composite sample.
pub fn spawn(feeds: &Feeds, params: Params<'_>) -> Spawned {
    let Params {
        endpoints,
        decimals,
        invert,
        latch,
        guards,
        adopted,
        metrics,
        pair_metrics,
        label,
        recorder,
        history,
    } = params;
    let venues: Vec<VenueFeed> = feeds
        .sources
        .iter()
        .map(|source| {
            let venue_metrics = metrics.for_venue(&label, source.venue.as_str());
            let connected = feed::Connected::default();
            let rx = feed::spawn_feed(
                endpoints.http(),
                source.venue,
                endpoints.for_venue(source.venue),
                &source.symbol,
                decimals,
                invert,
                Gated::new(venue_metrics.clone(), adopted.clone()),
                recorder.clone(),
                connected.clone(),
            );
            VenueFeed {
                venue: source.venue,
                symbol: source.symbol.clone(),
                rx,
                metrics: venue_metrics,
                connected,
            }
        })
        .collect();

    let (tx, rx) = watch::channel(None);
    // One forwarder per venue turns "this receiver changed" into a message, so the
    // composite is a plain loop over one channel rather than a select over N receivers
    // rebuilt on every tick. A forwarder ends when the composite is gone (its send fails),
    // dropping its receiver, which is what lets the venue feed's socket close.
    let (changes_tx, mut changes) = mpsc::channel::<(usize, Option<PriceSample>)>(64);
    for (i, venue) in venues.iter().enumerate() {
        let mut rx = venue.rx.clone();
        let changes_tx = changes_tx.clone();
        tokio::spawn(async move {
            // The sample already there counts as a change: a feed that delivered before
            // this forwarder polled it must not be waited on for its next tick.
            rx.mark_changed();
            while rx.changed().await.is_ok() {
                let sample = *rx.borrow_and_update();
                if changes_tx.send((i, sample)).await.is_err() {
                    return;
                }
            }
        });
    }
    drop(changes_tx);

    let judges_sources = guards.iter().any(|guard| guard.kind != "deviation");
    let mut composite = Composite {
        sources: feeds.sources.clone(),
        min_sources: feeds.min_sources,
        max_lag: feeds.checks.max_lag,
        connected: venues.iter().map(|v| v.connected.clone()).collect(),
        venue_metrics: venues
            .iter()
            .map(|v| Gated::new(v.metrics.clone(), adopted.clone()))
            .collect(),
        late: vec![false; feeds.sources.len()],
        latest: vec![None; feeds.sources.len()],
        decimals,
        invert,
        latch,
        guards,
        judges_sources,
        metrics: Gated::new(pair_metrics, adopted),
        label,
        recorder,
        history,
    };
    tokio::spawn(async move {
        tokio::select! {
            // The pair is finished (every receiver gone: a breaker halt, in practice), so
            // its series say "no feed here" the way a static pair's do. See the same arm in
            // `feed::spawn_feed` for why NaN and not 0.
            _ = tx.closed() => {
                composite.metrics.record(|m| {
                    m.feed_up.set(f64::NAN);
                    m.feed_last_tick.set(f64::NAN);
                    m.feed_sample_current.set(f64::NAN);
                    m.feed_sources_fresh.set(f64::NAN);
                    m.feed_sources_min.set(f64::NAN);
                });
            }
            _ = async {
                // The timer is for the case no venue ticks at all: every source dead at
                // once. Without it nothing would run here, `feed_up` would stay at 1 and
                // `feed_sources_fresh` frozen while the last composite quietly aged out
                // downstream. The tick re-reads freshness and the gauges; it publishes
                // nothing, so an idle but live set of venues is not re-stamped as new.
                let mut audit = tokio::time::interval(FRESHNESS_AUDIT);
                audit.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
                loop {
                    tokio::select! {
                        changed = changes.recv() => {
                            let Some((i, sample)) = changed else { break };
                            composite.latest[i] = sample;
                            composite.judge_lateness(i);
                            composite.recompute(&tx);
                        }
                        _ = audit.tick() => composite.audit_freshness(),
                    }
                }
            } => {}
        }
    });
    Spawned { rx, venues }
}

/// [`spawn`], then waits for the first composite sample. Shared by startup and `--check`
/// so the two agree by construction: a source that fails here is one the run refuses to
/// start on, which is what makes checking it worth anything.
pub async fn connect(feeds: &Feeds, params: Params<'_>) -> Result<Spawned> {
    let mut spawned = spawn(feeds, params);
    let waited = tokio::time::timeout(FIRST_PRICE_TIMEOUT, spawned.rx.wait_for(|s| s.is_some()))
        .await
        .map(|first| first.map(|_| ()));
    match waited {
        Ok(Ok(())) => Ok(spawned),
        Ok(Err(_)) => Err(eyre!("the composite task died")),
        Err(_) => {
            // Name the venues that never delivered: with several, the one that is wrong is
            // the only useful thing to say.
            let silent: Vec<String> = spawned
                .venues
                .iter()
                .filter(|v| v.rx.borrow().is_none())
                .map(VenueFeed::name)
                .collect();
            Err(eyre!(
                "no price from {} within {FIRST_PRICE_TIMEOUT:?} (need {} of {} sources); \
                 wrong symbol or blocked endpoint?",
                silent.join(", "),
                feeds.min_sources,
                feeds.sources.len()
            ))
        }
    }
}

/// The averaging state: the latest sample from each source and where the result goes.
struct Composite {
    sources: Vec<Source>,
    min_sources: usize,
    /// `max_source_lag_ms`: how late a venue's price may arrive behind its exchange stamp
    /// and still count as fresh. `None` leaves every venue judged by silence alone.
    max_lag: Option<Duration>,
    /// Each source's feed flag, where the feed task leaves how late its last price was.
    connected: Vec<feed::Connected>,
    /// Each source's series, for `late`.
    venue_metrics: Vec<Gated<VenueMetrics>>,
    /// Whether each source's latest price arrived later than `max_lag`, judged when that
    /// price arrived ([`Self::judge_lateness`]), so a stale reading cannot flip between
    /// frames. Always `false` without `max_lag` and for a venue whose frames carry no stamp.
    late: Vec<bool>,
    latest: Vec<Option<PriceSample>>,
    decimals: u32,
    invert: bool,
    latch: Latch,
    guards: Vec<BoundGuard>,
    /// Whether any guard reads the sources behind a sample: the built-in deviation guard
    /// does not, and a lane with only that one builds no `SourceSample`s per sample.
    judges_sources: bool,
    metrics: Gated<PairMetrics>,
    label: String,
    recorder: Option<Recorder>,
    history: Option<Arc<Mutex<PriceHistory>>>,
}

impl Composite {
    /// What each fresh venue contributed, for a guard that judges whether they agree: its
    /// share of the weight over the fresh ones, its own book, and how old it was.
    fn source_samples(
        &self,
        fresh: &[(&Source, PriceSample)],
        now: Instant,
        wall: SystemTime,
    ) -> Vec<SourceSample> {
        let total: f64 = fresh
            .iter()
            .map(|(source, _)| scaled_to_f64(source.weight, crate::config::WEIGHT_DECIMALS))
            .sum();
        fresh
            .iter()
            .map(|(source, sample)| {
                let weight = scaled_to_f64(source.weight, crate::config::WEIGHT_DECIMALS);
                SourceSample::new(
                    source.venue.as_str(),
                    if total > 0.0 { weight / total } else { 0.0 },
                    sample.mid,
                    sample.delta,
                    sample.age_at(now, wall),
                    self.invert,
                    self.decimals,
                )
            })
            .collect()
    }

    /// Trips the lane, if nothing has yet, and records the transition once: the gauge and
    /// the counter the dashboard and the page read, and the source series. A replayed
    /// trip records nothing, so a counter cannot climb once per tick for the life of a
    /// halted process and make `rate()` read a dead feed as a trip storm.
    /// The latch records the transition itself (see `Latch::trip`); a second trip changes
    /// nothing, and says nothing.
    fn trip(&self, source: &'static str, cause: Cause, reason: TripReason) {
        self.latch.trip(source, cause, reason);
    }
    /// Whether source `i`'s price, which just arrived, came later than `max_lag` behind
    /// its exchange stamp, and the venue's `late` gauge. A venue late on one frame stays
    /// out until a prompt frame arrives: a backlog delivers old frames one after another,
    /// each late, and the first frame that is not is the one that says the queue drained.
    fn judge_lateness(&mut self, i: usize) {
        let late = match (self.max_lag, self.connected[i].price_lag()) {
            (Some(limit), Some(feed::PriceLag::Stamped(lag))) => lag > limit,
            _ => false,
        };
        if late != self.late[i] {
            let venue = self.sources[i].venue;
            if late {
                tracing::warn!(
                    "[{}] {venue} is delivering prices late behind its own timestamps; left \
                     out of the average until a prompt one arrives",
                    self.label
                );
            } else {
                tracing::info!(
                    "[{}] {venue} is delivering prices on time again",
                    self.label
                );
            }
        }
        self.late[i] = late;
        self.venue_metrics[i].record(|m| m.late.set(if late { 1.0 } else { 0.0 }));
    }

    /// The sources whose latest sample is fresh at `(now, wall)`: delivered within
    /// [`MAX_PRICE_AGE`] and, under `max_lag`, not late behind its stamp. With the gauges
    /// that describe the count written.
    fn fresh_sources(&self, now: Instant, wall: SystemTime) -> Vec<(&Source, PriceSample)> {
        let fresh: Vec<(&Source, PriceSample)> = self
            .sources
            .iter()
            .zip(&self.latest)
            .zip(&self.late)
            .filter_map(|((source, sample), late)| {
                sample
                    .filter(|s| !late && s.age_at(now, wall) <= MAX_PRICE_AGE)
                    .map(|s| (source, s))
            })
            .collect();
        let min = self.min_sources as f64;
        self.metrics.record(|m| {
            m.feed_sources_fresh.set(fresh.len() as f64);
            m.feed_sources_min.set(min);
        });
        if fresh.len() < self.min_sources {
            // `feed_up` is the pair's "can publish" signal, and right now it cannot.
            self.metrics.record(|m| {
                m.feed_up.set(0.0);
                m.feed_sample_current.set(0.0);
            });
        }
        fresh
    }

    /// The timer's work: the gauges, from what is fresh right now, and no sample.
    fn audit_freshness(&self) {
        let _ = self.fresh_sources(Instant::now(), SystemTime::now());
    }

    /// Averages the fresh sources and publishes the result, or publishes nothing when too
    /// few are fresh.
    fn recompute(&mut self, tx: &watch::Sender<Option<PriceSample>>) {
        let now = Instant::now();
        let wall = SystemTime::now();
        let fresh = self.fresh_sources(now, wall);
        if fresh.len() < self.min_sources {
            // Nothing is written to the channel: the last composite stays and ages out
            // downstream, which is how a single stalled feed always behaved.
            return;
        }
        let Some((delta, mid)) = weighted_average(&fresh) else {
            // Only reachable with weights and prices whose product leaves a uint256, which
            // no venue prints; treated like too few sources rather than panicking the task.
            tracing::warn!(
                "[{}] the weighted mid overflows a uint256; not publishing this tick",
                self.label
            );
            return;
        };

        // Judged before it is published to the channel, so a reader can never see a
        // sample no guard has. The verdict is not acted on here (nothing this task holds
        // can withdraw a quote): the loop consults the latch when it next runs, or wakes
        // on it, and does the withdrawing and the announcing. A tripped lane is judged no
        // further, which is the latching the breaker used to do for itself.
        if !self.guards.is_empty() && self.latch.tripped().is_none() {
            let sources = if self.judges_sources {
                self.source_samples(&fresh, now, wall)
            } else {
                Vec::new()
            };
            let sample = CompositeSample::new(now, mid, delta, self.invert, self.decimals, sources);
            let mut tripped = None;
            for g in &mut self.guards {
                let mut out = crate::pricing::Diagnostics::new(g.bound.diagnostics.len());
                // A guard is the binary's code, and this task feeds every reader of the
                // lane: its panic is that lane's trip, not this task's death.
                let verdict = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    g.bound.timed(|| g.guard.assess(&sample, &mut out))
                }));
                g.bound.publish(&out);
                match verdict {
                    Ok(Verdict::Pass) => {}
                    Ok(Verdict::Trip(reason)) => {
                        tripped = Some((g.kind, Cause::Guard, reason));
                        break;
                    }
                    Err(payload) => {
                        let reason = crate::guard::panic_reason(g.kind, "assess", &*payload);
                        tripped = Some((g.kind, Cause::Panic, reason));
                        break;
                    }
                }
            }
            if let Some((source, cause, reason)) = tripped {
                self.trip(source, cause, reason);
            }
        }

        if self.history.is_some() || self.recorder.is_some() {
            // The market's own mid, whichever way this lane carries it. Through f64: the
            // history is an estimate of σ and the recorder a dashboard series, neither a
            // published number.
            let market_mid = scaled_to_f64(mid, self.decimals);
            let market_mid = if self.invert {
                1.0 / market_mid
            } else {
                market_mid
            };
            if let Some(history) = &self.history {
                crate::volatile::lock(history).record(market_mid, now);
            }
            if let Some(recorder) = &self.recorder {
                recorder.book(&self.label, market_mid, scaled_to_f64(delta, self.decimals));
            }
        }
        self.metrics.record(|m| {
            m.feed_up.set(1.0);
            m.feed_ticks.inc();
            m.feed_mid.set(scaled_to_f64(mid, self.decimals));
            m.feed_delta.set(scaled_to_f64(delta, self.decimals));
            m.feed_last_tick.set(
                wall.duration_since(SystemTime::UNIX_EPOCH)
                    .map(|d| d.as_secs_f64())
                    .unwrap_or(0.0),
            );
            m.feed_sample_current.set(1.0);
        });
        tx.send_replace(Some(PriceSample {
            delta,
            mid,
            at: now,
            wall,
        }));
    }
}

/// `(Σ w·delta / Σ w, Σ w·mid / Σ w)` over `fresh`, or `None` if a product overflows. The
/// weights' scale cancels, so the result is at the samples' own scale. Integer division
/// truncates, the same as every other scaled quotient in the pusher.
pub(crate) fn weighted_average(fresh: &[(&Source, PriceSample)]) -> Option<(U256, U256)> {
    let mut sum_w = U256::zero();
    let mut sum_mid = U256::zero();
    let mut sum_delta = U256::zero();
    for (source, sample) in fresh {
        sum_w = sum_w.checked_add(source.weight)?;
        sum_mid = sum_mid.checked_add(source.weight.checked_mul(sample.mid)?)?;
        sum_delta = sum_delta.checked_add(source.weight.checked_mul(sample.delta)?)?;
    }
    if sum_w.is_zero() {
        return None;
    }
    Some((sum_delta / sum_w, sum_mid / sum_w))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::breaker::BreakerConfig;
    use crate::config::WEIGHT_DECIMALS;
    use crate::feed::parse_decimal_scaled;
    use crate::guard::{Cause, MarketGuard, Verdict, deviation::DeviationGuard};
    use futures_util::SinkExt;
    use tokio::net::TcpListener;
    use tokio_tungstenite::accept_async;
    use tokio_tungstenite::tungstenite::Message;

    fn source(venue: VenueId, symbol: &str, weight: &str) -> Source {
        Source {
            venue,
            symbol: symbol.to_owned(),
            weight: parse_decimal_scaled(weight, WEIGHT_DECIMALS).unwrap(),
        }
    }

    fn sample(mid: u64, delta: u64) -> PriceSample {
        PriceSample {
            delta: U256::from(delta),
            mid: U256::from(mid),
            at: Instant::now(),
            wall: SystemTime::now(),
        }
    }

    #[test]
    fn the_average_is_weighted_and_the_weights_scale_cancels() {
        let a = source(VenueId::Binance, "A", "2");
        let b = source(VenueId::Binance, "B", "1");
        // (2·3000 + 1·3300) / 3 = 3100; (2·10 + 1·40) / 3 = 20
        let (delta, mid) =
            weighted_average(&[(&a, sample(3000, 10)), (&b, sample(3300, 40))]).unwrap();
        assert_eq!(mid, U256::from(3100u64));
        assert_eq!(delta, U256::from(20u64));
        // Fractional weights mean the same thing.
        let a = source(VenueId::Binance, "A", "0.5");
        let b = source(VenueId::Binance, "B", "0.25");
        let (_, mid) = weighted_average(&[(&a, sample(3000, 0)), (&b, sample(3300, 0))]).unwrap();
        assert_eq!(mid, U256::from(3100u64));
    }

    #[test]
    fn a_single_source_averages_to_itself() {
        let a = source(VenueId::Binance, "A", "1");
        let (delta, mid) = weighted_average(&[(&a, sample(123_456, 789))]).unwrap();
        assert_eq!((delta, mid), (U256::from(789u64), U256::from(123_456u64)));
    }

    #[test]
    fn an_overflowing_product_is_none_rather_than_a_panic() {
        let a = Source {
            venue: VenueId::Binance,
            symbol: "A".into(),
            weight: U256::MAX,
        };
        assert!(weighted_average(&[(&a, sample(2, 0))]).is_none());
    }

    /// A mock venue: one WebSocket server that sends the given bookTicker frames and then
    /// holds the connection open.
    async fn mock_venue(frames: Vec<String>, gap: Duration) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            while let Ok((stream, _)) = listener.accept().await {
                let frames = frames.clone();
                tokio::spawn(async move {
                    let Ok(mut ws) = accept_async(stream).await else {
                        return;
                    };
                    for frame in frames {
                        if ws.send(Message::text(frame)).await.is_err() {
                            return;
                        }
                        tokio::time::sleep(gap).await;
                    }
                    std::future::pending::<()>().await;
                });
            }
        });
        format!("ws://{addr}")
    }

    fn ticker(px: &str) -> String {
        format!(r#"{{"u":1,"s":"TESTUSDT","b":"{px}","B":"1","a":"{px}","A":"1"}}"#)
    }

    /// Test params: adopted at once (there is no other generation to lose a race with),
    /// with a latch and whatever guards the test wants judging, the deviation guard
    /// included when it passes a `BreakerConfig`.
    fn params<'a>(
        endpoints: &'a Endpoints,
        metrics: &'a Metrics,
        latch: Latch,
        deviation: Option<BreakerConfig>,
        mut guards: Vec<BoundGuard>,
    ) -> Params<'a> {
        let adopted = Adoption::new();
        adopted.adopt();
        let pair_metrics = metrics.for_pair("TEST");
        if let Some(config) = deviation {
            let gated = Gated::new(pair_metrics.clone(), adopted.clone());
            guards.insert(
                0,
                BoundGuard::built_in(
                    "deviation",
                    Box::new(DeviationGuard::new(config, Some(gated))),
                ),
            );
        }
        Params {
            endpoints,
            decimals: 8,
            invert: false,
            latch,
            guards,
            adopted,
            metrics,
            pair_metrics,
            label: "TEST".to_owned(),
            recorder: None,
            history: None,
        }
    }

    fn two_percent() -> BreakerConfig {
        BreakerConfig {
            threshold_scaled: Some(parse_decimal_scaled("0.02", 8).unwrap()),
            window: None,
            decimals: 8,
        }
    }

    /// The deviation guard judges the composite where it is computed, not where it is
    /// quoted: this test never reads the first two samples, which is exactly what an outage
    /// looks like from the feed. Also asserts the trip's metrics: it must flip
    /// `breaker_tripped`, count `breaker_trips`, set `trip_source`, and leave
    /// `breaker_deviation_ratio` above the limit it crossed.
    #[tokio::test]
    async fn the_composite_judges_every_sample_even_when_nobody_is_reading() {
        // 1.0000, then +0.1% (inside a 2% threshold), then +5.9% from *that*.
        let url = mock_venue(
            vec![
                ticker("1.00000000"),
                ticker("1.00100000"),
                ticker("1.06000000"),
            ],
            Duration::from_millis(50),
        )
        .await;
        let endpoints = Endpoints::single(VenueId::Binance, url);
        let metrics = Metrics::new().unwrap();
        let latch = Latch::new();
        // The lane's series on the latch, as `build_live` attaches them: the latch records
        // the trip's transition, not the composite.
        let adopted = crate::feed::Adoption::new();
        adopted.adopt();
        latch.attach_metrics(Gated::new(metrics.for_pair("TEST"), adopted));
        let feeds = Feeds {
            sources: vec![source(VenueId::Binance, "TESTUSDT", "1")],
            min_sources: 1,
            checks: Default::default(),
        };
        let mut spawned = spawn(
            &feeds,
            params(
                &endpoints,
                &metrics,
                latch.clone(),
                Some(two_percent()),
                Vec::new(),
            ),
        );
        tokio::time::timeout(
            Duration::from_secs(5),
            spawned
                .rx
                .wait_for(|s| matches!(s, Some(p) if p.mid == U256::from(106_000_000u64))),
        )
        .await
        .expect("no third price")
        .unwrap();

        let trip = latch
            .tripped()
            .expect("a 5.9% jump between two unread samples must trip the lane");
        assert_eq!((trip.source, trip.cause), ("deviation", Cause::Guard));
        assert!(
            trip.reason.alarm.ends_with("(previous 1.001, new 1.06)"),
            "judged against the sample before it: {}",
            trip.reason.alarm
        );
        let m = metrics.for_pair("TEST");
        assert_eq!(m.feed_ticks.get(), 3, "every composite sample counted");
        assert_eq!(m.breaker_tripped.get(), 1.0, "the trip metric must flip");
        assert_eq!(m.breaker_trips.get(), 1, "exactly one trip counted");
        assert!(m.breaker_deviation_ratio.get() > 1.0);
        assert_eq!(metrics.trip_source("TEST", "deviation", "guard").get(), 1.0);
        assert_eq!(m.feed_sources_fresh.get(), 1.0);
        assert_eq!(m.feed_up.get(), 1.0);
    }

    /// A market guard that counts its calls and never trips.
    struct Counting(Arc<std::sync::atomic::AtomicUsize>);

    impl MarketGuard for Counting {
        fn assess(&mut self, _: &CompositeSample, _: &mut crate::pricing::Diagnostics) -> Verdict {
            self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Verdict::Pass
        }
    }

    /// A tripped latch stops the judging, not the publishing: readers keep seeing samples
    /// (the loop needs them to render the block header), and no guard runs again, so a
    /// registered guard behind the deviation guard sees the samples before the trip and
    /// none after.
    #[tokio::test]
    async fn after_a_trip_the_composite_publishes_but_judges_no_more() {
        let url = mock_venue(
            vec![
                ticker("1.00000000"),
                ticker("1.06000000"),
                ticker("1.07000000"),
                ticker("1.08000000"),
            ],
            Duration::from_millis(50),
        )
        .await;
        let endpoints = Endpoints::single(VenueId::Binance, url);
        let metrics = Metrics::new().unwrap();
        let latch = Latch::new();
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let counting = BoundGuard::built_in("counting", Box::new(Counting(calls.clone())));
        let feeds = Feeds {
            sources: vec![source(VenueId::Binance, "TESTUSDT", "1")],
            min_sources: 1,
            checks: Default::default(),
        };
        let mut spawned = spawn(
            &feeds,
            params(
                &endpoints,
                &metrics,
                latch.clone(),
                Some(two_percent()),
                vec![counting],
            ),
        );
        tokio::time::timeout(
            Duration::from_secs(5),
            spawned
                .rx
                .wait_for(|s| matches!(s, Some(p) if p.mid == U256::from(108_000_000u64))),
        )
        .await
        .expect("samples keep publishing after the trip")
        .unwrap();
        assert_eq!(latch.tripped().map(|t| t.source), Some("deviation"));
        // The first sample passed both guards; the second tripped the deviation guard, so
        // the counting guard never saw it, nor the two after it.
        assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 1);
    }

    /// What a registered guard is handed beside the mid: the fresh sources behind the
    /// sample, by venue, weight and age, so a venue-agreement guard can be written. (The
    /// built-in deviation guard reads none of it, and a lane with only that guard builds
    /// none.)
    type SeenSources = Arc<std::sync::Mutex<Vec<Vec<(&'static str, f64)>>>>;
    struct Seeing(SeenSources);

    impl MarketGuard for Seeing {
        fn assess(
            &mut self,
            sample: &CompositeSample,
            _: &mut crate::pricing::Diagnostics,
        ) -> Verdict {
            self.0.lock().unwrap().push(
                sample
                    .sources()
                    .iter()
                    .map(|source| (source.venue, source.weight))
                    .collect(),
            );
            Verdict::Pass
        }
    }

    #[tokio::test]
    async fn a_registered_market_guard_sees_the_venues_behind_the_sample() {
        let url = mock_venue(
            vec![ticker("1.00000000"), ticker("1.00100000")],
            Duration::from_millis(50),
        )
        .await;
        let endpoints = Endpoints::single(VenueId::Binance, url);
        let metrics = Metrics::new().unwrap();
        let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
        let seeing = BoundGuard::built_in("seeing", Box::new(Seeing(seen.clone())));
        let feeds = Feeds {
            sources: vec![source(VenueId::Binance, "TESTUSDT", "1")],
            min_sources: 1,
            checks: Default::default(),
        };
        let mut spawned = spawn(
            &feeds,
            params(&endpoints, &metrics, Latch::new(), None, vec![seeing]),
        );
        tokio::time::timeout(
            Duration::from_secs(5),
            spawned
                .rx
                .wait_for(|s| matches!(s, Some(p) if p.mid == U256::from(100_100_000u64))),
        )
        .await
        .expect("samples publish")
        .unwrap();
        let seen = seen.lock().unwrap();
        assert!(!seen.is_empty());
        assert!(
            seen.iter()
                .all(|sources| sources == &vec![("binance", 1.0)]),
            "{seen:?}"
        );
    }

    /// A guard that panics trips the lane with `cause = panic`, naming the guard and the
    /// call, and the composite task survives to keep publishing.
    struct Panicking;

    impl MarketGuard for Panicking {
        fn assess(&mut self, _: &CompositeSample, _: &mut crate::pricing::Diagnostics) -> Verdict {
            panic!("dispersion index out of range")
        }
    }

    #[tokio::test]
    async fn a_panicking_market_guard_trips_the_lane_and_keeps_the_composite_alive() {
        let url = mock_venue(
            vec![ticker("1.00000000"), ticker("1.00100000")],
            Duration::from_millis(50),
        )
        .await;
        let endpoints = Endpoints::single(VenueId::Binance, url);
        let metrics = Metrics::new().unwrap();
        let latch = Latch::new();
        // The lane's series on the latch, as `build_live` attaches them: the latch records
        // the trip's transition, not the composite.
        let adopted = crate::feed::Adoption::new();
        adopted.adopt();
        latch.attach_metrics(Gated::new(metrics.for_pair("TEST"), adopted));
        let fragile = BoundGuard::built_in("dispersion", Box::new(Panicking));
        let feeds = Feeds {
            sources: vec![source(VenueId::Binance, "TESTUSDT", "1")],
            min_sources: 1,
            checks: Default::default(),
        };
        let mut spawned = spawn(
            &feeds,
            params(&endpoints, &metrics, latch.clone(), None, vec![fragile]),
        );
        tokio::time::timeout(
            Duration::from_secs(5),
            spawned
                .rx
                .wait_for(|s| matches!(s, Some(p) if p.mid == U256::from(100_100_000u64))),
        )
        .await
        .expect("the composite keeps publishing after a guard panicked")
        .unwrap();
        let trip = latch.tripped().expect("a panic trips the lane");
        assert_eq!((trip.source, trip.cause), ("dispersion", Cause::Panic));
        assert_eq!(
            trip.reason.alarm,
            "assess of `dispersion` panicked: dispersion index out of range"
        );
        assert_eq!(
            metrics.trip_source("TEST", "dispersion", "panic").get(),
            1.0
        );
    }

    /// The windowed ratio reaches its gauge on an ordinary sample, the same way the tick
    /// ratio does.
    #[tokio::test]
    async fn the_window_ratio_reaches_its_gauge() {
        // The window buckets by whole second (see `CircuitBreaker::record`); without a gap
        // past that boundary the second sample would collapse into the first's bucket.
        let url = mock_venue(
            vec![ticker("1.00000000"), ticker("1.05000000")],
            Duration::from_millis(1_100),
        )
        .await;
        let endpoints = Endpoints::single(VenueId::Binance, url);
        let metrics = Metrics::new().unwrap();
        // A generous tick limit, so the tick-to-tick check cannot trip on this 5% move and
        // return before the window is measured; a 10% window over 5 blocks, so the 5% move
        // lands at exactly half the limit.
        let windowed = BreakerConfig {
            threshold_scaled: Some(parse_decimal_scaled("0.50", 8).unwrap()),
            window: Some(crate::breaker::WindowConfig {
                threshold_scaled: parse_decimal_scaled("0.10", 8).unwrap(),
                blocks: 5,
                span: Duration::from_secs(5 * crate::update::BLOCK_TIME_SECS),
            }),
            decimals: 8,
        };
        let feeds = Feeds {
            sources: vec![source(VenueId::Binance, "TESTUSDT", "1")],
            min_sources: 1,
            checks: Default::default(),
        };
        let mut spawned = spawn(
            &feeds,
            params(
                &endpoints,
                &metrics,
                Latch::new(),
                Some(windowed),
                Vec::new(),
            ),
        );
        tokio::time::timeout(
            Duration::from_secs(5),
            spawned
                .rx
                .wait_for(|s| matches!(s, Some(p) if p.mid == U256::from(105_000_000u64))),
        )
        .await
        .expect("no second price")
        .unwrap();
        let ratio = metrics
            .for_pair("TEST")
            .breaker_window_deviation_ratio
            .get();
        assert!((ratio - 0.5).abs() < 1e-9, "got {ratio}");
    }

    /// Two venues at different prices average by weight, and a venue that never delivers
    /// is simply not in the average while `min_sources` is met by the others.
    #[tokio::test]
    async fn two_venues_average_by_weight_and_a_silent_one_is_left_out() {
        let fast = mock_venue(vec![ticker("3000.00000000")], Duration::ZERO).await;
        let slow = mock_venue(vec![ticker("3300.00000000")], Duration::ZERO).await;
        let silent = mock_venue(vec![], Duration::ZERO).await;
        // Three sources on one venue id, pointed at three different mocks through the
        // symbol: the endpoint is per venue, so give each mock its own "venue" by making
        // the URL depend on the symbol via a small router.
        let router = route(vec![
            ("fast", fast.clone()),
            ("slow", slow.clone()),
            ("silent", silent.clone()),
        ])
        .await;
        let endpoints = Endpoints::single(VenueId::Binance, router);
        let metrics = Metrics::new().unwrap();
        let feeds = Feeds {
            sources: vec![
                source(VenueId::Binance, "fast", "2"),
                source(VenueId::Binance, "slow", "1"),
                source(VenueId::Binance, "silent", "1"),
            ],
            min_sources: 2,
            checks: Default::default(),
        };
        let mut spawned = spawn(
            &feeds,
            params(&endpoints, &metrics, Latch::new(), None, Vec::new()),
        );
        // Wait until both live venues are in: (2·3000 + 1·3300)/3 = 3100.
        let sample = tokio::time::timeout(
            Duration::from_secs(5),
            spawned
                .rx
                .wait_for(|s| matches!(s, Some(p) if p.mid == U256::from(310_000_000_000u64))),
        )
        .await
        .expect("no composite of the two live venues")
        .unwrap()
        .unwrap();
        assert_eq!(sample.delta, U256::zero());
        let m = metrics.for_pair("TEST");
        assert_eq!(m.feed_sources_fresh.get(), 2.0);
        assert_eq!(m.feed_sources_min.get(), 2.0);
        assert_eq!(m.feed_up.get(), 1.0);
        assert_eq!(
            metrics.for_venue("TEST", "binance").ticks.get(),
            2,
            "one accepted sample per live venue, on the shared venue series"
        );
    }

    /// Below `min_sources` nothing is published: the composite holds its last value and
    /// `feed_up` says the pair cannot publish.
    #[tokio::test]
    async fn fewer_fresh_venues_than_min_sources_publishes_nothing() {
        let live = mock_venue(vec![ticker("1.00000000")], Duration::ZERO).await;
        let silent = mock_venue(vec![], Duration::ZERO).await;
        let router = route(vec![("live", live), ("silent", silent)]).await;
        let endpoints = Endpoints::single(VenueId::Binance, router);
        let metrics = Metrics::new().unwrap();
        let feeds = Feeds {
            sources: vec![
                source(VenueId::Binance, "live", "1"),
                source(VenueId::Binance, "silent", "1"),
            ],
            min_sources: 2,
            checks: Default::default(),
        };
        let spawned = spawn(
            &feeds,
            params(&endpoints, &metrics, Latch::new(), None, Vec::new()),
        );
        // Give the live venue time to deliver and the composite to consider it.
        tokio::time::timeout(Duration::from_secs(5), async {
            while metrics.for_pair("TEST").feed_sources_fresh.get() != 1.0 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("the live venue never delivered");
        assert!(
            spawned.rx.borrow().is_none(),
            "one of two required venues is not enough"
        );
        assert_eq!(metrics.for_pair("TEST").feed_up.get(), 0.0);
        let err = match connect(
            &feeds,
            params(&endpoints, &metrics, Latch::new(), None, Vec::new()),
        )
        .await
        {
            Ok(_) => panic!("one of two required venues is not enough to connect either"),
            Err(err) => err.to_string(),
        };
        assert!(
            err.contains("binance:silent"),
            "the venue that never delivered is named: {err}"
        );
    }

    /// A WebSocket server that dials the mock whose name is the path's symbol and proxies
    /// frames one way. Lets one venue id (one endpoint) stand in for several mocks.
    ///
    /// The handshake callback's return type is tungstenite's, `Err` being a whole HTTP
    /// response, which newer clippy flags as a large error; not ours to shrink.
    #[allow(clippy::result_large_err)]
    async fn route(mocks: Vec<(&'static str, String)>) -> String {
        use futures_util::StreamExt;
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            while let Ok((stream, _)) = listener.accept().await {
                let mocks = mocks.clone();
                tokio::spawn(async move {
                    let mut path = None;
                    let Ok(mut client) = tokio_tungstenite::accept_hdr_async(
                        stream,
                        |req: &tokio_tungstenite::tungstenite::handshake::server::Request, resp| {
                            path = Some(req.uri().path().to_owned());
                            Ok(resp)
                        },
                    )
                    .await
                    else {
                        return;
                    };
                    // `/<symbol>@bookTicker`, as the Binance adapter builds it.
                    let path = path.unwrap_or_default();
                    let symbol = path.trim_start_matches('/').split('@').next().unwrap_or("");
                    let Some((_, upstream)) = mocks.iter().find(|(name, _)| *name == symbol) else {
                        return;
                    };
                    let Ok(mut upstream) = crate::ws::connect(upstream.as_str()).await else {
                        return;
                    };
                    while let Some(Ok(msg)) = upstream.next().await {
                        if client.send(msg).await.is_err() {
                            return;
                        }
                    }
                });
            }
        });
        format!("ws://{addr}")
    }

    /// A venue delivering prices late behind its own timestamps is left out of the average
    /// under `max_source_lag_ms`, as a silent one is, and `quote_updater_venue_late` says
    /// so, until a prompt frame arrives. Through real sockets: two scripted venues, OKX
    /// stamping its frames and Binance not.
    #[tokio::test]
    async fn a_venue_late_behind_its_stamps_is_left_out_until_a_prompt_frame() {
        let (binance_url, binance) = scripted_venue().await;
        let (okx_url, okx) = scripted_venue().await;
        let table = std::collections::BTreeMap::from([
            ("binance".to_owned(), binance_url),
            ("okx".to_owned(), okx_url),
        ]);
        let endpoints = Endpoints::resolve(None, &table).unwrap();
        let metrics = Metrics::new().unwrap();
        let feeds = Feeds {
            sources: vec![
                source(VenueId::Binance, "ETHUSDT", "1"),
                source(VenueId::Okx, "ETH-USDT", "1"),
            ],
            min_sources: 1,
            checks: crate::config::SourceChecks {
                max_lag: Some(Duration::from_millis(1_500)),
            },
        };
        let mut spawned = spawn(
            &feeds,
            params(&endpoints, &metrics, Latch::new(), None, Vec::new()),
        );
        let mid_of = |px: u64| U256::from(px * 100_000_000);
        let wait_for = |rx: &mut watch::Receiver<Option<PriceSample>>, mid: U256| {
            let mut rx = rx.clone();
            async move {
                tokio::time::timeout(
                    Duration::from_secs(5),
                    rx.wait_for(move |s| matches!(s, Some(p) if p.mid == mid)),
                )
                .await
                .unwrap_or_else(|_| panic!("no composite at {mid}"))
                .unwrap();
            }
        };
        let okx_late = metrics.for_venue("TEST", "okx").late.clone();

        // Both venues on time: averaged, OKX not late.
        binance.send(book_ticker("4000.00")).unwrap();
        okx.send(okx_ticker("4100.00", now_ms())).unwrap();
        wait_for(&mut spawned.rx, mid_of(4050)).await;
        assert_eq!(okx_late.get(), 0.0);

        // OKX's next frame was made 30s ago: it is behind, and drops out of the average.
        okx.send(okx_ticker("4100.00", now_ms() - 30_000)).unwrap();
        wait_for(&mut spawned.rx, mid_of(4000)).await;
        assert_eq!(okx_late.get(), 1.0);
        assert_eq!(metrics.for_pair("TEST").feed_sources_fresh.get(), 1.0);

        // A prompt frame brings it back.
        okx.send(okx_ticker("4200.00", now_ms())).unwrap();
        wait_for(&mut spawned.rx, mid_of(4100)).await;
        assert_eq!(okx_late.get(), 0.0);
        assert_eq!(metrics.for_pair("TEST").feed_sources_fresh.get(), 2.0);
    }

    /// Without the key the same late frame is averaged as it always was.
    #[tokio::test]
    async fn without_the_key_a_late_frame_is_averaged_as_before() {
        let (binance_url, binance) = scripted_venue().await;
        let (okx_url, okx) = scripted_venue().await;
        let table = std::collections::BTreeMap::from([
            ("binance".to_owned(), binance_url),
            ("okx".to_owned(), okx_url),
        ]);
        let endpoints = Endpoints::resolve(None, &table).unwrap();
        let metrics = Metrics::new().unwrap();
        let feeds = Feeds {
            sources: vec![
                source(VenueId::Binance, "ETHUSDT", "1"),
                source(VenueId::Okx, "ETH-USDT", "1"),
            ],
            min_sources: 1,
            checks: Default::default(),
        };
        let mut spawned = spawn(
            &feeds,
            params(&endpoints, &metrics, Latch::new(), None, Vec::new()),
        );
        binance.send(book_ticker("4000.00")).unwrap();
        okx.send(okx_ticker("4100.00", now_ms() - 30_000)).unwrap();
        tokio::time::timeout(
            Duration::from_secs(5),
            spawned
                .rx
                .wait_for(|s| matches!(s, Some(p) if p.mid == U256::from(405_000_000_000u64))),
        )
        .await
        .expect("the late frame is averaged in")
        .unwrap();
        assert_eq!(metrics.for_venue("TEST", "okx").late.get(), 0.0);
    }

    /// A websocket venue that sends exactly the frames it is handed, as text.
    async fn scripted_venue() -> (String, tokio::sync::mpsc::UnboundedSender<String>) {
        use futures_util::SinkExt;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (tx, mut frames) = tokio::sync::mpsc::unbounded_channel::<String>();
        tokio::spawn(async move {
            let Ok((stream, _)) = listener.accept().await else {
                return;
            };
            let Ok(mut ws) = tokio_tungstenite::accept_async(stream).await else {
                return;
            };
            while let Some(frame) = frames.recv().await {
                let frame = tokio_tungstenite::tungstenite::Message::text(frame);
                if ws.send(frame).await.is_err() {
                    return;
                }
            }
        });
        (format!("ws://{addr}"), tx)
    }

    fn book_ticker(px: &str) -> String {
        format!(r#"{{"u":1,"s":"ETHUSDT","b":"{px}","B":"1","a":"{px}","A":"1"}}"#)
    }

    fn okx_ticker(px: &str, ts_ms: u64) -> String {
        format!(
            r#"{{"arg":{{"channel":"tickers","instId":"ETH-USDT"}},"data":[{{"instId":"ETH-USDT","bidPx":"{px}","askPx":"{px}","ts":"{ts_ms}"}}]}}"#
        )
    }

    fn now_ms() -> u64 {
        SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64
    }
}
