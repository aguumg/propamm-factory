//! The composite: one pair's `(delta, mid)`, averaged by weight over the venues it reads,
//! recomputed whenever any of them delivers, and judged by the pair's circuit breaker and
//! recorded into its σ history here, because these must see the number the lane
//! publishes, not any one venue's.
//!
//! A venue whose latest sample is older than [`MAX_PRICE_AGE`] drops out of the average
//! and its weight is spread over the rest; the pair keeps publishing as long as at least
//! `min_sources` venues are fresh, and stops (by withdrawing its sample, so the loop
//! refuses the next tick) when fewer are. A single venue with weight 1 is exactly the feed
//! the pusher had before venues were plural.
//!
//! A venue quoted in USD on a pair quoted in a dollar token is converted by the pair's FX
//! legs (`[pairs.fx]`) before it is averaged, and left out whenever they cannot vouch for
//! the rate; see [`crate::config::Fx`].

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime};

use ethrex_common::U256;
use eyre::{Result, eyre};
use tokio::sync::{mpsc, watch};

use crate::config::{FX_DECIMALS, Feeds, Fx, Quote, Source};
use crate::feed::{self, Adoption, Gated, PriceSample, scaled_to_f64};
use crate::guard::{BoundGuard, Cause, CompositeSample, Latch, SourceSample, TripReason, Verdict};
use crate::metrics::{FxTrip, Metrics, PairMetrics, VenueMetrics};
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
    /// One of the pair's FX legs rather than a source: averaged into nothing, read only
    /// for the rate its USD sources are converted by.
    pub fx: bool,
}

impl VenueFeed {
    /// `binance:ETHUSDC`, how a venue is named in every line about it.
    pub fn name(&self) -> String {
        format!("{}:{}", self.venue, self.symbol)
    }

    /// The `venue` label of this feed's series: the venue's name for a source, and
    /// `fx:<venue>` for an FX leg, which often reads the same venue as one of the sources
    /// and must not share its series.
    pub fn series(&self) -> String {
        series_name(self.venue, self.fx)
    }
}

fn series_name(venue: VenueId, fx: bool) -> String {
    if fx {
        format!("fx:{venue}")
    } else {
        venue.as_str().to_owned()
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
    /// The FX legs ([`VenueFeed::name`]) that had not delivered by [`FIRST_PRICE_TIMEOUT`],
    /// as [`connect`] found them; empty from [`spawn`], which waits for nothing.
    pub silent_fx: Vec<String>,
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
    // The sources, then the FX legs. The legs are dialled the same way, so they are judged
    // fresh and late by the same rules, but never inverted: the rate is the quote token's
    // price in USD whichever way the lane publishes.
    let legs = feeds.fx.iter().flat_map(|fx| &fx.legs);
    let venues: Vec<VenueFeed> = feeds
        .sources
        .iter()
        .map(|source| (source.venue, &source.symbol, false))
        .chain(legs.map(|leg| (leg.venue, &leg.symbol, true)))
        .map(|(venue, symbol, fx)| {
            let venue_metrics = metrics.for_venue(&label, &series_name(venue, fx));
            let connected = feed::Connected::default();
            let rx = feed::spawn_feed(
                endpoints.http(),
                venue,
                endpoints.for_venue(venue),
                symbol,
                decimals,
                invert && !fx,
                Gated::new(venue_metrics.clone(), adopted.clone()),
                recorder.clone(),
                connected.clone(),
            );
            VenueFeed {
                venue,
                symbol: symbol.clone(),
                rx,
                metrics: venue_metrics,
                connected,
                fx,
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
        late: vec![false; venues.len()],
        latest: vec![None; venues.len()],
        fx: feeds.fx.clone(),
        fx_tripped: None,
        fx_said: None,
        started: Instant::now(),
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
                    m.fx_absent();
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
    Spawned {
        rx,
        venues,
        silent_fx: Vec::new(),
    }
}

/// [`spawn`], then waits for the first composite sample. Shared by startup and `--check`
/// so the two agree by construction: a source that fails here is one the run refuses to
/// start on, which is what makes checking it worth anything.
///
/// The FX legs are then given the rest of the same [`FIRST_PRICE_TIMEOUT`] to deliver, and
/// any still silent are named in [`Spawned::silent_fx`]. Not an error here: the sources
/// that need no rate are publishing, and the caller says what a silent leg means (a
/// warning on a run, a failure under `--check`). Without it a wrong FX symbol would keep
/// the USD sources out for good with only an alert to say so.
pub async fn connect(feeds: &Feeds, params: Params<'_>) -> Result<Spawned> {
    let deadline = tokio::time::Instant::now() + FIRST_PRICE_TIMEOUT;
    let mut spawned = spawn(feeds, params);
    let waited = tokio::time::timeout_at(deadline, spawned.rx.wait_for(|s| s.is_some()))
        .await
        .map(|first| first.map(|_| ()));
    match waited {
        Ok(Ok(())) => {
            for leg in spawned.venues.iter().filter(|v| v.fx) {
                let mut rx = leg.rx.clone();
                let _ = tokio::time::timeout_at(deadline, rx.wait_for(|s| s.is_some())).await;
            }
            spawned.silent_fx = spawned
                .venues
                .iter()
                .filter(|v| v.fx && v.rx.borrow().is_none())
                .map(VenueFeed::name)
                .collect();
            Ok(spawned)
        }
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
///
/// `connected`, `venue_metrics`, `late` and `latest` hold the sources first, in order, and
/// then the FX legs, so index `i` is `sources[i]` below `sources.len()` and an FX leg from
/// there on.
struct Composite {
    sources: Vec<Source>,
    min_sources: usize,
    /// `max_source_lag_ms`: how late a venue's price may arrive behind its exchange stamp
    /// and still count as fresh. `None` leaves every venue judged by silence alone.
    max_lag: Option<Duration>,
    /// Each feed's flag, where the feed task leaves how late its last price was.
    connected: Vec<feed::Connected>,
    /// Each feed's series, for `late` and `converted`.
    venue_metrics: Vec<Gated<VenueMetrics>>,
    /// Whether each feed's latest price arrived later than `max_lag`, judged when that
    /// price arrived ([`Self::judge_lateness`]), so a stale reading cannot flip between
    /// frames. Always `false` without `max_lag` and for a venue whose frames carry no stamp.
    late: Vec<bool>,
    latest: Vec<Option<PriceSample>>,
    /// `[pairs.fx]`, on a pair with sources quoted in USD.
    fx: Option<Fx>,
    /// Why the FX rate was refused at the last judging, if it was.
    fx_tripped: Option<FxTrip>,
    /// What the log last said about the rate, so it says each change once.
    fx_said: Option<FxTrip>,
    /// When the composite was spawned: the legs are still being dialled until
    /// [`FIRST_PRICE_TIMEOUT`] after it, and a rate stale for that alone is not said.
    started: Instant,
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
            let venue = self.feed_name(i);
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

    /// How feed `i` is named in a line about it: the venue for a source, the venue and
    /// symbol for an FX leg.
    fn feed_name(&self, i: usize) -> String {
        match self.sources.get(i) {
            Some(source) => source.venue.to_string(),
            None => self
                .fx
                .iter()
                .flat_map(|fx| &fx.legs)
                .nth(i - self.sources.len())
                .map_or_else(String::new, |leg| {
                    format!("{}:{} (fx)", leg.venue, leg.symbol)
                }),
        }
    }

    /// The FX rate the USD sources are converted by at `(now, wall)`, or `None` when it
    /// cannot be used and they are left out: judged over the legs that are fresh by the
    /// sources' own rules, with its gauges written and a change of verdict said once.
    fn judge_fx(&mut self, now: Instant, wall: SystemTime) -> Option<FxRate> {
        let fx = self.fx.as_ref()?;
        let n = self.sources.len();
        let fresh: Vec<PriceSample> = self.latest[n..]
            .iter()
            .zip(&self.late[n..])
            .filter_map(|(sample, late)| {
                sample.filter(|s| !late && s.age_at(now, wall) <= MAX_PRICE_AGE)
            })
            .collect();
        let verdict = fx_rate(&fresh, fx, self.decimals);
        let tripped = verdict.as_ref().err().copied();
        let mean = mean_rate(&fresh, self.decimals);
        self.metrics.record(|m| {
            m.fx_rate.set(mean.unwrap_or(f64::NAN));
            m.fx_sources_fresh.set(fresh.len() as f64);
            for reason in FxTrip::ALL {
                m.fx_guard_tripped(reason)
                    .set(if tripped == Some(reason) { 1.0 } else { 0.0 });
            }
        });
        self.fx_tripped = tripped;
        let dialling = tripped == Some(FxTrip::Stale)
            && self.fx_said.is_none()
            && now < self.started + FIRST_PRICE_TIMEOUT;
        if tripped != self.fx_said && !dialling {
            match tripped {
                Some(reason) => tracing::warn!(
                    "[{}] FX rate refused ({}: {} of {} legs fresh, mean {}); the USD sources \
                     are left out of the average until it is usable",
                    self.label,
                    reason.as_str(),
                    fresh.len(),
                    fx.legs.len(),
                    mean.map_or_else(|| "none".to_owned(), |m| m.to_string()),
                ),
                None => tracing::info!(
                    "[{}] FX rate usable again; the USD sources are back in the average",
                    self.label
                ),
            }
            self.fx_said = tripped;
        }
        verdict.ok()
    }

    /// The sources whose latest sample is fresh at `(now, wall)`: delivered within
    /// [`MAX_PRICE_AGE`] and, under `max_lag`, not late behind its stamp. A source quoted
    /// in USD is in only with `fx`, the rate [`Self::judge_fx`] allowed, and converted by
    /// it. With the gauges that describe the count written.
    fn fresh_sources(
        &self,
        now: Instant,
        wall: SystemTime,
        fx: Option<FxRate>,
    ) -> Vec<(&Source, PriceSample)> {
        let fresh: Vec<(&Source, PriceSample)> = self
            .sources
            .iter()
            .zip(&self.latest)
            .zip(&self.late)
            .zip(&self.venue_metrics)
            .filter_map(|(((source, sample), late), venue_metrics)| {
                let sample = sample.filter(|s| !late && s.age_at(now, wall) <= MAX_PRICE_AGE);
                let sample = match source.quote {
                    None => sample,
                    Some(Quote::Usd) => {
                        let converted = sample.zip(fx).and_then(|(sample, fx)| {
                            convert(sample, fx, self.decimals, self.invert)
                        });
                        venue_metrics.record(|m| {
                            m.converted.set(if converted.is_some() { 1.0 } else { 0.0 })
                        });
                        converted
                    }
                };
                sample.map(|s| (source, s))
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
    fn audit_freshness(&mut self) {
        let (now, wall) = (Instant::now(), SystemTime::now());
        let fx = self.judge_fx(now, wall);
        let _ = self.fresh_sources(now, wall, fx);
    }

    /// Averages the fresh sources and publishes the result, or publishes nothing when too
    /// few are fresh.
    fn recompute(&mut self, tx: &watch::Sender<Option<PriceSample>>) {
        let now = Instant::now();
        let wall = SystemTime::now();
        let fx = self.judge_fx(now, wall);
        let fresh = self.fresh_sources(now, wall, fx);
        if fresh.len() < self.min_sources {
            // The last composite is withdrawn, not left to age out. For venues that went
            // silent that changes nothing: the last composite was published at the last
            // frame, so by the time they are dropped it is as old as their silence and the
            // loop already refuses it. For a venue dropped as late it is the whole point:
            // the price it last gave was already behind, and the loop would otherwise keep
            // quoting it for up to MAX_PRICE_AGE.
            if tx.borrow().is_some() {
                let late: Vec<&str> = self
                    .sources
                    .iter()
                    .zip(&self.late)
                    .filter(|(_, late)| **late)
                    .map(|(source, _)| source.venue.as_str())
                    .collect();
                // The USD sources out for the rate is a cause of its own, and not one a
                // venue's own gauges show.
                let fx_out =
                    self.fx_tripped
                        .filter(|_| fx.is_none())
                        .map_or_else(String::new, |reason| {
                            format!(
                                "; the USD sources are out, FX rate refused ({})",
                                reason.as_str()
                            )
                        });
                tracing::warn!(
                    "[{}] {} of {} venues fresh, {} needed{}{fx_out}; withdrawing until enough \
                     are",
                    self.label,
                    fresh.len(),
                    self.sources.len(),
                    self.min_sources,
                    if late.is_empty() {
                        String::new()
                    } else {
                        format!(" ({} late)", late.join(", "))
                    }
                );
                tx.send_replace(None);
            }
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

/// The rate a pair's USD sources are converted by: the price of one quote token in USD at
/// the price scale, and the stamp of the oldest leg it was read from.
#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) struct FxRate {
    pub rate: U256,
    pub at: Instant,
    pub wall: SystemTime,
}

/// The legs' plain mean as a number, for the gauge and the log; `None` with no leg.
fn mean_rate(legs: &[PriceSample], decimals: u32) -> Option<f64> {
    (!legs.is_empty()).then(|| {
        legs.iter()
            .map(|leg| scaled_to_f64(leg.mid, decimals))
            .sum::<f64>()
            / legs.len() as f64
    })
}

/// The rate `fresh` legs give under `fx`'s rules, or why they give none: too few of them,
/// a mean further from 1 than `max_depeg`, or a spread (highest less lowest, over the mean)
/// wider than `max_disagreement`. The comparisons are in `f64`, as the breaker's ratios
/// are; the rate itself is the integer mean, so the conversion is exact.
pub(crate) fn fx_rate(fresh: &[PriceSample], fx: &Fx, decimals: u32) -> Result<FxRate, FxTrip> {
    if fresh.len() < fx.min_sources {
        return Err(FxTrip::Stale);
    }
    let mean = mean_rate(fresh, decimals).ok_or(FxTrip::Stale)?;
    if (mean - 1.0).abs() > scaled_to_f64(fx.max_depeg, FX_DECIMALS) {
        return Err(FxTrip::Depeg);
    }
    let reading = |leg: &PriceSample| scaled_to_f64(leg.mid, decimals);
    let high = fresh.iter().map(reading).fold(f64::MIN, f64::max);
    let low = fresh.iter().map(reading).fold(f64::MAX, f64::min);
    if (high - low) / mean > scaled_to_f64(fx.max_disagreement, FX_DECIMALS) {
        return Err(FxTrip::Disagreement);
    }
    // Saturating: a sum near a uint256 is a mean the depeg check above already refused.
    let sum = fresh
        .iter()
        .fold(U256::zero(), |sum, leg| sum.saturating_add(leg.mid));
    let rate = sum / U256::from(fresh.len());
    // A mean that rounds to zero at the price scale is as far from 1 as a rate gets, and
    // would be divided by.
    if rate.is_zero() {
        return Err(FxTrip::Depeg);
    }
    let oldest = &fresh[0];
    Ok(FxRate {
        rate,
        at: fresh.iter().map(|leg| leg.at).fold(oldest.at, Instant::min),
        wall: fresh
            .iter()
            .map(|leg| leg.wall)
            .fold(oldest.wall, SystemTime::min),
    })
}

/// A USD source's sample in the pair's quote token: in market orientation the USD mid
/// over the rate (USD per base, over USD per quote token, is quote token per base), so on
/// an inverted lane, which carries the reciprocal, the lane mid times the rate. The delta
/// is a fraction of the mid and stays as it is. Stamped with the older of the sample's
/// and the rate's clocks, so the sample ages with whichever it was made of ages first.
/// `None` only for a product that leaves a uint256.
pub(crate) fn convert(
    sample: PriceSample,
    fx: FxRate,
    decimals: u32,
    invert: bool,
) -> Option<PriceSample> {
    let one = U256::exp10(decimals as usize);
    let mid = if invert {
        sample.mid.checked_mul(fx.rate)? / one
    } else {
        sample.mid.checked_mul(one)? / fx.rate
    };
    Some(PriceSample {
        delta: sample.delta,
        mid,
        at: sample.at.min(fx.at),
        wall: sample.wall.min(fx.wall),
    })
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
            quote: None,
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
            quote: None,
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
            fx: None,
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
            fx: None,
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
            fx: None,
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
            fx: None,
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
            fx: None,
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
            fx: None,
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
            fx: None,
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
            fx: None,
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
            fx: None,
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

    /// With one venue, a late frame leaves no venue to average: the composite withdraws
    /// its sample at once rather than leaving the last price to age out, and the next
    /// prompt frame puts a price back.
    #[tokio::test]
    async fn a_late_sole_venue_withdraws_the_sample_at_once() {
        let (okx_url, okx) = scripted_venue().await;
        let table = std::collections::BTreeMap::from([("okx".to_owned(), okx_url)]);
        let endpoints = Endpoints::resolve(None, &table).unwrap();
        let metrics = Metrics::new().unwrap();
        let feeds = Feeds {
            sources: vec![source(VenueId::Okx, "ETH-USDT", "1")],
            min_sources: 1,
            checks: crate::config::SourceChecks {
                max_lag: Some(Duration::from_millis(1_500)),
            },
            fx: None,
        };
        let mut spawned = spawn(
            &feeds,
            params(&endpoints, &metrics, Latch::new(), None, Vec::new()),
        );
        okx.send(okx_ticker("4100.00", now_ms())).unwrap();
        tokio::time::timeout(Duration::from_secs(5), spawned.rx.wait_for(|s| s.is_some()))
            .await
            .expect("a first composite")
            .unwrap();

        okx.send(okx_ticker("4100.00", now_ms() - 30_000)).unwrap();
        tokio::time::timeout(Duration::from_secs(5), spawned.rx.wait_for(|s| s.is_none()))
            .await
            .expect("the late frame withdraws the sample at once")
            .unwrap();
        assert_eq!(metrics.for_pair("TEST").feed_up.get(), 0.0);

        okx.send(okx_ticker("4200.00", now_ms())).unwrap();
        tokio::time::timeout(
            Duration::from_secs(5),
            spawned
                .rx
                .wait_for(|s| matches!(s, Some(p) if p.mid == U256::from(420_000_000_000u64))),
        )
        .await
        .expect("a prompt frame puts a price back")
        .unwrap();
    }
    /// A sample at `px` (whole units, 8 decimals) stamped `age` ago.
    fn aged(px: &str, age: Duration) -> PriceSample {
        PriceSample {
            delta: U256::from(12_345u64),
            mid: parse_decimal_scaled(px, 8).unwrap(),
            at: Instant::now() - age,
            wall: SystemTime::now() - age,
        }
    }

    fn fx_config(min_sources: usize) -> Fx {
        Fx {
            legs: vec![
                crate::config::FxLeg {
                    venue: VenueId::Okx,
                    symbol: "USDT-USD".into(),
                },
                crate::config::FxLeg {
                    venue: VenueId::Okx,
                    symbol: "USDT-USD-B".into(),
                },
            ],
            min_sources,
            max_depeg: parse_decimal_scaled("0.005", FX_DECIMALS).unwrap(),
            max_disagreement: parse_decimal_scaled("0.0005", FX_DECIMALS).unwrap(),
        }
    }

    /// The rate is the legs' plain mean, and a USD mid is divided by it: 3992 USD at 0.998
    /// USD per quote token is 4000 of the quote token. The delta is a fraction of the mid
    /// and is left as it is.
    #[test]
    fn a_usd_mid_is_divided_by_the_mean_of_the_legs() {
        let legs = [
            aged("0.9979", Duration::ZERO),
            aged("0.9981", Duration::ZERO),
        ];
        let rate = fx_rate(&legs, &fx_config(2), 8).unwrap();
        assert_eq!(rate.rate, U256::from(99_800_000u64));
        let usd = aged("3992", Duration::ZERO);
        let converted = convert(usd, rate, 8, false).unwrap();
        assert_eq!(converted.mid, U256::from(400_000_000_000u64));
        assert_eq!(converted.delta, usd.delta);
    }

    /// An inverted lane carries the reciprocal, so its USD sample is multiplied by the
    /// rate: the same price as the direct lane's conversion, read the other way up.
    #[test]
    fn an_inverted_lane_converts_to_the_reciprocal_of_the_direct_one() {
        let decimals = 18;
        let one = U256::exp10(decimals as usize);
        let rate = FxRate {
            rate: parse_decimal_scaled("0.998", decimals).unwrap(),
            at: Instant::now(),
            wall: SystemTime::now(),
        };
        let usd = parse_decimal_scaled("3992", decimals).unwrap();
        let direct = PriceSample {
            delta: U256::zero(),
            mid: usd,
            at: Instant::now(),
            wall: SystemTime::now(),
        };
        let inverted = PriceSample {
            mid: one * one / usd,
            ..direct
        };
        let direct = convert(direct, rate, decimals, false).unwrap().mid;
        let inverted = convert(inverted, rate, decimals, true).unwrap().mid;
        assert_eq!(direct, parse_decimal_scaled("4000", decimals).unwrap());
        // 1/4000 = 0.00025, to the last digit the two truncations leave.
        let expected = parse_decimal_scaled("0.00025", decimals).unwrap();
        assert!(
            expected - inverted <= U256::one(),
            "inverted {inverted}, expected {expected}"
        );
        assert!(
            (scaled_to_f64(direct, decimals) * scaled_to_f64(inverted, decimals) - 1.0).abs()
                < 1e-12
        );
    }

    /// A converted sample is as old as the oldest thing it was made of: an old USD price
    /// over a new rate is old, and a new USD price over a rate from an older leg is as old
    /// as that leg.
    #[test]
    fn the_converted_sample_is_as_old_as_its_oldest_part() {
        let twenty = Duration::from_secs(20);
        let legs = [aged("0.998", Duration::ZERO), aged("0.998", twenty)];
        let rate = fx_rate(&legs, &fx_config(2), 8).unwrap();
        assert_eq!((rate.at, rate.wall), (legs[1].at, legs[1].wall));
        let fresh = convert(aged("3992", Duration::ZERO), rate, 8, false).unwrap();
        assert_eq!((fresh.at, fresh.wall), (legs[1].at, legs[1].wall));
        let old = aged("3992", Duration::from_secs(25));
        let converted = convert(old, rate, 8, false).unwrap();
        assert_eq!((converted.at, converted.wall), (old.at, old.wall));
        assert!(fresh.age_at(Instant::now(), SystemTime::now()) >= twenty);
    }

    /// The rate is refused, and why: fewer legs than `min_sources`, a mean further from 1
    /// than `max_depeg`, legs further apart than `max_disagreement`. One leg is enough
    /// when the stanza says so.
    #[test]
    fn the_rate_is_refused_when_stale_off_peg_or_split() {
        let fresh = |px: &str| aged(px, Duration::ZERO);
        let both = fx_config(2);
        assert_eq!(fx_rate(&[fresh("0.998")], &both, 8), Err(FxTrip::Stale));
        assert_eq!(fx_rate(&[], &fx_config(1), 8), Err(FxTrip::Stale));
        assert!(fx_rate(&[fresh("0.998")], &fx_config(1), 8).is_ok());
        assert_eq!(
            fx_rate(&[fresh("0.99"), fresh("0.99")], &both, 8),
            Err(FxTrip::Depeg)
        );
        assert_eq!(
            fx_rate(&[fresh("1.0051"), fresh("1.0051")], &both, 8),
            Err(FxTrip::Depeg)
        );
        assert!(fx_rate(&[fresh("1.0049"), fresh("1.0049")], &both, 8).is_ok());
        // 15 bp apart, over a 5 bp limit.
        assert_eq!(
            fx_rate(&[fresh("0.998"), fresh("0.9995")], &both, 8),
            Err(FxTrip::Disagreement)
        );
        assert!(fx_rate(&[fresh("0.998"), fresh("0.9984")], &both, 8).is_ok());
    }

    /// An OKX-dialect venue serving several markets on one endpoint: each connection's
    /// subscribe names its market, which is then sent what the test sends under that name,
    /// and every text `ping` is answered `pong`, so a market sent nothing more stays live;
    /// except on the `mute` markets, which never answer one and so go quiet.
    async fn okx_desk(
        markets: &[&str],
        mute: &[&str],
    ) -> (
        String,
        std::collections::HashMap<String, tokio::sync::mpsc::UnboundedSender<String>>,
    ) {
        use futures_util::StreamExt;
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let mut senders = std::collections::HashMap::new();
        let mut receivers = std::collections::HashMap::new();
        for market in markets {
            let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<String>();
            senders.insert((*market).to_owned(), tx);
            receivers.insert((*market).to_owned(), rx);
        }
        let receivers = Arc::new(Mutex::new(receivers));
        let mute: Arc<Vec<String>> = Arc::new(mute.iter().map(|m| (*m).to_owned()).collect());
        tokio::spawn(async move {
            while let Ok((stream, _)) = listener.accept().await {
                let receivers = receivers.clone();
                let mute = mute.clone();
                tokio::spawn(async move {
                    let Ok(mut ws) = accept_async(stream).await else {
                        return;
                    };
                    let Some(Ok(Message::Text(subscribe))) = ws.next().await else {
                        return;
                    };
                    let subscribe: serde_json::Value =
                        serde_json::from_str(subscribe.as_str()).unwrap();
                    let market = subscribe["args"][0]["instId"].as_str().unwrap_or_default();
                    let Some(mut frames) = receivers.lock().unwrap().remove(market) else {
                        return;
                    };
                    let answers = !mute.iter().any(|m| m == market);
                    loop {
                        tokio::select! {
                            frame = frames.recv() => {
                                let Some(frame) = frame else { return };
                                if ws.send(Message::text(frame)).await.is_err() {
                                    return;
                                }
                            }
                            msg = ws.next() => match msg {
                                Some(Ok(Message::Text(text)))
                                    if answers && text.as_str() == "ping" =>
                                {
                                    if ws.send(Message::text("pong")).await.is_err() {
                                        return;
                                    }
                                }
                                Some(Ok(_)) => {}
                                _ => return,
                            }
                        }
                    }
                });
            }
        });
        (format!("ws://{addr}"), senders)
    }

    fn okx_frame(market: &str, px: &str, ts_ms: u64) -> String {
        format!(
            r#"{{"arg":{{"channel":"tickers","instId":"{market}"}},"data":[{{"instId":"{market}","bidPx":"{px}","askPx":"{px}","ts":"{ts_ms}"}}]}}"#
        )
    }

    /// A pair on one USD market (weight 1, `quote = "USD"`) and one USDT market (weight 1),
    /// converted by two USDT/USD legs, all on the OKX desk.
    fn usd_pair(min_sources: usize, fx_min: usize, max_lag: Option<Duration>) -> Feeds {
        Feeds {
            sources: vec![
                Source {
                    quote: Some(Quote::Usd),
                    ..source(VenueId::Okx, "ETH-USD", "1")
                },
                source(VenueId::Okx, "ETH-USDT", "1"),
            ],
            min_sources,
            checks: crate::config::SourceChecks { max_lag },
            fx: Some(fx_config(fx_min)),
        }
    }

    const MARKETS: [&str; 4] = ["ETH-USD", "ETH-USDT", "USDT-USD", "USDT-USD-B"];

    async fn wait_for_mid(rx: &watch::Receiver<Option<PriceSample>>, mid: Option<u64>) {
        wait_for_mid_within(rx, mid, Duration::from_secs(5)).await;
    }

    async fn wait_for_mid_within(
        rx: &watch::Receiver<Option<PriceSample>>,
        mid: Option<u64>,
        within: Duration,
    ) {
        let mut rx = rx.clone();
        tokio::time::timeout(
            within,
            rx.wait_for(move |s| s.map(|p| p.mid) == mid.map(|px| U256::from(px * 100_000_000))),
        )
        .await
        .unwrap_or_else(|_| panic!("no composite at {mid:?}"))
        .unwrap();
    }

    async fn wait_for_gauge(gauge: &prometheus::Gauge, value: f64) {
        tokio::time::timeout(Duration::from_secs(5), async {
            while gauge.get() != value {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("gauge stayed at {}, not {value}", gauge.get()));
    }

    /// With the rate usable the USD market is converted and averaged with the USDT one:
    /// (3992 / 0.998 + 4010) / 2 = 4005. Off its peg the USD market is left out and the
    /// pair quotes on the USDT market alone; with legs that disagree, the same; back on
    /// the peg, back in. The gauges say which.
    #[tokio::test]
    async fn usd_sources_are_converted_and_left_out_while_the_rate_is_off_peg_or_split() {
        let (url, desk) = okx_desk(&MARKETS, &[]).await;
        let endpoints = Endpoints::single(VenueId::Okx, url);
        let metrics = Metrics::new().unwrap();
        let spawned = spawn(
            &usd_pair(1, 2, None),
            params(&endpoints, &metrics, Latch::new(), None, Vec::new()),
        );
        let send =
            |market: &str, px: &str| desk[market].send(okx_frame(market, px, now_ms())).unwrap();
        let m = metrics.for_pair("TEST");
        let converted = metrics.for_venue("TEST", "okx").converted.clone();

        send("ETH-USD", "3992");
        send("ETH-USDT", "4010");
        send("USDT-USD", "0.998");
        send("USDT-USD-B", "0.998");
        wait_for_mid(&spawned.rx, Some(4005)).await;
        assert_eq!(m.fx_rate.get(), 0.998);
        assert_eq!(m.fx_sources_fresh.get(), 2.0);
        assert_eq!(converted.get(), 1.0);
        for reason in FxTrip::ALL {
            assert_eq!(m.fx_guard_tripped(reason).get(), 0.0, "{}", reason.as_str());
        }
        assert_eq!(
            spawned
                .venues
                .iter()
                .map(VenueFeed::series)
                .collect::<Vec<_>>(),
            ["okx", "okx", "fx:okx", "fx:okx"],
            "the legs have series of their own"
        );

        // One leg moved and not yet the other reads as a disagreement first; wait for both.
        send("USDT-USD", "0.99");
        send("USDT-USD-B", "0.99");
        wait_for_gauge(m.fx_guard_tripped(FxTrip::Depeg), 1.0).await;
        wait_for_mid(&spawned.rx, Some(4010)).await;
        assert_eq!(converted.get(), 0.0);
        assert_eq!(m.feed_sources_fresh.get(), 1.0);

        send("USDT-USD", "0.998");
        send("USDT-USD-B", "0.9995");
        wait_for_gauge(m.fx_guard_tripped(FxTrip::Disagreement), 1.0).await;
        assert_eq!(m.fx_guard_tripped(FxTrip::Depeg).get(), 0.0);
        assert_eq!(
            spawned.rx.borrow().map(|s| s.mid),
            Some(U256::from(401_000_000_000u64))
        );

        send("USDT-USD-B", "0.998");
        wait_for_mid(&spawned.rx, Some(4005)).await;
        assert_eq!(m.fx_guard_tripped(FxTrip::Disagreement).get(), 0.0);
    }

    /// An FX leg late behind its own stamp is left out like a late source, and with both
    /// legs required the USD market goes with it; under an `fx.min_sources` of 1 the other
    /// leg carries the rate alone.
    #[tokio::test]
    async fn a_late_fx_leg_takes_the_usd_sources_out_unless_one_leg_is_enough() {
        for (fx_min, expected) in [(2, 4010), (1, 4005)] {
            let (url, desk) = okx_desk(&MARKETS, &[]).await;
            let endpoints = Endpoints::single(VenueId::Okx, url);
            let metrics = Metrics::new().unwrap();
            let spawned = spawn(
                &usd_pair(1, fx_min, Some(Duration::from_millis(1_500))),
                params(&endpoints, &metrics, Latch::new(), None, Vec::new()),
            );
            let send = |market: &str, px: &str, ts: u64| {
                desk[market].send(okx_frame(market, px, ts)).unwrap()
            };
            send("ETH-USD", "3992", now_ms());
            send("ETH-USDT", "4010", now_ms());
            send("USDT-USD", "0.998", now_ms());
            send("USDT-USD-B", "0.998", now_ms());
            wait_for_mid(&spawned.rx, Some(4005)).await;

            // Within MAX_PRICE_AGE of arriving, so only its lateness can leave it out. (Both
            // legs share the desk's `fx:okx` series, so its `late` gauge is not read here.)
            send("USDT-USD-B", "0.998", now_ms() - 30_000);
            let m = metrics.for_pair("TEST");
            wait_for_gauge(&m.fx_sources_fresh, 1.0).await;
            // Any composite after the late frame was judged: the USDT market ticks again.
            send("ETH-USDT", "4010", now_ms());
            wait_for_mid(&spawned.rx, Some(expected)).await;
            let stale = if fx_min == 2 { 1.0 } else { 0.0 };
            assert_eq!(
                m.fx_guard_tripped(FxTrip::Stale).get(),
                stale,
                "fx min {fx_min}"
            );
        }
    }

    /// The pair's `min_sources` is judged on what is left once the USD sources are out:
    /// with both markets required, a rate off its peg withdraws the pair, and the rate back
    /// on it brings the pair back.
    #[tokio::test]
    async fn the_pairs_min_sources_is_judged_after_the_usd_sources_drop() {
        let (url, desk) = okx_desk(&MARKETS, &[]).await;
        let endpoints = Endpoints::single(VenueId::Okx, url);
        let metrics = Metrics::new().unwrap();
        let spawned = spawn(
            &usd_pair(2, 2, None),
            params(&endpoints, &metrics, Latch::new(), None, Vec::new()),
        );
        let send =
            |market: &str, px: &str| desk[market].send(okx_frame(market, px, now_ms())).unwrap();
        send("ETH-USD", "3992");
        send("ETH-USDT", "4010");
        send("USDT-USD", "0.998");
        send("USDT-USD-B", "0.998");
        wait_for_mid(&spawned.rx, Some(4005)).await;

        send("USDT-USD", "0.99");
        send("USDT-USD-B", "0.99");
        wait_for_mid(&spawned.rx, None).await;
        assert_eq!(metrics.for_pair("TEST").feed_up.get(), 0.0);

        send("USDT-USD", "0.998");
        send("USDT-USD-B", "0.998");
        wait_for_mid(&spawned.rx, Some(4005)).await;
    }

    /// Without `quote` a USD market is averaged as read, as it always was: no rate, no
    /// conversion, and the FX series say there is none.
    #[tokio::test]
    async fn a_usd_market_without_quote_is_averaged_as_read() {
        let (url, desk) = okx_desk(&MARKETS, &[]).await;
        let endpoints = Endpoints::single(VenueId::Okx, url);
        let metrics = Metrics::new().unwrap();
        let feeds = Feeds {
            sources: vec![
                source(VenueId::Okx, "ETH-USD", "1"),
                source(VenueId::Okx, "ETH-USDT", "1"),
            ],
            min_sources: 1,
            checks: Default::default(),
            fx: None,
        };
        let spawned = spawn(
            &feeds,
            params(&endpoints, &metrics, Latch::new(), None, Vec::new()),
        );
        let send =
            |market: &str, px: &str| desk[market].send(okx_frame(market, px, now_ms())).unwrap();
        send("ETH-USD", "3990");
        send("ETH-USDT", "4010");
        wait_for_mid(&spawned.rx, Some(4000)).await;
        assert_eq!(spawned.venues.len(), 2, "no legs dialled");
        assert_eq!(
            metrics.for_pair("TEST").fx_sources_fresh.get(),
            0.0,
            "never written"
        );
        assert_eq!(metrics.for_venue("TEST", "okx").converted.get(), 0.0);
    }

    /// A quiet FX leg on a live socket is a rate that has not moved: the pongs that keep
    /// its socket up refresh its sample past MAX_PRICE_AGE, so the USD market stays in,
    /// converted. A pong carries no exchange stamp, so it says nothing about lateness and
    /// the leg is not read as late under `max_source_lag_ms`. Real time, so the test takes
    /// a little over MAX_PRICE_AGE, as `feed`'s pong test does.
    #[tokio::test]
    async fn a_quiet_fx_leg_kept_alive_by_pongs_keeps_the_usd_sources_in() {
        let (url, desk) = okx_desk(&MARKETS, &[]).await;
        let endpoints = Endpoints::single(VenueId::Okx, url);
        let metrics = Metrics::new().unwrap();
        let mut spawned = spawn(
            &usd_pair(2, 2, Some(Duration::from_millis(1_500))),
            params(&endpoints, &metrics, Latch::new(), None, Vec::new()),
        );
        let send =
            |market: &str, px: &str| desk[market].send(okx_frame(market, px, now_ms())).unwrap();
        send("ETH-USD", "3992");
        send("ETH-USDT", "4010");
        send("USDT-USD", "0.998");
        send("USDT-USD-B", "0.998");
        wait_for_mid(&spawned.rx, Some(4005)).await;
        let started = Instant::now();
        let later = tokio::time::timeout(
            MAX_PRICE_AGE + Duration::from_secs(25),
            spawned.rx.wait_for(|s| {
                s.is_some_and(|s| s.at >= started + MAX_PRICE_AGE + Duration::from_secs(5))
            }),
        )
        .await
        .expect("the pongs did not keep the composite publishing")
        .unwrap()
        .unwrap();
        assert_eq!(
            later.mid,
            U256::from(400_500_000_000u64),
            "still converted and averaged"
        );
        let m = metrics.for_pair("TEST");
        assert_eq!(m.fx_sources_fresh.get(), 2.0);
        assert_eq!(m.fx_guard_tripped(FxTrip::Stale).get(), 0.0);
        assert_eq!(metrics.for_venue("TEST", "fx:okx").late.get(), 0.0);
    }
    /// An inverted lane converts too, and its FX legs are not inverted with it: the rate
    /// stays 0.998, not its reciprocal 1.002 (which would pass `max_depeg` and price the USD
    /// market 0.4% off), and the lane mid is the reciprocal of the direct one, 1/4000.
    #[tokio::test]
    async fn an_inverted_pair_converts_its_usd_sources_with_the_rate_upright() {
        let (url, desk) = okx_desk(&MARKETS, &[]).await;
        let endpoints = Endpoints::single(VenueId::Okx, url);
        let metrics = Metrics::new().unwrap();
        let mut feeds = usd_pair(1, 2, None);
        feeds.sources.truncate(1);
        let mut params = params(&endpoints, &metrics, Latch::new(), None, Vec::new());
        params.invert = true;
        params.decimals = 18;
        let mut spawned = spawn(&feeds, params);
        let send =
            |market: &str, px: &str| desk[market].send(okx_frame(market, px, now_ms())).unwrap();
        send("ETH-USD", "3992");
        send("USDT-USD", "0.998");
        send("USDT-USD-B", "0.998");
        let sample =
            tokio::time::timeout(Duration::from_secs(5), spawned.rx.wait_for(|s| s.is_some()))
                .await
                .expect("no converted composite")
                .unwrap()
                .unwrap();
        let lane = scaled_to_f64(sample.mid, 18);
        assert!((lane * 4000.0 - 1.0).abs() < 1e-12, "lane mid {lane}");
        assert_eq!(metrics.for_pair("TEST").fx_rate.get(), 0.998);
    }

    /// At startup the FX legs may deliver after the sources: the pair quotes on what needs
    /// no rate first, and the USD sources join once the rate arrives.
    #[tokio::test]
    async fn the_usd_sources_join_once_the_fx_legs_deliver() {
        let (url, desk) = okx_desk(&MARKETS, &[]).await;
        let endpoints = Endpoints::single(VenueId::Okx, url);
        let metrics = Metrics::new().unwrap();
        let spawned = spawn(
            &usd_pair(1, 2, None),
            params(&endpoints, &metrics, Latch::new(), None, Vec::new()),
        );
        let send =
            |market: &str, px: &str| desk[market].send(okx_frame(market, px, now_ms())).unwrap();
        send("ETH-USD", "3992");
        send("ETH-USDT", "4010");
        wait_for_mid(&spawned.rx, Some(4010)).await;
        let m = metrics.for_pair("TEST");
        assert_eq!(m.fx_guard_tripped(FxTrip::Stale).get(), 1.0);
        assert!(m.fx_rate.get().is_nan(), "no leg, no rate");

        send("USDT-USD", "0.998");
        send("USDT-USD-B", "0.998");
        wait_for_mid(&spawned.rx, Some(4005)).await;
        assert_eq!(m.fx_guard_tripped(FxTrip::Stale).get(), 0.0);
    }

    /// `connect` gives the FX legs the rest of FIRST_PRICE_TIMEOUT and names one still
    /// silent, for the run to warn about and `--check` to fail on, rather than starting as
    /// if the USD sources were merely out for a while. Real time: one FIRST_PRICE_TIMEOUT.
    #[tokio::test]
    async fn connect_names_an_fx_leg_that_never_delivers() {
        let (url, desk) = okx_desk(&MARKETS, &[]).await;
        let endpoints = Endpoints::single(VenueId::Okx, url);
        let metrics = Metrics::new().unwrap();
        for market in ["ETH-USD", "ETH-USDT", "USDT-USD"] {
            let px = if market == "USDT-USD" {
                "0.998"
            } else {
                "4000"
            };
            desk[market].send(okx_frame(market, px, now_ms())).unwrap();
        }
        let spawned = connect(
            &usd_pair(1, 2, None),
            params(&endpoints, &metrics, Latch::new(), None, Vec::new()),
        )
        .await
        .expect("the sources that need no rate are enough to start");
        assert_eq!(spawned.silent_fx, ["okx:USDT-USD-B"]);

        // Every leg delivering: nothing named, and no wait beyond the legs' first frames.
        let (url, desk) = okx_desk(&MARKETS, &[]).await;
        let endpoints = Endpoints::single(VenueId::Okx, url);
        for market in MARKETS {
            let px = if market.starts_with("USDT") {
                "0.998"
            } else {
                "4000"
            };
            desk[market].send(okx_frame(market, px, now_ms())).unwrap();
        }
        let started = Instant::now();
        let spawned = connect(
            &usd_pair(1, 2, None),
            params(&endpoints, &metrics, Latch::new(), None, Vec::new()),
        )
        .await
        .unwrap();
        assert!(spawned.silent_fx.is_empty());
        assert!(started.elapsed() < Duration::from_secs(5));
    }

    /// An FX leg whose socket goes quiet (it stops answering pings, so nothing refreshes
    /// its sample) ages out at MAX_PRICE_AGE like a silent source, and with both legs
    /// required the USD sources go with it. Real time: a little over MAX_PRICE_AGE.
    #[tokio::test]
    async fn a_silent_fx_leg_ages_out_and_takes_the_usd_sources_with_it() {
        let (url, desk) = okx_desk(&MARKETS, &["USDT-USD-B"]).await;
        let endpoints = Endpoints::single(VenueId::Okx, url);
        let metrics = Metrics::new().unwrap();
        let spawned = spawn(
            &usd_pair(1, 2, None),
            params(&endpoints, &metrics, Latch::new(), None, Vec::new()),
        );
        let send =
            |market: &str, px: &str| desk[market].send(okx_frame(market, px, now_ms())).unwrap();
        send("ETH-USD", "3992");
        send("ETH-USDT", "4010");
        send("USDT-USD", "0.998");
        send("USDT-USD-B", "0.998");
        wait_for_mid(&spawned.rx, Some(4005)).await;
        let m = metrics.for_pair("TEST");
        tokio::time::timeout(MAX_PRICE_AGE + Duration::from_secs(15), async {
            while m.fx_guard_tripped(FxTrip::Stale).get() != 1.0 {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        })
        .await
        .expect("the quiet leg never aged out");
        assert_eq!(m.fx_sources_fresh.get(), 1.0);
        // The answering markets' pongs keep recomputing: the next composite is without
        // the USD market.
        wait_for_mid_within(&spawned.rx, Some(4010), Duration::from_secs(15)).await;
    }
}
