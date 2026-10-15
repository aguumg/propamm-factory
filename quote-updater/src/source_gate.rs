//! The quote loop's checks on a pair's primary source: withdraw while its price is stale
//! or disagrees with the other sources, and quote again as soon as it is neither. The
//! settings, and what "primary" and "stale" mean, are on [`SourceChecks`].
//!
//! Judged on every tick the quote loop prices, after the core's freshness gate and before
//! the pricer, so a refusal here withdraws the quote the same way a stale feed does: an
//! empty-tx cancel at every builder, kept in force until a tick passes. Nothing latches.
//!
//! Why the 30-second rule does not cover this: a venue's sample is re-stamped by every
//! frame its socket delivers, so a stream that is alive but running behind the exchange
//! keeps a price that is tens of seconds old looking current. The check here ages the price
//! by the exchange's own stamp when the venue sends one ([`PriceClock`]), and compares it
//! with the other venues, which catches a late primary whichever clock it is aged by.

use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Instant, SystemTime};

use ethrex_common::U256;
use tokio::sync::watch;

use crate::composite::VenueFeed;
use crate::config::{Feeds, SourceChecks};
use crate::feed::{Connected, PriceClock, PriceSample, format_scaled, scaled_to_f64};
use crate::metrics::PairMetrics;
use crate::update::{MAX_PRICE_AGE, Unusable, UnusableKind};

/// Why the gate withdrew a pair: the two refusals it declares, as metric labels.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SourceFault {
    /// The primary's latest price is too old, or it has none.
    Stale,
    /// The primary's mid is too far from a fresh source's.
    Divergence,
}

impl SourceFault {
    /// Declaration order is load-bearing: `PairMetrics` indexes its children by it.
    pub const ALL: [SourceFault; 2] = [SourceFault::Stale, SourceFault::Divergence];

    /// The label, the same as the refusal's (`price_unusable_total{reason}`).
    pub fn as_str(self) -> &'static str {
        self.kind().as_str()
    }

    fn kind(self) -> UnusableKind {
        match self {
            SourceFault::Stale => UnusableKind::SourceStale,
            SourceFault::Divergence => UnusableKind::SourceDivergence,
        }
    }
}

/// One source as the gate reads it: the venue's latest sample and its price clock.
#[derive(Clone)]
pub(crate) struct GateSource {
    /// `binance:ETHUSDT`, as every line names a venue.
    pub(crate) name: String,
    pub(crate) rx: watch::Receiver<Option<PriceSample>>,
    pub(crate) connected: Connected,
}

impl GateSource {
    /// What the gate reads off a venue the composite dialled.
    pub(crate) fn of(venue: &VenueFeed) -> Self {
        Self {
            name: venue.name(),
            rx: venue.rx.clone(),
            connected: venue.connected.clone(),
        }
    }

    fn sample(&self) -> Option<PriceSample> {
        *self.rx.borrow()
    }
}

/// A pair's source checks, live. Shared by every attempt of the lane's quote loop through
/// its `ValueSource`.
pub(crate) struct SourceGate {
    /// The pair's label, for the lines this says.
    label: String,
    sources: Vec<GateSource>,
    primary: usize,
    checks: SourceChecks,
    decimals: u32,
    metrics: Option<PairMetrics>,
    /// The fault judged at the last tick, and when, so the time until the next tick can be
    /// counted under it.
    last: Mutex<Option<(Instant, Option<SourceFault>)>>,
    /// Whether the divergence check last found no fresh source to compare the primary
    /// with, so the line saying so is said once per stretch rather than every tick.
    peerless: AtomicBool,
}

impl SourceGate {
    /// The gate for `feeds`, reading `sources` (in `feeds.sources` order), or `None` when
    /// the pair sets neither check, which leaves its quoting exactly as it was.
    pub(crate) fn new(
        label: &str,
        feeds: &Feeds,
        sources: Vec<GateSource>,
        decimals: u32,
        metrics: Option<PairMetrics>,
    ) -> Option<Self> {
        if !feeds.checks.any() {
            return None;
        }
        // By construction: the composite dials one venue per configured source, in order.
        assert_eq!(
            sources.len(),
            feeds.sources.len(),
            "one gate source per configured source"
        );
        Some(Self {
            label: label.to_owned(),
            primary: feeds.primary(),
            sources,
            checks: feeds.checks.clone(),
            decimals,
            metrics,
            last: Mutex::new(None),
            peerless: AtomicBool::new(false),
        })
    }

    /// This tick's verdict, recorded into the withdrawn-time series.
    pub(crate) fn check(&self, now: Instant, wall: SystemTime) -> Result<(), Unusable> {
        let verdict = self.judge(now, wall);
        self.record(now, verdict.as_ref().err().map(|(fault, _)| *fault));
        verdict.map_err(|(fault, message)| Unusable::new(fault.kind(), message))
    }

    /// The monotonic instant the primary's price will be too old, if it is fresh now and
    /// `max_source_age_ms` is set: the quote loop sleeps until then, so a primary that
    /// simply stops is withdrawn on time even if nothing else wakes the loop. `None` when
    /// there is nothing to wait for: no age limit, or a primary already stale, which only
    /// a new price can change (and a new price wakes the loop by itself).
    pub(crate) fn stale_at(&self, now: Instant, wall: SystemTime) -> Option<Instant> {
        let limit = self.checks.max_age?;
        let clock = self.sources[self.primary].connected.price_clock()?;
        (clock.age_at(now, wall) <= limit).then(|| clock.stale_after(limit))
    }

    fn judge(&self, now: Instant, wall: SystemTime) -> Result<(), (SourceFault, String)> {
        let primary = &self.sources[self.primary];
        let stale = |why: String| {
            Err((
                SourceFault::Stale,
                format!(
                    "primary source {} {why}; withdrawn until it is fresh",
                    primary.name
                ),
            ))
        };
        match self.checks.max_age {
            Some(limit) => match primary.connected.price_clock() {
                None => return stale("has not priced yet".to_owned()),
                Some(clock) => {
                    let age = clock.age_at(now, wall);
                    if age > limit {
                        return stale(format!(
                            "price is {age:.1?} old by {} (max_source_age_ms {})",
                            clock_name(&clock),
                            limit.as_millis()
                        ));
                    }
                }
            },
            // Only the divergence is set: the primary is held to the rule every source is
            // already held to, so a primary that has dropped out of the average withdraws
            // the pair rather than leaving it quoting on the others.
            None => match primary.sample() {
                Some(sample) if sample.age_at(now, wall) <= MAX_PRICE_AGE => {}
                Some(sample) => {
                    return stale(format!("price is {:.0?} old", sample.age_at(now, wall)));
                }
                None => return stale("has not priced yet".to_owned()),
            },
        }
        let Some(limit) = self.checks.max_divergence else {
            return Ok(());
        };
        let Some(primary_mid) = primary.sample().map(|s| s.mid) else {
            return stale("has not priced yet".to_owned());
        };
        let scale = U256::exp10(self.decimals as usize);
        // The secondaries the check compares against: the fresh ones and, when any of
        // those has its freshness vouched for by the exchange's own stamp, only those. An
        // unstamped source is fresh by arrival, which a backlog does not change, so it is
        // trusted only when no stamped source is fresh to say otherwise.
        let fresh: Vec<(&GateSource, U256, bool)> = self
            .sources
            .iter()
            .enumerate()
            .filter(|(i, other)| *i != self.primary && self.is_fresh(other, now, wall))
            .filter_map(|(_, other)| {
                let mid = other.sample()?.mid;
                let stamped = other
                    .connected
                    .price_clock()
                    .is_some_and(|clock| clock.by_exchange_time());
                Some((other, mid, stamped))
            })
            .collect();
        let any_stamped = fresh.iter().any(|(_, _, stamped)| *stamped);
        // The nearest of them decides: the pair withdraws when the primary is away from
        // every source compared, so one secondary off on its own (a bad tick, a book of its
        // own) cannot withdraw a primary that another fresh source agrees with. With one
        // secondary that one decides. A secondary whose mid is zero is no comparison.
        let mut nearest: Option<(U256, &GateSource, U256)> = None;
        for (other, other_mid, stamped) in fresh {
            if any_stamped && !stamped {
                continue;
            }
            let Some(divergence) = divergence(primary_mid, other_mid, scale) else {
                continue;
            };
            if nearest.is_none_or(|(nearest, _, _)| divergence < nearest) {
                nearest = Some((divergence, other, other_mid));
            }
        }
        self.say_peerless(nearest.is_none(), primary);
        match nearest {
            Some((divergence, other, other_mid)) if divergence > limit => Err((
                SourceFault::Divergence,
                format!(
                    "primary source {} mid {} is {} from {} mid {} (max_source_divergence {}), \
                     the nearest fresh source; withdrawn until they agree",
                    primary.name,
                    format_scaled(primary_mid, self.decimals),
                    percent(divergence, self.decimals),
                    other.name,
                    format_scaled(other_mid, self.decimals),
                    percent(limit, self.decimals),
                ),
            )),
            _ => Ok(()),
        }
    }

    /// Sets `quote_updater_source_divergence_inactive`, and says, once each way, that the
    /// divergence check has stopped comparing (no other source is fresh, so a late primary
    /// would go unseen unless its own age shows it) and that it has started again. Each
    /// venue's age is in `quote_updater_venue_source_age_ms`.
    fn say_peerless(&self, peerless: bool, primary: &GateSource) {
        if let Some(metrics) = &self.metrics {
            metrics
                .source_divergence_inactive
                .set(if peerless { 1.0 } else { 0.0 });
        }
        if self.peerless.swap(peerless, Ordering::Relaxed) == peerless {
            return;
        }
        if peerless {
            tracing::warn!(
                "[{}] max_source_divergence is not checking: no source but the primary {} \
                 has a fresh price to compare with",
                self.label,
                primary.name
            );
        } else {
            tracing::info!(
                "[{}] max_source_divergence is checking {} again",
                self.label,
                primary.name
            );
        }
    }

    /// Whether a source other than the primary counts for the divergence check: held to the
    /// 30-second rule like every source, and to `max_source_age_ms` too when it is set, so a
    /// source that is itself running late cannot withdraw a pair whose primary is fine.
    fn is_fresh(&self, source: &GateSource, now: Instant, wall: SystemTime) -> bool {
        let sampled = source
            .sample()
            .is_some_and(|s| s.age_at(now, wall) <= MAX_PRICE_AGE);
        let timely = match self.checks.max_age {
            None => true,
            Some(limit) => source
                .connected
                .price_clock()
                .is_some_and(|clock| clock.age_at(now, wall) <= limit),
        };
        sampled && timely
    }

    /// The withdrawn gauges, and the time since the last tick into the counter of the
    /// fault that tick judged: the time a pair spent withdrawn is credited when the next
    /// tick looks, which is at most a block later.
    fn record(&self, now: Instant, fault: Option<SourceFault>) {
        let mut last = self.last.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(metrics) = &self.metrics {
            if let Some((since, Some(was))) = *last {
                metrics
                    .source_withdrawn_seconds(was)
                    .inc_by(now.saturating_duration_since(since).as_secs_f64());
            }
            for kind in SourceFault::ALL {
                metrics
                    .source_withdrawn(kind)
                    .set(if fault == Some(kind) { 1.0 } else { 0.0 });
            }
        }
        *last = Some((now, fault));
    }
}

/// `|a - b| / b`, scaled by `scale`, or `None` for a zero `b` or a product past a uint256.
fn divergence(a: U256, b: U256, scale: U256) -> Option<U256> {
    if b.is_zero() {
        return None;
    }
    let gap = if a > b { a - b } else { b - a };
    Some(gap.checked_mul(scale)? / b)
}

/// A fraction scaled by 10^`decimals`, as a percentage to a hundredth of a basis point.
fn percent(fraction: U256, decimals: u32) -> String {
    format!("{:.4}%", scaled_to_f64(fraction, decimals) * 100.0)
}

fn clock_name(clock: &PriceClock) -> &'static str {
    if clock.by_exchange_time() {
        "exchange time"
    } else {
        "arrival"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{Source, WEIGHT_DECIMALS};
    use crate::feed::parse_decimal_scaled;
    use crate::venue::VenueId;
    use std::time::Duration;

    /// A price clock `age` old by arrival, arriving `lag` behind its exchange stamp.
    fn clock_aged(
        now: Instant,
        wall: SystemTime,
        age: Duration,
        lag: Option<Duration>,
    ) -> PriceClock {
        PriceClock::new(now - age, wall - age, lag)
    }

    const DECIMALS: u32 = 8;

    fn price(text: &str) -> U256 {
        parse_decimal_scaled(text, DECIMALS).unwrap()
    }

    /// A pair of sources, the first the primary by weight, under `checks`.
    struct Fixture {
        gate: SourceGate,
        txs: Vec<watch::Sender<Option<PriceSample>>>,
        connected: Vec<Connected>,
        metrics: crate::metrics::Metrics,
    }

    fn fixture(checks: SourceChecks) -> Fixture {
        fixture_of(
            &[
                (VenueId::Binance, "ETHUSDT", "2"),
                (VenueId::Okx, "ETH-USDT", "1"),
            ],
            checks,
        )
    }

    /// `sources` as `(venue, symbol, weight)`, under `checks`.
    fn fixture_of(sources: &[(VenueId, &str, &str)], checks: SourceChecks) -> Fixture {
        let feeds = Feeds {
            sources: sources
                .iter()
                .map(|(venue, symbol, weight)| Source {
                    venue: *venue,
                    symbol: (*symbol).into(),
                    weight: parse_decimal_scaled(weight, WEIGHT_DECIMALS).unwrap(),
                })
                .collect(),
            min_sources: 1,
            checks,
        };
        let metrics = crate::metrics::Metrics::new().unwrap();
        let mut txs = Vec::new();
        let mut connected = Vec::new();
        let mut sources = Vec::new();
        for source in &feeds.sources {
            let (tx, rx) = watch::channel(None);
            let flag = Connected::default();
            sources.push(GateSource {
                name: format!("{}:{}", source.venue, source.symbol),
                rx,
                connected: flag.clone(),
            });
            txs.push(tx);
            connected.push(flag);
        }
        let gate = SourceGate::new(
            "ETH/USDT",
            &feeds,
            sources,
            DECIMALS,
            Some(metrics.for_pair("ETH/USDT")),
        )
        .expect("checks are on");
        Fixture {
            gate,
            txs,
            connected,
            metrics,
        }
    }

    impl Fixture {
        /// Source `i` priced `mid`, its sample `sample_age` old (ping-refreshed) and its
        /// price clock `price_age` old by arrival plus `lag` behind the exchange.
        fn price(
            &self,
            i: usize,
            mid: &str,
            now: Instant,
            wall: SystemTime,
            price_age: Duration,
            lag: Option<Duration>,
        ) {
            self.txs[i].send_replace(Some(PriceSample {
                delta: U256::zero(),
                mid: price(mid),
                at: now,
                wall,
            }));
            self.connected[i].priced(clock_aged(now, wall, price_age, lag));
        }
    }

    fn checks(max_age_ms: Option<u64>, max_divergence: Option<&str>) -> SourceChecks {
        SourceChecks {
            max_age: max_age_ms.map(Duration::from_millis),
            max_divergence: max_divergence.map(price),
        }
    }

    fn fault(result: Result<(), Unusable>) -> Option<UnusableKind> {
        result.err().map(|err| err.kind().expect("a core kind"))
    }

    #[test]
    fn a_pair_that_sets_neither_check_has_no_gate() {
        let feeds = Feeds::single_binance("ETHUSDT");
        let (_tx, rx) = watch::channel(None);
        let source = GateSource {
            name: "binance:ETHUSDT".into(),
            rx,
            connected: Connected::default(),
        };
        assert!(SourceGate::new("ETH/USDT", &feeds, vec![source], DECIMALS, None).is_none());
    }

    /// Arrival alone when the venue does not stamp its frames; arrival plus the lag over
    /// the venue's usual one when it does: a price that arrived 100ms ago, 2s later than
    /// usual behind its stamp, is 2.1s old.
    #[test]
    fn a_price_is_aged_by_its_lag_over_the_usual_when_stamped_and_by_arrival_when_not() {
        let (now, wall) = (Instant::now(), SystemTime::now());
        let arrival = clock_aged(now, wall, Duration::from_millis(100), None);
        assert_eq!(arrival.age_at(now, wall), Duration::from_millis(100));
        assert!(!arrival.by_exchange_time());

        let late = clock_aged(
            now,
            wall,
            Duration::from_millis(100),
            Some(Duration::from_secs(2)),
        );
        assert_eq!(late.age_at(now, wall), Duration::from_millis(2_100));
        assert!(late.by_exchange_time());
        // Due when the age crosses the limit, on the monotonic clock.
        assert_eq!(
            late.stale_after(Duration::from_secs(3)),
            now - Duration::from_millis(100) + Duration::from_secs(1)
        );
    }

    /// A frame stamped `delay_ms` before it arrived, `at_ms` into a run started at
    /// `(start, wall0)`, on a host clock `skew_ms` off the exchange's (positive is ahead).
    fn frame(
        floor: &mut crate::feed::LagFloor,
        (start, wall0): (Instant, SystemTime),
        skew_ms: i64,
        at_ms: u64,
        delay_ms: i64,
    ) -> crate::feed::Lag {
        let now = start + Duration::from_millis(at_ms);
        let true_wall = wall0 + Duration::from_millis(at_ms);
        let shift = |t: SystemTime, ms: i64| {
            if ms >= 0 {
                t + Duration::from_millis(ms as u64)
            } else {
                t - Duration::from_millis((-ms) as u64)
            }
        };
        floor.lag(now, shift(true_wall, skew_ms), shift(true_wall, -delay_ms))
    }

    fn ms(n: u64) -> Duration {
        Duration::from_millis(n)
    }

    /// A host clock off by up to the cap either way: the usual lag is subtracted, so an
    /// on-time frame reads as on time and a 30s backlog as 30s. A backlog that outlasts
    /// the window is still counted, less the cap.
    #[test]
    fn the_lag_floor_removes_a_small_skew_in_either_direction() {
        for skew_ms in [800i64, 0, -800, -1_500] {
            let mut floor = crate::feed::LagFloor::default();
            let run = (Instant::now(), SystemTime::now());
            let mut at = |at_ms, delay| frame(&mut floor, run, skew_ms, at_ms, delay).over_floor;
            assert_eq!(at(0, 80), Duration::ZERO, "skew {skew_ms}");
            assert_eq!(at(1_000, 60), Duration::ZERO, "skew {skew_ms}");
            assert_eq!(at(2_000, 120), ms(60), "skew {skew_ms}");
            assert_eq!(at(600_000, 30_060), ms(30_000), "skew {skew_ms}");
            let held = at(600_000 + 3_600_000, 30_060);
            // Every frame in the window is now 30s late: the floor is that, capped.
            let expected = 30_060 + skew_ms - crate::feed::LAG_FLOOR_MAX_MS;
            assert_eq!(held, ms(expected as u64), "skew {skew_ms}");
        }
    }

    /// Dialled while the venue is already 30s behind: at most the cap is taken as usual,
    /// so the backlog shows from the first frame.
    #[test]
    fn a_venue_late_from_its_first_frame_is_still_late() {
        let mut floor = crate::feed::LagFloor::default();
        let run = (Instant::now(), SystemTime::now());
        let first = frame(&mut floor, run, 0, 0, 30_000);
        assert_eq!(first.over_floor, ms(29_000));
        assert!(!first.future);
    }

    /// A stamp from the future is counted and left out of the floor: later frames are not
    /// made to look late by it, and a real backlog still reads as itself.
    #[test]
    fn a_future_stamp_does_not_move_the_floor() {
        let mut floor = crate::feed::LagFloor::default();
        let run = (Instant::now(), SystemTime::now());
        assert_eq!(frame(&mut floor, run, 0, 0, 50).over_floor, Duration::ZERO);
        let future = frame(&mut floor, run, 0, 1_000, -10_000);
        assert!(future.future);
        assert_eq!(future.over_floor, Duration::ZERO);
        assert_eq!(
            frame(&mut floor, run, 0, 2_000, 50).over_floor,
            Duration::ZERO
        );
        assert_eq!(
            frame(&mut floor, run, 0, 3_000, 30_050).over_floor,
            ms(30_000)
        );
    }

    /// The host clock steps. NTP correcting a clock that ran 1.5s slow steps it forward,
    /// and against the floor measured before the step every later frame would read 1.5s
    /// late for the whole window, holding the pair withdrawn; the floor starts again
    /// instead. A step back far enough makes the stamps read as from the future: counted,
    /// and kept out of the floor, so stepping right again leaves nothing behind.
    #[test]
    fn a_host_clock_step_restarts_the_floor() {
        let run = (Instant::now(), SystemTime::now());
        let mut floor = crate::feed::LagFloor::default();
        assert_eq!(
            frame(&mut floor, run, -1_500, 0, 50).over_floor,
            Duration::ZERO
        );
        assert_eq!(
            frame(&mut floor, run, 0, 1_000, 50).over_floor,
            Duration::ZERO,
            "corrected forward: not 1.5s late"
        );
        assert_eq!(
            frame(&mut floor, run, 0, 2_000, 30_050).over_floor,
            ms(30_000)
        );

        let mut floor = crate::feed::LagFloor::default();
        assert_eq!(frame(&mut floor, run, 0, 0, 50).over_floor, Duration::ZERO);
        let back = frame(&mut floor, run, -60_000, 1_000, 50);
        assert!(back.future);
        assert_eq!(back.over_floor, Duration::ZERO);
        assert_eq!(
            frame(&mut floor, run, 0, 2_000, 50).over_floor,
            Duration::ZERO
        );
        assert_eq!(
            frame(&mut floor, run, 0, 3_000, 30_050).over_floor,
            ms(30_000)
        );
    }

    /// A host clock further ahead than the cap fails closed: every stamped price reads as
    /// late by the excess.
    #[test]
    fn a_skew_beyond_the_cap_reads_late() {
        let mut floor = crate::feed::LagFloor::default();
        let run = (Instant::now(), SystemTime::now());
        assert_eq!(frame(&mut floor, run, 5_000, 0, 50).over_floor, ms(4_050));
    }

    #[test]
    fn a_stale_primary_withdraws_and_a_fresh_one_resumes() {
        let f = fixture(checks(Some(1_500), None));
        let (now, wall) = (Instant::now(), SystemTime::now());
        // No price from the primary yet.
        assert_eq!(
            fault(f.gate.check(now, wall)),
            Some(UnusableKind::SourceStale)
        );

        f.price(1, "4000", now, wall, Duration::ZERO, Some(Duration::ZERO));
        // Arrived just now, but made 2s before that: stale by exchange time.
        f.price(
            0,
            "4000",
            now,
            wall,
            Duration::ZERO,
            Some(Duration::from_secs(2)),
        );
        let err = f.gate.check(now, wall).unwrap_err();
        assert_eq!(err.kind(), Some(UnusableKind::SourceStale));
        assert!(
            err.to_string()
                .contains("binance:ETHUSDT price is 2.0s old by exchange time"),
            "{err}"
        );
        assert_eq!(
            f.gate.stale_at(now, wall),
            None,
            "only a new price can lift it"
        );

        // A fresh price resumes it, and arms the timer for when it would age out.
        f.price(0, "4000", now, wall, Duration::from_millis(500), None);
        assert!(f.gate.check(now, wall).is_ok());
        assert_eq!(
            f.gate.stale_at(now, wall),
            Some(now - Duration::from_millis(500) + Duration::from_millis(1_500))
        );
        // By arrival, past the limit.
        let later = now + Duration::from_millis(1_200);
        let err = f
            .gate
            .check(later, wall + Duration::from_millis(1_200))
            .unwrap_err();
        assert!(err.to_string().contains("by arrival"), "{err}");
    }

    #[test]
    fn divergence_from_a_fresh_source_withdraws_and_agreement_resumes() {
        let f = fixture(checks(None, Some("0.001")));
        let (now, wall) = (Instant::now(), SystemTime::now());
        f.price(0, "4000", now, wall, Duration::ZERO, None);
        f.price(1, "4003", now, wall, Duration::ZERO, None);
        assert!(f.gate.check(now, wall).is_ok(), "7.5bp is inside 10bp");

        f.price(1, "4016", now, wall, Duration::ZERO, None);
        let err = f.gate.check(now, wall).unwrap_err();
        assert_eq!(err.kind(), Some(UnusableKind::SourceDivergence));
        assert!(
            err.to_string()
                .contains("binance:ETHUSDT mid 4000 is 0.3984% from okx:ETH-USDT mid 4016"),
            "{err}"
        );

        f.price(0, "4016", now, wall, Duration::ZERO, None);
        assert!(f.gate.check(now, wall).is_ok());
    }

    /// The other source only counts while it is fresh itself: a late secondary cannot
    /// withdraw a pair whose primary is fine.
    #[test]
    fn a_stale_secondary_is_not_compared() {
        let f = fixture(checks(Some(1_500), Some("0.001")));
        let (now, wall) = (Instant::now(), SystemTime::now());
        f.price(0, "4000", now, wall, Duration::ZERO, Some(Duration::ZERO));
        f.price(
            1,
            "4100",
            now,
            wall,
            Duration::ZERO,
            Some(Duration::from_secs(5)),
        );
        assert!(f.gate.check(now, wall).is_ok());
    }

    /// A secondary whose frames carry no time (Binance spot) is fresh by arrival, which a
    /// backlog does not change. While a stamped secondary is fresh, only stamped ones are
    /// compared, so the backlogged one cannot withdraw a primary that is fine; with no
    /// stamped secondary fresh it is the only check there is, and it is compared.
    #[test]
    fn a_secondary_without_timestamps_with_a_backlog() {
        let f = fixture_of(
            &[
                (VenueId::Okx, "ETH-USDT", "2"),
                (VenueId::Binance, "ETHUSDT", "1"),
                (VenueId::Gate, "ETH_USDT", "1"),
            ],
            checks(Some(1_500), Some("0.001")),
        );
        let (now, wall) = (Instant::now(), SystemTime::now());
        let on_time = Some(Duration::ZERO);
        f.price(0, "4016", now, wall, Duration::ZERO, on_time);
        // Binance 30s behind: an old price, arriving on time.
        f.price(1, "4000", now, wall, Duration::ZERO, None);
        f.price(2, "4016", now, wall, Duration::ZERO, on_time);
        assert!(
            f.gate.check(now, wall).is_ok(),
            "gate agrees; binance is not asked"
        );

        // Gate falls behind: Binance is the only check left, and it is used.
        f.price(
            2,
            "4016",
            now,
            wall,
            Duration::ZERO,
            Some(Duration::from_secs(5)),
        );
        let err = f.gate.check(now, wall).unwrap_err();
        assert_eq!(err.kind(), Some(UnusableKind::SourceDivergence), "{err}");
        assert!(err.to_string().contains("binance:ETHUSDT"), "{err}");
    }

    /// Three or more sources: the primary withdraws only when it is away from every source
    /// compared, so one secondary off on its own does not withdraw it, and a primary off
    /// from all of them does. The 10-08 shape, with two Binance markets behind the same
    /// backlog (one primary, one secondary, both unstamped) and OKX current: OKX is the one
    /// compared, and the pair withdraws.
    #[test]
    fn three_sources_with_one_outlier() {
        let f = fixture_of(
            &[
                (VenueId::Binance, "ETHUSDT", "2"),
                (VenueId::Okx, "ETH-USDT", "1"),
                (VenueId::Gate, "ETH_USDT", "1"),
            ],
            checks(None, Some("0.001")),
        );
        let (now, wall) = (Instant::now(), SystemTime::now());
        let on_time = Some(Duration::ZERO);
        f.price(0, "4000", now, wall, Duration::ZERO, None);
        f.price(1, "4000", now, wall, Duration::ZERO, on_time);
        f.price(2, "4020", now, wall, Duration::ZERO, on_time);
        assert!(
            f.gate.check(now, wall).is_ok(),
            "the outlier is a secondary"
        );

        f.price(0, "4020", now, wall, Duration::ZERO, None);
        f.price(2, "4001", now, wall, Duration::ZERO, on_time);
        let err = f.gate.check(now, wall).unwrap_err();
        assert_eq!(err.kind(), Some(UnusableKind::SourceDivergence), "{err}");
        assert!(
            err.to_string().contains("gate:ETH_USDT mid 4001"),
            "the nearest: {err}"
        );

        let f = fixture_of(
            &[
                (VenueId::Binance, "ETHUSDT", "2"),
                (VenueId::Binance, "ETHUSDC", "1"),
                (VenueId::Okx, "ETH-USDT", "1"),
            ],
            checks(None, Some("0.001")),
        );
        f.price(0, "4000", now, wall, Duration::ZERO, None);
        f.price(1, "4000", now, wall, Duration::ZERO, None);
        f.price(2, "4016", now, wall, Duration::ZERO, on_time);
        let err = f.gate.check(now, wall).unwrap_err();
        assert_eq!(err.kind(), Some(UnusableKind::SourceDivergence), "{err}");
    }

    /// With a Binance primary the age is by arrival: frames that keep arriving with an old
    /// price pass `max_source_age_ms` (the divergence check is what catches that), and
    /// silence past the limit does not.
    #[test]
    fn max_age_with_a_binance_primary_catches_silence_only() {
        let f = fixture(checks(Some(1_500), None));
        let (now, wall) = (Instant::now(), SystemTime::now());
        f.price(0, "4000", now, wall, Duration::from_millis(100), None);
        assert!(
            f.gate.check(now, wall).is_ok(),
            "arrived 100ms ago, whatever its price"
        );
        f.price(0, "4000", now, wall, Duration::from_millis(1_600), None);
        let err = f.gate.check(now, wall).unwrap_err();
        assert_eq!(err.kind(), Some(UnusableKind::SourceStale), "{err}");
        assert!(err.to_string().contains("by arrival"), "{err}");
    }

    /// A secondary whose mid is zero is no comparison: no panic, no withdraw, and the check
    /// says it is comparing nothing.
    #[test]
    fn a_secondary_with_a_zero_mid_is_not_compared() {
        let f = fixture(checks(None, Some("0.001")));
        let (now, wall) = (Instant::now(), SystemTime::now());
        f.price(0, "4000", now, wall, Duration::ZERO, None);
        f.price(1, "0", now, wall, Duration::ZERO, Some(Duration::ZERO));
        assert!(f.gate.check(now, wall).is_ok());
        let pair = f.metrics.for_pair("ETH/USDT");
        assert_eq!(pair.source_divergence_inactive.get(), 1.0);
    }

    /// The divergence check comparing nothing is a gauge, not only a log line: a stamped
    /// secondary read as late (here a host clock 5s ahead, past the floor's cap) raises it,
    /// and a fresh one lowers it.
    #[test]
    fn the_divergence_check_comparing_nothing_is_a_gauge() {
        let f = fixture(checks(Some(1_500), Some("0.001")));
        let pair = f.metrics.for_pair("ETH/USDT");
        let (now, wall) = (Instant::now(), SystemTime::now());
        f.price(0, "4000", now, wall, Duration::ZERO, None);
        f.price(
            1,
            "4016",
            now,
            wall,
            Duration::ZERO,
            Some(Duration::from_secs(4)),
        );
        assert!(f.gate.check(now, wall).is_ok());
        assert_eq!(pair.source_divergence_inactive.get(), 1.0);
        f.price(1, "4000", now, wall, Duration::ZERO, Some(Duration::ZERO));
        assert!(f.gate.check(now, wall).is_ok());
        assert_eq!(pair.source_divergence_inactive.get(), 0.0);
    }

    /// With only the divergence set, the primary is held to the 30-second rule: one that
    /// has dropped out of the average withdraws the pair.
    #[test]
    fn divergence_alone_holds_the_primary_to_the_thirty_second_rule() {
        let f = fixture(checks(None, Some("0.001")));
        let (now, wall) = (Instant::now(), SystemTime::now());
        f.price(1, "4000", now, wall, Duration::ZERO, None);
        assert_eq!(
            fault(f.gate.check(now, wall)),
            Some(UnusableKind::SourceStale)
        );
        f.txs[0].send_replace(Some(PriceSample {
            delta: U256::zero(),
            mid: price("4000"),
            at: now - Duration::from_secs(31),
            wall: wall - Duration::from_secs(31),
        }));
        assert_eq!(
            fault(f.gate.check(now, wall)),
            Some(UnusableKind::SourceStale)
        );
    }

    #[test]
    fn the_time_withdrawn_is_counted_under_its_reason() {
        let f = fixture(checks(Some(1_500), None));
        let (now, wall) = (Instant::now(), SystemTime::now());
        let pair = f.metrics.for_pair("ETH/USDT");
        assert!(f.gate.check(now, wall).is_err());
        assert_eq!(pair.source_withdrawn(SourceFault::Stale).get(), 1.0);
        assert_eq!(pair.source_withdrawn(SourceFault::Divergence).get(), 0.0);
        f.price(0, "4000", now, wall, Duration::ZERO, None);
        let later = now + Duration::from_secs(3);
        // Fresh as of `later`.
        f.connected[0].priced(clock_aged(later, wall, Duration::ZERO, None));
        assert!(f.gate.check(later, wall).is_ok());
        assert_eq!(pair.source_withdrawn(SourceFault::Stale).get(), 0.0);
        assert_eq!(pair.source_withdrawn_seconds(SourceFault::Stale).get(), 3.0);
        assert_eq!(
            pair.source_withdrawn_seconds(SourceFault::Divergence).get(),
            0.0
        );
    }

    /// The quote loop's timer: due when the primary's price would cross the limit, so a
    /// primary that simply stops withdraws on time; never due without the check, or once
    /// the primary is already stale.
    #[tokio::test]
    async fn the_loop_is_woken_when_the_primary_ages_past_the_limit() {
        let f = fixture(checks(Some(80), None));
        let (now, wall) = (Instant::now(), SystemTime::now());
        f.price(0, "4000", now, wall, Duration::ZERO, None);
        let (_tx, rx) = watch::channel(None);
        let source = crate::update::ValueSource::feed_for_tests(rx.clone(), None, false, DECIMALS)
            .with_source_gate(f.gate);
        let started = Instant::now();
        tokio::time::timeout(Duration::from_secs(2), source.source_stale())
            .await
            .expect("due once the primary ages past 80ms");
        assert!(started.elapsed() >= Duration::from_millis(70));
        // Stale now: nothing to wait for until a new price.
        assert!(
            tokio::time::timeout(Duration::from_millis(100), source.source_stale())
                .await
                .is_err()
        );
        // And a pair without the check never wakes for it.
        let plain = crate::update::ValueSource::feed_for_tests(rx, None, false, DECIMALS);
        assert!(
            tokio::time::timeout(Duration::from_millis(100), plain.source_stale())
                .await
                .is_err()
        );
    }

    /// `quote_updater_venue_source_age_ms` is read off the feed's clock at scrape time,
    /// NaN before the first price.
    #[test]
    fn the_venue_source_age_is_read_at_scrape_time() {
        let metrics = crate::metrics::Metrics::new().unwrap();
        let flag = Connected::default();
        metrics.watch_venue("ETH/USDT", "okx", flag.clone());
        let text = metrics.render().unwrap();
        assert!(
            text.contains(r#"quote_updater_venue_source_age_ms{pair="ETH/USDT",venue="okx"} NaN"#),
            "{text}"
        );
        let (now, wall) = (Instant::now(), SystemTime::now());
        flag.priced(clock_aged(
            now,
            wall,
            Duration::ZERO,
            Some(Duration::from_secs(25)),
        ));
        let text = metrics.render().unwrap();
        let line = text
            .lines()
            .find(|l| l.starts_with(r#"quote_updater_venue_source_age_ms{pair="ETH/USDT""#))
            .unwrap();
        let ms: f64 = line.rsplit(' ').next().unwrap().parse().unwrap();
        assert!((25_000.0..26_000.0).contains(&ms), "{line}");
    }

    /// The 2026-10-08 pattern, replayed through real sockets, the composite and the quote
    /// loop's pricing: the primary's stream stays alive and on time but carries a price
    /// that has stopped moving (the exchange's queue was 21-40s behind), while the other
    /// venue moves 40bp. Binance's bookTicker carries no event time, so the age check alone
    /// cannot see this; the divergence check does, on the first composite sample that
    /// carries the move. When the primary catches up, the pair quotes again.
    #[tokio::test]
    async fn a_late_primary_is_withdrawn_on_the_first_sample_that_shows_the_move() {
        let (binance_url, binance) = scripted_venue().await;
        let (okx_url, okx) = scripted_venue().await;
        let lane = Lane::start(
            &binance_url,
            &okx_url,
            ("2", "1"),
            checks(Some(1_500), Some("0.001")),
        )
        .await;

        binance.send(book_ticker("4000.00")).unwrap();
        okx.send(okx_ticker("4000.00", now_ms())).unwrap();
        let mut rx = lane.rx.clone();
        lane.until_both_priced().await;
        wait_for_mid(&mut rx, |mid| mid == price("4000")).await;
        assert!(lane.price().is_ok(), "both venues agree");

        // The primary keeps arriving, each frame on time by our clock, at the old price.
        binance.send(book_ticker("4000.00")).unwrap();
        okx.send(okx_ticker("4016.00", now_ms())).unwrap();
        wait_for_mid(&mut rx, |mid| mid > price("4000")).await;
        let err = lane
            .price()
            .expect_err("a 40bp disagreement withdraws at once");
        assert_eq!(err.kind(), Some(UnusableKind::SourceDivergence), "{err}");

        // Each further stale frame from the primary keeps it withdrawn.
        binance.send(book_ticker("4000.00")).unwrap();
        lane.until_venue_mid(0, price("4000")).await;
        assert_eq!(
            lane.price().unwrap_err().kind(),
            Some(UnusableKind::SourceDivergence)
        );

        // The backlog drains: the primary catches up and the pair quotes again.
        binance.send(book_ticker("4016.00")).unwrap();
        wait_for_mid(&mut rx, |mid| mid == price("4016")).await;
        assert!(lane.price().is_ok(), "{:?}", lane.price().err());
    }

    /// The same incident on a venue whose frames carry the exchange's time, with our clock
    /// 800ms ahead of the exchange's, level, and 800ms behind: on-time frames quote, a primary
    /// whose prices start arriving 30s after their stamps is stale on the first such frame,
    /// without waiting for the market to move, and quotes again when it catches up.
    #[tokio::test]
    async fn a_primary_thirty_seconds_behind_by_exchange_time_is_withdrawn_on_arrival() {
        for skew_ms in [800i64, 0, -800] {
            let (binance_url, binance) = scripted_venue().await;
            let (okx_url, okx) = scripted_venue().await;
            // OKX is the primary here: it is the venue that stamps its frames.
            let lane = Lane::start(
                &binance_url,
                &okx_url,
                ("1", "2"),
                checks(Some(1_500), None),
            )
            .await;
            // What a stamp made `ago_ms` before now reads as, against our skewed clock.
            let stamp = |ago_ms: i64| (now_ms() as i64 - skew_ms - ago_ms) as u64;
            binance.send(book_ticker("4000.00")).unwrap();
            okx.send(okx_ticker("4000.00", stamp(50))).unwrap();
            let mut rx = lane.rx.clone();
            lane.until_both_priced().await;
            wait_for_mid(&mut rx, |_| true).await;
            assert!(
                lane.price().is_ok(),
                "skew {skew_ms}: {:?}",
                lane.price().err()
            );

            okx.send(okx_ticker("4000.01", stamp(30_050))).unwrap();
            lane.until_venue_mid(1, price("4000.01")).await;
            let err = lane.price().expect_err("30s behind is stale");
            assert_eq!(err.kind(), Some(UnusableKind::SourceStale), "{err}");
            assert!(err.to_string().contains("by exchange time"), "{err}");

            okx.send(okx_ticker("4000.02", stamp(50))).unwrap();
            lane.until_venue_mid(1, price("4000.02")).await;
            assert!(
                lane.price().is_ok(),
                "skew {skew_ms}: {:?}",
                lane.price().err()
            );
        }
    }

    /// The failure the floor exists for: a host clock 800ms ahead read every OKX price as
    /// 800ms old, so with `max_source_age_ms` under that the secondary was dropped from
    /// the divergence check and a late primary went unseen. With the floor it stays
    /// compared.
    #[tokio::test]
    async fn a_skewed_clock_does_not_switch_the_divergence_check_off() {
        let (binance_url, binance) = scripted_venue().await;
        let (okx_url, okx) = scripted_venue().await;
        let lane = Lane::start(
            &binance_url,
            &okx_url,
            ("2", "1"),
            checks(Some(500), Some("0.001")),
        )
        .await;
        let ahead = |ms: u64| ms - 800;
        binance.send(book_ticker("4000.00")).unwrap();
        okx.send(okx_ticker("4000.00", ahead(now_ms()))).unwrap();
        let mut rx = lane.rx.clone();
        lane.until_both_priced().await;
        wait_for_mid(&mut rx, |mid| mid == price("4000")).await;
        assert!(lane.price().is_ok(), "{:?}", lane.price().err());

        binance.send(book_ticker("4000.00")).unwrap();
        okx.send(okx_ticker("4016.00", ahead(now_ms()))).unwrap();
        wait_for_mid(&mut rx, |mid| mid > price("4000")).await;
        let err = lane.price().expect_err("still compared, so 40bp withdraws");
        assert_eq!(err.kind(), Some(UnusableKind::SourceDivergence), "{err}");
    }

    /// Two sources with no checks set: no gate, and the late primary is quoted from, as it
    /// always was.
    #[tokio::test]
    async fn without_the_settings_a_late_primary_is_quoted_as_before() {
        let (binance_url, binance) = scripted_venue().await;
        let (okx_url, okx) = scripted_venue().await;
        let lane = Lane::start(&binance_url, &okx_url, ("2", "1"), checks(None, None)).await;
        binance.send(book_ticker("4000.00")).unwrap();
        okx.send(okx_ticker("4016.00", now_ms() - 30_000)).unwrap();
        let mut rx = lane.rx.clone();
        lane.until_both_priced().await;
        wait_for_mid(&mut rx, |mid| mid > price("4000")).await;
        assert!(lane.price().is_ok());
    }

    /// A lane as `build_live` assembles one: two venues dialled through the composite, a
    /// `feed` pricer reading it, and the source gate over the venues.
    struct Lane {
        source: crate::update::ValueSource,
        rx: watch::Receiver<Option<PriceSample>>,
        venues: Vec<crate::composite::VenueFeed>,
        _metrics: crate::metrics::Metrics,
    }

    impl Lane {
        async fn start(
            binance: &str,
            okx: &str,
            (binance_weight, okx_weight): (&str, &str),
            checks: SourceChecks,
        ) -> Lane {
            let table = std::collections::BTreeMap::from([
                ("binance".to_owned(), binance.to_owned()),
                ("okx".to_owned(), okx.to_owned()),
            ]);
            let endpoints = crate::venue::Endpoints::resolve(None, &table).unwrap();
            let metrics = crate::metrics::Metrics::new().unwrap();
            let feeds = Feeds {
                sources: vec![
                    Source {
                        venue: VenueId::Binance,
                        symbol: "ETHUSDT".into(),
                        weight: parse_decimal_scaled(binance_weight, WEIGHT_DECIMALS).unwrap(),
                    },
                    Source {
                        venue: VenueId::Okx,
                        symbol: "ETH-USDT".into(),
                        weight: parse_decimal_scaled(okx_weight, WEIGHT_DECIMALS).unwrap(),
                    },
                ],
                min_sources: 1,
                checks,
            };
            let adopted = crate::feed::Adoption::new();
            adopted.adopt();
            let spawned = crate::composite::spawn(
                &feeds,
                crate::composite::Params {
                    endpoints: &endpoints,
                    decimals: DECIMALS,
                    invert: false,
                    latch: crate::guard::Latch::new(),
                    guards: Vec::new(),
                    adopted,
                    metrics: &metrics,
                    pair_metrics: metrics.for_pair("ETH/USDT"),
                    label: "ETH/USDT".to_owned(),
                    recorder: None,
                    history: None,
                },
            );
            let mut source = crate::update::ValueSource::feed_for_tests(
                spawned.rx.clone(),
                Some(price("0.0005")),
                false,
                DECIMALS,
            );
            let sources = spawned.venues.iter().map(GateSource::of).collect();
            if let Some(gate) = SourceGate::new(
                "ETH/USDT",
                &feeds,
                sources,
                DECIMALS,
                Some(metrics.for_pair("ETH/USDT")),
            ) {
                source = source.with_source_gate(gate);
            }
            Lane {
                source,
                rx: spawned.rx,
                venues: spawned.venues,
                _metrics: metrics,
            }
        }

        fn price(&self) -> Result<crate::update::Priced, Unusable> {
            self.source
                .current_detailed(&crate::config::MidBand::default())
        }

        async fn until_both_priced(&self) {
            for venue in &self.venues {
                let mut rx = venue.rx.clone();
                tokio::time::timeout(Duration::from_secs(5), rx.wait_for(|s| s.is_some()))
                    .await
                    .expect("the venue never priced")
                    .unwrap();
            }
        }

        async fn until_venue_mid(&self, i: usize, mid: U256) {
            let mut rx = self.venues[i].rx.clone();
            rx.mark_changed();
            tokio::time::timeout(Duration::from_secs(5), async {
                while rx.changed().await.is_ok() {
                    if rx.borrow_and_update().is_some_and(|s| s.mid == mid) {
                        return;
                    }
                }
            })
            .await
            .expect("the venue never delivered that price");
        }
    }

    /// Waits for the composite to publish a mid `matches` accepts.
    async fn wait_for_mid(
        rx: &mut watch::Receiver<Option<PriceSample>>,
        matches: impl Fn(U256) -> bool,
    ) {
        tokio::time::timeout(
            Duration::from_secs(5),
            rx.wait_for(|s| s.is_some_and(|s| matches(s.mid))),
        )
        .await
        .expect("the composite never published that mid")
        .unwrap();
    }

    /// A venue that sends whatever the test hands it, when it hands it.
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
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64
    }
}
