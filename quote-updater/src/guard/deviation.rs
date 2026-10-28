//! `deviation`: the built-in market guard, the circuit breaker as it always was minus the
//! latch, which is the lane's now (`guard::Latch`).
//!
//! A jump detector, tick to tick: quoting stops the moment one observed mid jumps further
//! than the configured fraction from the one before it. It is not a bound on where the
//! price may end up (a move that arrives in small steps never trips it, however far it
//! travels, because that is a real market move and `min_mid`/`max_mid` bounds the
//! destination). Tick to tick means between consecutive samples: after the composite has
//! published nothing for longer than [`MAX_PRICE_AGE`] (every venue out, or a market that
//! closes for the night or the weekend, as a stock does), the pair was not quoting, and the
//! first sample after the gap is a fresh start rather than a move from the one before it.
//! A move across a gap is one the pair never offered a price through; the band still
//! bounds where it lands. A second, optional check measures the move over a trailing
//! window of N blocks: at a short window that is still feed integrity, at a long one it is
//! a policy to stop making markets when the market moves this hard. Both trip the same
//! latch.
//!
//! Pure and IO-free on purpose: no clock, no channels, no logging. The composite acts on
//! the verdicts, which is what makes every transition testable without tokio.

use std::{
    collections::VecDeque,
    sync::{Arc, Mutex},
    time::Instant,
};

use ethrex_common::U256;

use crate::{
    breaker::{BreakerConfig, percent},
    feed::{Gated, format_scaled},
    guard::{CompositeSample, MarketGuard, TripReason, Verdict},
    metrics::PairMetrics,
    pricing::Diagnostics,
    update::MAX_PRICE_AGE,
};

/// Everything the operator lines about a trip need, rendered where the arithmetic lives.
///
/// Formatted here rather than at the call site so the numbers in the message cannot drift
/// from the numbers in the decision: the same `diff` that crossed the threshold is the one
/// reported. Kept on the guard after a trip, for the tests that pin the numbers.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Deviation {
    /// How far the mid moved, as a percentage, e.g. `"4.10%"`.
    pub(crate) moved: String,
    /// The configured limit it crossed, e.g. `"2.00%"`.
    pub(crate) limit: String,
    /// The mid before the one that tripped it, in whole-token units.
    pub(crate) reference: String,
    /// The mid that tripped it, in whole-token units.
    pub(crate) mid: String,
    /// `" over 1000 blocks"` for a windowed trip, empty for a tick-to-tick one. Rendered
    /// here so the unit in the message is the unit the operator configured.
    pub(crate) span: String,
    /// What `reference` is: `"previous"` for the tick check, `"anchor"` for the window.
    pub(crate) reference_label: &'static str,
}

impl Deviation {
    /// The three strings a trip is shown as, exactly as the breaker always printed them:
    /// the block header, the alarm, and the re-arm tail.
    fn reason(&self) -> TripReason {
        TripReason {
            header: format!(
                "price deviation {}{} exceeds the {} limit",
                self.moved, self.span, self.limit
            ),
            alarm: format!(
                "circuit breaker tripped: mid moved {}{} > {} ({} {}, new {})",
                self.moved, self.span, self.limit, self.reference_label, self.reference, self.mid
            ),
            rearm: format!(
                "on a {} move{} from {} {} to {} (limit {})",
                self.moved, self.span, self.reference_label, self.reference, self.mid, self.limit
            ),
        }
    }
}

/// The ratios the guard last measured, as a fraction of each limit, shared with the lane so
/// adoption can seed the gauges from them: the guard judges from the moment the feed is
/// dialled, before this generation owns the pair's series, and a lane that trips in that
/// window is judged no further, so nothing would ever write the tripping ratio otherwise.
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct Ratios {
    pub(crate) tick: Option<f64>,
    pub(crate) window: Option<f64>,
}

/// The shared readout of [`Ratios`].
pub(crate) type Readout = Arc<Mutex<Ratios>>;

/// The deviation guard: the tick-to-tick check and the optional window, from the pair's
/// `max_deviation*` keys.
pub(crate) struct DeviationGuard {
    config: BreakerConfig,
    /// `config.scale()`, computed once: it is on the path of every sample.
    scale: U256,
    /// The last mid this guard approved and when it was sampled, None until the first:
    /// startup has nothing to deviate from and always passes. A reference older than
    /// [`MAX_PRICE_AGE`] when the next sample arrives is dropped the same way: the pair was
    /// not quoting across that gap (see the module doc).
    reference: Option<(U256, Instant)>,
    /// One bucket per second of the trailing window, oldest first; empty for a guard with
    /// no window configured.
    window: VecDeque<(Instant, U256)>,
    /// The ratio `assess` last measured. See `last_deviation_ratio`.
    last_ratio: Option<f64>,
    /// The windowed ratio `assess` last measured. See `last_window_deviation_ratio`.
    last_window_ratio: Option<f64>,
    /// The numbers of the last trip, for the tests that pin them.
    last_deviation: Option<Deviation>,
    /// The last ratios, for adoption to seed the gauges from. See [`Ratios`].
    readout: Readout,
    /// Where the ratios are recorded for the dashboard, under the series names they always
    /// had: the one place a built-in guard is not "just another extension". Gated on the
    /// lane's adoption like the composite's own gauges, because this guard judges from the
    /// moment the feed is dialled, which on a reload is while the lane it replaces still
    /// owns the series. `None` under `--check`, which serves no metrics, and in tests.
    metrics: Option<Gated<PairMetrics>>,
}

impl DeviationGuard {
    pub(crate) fn new(config: BreakerConfig, metrics: Option<Gated<PairMetrics>>) -> Self {
        Self {
            config,
            scale: config.scale(),
            reference: None,
            window: VecDeque::new(),
            last_ratio: None,
            last_window_ratio: None,
            last_deviation: None,
            readout: Readout::default(),
            metrics,
        }
    }

    /// The shared readout of the ratios this guard last measured.
    pub(crate) fn readout(&self) -> Readout {
        Arc::clone(&self.readout)
    }

    /// The absolute distance between two mids. Unsigned: the guard is symmetric, and the
    /// direction is already visible in the reference/mid pair a trip carries.
    fn deviation(&self, mid: U256, reference: U256) -> U256 {
        if mid > reference {
            mid - reference
        } else {
            reference - mid
        }
    }

    /// Whether `diff` against `reference` crosses the configured fraction. `full_mul` so
    /// `diff/reference` is compared with `threshold_scaled/scale` without dividing: no
    /// intermediate rounding, and the comparison stays exact.
    ///
    /// In U512 because mids may use all 216 bits their slot allows and the scale reaches
    /// 10^59, whose product wraps a U256: a guard must not fail open on arithmetic.
    /// Strictly greater: exactly at the threshold does not trip.
    fn beyond(&self, diff: U256, reference: U256, threshold: U256) -> bool {
        diff.full_mul(self.scale) > threshold.full_mul(reference)
    }

    /// `(diff/reference) / (threshold_scaled/scale)`: 1.0 exactly at the limit.
    ///
    /// f64 here and nowhere else in this module: it is an observability value, never an
    /// input to `beyond`, which stays exact in U256. A zero reference yields 0.0 rather
    /// than an infinity: a zero mid is refused upstream, and an infinity on a dashboard
    /// reads as a bug in the gauge rather than in the feed.
    fn ratio(&self, diff: U256, reference: U256, threshold: U256) -> f64 {
        let d = crate::feed::scaled_to_f64(diff, self.config.decimals);
        let r = crate::feed::scaled_to_f64(reference, self.config.decimals);
        let limit = crate::feed::scaled_to_f64(threshold, self.config.decimals);
        if r == 0.0 || limit == 0.0 {
            0.0
        } else {
            (d / r) / limit
        }
    }

    /// The last tick-to-tick move `assess` measured, as a fraction of the configured limit:
    /// 0.5 is half way to a trip, 1.0 is exactly at it. `None` until a second sample
    /// arrives, because the first mid has nothing to deviate from.
    #[cfg(test)]
    pub(crate) fn last_deviation_ratio(&self) -> Option<f64> {
        self.last_ratio
    }

    /// The last windowed move `assess` measured, as a fraction of the windowed limit.
    /// `None` until a second sample arrives.
    #[cfg(test)]
    pub(crate) fn last_window_deviation_ratio(&self) -> Option<f64> {
        self.last_window_ratio
    }

    /// The numbers behind the last trip, if there was one.
    #[cfg(test)]
    pub(crate) fn last_deviation(&self) -> Option<&Deviation> {
        self.last_deviation.as_ref()
    }

    /// Appends `mid` to the window, collapsing everything inside one second into a single
    /// bucket. `bookTicker` can print many times a second; unbucketed, an hours-long
    /// window would hold hundreds of thousands of entries instead of one per second.
    ///
    /// One consequence, in the first second of a guard's life only: while the window holds
    /// just this one bucket, overwriting its value makes the anchor the very mid being
    /// judged, so `diff` is zero and the windowed check cannot trip however large the
    /// move. It is bounded to that second: once `prune` has reduced the window to a single
    /// bucket, that survivor is by construction at least `span` old, so a later sample
    /// pushes a new bucket rather than overwriting it. A pair that also sets
    /// `max_deviation` is covered throughout by the tick check; a window-only pair is
    /// unguarded for that one second after a start or a lane rebuild.
    fn record(window: &mut VecDeque<(Instant, U256)>, mid: U256, at: Instant) {
        match window.back_mut() {
            Some((last, value))
                if at.saturating_duration_since(*last) < std::time::Duration::from_secs(1) =>
            {
                *value = mid;
            }
            _ => window.push_back((at, mid)),
        }
    }

    /// Drops buckets that are no longer needed, keeping the newest one at or *before* the
    /// trailing edge.
    ///
    /// Pruning everything older than `at - span` instead would leave a feed that went
    /// quiet with no bucket near the edge, so the anchor would be whatever recent sample
    /// survived and the window would silently shorten to whatever the feed delivered,
    /// weakening the guard exactly when the feed is behaving oddly. Retaining the boundary
    /// bucket bounds the true lookback to `(span - 1s, span]`, not exactly `span`:
    /// `record` overwrites a bucket's value while keeping its timestamp, so the anchor's
    /// value can have been observed up to a second after the timestamp it is filed under.
    /// A shorter lookback measures a smaller net move, so this makes the guard marginally
    /// more permissive than `span` implies, never stricter.
    ///
    /// `len() >= 2` also means a quiet feed can leave a single old bucket anchoring the
    /// window indefinitely (nothing pops it until a second bucket arrives), so the real
    /// lookback can run far *longer* than `span` while the trip line still reports the
    /// configured block count. A separate, deliberate tradeoff: see the paragraph above
    /// on why letting the window silently shrink instead would be worse.
    fn prune(window: &mut VecDeque<(Instant, U256)>, at: Instant, span: std::time::Duration) {
        while window.len() >= 2 && at.saturating_duration_since(window[1].0) >= span {
            window.pop_front();
        }
    }

    /// The buffer's length, for the test that pins the same-second collapse.
    #[cfg(test)]
    fn window_len(&self) -> usize {
        self.window.len()
    }

    fn describe(
        &self,
        diff: U256,
        reference: U256,
        mid: U256,
        threshold: U256,
        span: String,
        reference_label: &'static str,
    ) -> Deviation {
        // The comparison is exact but the rendering truncates, so a move that crossed the
        // limit by less than two decimals show would read `2.00% > 2.00%`: an alarm that
        // looks like a misfire. The move is rendered at the first precision that tells it
        // apart from the limit, or at the finest there is if none does.
        let both_at = |decimals| {
            (
                crate::breaker::percent_at_least(diff, reference, decimals),
                crate::breaker::percent_at_least(threshold, self.scale, decimals),
            )
        };
        let moved = [2u32, 4, 6, 8]
            .into_iter()
            .map(both_at)
            .find(|(moved, limit)| moved != limit)
            .map_or_else(
                || crate::breaker::percent_at_least(diff, reference, 8),
                |(moved, _)| moved,
            );
        Deviation {
            moved,
            limit: percent(threshold, self.scale),
            reference: format_scaled(reference, self.config.decimals),
            mid: format_scaled(mid, self.config.decimals),
            span,
            reference_label,
        }
    }

    /// Records the ratios on the legacy gauges, when this guard has them and the lane owns
    /// its series, and on the readout regardless.
    fn publish_ratios(&self) {
        *self.readout.lock().unwrap_or_else(|e| e.into_inner()) = Ratios {
            tick: self.last_ratio,
            window: self.last_window_ratio,
        };
        if let Some(gated) = &self.metrics {
            gated.record(|m| {
                if let Some(ratio) = self.last_ratio {
                    m.breaker_deviation_ratio.set(ratio);
                }
                if let Some(ratio) = self.last_window_ratio {
                    m.breaker_window_deviation_ratio.set(ratio);
                }
            });
        }
    }

    fn trip(&mut self, deviation: Deviation) -> Verdict {
        let reason = deviation.reason();
        self.last_deviation = Some(deviation);
        self.publish_ratios();
        Verdict::Trip(reason)
    }
}

impl MarketGuard for DeviationGuard {
    /// Judges one mid against the one before it, and takes it as the new reference if it
    /// passes. Tick to tick, and deliberately not against the last mid *delivered* to a
    /// builder: a reference pinned to the last delivered quote would freeze whenever
    /// nothing was reaching the wire, and then measure live prices against a stale one, so
    /// ordinary drift on a healthy feed would trip the guard. An outage on the delivery
    /// side must not be able to manufacture a halt on the price side.
    ///
    /// No tripped state here: the lane's latch is what stops the composite asking, so
    /// after a trip this guard simply keeps its last reference, which a fresh lane never
    /// sees.
    fn assess(&mut self, sample: &CompositeSample, _out: &mut Diagnostics) -> Verdict {
        let (mid, at) = (sample.mid, sample.at);
        let window_config = self.config.window;
        if let Some(config) = window_config {
            Self::record(&mut self.window, mid, at);
            Self::prune(&mut self.window, at, config.span);
        }
        let anchor = self.window.front().map(|&(_, mid)| mid);

        // Nothing seen yet, or nothing for longer than the pair quotes a price, so there is
        // nothing tick to tick to deviate from: startup extends the same trust to its first
        // price that the service already does, and so does the first price after a gap. The
        // window keeps measuring over its whole span regardless.
        let first = self.reference.is_none();
        let previous = self
            .reference
            .filter(|(_, since)| at.saturating_duration_since(*since) <= MAX_PRICE_AGE)
            .map(|(previous, _)| previous);

        if let (Some(threshold), Some(previous)) = (self.config.threshold_scaled, previous) {
            let diff = self.deviation(mid, previous);
            self.last_ratio = Some(self.ratio(diff, previous, threshold));
            if self.beyond(diff, previous, threshold) {
                let deviation =
                    self.describe(diff, previous, mid, threshold, String::new(), "previous");
                self.reference = Some((mid, at));
                return self.trip(deviation);
            }
        }

        if let (Some(config), Some(anchor), false) = (window_config, anchor, first) {
            let diff = self.deviation(mid, anchor);
            self.last_window_ratio = Some(self.ratio(diff, anchor, config.threshold_scaled));
            if self.beyond(diff, anchor, config.threshold_scaled) {
                let span = format!(" over {} blocks", config.blocks);
                let deviation =
                    self.describe(diff, anchor, mid, config.threshold_scaled, span, "anchor");
                self.reference = Some((mid, at));
                return self.trip(deviation);
            }
        }

        self.reference = Some((mid, at));
        self.publish_ratios();
        Verdict::Pass
    }
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use ethrex_common::U256;

    use super::*;
    use crate::{
        breaker::{BreakerConfig, WindowConfig},
        feed::parse_decimal_scaled,
        guard::{CompositeSample, MarketGuard, Verdict},
        pricing::Diagnostics,
    };

    /// 6 decimals keeps the numbers in the tests readable ("100" = 100_000_000).
    fn config(threshold: &str) -> BreakerConfig {
        BreakerConfig {
            threshold_scaled: Some(parse_decimal_scaled(threshold, 6).unwrap()),
            window: None,
            decimals: 6,
        }
    }

    /// A guard with both checks armed. `blocks` at 12s each: 5 blocks is a minute.
    fn config_windowed(tick: &str, window: &str, blocks: u64) -> BreakerConfig {
        BreakerConfig {
            threshold_scaled: Some(parse_decimal_scaled(tick, 6).unwrap()),
            window: Some(WindowConfig {
                threshold_scaled: parse_decimal_scaled(window, 6).unwrap(),
                blocks,
                span: Duration::from_secs(blocks * crate::update::BLOCK_TIME_SECS),
            }),
            decimals: 6,
        }
    }

    fn guard(config: BreakerConfig) -> DeviationGuard {
        DeviationGuard::new(config, None)
    }

    fn mid(value: &str) -> U256 {
        parse_decimal_scaled(value, 6).unwrap()
    }

    /// The tick-to-tick tests configure no window, so any instant serves. One fixed value
    /// for the whole suite keeps repeated calls from advancing a window that these tests
    /// do not have.
    fn now() -> Instant {
        use std::sync::OnceLock;
        static START: OnceLock<Instant> = OnceLock::new();
        *START.get_or_init(Instant::now)
    }

    /// One second of elapsed time per step, so a window of N blocks is 12N steps.
    fn at(second: u64) -> Instant {
        now() + Duration::from_secs(second)
    }

    /// A composite sample carrying only what this guard reads: the mid and when.
    fn sample(mid: U256, at: Instant) -> CompositeSample {
        CompositeSample::new(at, mid, U256::zero(), false, 6, Vec::new())
    }

    /// `assess`, with a diagnostics buffer this guard never writes (it writes its legacy
    /// gauges instead).
    fn assess(guard: &mut DeviationGuard, mid: U256, at: Instant) -> Verdict {
        guard.assess(&sample(mid, at), &mut Diagnostics::new(0))
    }

    /// Startup has nothing to deviate from, so whatever the first price is, it is the
    /// baseline: the same trust the service already extends to the first price it quotes.
    #[test]
    fn the_first_mid_ever_cannot_trip() {
        let mut g = guard(config("0.02"));
        assert_eq!(assess(&mut g, mid("123456"), now()), Verdict::Pass);
    }

    /// The reference is the previous observation, not the first one: a third price 3% from
    /// the start but under 2% from the one before it must still pass.
    #[test]
    fn a_move_within_the_threshold_passes_and_updates_the_reference() {
        let mut g = guard(config("0.02"));
        assert_eq!(assess(&mut g, mid("100"), now()), Verdict::Pass);
        assert_eq!(assess(&mut g, mid("101.5"), now()), Verdict::Pass);
        assert_eq!(assess(&mut g, mid("103"), now()), Verdict::Pass);
    }

    /// Regression test for the halt the delivery-gated design manufactured: with the
    /// reference pinned to the last mid delivered to a builder, an outage on the delivery
    /// side froze it, and ordinary drift on a healthy feed was then measured against a
    /// stale price. Tick to tick, an outage cannot do that: the guard neither knows nor
    /// cares whether anything reached the wire.
    #[test]
    fn an_outage_on_the_delivery_side_cannot_manufacture_a_trip() {
        let mut g = guard(config("0.02"));
        assert_eq!(assess(&mut g, mid("4000"), now()), Verdict::Pass);
        for step in 1..=20u64 {
            let value = 4000 + step * 4; // +0.1% a step, +2.1% overall
            assert_eq!(
                assess(&mut g, U256::from(value) * U256::exp10(6), now()),
                Verdict::Pass,
                "drift to {value} must not trip a jump detector"
            );
        }
        assert!(matches!(
            assess(&mut g, mid("4300"), now()),
            Verdict::Trip(_)
        ));
    }

    /// Tick to tick is between consecutive samples. After a gap longer than the pair quotes
    /// a price for (every venue out, a market closed for the night), the first sample is a
    /// fresh start: the pair offered no price across the gap, so the move across it is not
    /// one it quoted through. A gap inside that limit is an ordinary tick.
    #[test]
    fn the_first_sample_after_a_gap_longer_than_a_price_lives_is_a_fresh_start() {
        let mut g = guard(config("0.02"));
        let start = now();
        assert_eq!(assess(&mut g, mid("100"), start), Verdict::Pass);
        // 3% up after a gap just past MAX_PRICE_AGE: not a move the pair quoted through.
        let after_gap = start + MAX_PRICE_AGE + Duration::from_millis(1);
        assert_eq!(assess(&mut g, mid("103"), after_gap), Verdict::Pass);
        // The sample after the gap is the new reference: 3% more, a second later, trips.
        assert!(matches!(
            assess(&mut g, mid("106.1"), after_gap + Duration::from_secs(1)),
            Verdict::Trip(_)
        ));
        // Exactly MAX_PRICE_AGE apart is still consecutive.
        let mut g = guard(config("0.02"));
        assert_eq!(assess(&mut g, mid("100"), start), Verdict::Pass);
        assert!(matches!(
            assess(&mut g, mid("103"), start + MAX_PRICE_AGE),
            Verdict::Trip(_)
        ));
        // The window is a bound over its span, not a tick: it still trips across the gap.
        let mut w = guard(config_windowed("0.50", "0.10", 5));
        assert_eq!(assess(&mut w, mid("100"), start), Verdict::Pass);
        assert!(matches!(
            assess(&mut w, mid("111"), start + MAX_PRICE_AGE * 2),
            Verdict::Trip(_)
        ));
    }

    /// The point of the feature, in both directions: a jump or a drop past the threshold
    /// trips, and the numbers the operator line carries are the ones that crossed it.
    #[test]
    fn a_jump_beyond_the_threshold_trips() {
        let mut up = guard(config("0.02"));
        assert_eq!(assess(&mut up, mid("100"), now()), Verdict::Pass);
        assert!(matches!(
            assess(&mut up, mid("103"), now()),
            Verdict::Trip(_)
        ));
        assert_eq!(
            up.last_deviation(),
            Some(&Deviation {
                moved: "3.00%".to_owned(),
                limit: "2.00%".to_owned(),
                reference: "100".to_owned(),
                mid: "103".to_owned(),
                span: String::new(),
                reference_label: "previous",
            })
        );

        let mut down = guard(config("0.02"));
        assert_eq!(assess(&mut down, mid("100"), now()), Verdict::Pass);
        assert!(matches!(
            assess(&mut down, mid("97"), now()),
            Verdict::Trip(_)
        ));
        let d = down.last_deviation().unwrap();
        assert_eq!((d.moved.as_str(), d.mid.as_str()), ("3.00%", "97"));
    }

    /// The three strings a trip renders are the ones today's operator sees, byte for byte:
    /// the block header, the alarm, and the re-arm tail. Golden here so `quoting.rs` and
    /// `service.rs` assemble them without knowing the numbers.
    #[test]
    fn a_trip_renders_todays_three_strings() {
        let mut g = guard(config("0.02"));
        assert_eq!(assess(&mut g, mid("100"), now()), Verdict::Pass);
        let Verdict::Trip(reason) = assess(&mut g, mid("103"), now()) else {
            panic!("3% up must trip a 2% guard");
        };
        assert_eq!(
            reason.header,
            "price deviation 3.00% exceeds the 2.00% limit"
        );
        assert_eq!(
            reason.alarm,
            "circuit breaker tripped: mid moved 3.00% > 2.00% (previous 100, new 103)"
        );
        assert_eq!(
            reason.rearm,
            "on a 3.00% move from previous 100 to 103 (limit 2.00%)"
        );

        let mut w = guard(config_windowed("0.50", "0.10", 5));
        assert_eq!(assess(&mut w, mid("100"), at(0)), Verdict::Pass);
        let Verdict::Trip(reason) = assess(&mut w, mid("111"), at(300)) else {
            panic!("11% over the window must trip");
        };
        assert_eq!(
            reason.header,
            "price deviation 11.00% over 5 blocks exceeds the 10.00% limit"
        );
        assert_eq!(
            reason.alarm,
            "circuit breaker tripped: mid moved 11.00% over 5 blocks > 10.00% (anchor 100, new 111)"
        );
        assert_eq!(
            reason.rearm,
            "on a 11.00% move over 5 blocks from anchor 100 to 111 (limit 10.00%)"
        );
    }

    /// No latch here: a guard that has tripped is never asked again, because the composite
    /// checks the lane's latch before any guard runs. So the "replay the original trip"
    /// arm is gone with the breaker's own tripped state; after a trip the guard simply
    /// keeps judging from its last reference, which a fresh lane would never see.
    #[test]
    fn after_a_trip_the_guard_keeps_judging() {
        let mut g = guard(config("0.02"));
        assess(&mut g, mid("100"), now());
        assert!(matches!(
            assess(&mut g, mid("105"), now()),
            Verdict::Trip(_)
        ));
        assert_eq!(assess(&mut g, mid("105"), now()), Verdict::Pass);
    }

    /// Regression test for the alarm that read as a misfire: the comparison is exact but
    /// the rendering truncates to two decimals, so a move that crossed the limit by less
    /// than a basis point printed `mid moved 2.00% > 2.00%`. The move's precision extends
    /// until it can be told apart from the limit.
    #[test]
    fn a_move_just_over_the_limit_renders_distinguishably_from_it() {
        let mut g = guard(config("0.02"));
        assert_eq!(assess(&mut g, mid("100"), now()), Verdict::Pass);
        assert!(matches!(
            assess(&mut g, mid("102.004"), now()),
            Verdict::Trip(_)
        ));
        let d = g.last_deviation().unwrap();
        assert_eq!((d.limit.as_str(), d.moved.as_str()), ("2.00%", "2.0040%"));
    }

    /// The comparison is strictly greater: exactly-at-threshold is the boundary someone
    /// will one day "fix" by accident.
    #[test]
    fn an_exact_threshold_move_does_not_trip() {
        let mut g = guard(config("0.02"));
        assert_eq!(assess(&mut g, mid("100"), now()), Verdict::Pass);
        assert_eq!(assess(&mut g, mid("102"), now()), Verdict::Pass);
    }

    /// The explicit trade this guard makes: it detects discontinuities, not destinations.
    /// A move arriving in sub-threshold steps never trips however far it travels;
    /// `min_mid`/`max_mid` bounds where it may end up.
    #[test]
    fn a_slow_drift_never_trips() {
        let mut g = guard(config("0.02"));
        for value in 100..=120u64 {
            assert_eq!(
                assess(&mut g, U256::from(value) * U256::exp10(6), now()),
                Verdict::Pass,
                "step to {value} tripped"
            );
        }
    }

    /// The conservative post-outage policy: the reference is the last mid this guard saw,
    /// so a feed that goes away and comes back far from where it died trips instead of
    /// being taken on faith.
    #[test]
    fn a_feed_that_returns_far_from_where_it_died_trips() {
        let mut g = guard(config("0.02"));
        assert_eq!(assess(&mut g, mid("100"), now()), Verdict::Pass);
        assert!(matches!(
            assess(&mut g, mid("105"), now()),
            Verdict::Trip(_)
        ));
        assert_eq!(g.last_deviation().unwrap().reference, "100");
    }

    /// Regression test for the overflow found in design review: a mid may use all 216 bits
    /// its slot allows and the scale reaches 10^59, so diff × scale wraps a U256 (and
    /// panics in debug). full_mul (U512) keeps the guard total.
    #[test]
    fn the_deviation_check_survives_216_bit_mids() {
        let huge = BreakerConfig {
            threshold_scaled: Some(parse_decimal_scaled("0.5", 59).unwrap()),
            window: None,
            decimals: 59,
        };
        let reference = U256::one() << 214;
        let mut trips = guard(huge);
        assert_eq!(assess(&mut trips, reference, now()), Verdict::Pass);
        assert!(matches!(
            assess(&mut trips, U256::one() << 215, now()),
            Verdict::Trip(_)
        ));

        let mut passes = guard(huge);
        assert_eq!(assess(&mut passes, reference, now()), Verdict::Pass);
        assert_eq!(
            assess(&mut passes, reference + reference / U256::from(100), now()),
            Verdict::Pass
        );
    }

    #[test]
    fn the_first_mid_ever_reports_no_deviation() {
        let mut g = guard(config("0.02"));
        assess(&mut g, mid("1.0"), now());
        assert_eq!(g.last_deviation_ratio(), None);
    }

    #[test]
    fn a_sub_threshold_move_reports_its_share_of_the_limit() {
        let mut g = guard(config("0.02"));
        assess(&mut g, mid("1.0"), now());
        assess(&mut g, mid("1.01"), now()); // 1% against a 2% limit
        let r = g
            .last_deviation_ratio()
            .expect("a ratio after a second sample");
        assert!((r - 0.5).abs() < 1e-9, "got {r}");
    }

    #[test]
    fn a_move_that_does_not_move_reports_zero() {
        let mut g = guard(config("0.02"));
        assess(&mut g, mid("1.0"), now());
        assess(&mut g, mid("1.0"), now());
        assert_eq!(g.last_deviation_ratio(), Some(0.0));
    }

    #[test]
    fn a_trip_reports_a_ratio_above_one() {
        let mut g = guard(config("0.02"));
        assess(&mut g, mid("1.0"), now());
        assert!(matches!(
            assess(&mut g, mid("1.04"), now()),
            Verdict::Trip(_)
        ));
        let r = g.last_deviation_ratio().expect("a ratio after a trip");
        assert!(r > 1.0, "got {r}");
    }

    /// The gauges the dashboard reads are written by the guard itself, on every sample it
    /// judges: the legacy series names survive the move onto the guard trait.
    #[test]
    fn the_guard_writes_the_legacy_ratio_gauges_when_it_has_metrics() {
        let metrics = crate::metrics::Metrics::new().unwrap();
        let pair = metrics.for_pair("GAUGED/TEST");
        let adopted = crate::feed::Adoption::new();
        adopted.adopt();
        let gated = Gated::new(pair.clone(), adopted);
        let mut g = DeviationGuard::new(config_windowed("0.02", "0.10", 5), Some(gated));
        assess(&mut g, mid("100"), at(0));
        assess(&mut g, mid("101"), at(1));
        assert!((pair.breaker_deviation_ratio.get() - 0.5).abs() < 1e-9);
        assert!((pair.breaker_window_deviation_ratio.get() - 0.1).abs() < 1e-9);
    }

    /// The counterpart to `a_slow_drift_never_trips`, and the point of the window: steps
    /// far inside the tick threshold that cumulatively cross the windowed one do trip.
    #[test]
    fn a_slow_drift_trips_the_window() {
        let mut g = guard(config_windowed("0.02", "0.10", 5));
        assert_eq!(assess(&mut g, mid("100"), at(0)), Verdict::Pass);
        for step in 1..=10u64 {
            let value = 100 + step;
            assert_eq!(
                assess(&mut g, U256::from(value) * U256::exp10(6), at(step)),
                Verdict::Pass,
                "step to {value} tripped before the limit was crossed"
            );
        }
        assert!(matches!(
            assess(&mut g, mid("111"), at(11)),
            Verdict::Trip(_)
        ));
        let d = g.last_deviation().unwrap();
        assert_eq!(
            (
                d.limit.as_str(),
                d.span.as_str(),
                d.reference_label,
                d.reference.as_str()
            ),
            ("10.00%", " over 5 blocks", "anchor", "100")
        );
    }

    /// The anchor slides: a move large enough to trip inside one window is fine when it
    /// takes long enough, because at every instant the price a window ago was close.
    #[test]
    fn a_move_slower_than_the_window_never_trips() {
        let mut g = guard(config_windowed("0.02", "0.10", 5));
        for step in 0..=20u64 {
            let value = 100 + step;
            assert_eq!(
                assess(&mut g, U256::from(value) * U256::exp10(6), at(step * 30)),
                Verdict::Pass,
                "step to {value} at {}s tripped",
                step * 30
            );
        }
    }

    /// A quiet feed must not shorten the window. The bucket at or before the trailing edge
    /// is retained, so the move is measured over at least the configured span.
    #[test]
    fn a_sparse_feed_still_measures_over_the_whole_window() {
        let mut g = guard(config_windowed("0.50", "0.10", 5));
        assert_eq!(assess(&mut g, mid("100"), at(0)), Verdict::Pass);
        assert!(matches!(
            assess(&mut g, mid("111"), at(300)),
            Verdict::Trip(_)
        ));
        assert_eq!(g.last_deviation().unwrap().reference, "100");
    }

    /// A window shorter than its span judges against the oldest sample it has rather than
    /// withholding a verdict: `assess` has no state that declines to answer.
    #[test]
    fn a_partial_window_still_judges() {
        let mut g = guard(config_windowed("0.50", "0.10", 100));
        assert_eq!(assess(&mut g, mid("100"), at(0)), Verdict::Pass);
        assert!(matches!(
            assess(&mut g, mid("120"), at(1)),
            Verdict::Trip(_)
        ));
    }

    /// Many ticks a second collapse into one bucket, so the buffer is bounded by the
    /// window in seconds and not by the tick rate.
    #[test]
    fn a_burst_of_same_second_ticks_does_not_grow_the_buffer() {
        let mut g = guard(config_windowed("0.50", "0.50", 2));
        for i in 0..1000u64 {
            assess(&mut g, mid("100"), now() + Duration::from_millis(i));
        }
        assert!(
            g.window_len() <= 3,
            "buffer grew to {} on same-second ticks",
            g.window_len()
        );
    }

    /// The window is symmetric: the guard has no view about which way is dangerous, and
    /// the direction is already visible in the anchor/mid pair a trip carries.
    #[test]
    fn equal_moves_up_and_down_measure_the_same() {
        let mut up = guard(config_windowed("0.50", "0.10", 5));
        assert_eq!(assess(&mut up, mid("100"), at(0)), Verdict::Pass);
        assert_eq!(assess(&mut up, mid("105"), at(1)), Verdict::Pass);
        let up_ratio = up.last_window_deviation_ratio().expect("a second sample");

        let mut down = guard(config_windowed("0.50", "0.10", 5));
        assert_eq!(assess(&mut down, mid("100"), at(0)), Verdict::Pass);
        assert_eq!(assess(&mut down, mid("95"), at(1)), Verdict::Pass);
        let down_ratio = down.last_window_deviation_ratio().expect("a second sample");

        assert!(
            (up_ratio - down_ratio).abs() < 1e-9,
            "up {up_ratio} and down {down_ratio} must measure the same"
        );
    }

    /// The gauge counterpart of the tick ratio, for the same reason it is an accessor.
    #[test]
    fn the_window_ratio_reports_progress_toward_the_limit() {
        let mut g = guard(config_windowed("0.50", "0.10", 5));
        assert_eq!(assess(&mut g, mid("100"), at(0)), Verdict::Pass);
        assert_eq!(g.last_window_deviation_ratio(), None);
        assert_eq!(assess(&mut g, mid("105"), at(1)), Verdict::Pass);
        let ratio = g.last_window_deviation_ratio().expect("a second sample");
        assert!(
            (ratio - 0.5).abs() < 1e-9,
            "5% of a 10% limit is 0.5, got {ratio}"
        );
    }

    /// A user's market guard sees the market the way a person says it, whatever the lane's
    /// orientation, through the same helper a pricer's `Market` offers.
    #[test]
    fn a_composite_sample_from_testing_speaks_market_orientation() {
        let pair = crate::testing::pair_with(true, 18);
        let sample = crate::testing::composite_sample(&pair, 4000.0, 0.0001, now());
        assert!((sample.mid_f64() - 4000.0).abs() < 1e-9);
        assert!(sample.sources().is_empty());
    }
}
