//! The metric registry and the label-pre-bound handles every recording site uses.
//!
//! IO-free on purpose, exactly like `breaker.rs`: no sockets, no clock, no logging. The
//! listener lives in `exporter.rs`. That split is what lets every recording rule be
//! asserted against a throwaway `Registry` in a unit test, with no tokio and no network.
//!
//! Labels are bound once, at construction, never at the call site. A recording site holds
//! the child metric itself, so it costs one atomic increment rather than a hashmap lookup
//! and a `Vec<&str>` allocation — on a loop that requotes every 50ms, across every pair and
//! builder. It also makes a mislabelled series unrepresentable, which is the same reason
//! the pair label is derived from an on-chain `symbol()` call rather than configured.
//!
//! Every family here has a non-test caller: `pusher::run` constructs the registry and wires
//! the `/metrics` listener, and the feed, publish gate, RPC/landing, builder and node-push
//! call sites record into it. That matters for `cargo clippy --all-targets`, which treats an
//! unreached `pub` item here as an error the same way it would a private one (this module is
//! private to the crate, so `pub` signals no downstream consumer) — so an item added ahead of
//! its consumer needs a `#[allow(dead_code)]` naming the caller that is coming, removed once
//! that caller lands.

use eyre::Result;
use prometheus::{
    Gauge, GaugeVec, Histogram, HistogramOpts, HistogramVec, IntCounter, IntCounterVec, Opts,
    PullingGauge, Registry, TextEncoder,
};

use crate::update::UnusableKind;

/// Buckets for RPC call duration, in seconds. Straddles the documented ~50ms requote
/// budget so the histogram answers "are we blowing the budget" directly rather than by
/// interpolation.
///
/// The top finite bucket is `crate::RPC_TIMEOUT`, which is the slowest a timed call can
/// possibly be: `bounded` gives up there. It used to stop at 1.0, so every abandoned call
/// landed in +Inf, and `histogram_quantile` — which cannot interpolate past the highest
/// finite bound — reported a p95 of exactly 1.0 for calls that actually took three times
/// that. A histogram whose ceiling is below its subject's worst case reads as *faster* the
/// worse things get.
/// An extension call runs on the 50ms requote path, so its interesting range is well
/// under a millisecond up to the tens of milliseconds where it starts costing blocks.
const EXTENSION_BUCKETS: &[f64] = &[
    0.00001, 0.000025, 0.00005, 0.0001, 0.00025, 0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05,
];

const RPC_BUCKETS: &[f64] = &[
    0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.0, 3.0,
];

/// Buckets for one head poll, in seconds. RPC_BUCKETS' shape, with a ceiling set by what a
/// poll can actually cost.
///
/// `head::POLL_TIMEOUT` (2s) bounds only the `eth_blockNumber` half; the block fetch that
/// follows it when the head moved goes through `bounded`, so it can add another
/// `RPC_TIMEOUT` (3s) — and `poll_forever` times the pair, because that whole round is what
/// every pair waits on. 5.0 is therefore the real worst case, and the 2.0 bucket below it
/// still separates a poll abandoned at the timeout from one that merely dawdled.
const HEAD_BUCKETS: &[f64] = &[
    0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.0, 3.0, 5.0,
];

/// Buckets for builder ack latency, in seconds. Straddles the ~400ms quote-eviction window
/// for the same reason.
const ACK_BUCKETS: &[f64] = &[
    0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.2, 0.4, 0.8, 2.0, 5.0,
];

/// Which RPC call a duration or error belongs to. An enum rather than a `&str` so the
/// label set is closed by construction and cannot grow a typo'd member at 2am.
///
/// Only the calls a *pair* makes, which is what the `pair` label on these two families
/// asserts: `GetNonce` and `GetBalance` in `fetch_block_inputs`, `EthCall` in
/// `verify_landed`. The head poll used to be here as `GetBlockByNumber` and
/// `GetBlockNumber`; it moved out of the per-pair loop into the one watcher in `head.rs`,
/// which polls once on behalf of every pair, so no pair label could be true of it. Its
/// timing lives in [`HeadMetrics`] instead — unlabelled, like the thing it measures. The two
/// variants went with it rather than staying as `call` values nothing records: a registered
/// counter that never moves renders a real `0`, which reads as "this call never fails"
/// instead of "this call is not made here".
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RpcCall {
    GetNonce,
    GetBalance,
    EthCall,
}

impl RpcCall {
    /// Declaration order is load-bearing: children are indexed by `variant as usize`.
    pub const ALL: [RpcCall; 3] = [RpcCall::GetNonce, RpcCall::GetBalance, RpcCall::EthCall];

    pub fn as_str(self) -> &'static str {
        match self {
            RpcCall::GetNonce => "get_nonce",
            RpcCall::GetBalance => "get_balance",
            RpcCall::EthCall => "eth_call",
        }
    }
}

/// What the publish gate decided this tick. Mirrors `quoting::Action`, deliberately as its
/// own type: `Action` carries payloads and is free to change shape, while this is a label
/// set that dashboards depend on and must not.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PublishAction {
    Publish,
    Withdraw,
    Halt,
    Nothing,
}

impl PublishAction {
    /// Declaration order is load-bearing: children are indexed by `variant as usize`.
    pub const ALL: [PublishAction; 4] = [
        PublishAction::Publish,
        PublishAction::Withdraw,
        PublishAction::Halt,
        PublishAction::Nothing,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            PublishAction::Publish => "publish",
            PublishAction::Withdraw => "withdraw",
            PublishAction::Halt => "halt",
            PublishAction::Nothing => "nothing",
        }
    }
}

/// Whether a block's update reached the chain. Mirrors `crate::Landing`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LandingResult {
    Landed,
    Missed,
    Unknown,
    NotQuoted,
}

impl LandingResult {
    /// Declaration order is load-bearing: children are indexed by `variant as usize`.
    pub const ALL: [LandingResult; 4] = [
        LandingResult::Landed,
        LandingResult::Missed,
        LandingResult::Unknown,
        LandingResult::NotQuoted,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            LandingResult::Landed => "landed",
            LandingResult::Missed => "missed",
            LandingResult::Unknown => "unknown",
            LandingResult::NotQuoted => "not_quoted",
        }
    }
}

/// Why a bookTicker message was not turned into a sample.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TickReject {
    OneSided,
    Crossed,
    Malformed,
    Overflow,
}

impl TickReject {
    /// Declaration order is load-bearing: children are indexed by `variant as usize`.
    pub const ALL: [TickReject; 4] = [
        TickReject::OneSided,
        TickReject::Crossed,
        TickReject::Malformed,
        TickReject::Overflow,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            TickReject::OneSided => "one_sided",
            TickReject::Crossed => "crossed",
            TickReject::Malformed => "malformed",
            TickReject::Overflow => "overflow",
        }
    }
}

/// How a config reload ended.
///
/// The failure modes the file-authoritative model introduces, and the reason this is three
/// values rather than a bool: the operator has edited `pairs.toml` and believes the running
/// pairs now match it. Two different ways that belief can be wrong, and neither shows up
/// anywhere else — the pairs carry on quoting perfectly well on their old config, so every
/// other series in the process stays exactly as healthy as it was.
///
/// - `rejected`: the file did not validate, so **nothing** was touched. Consistent, at
///   least: every lane is on the previous config together.
/// - `partial`: the file validated and was applied, but at least one lane's feed would not
///   come up, so that lane is still on its old config (or was never started) while the rest
///   moved. A *mixture*, which is strictly worse to reason about than `rejected` — hence
///   its own value rather than being folded into `applied`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Reload {
    Applied,
    Rejected,
    Partial,
}

impl Reload {
    /// Declaration order is load-bearing: children are indexed by `variant as usize`.
    pub const ALL: [Reload; 3] = [Reload::Applied, Reload::Rejected, Reload::Partial];

    pub fn as_str(self) -> &'static str {
        match self {
            Reload::Applied => "applied",
            Reload::Rejected => "rejected",
            Reload::Partial => "partial",
        }
    }
}

/// How one `--mode node` push ended. Node mode sends `updateState` straight to the RPC, so
/// unlike builder mode there is a receipt to classify: `landed` is a mined, non-reverted
/// transaction whose read-back matched.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodePush {
    Landed,
    Failed,
    Reverted,
    Mismatch,
}

impl NodePush {
    /// Declaration order is load-bearing: children are indexed by `variant as usize`.
    pub const ALL: [NodePush; 4] = [
        NodePush::Landed,
        NodePush::Failed,
        NodePush::Reverted,
        NodePush::Mismatch,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            NodePush::Landed => "landed",
            NodePush::Failed => "failed",
            NodePush::Reverted => "reverted",
            NodePush::Mismatch => "mismatch",
        }
    }
}

/// Every metric family, plus the registry they are registered in.
pub struct Metrics {
    pub registry: Registry,
    /// The venue feeds whose `quote_updater_venue_feed_up` is read from their own flag at
    /// scrape time, by `(pair, venue)`: the generation that owns the pair's series.
    venue_connections:
        std::sync::Mutex<std::collections::HashMap<(String, String), crate::feed::Connected>>,
    feed_up: GaugeVec,
    feed_ticks: IntCounterVec,
    feed_last_tick: GaugeVec,
    feed_sample_current: GaugeVec,
    feed_mid: GaugeVec,
    feed_delta: GaugeVec,
    feed_sources_fresh: GaugeVec,
    feed_sources_min: GaugeVec,
    venue_feed_up: GaugeVec,
    venue_feed_ticks: IntCounterVec,
    venue_feed_rejected: IntCounterVec,
    venue_feed_last_tick: GaugeVec,
    venue_feed_sample_current: GaugeVec,
    venue_feed_mid: GaugeVec,
    venue_feed_delta: GaugeVec,
    venue_source_lag_ms: GaugeVec,
    venue_late: GaugeVec,
    venue_future_stamps: IntCounterVec,
    price_unusable: IntCounterVec,
    diagnostic: GaugeVec,
    trip_source: GaugeVec,
    guards: GaugeVec,
    extension_task_exits: IntCounterVec,
    extension_duration: HistogramVec,
    observer_dropped: IntCounterVec,
    observer_running: GaugeVec,
    publish_decisions: IntCounterVec,
    published_mid: GaugeVec,
    published_delta: GaugeVec,
    vault_balance: GaugeVec,
    token_price_usd: GaugeVec,
    vault_value_usd: GaugeVec,
    breaker_armed: GaugeVec,
    breaker_tripped: GaugeVec,
    breaker_trips: IntCounterVec,
    breaker_rearms: IntCounterVec,
    breaker_deviation_ratio: GaugeVec,
    breaker_window_deviation_ratio: GaugeVec,
    rpc_duration: HistogramVec,
    rpc_errors: IntCounterVec,
    sign_errors: IntCounterVec,
    target_blocks: IntCounterVec,
    signer_balance_wei: GaugeVec,
    signer_runway_updates: GaugeVec,
    landings: IntCounterVec,
    consecutive_landing_misses: GaugeVec,
    pair_restarts: IntCounterVec,
    builder_up: GaugeVec,
    builder_ack_latency: HistogramVec,
    builder_acks: IntCounterVec,
    builder_rejections: IntCounterVec,
    builder_withdrawals: IntCounterVec,
    builder_send_errors: IntCounterVec,
    builder_reconnects: IntCounterVec,
    builder_seq: GaugeVec,
    node_pushes: IntCounterVec,
}

/// One pair's pre-bound children. Cheap to clone: every prometheus child is `Arc`-backed,
/// so the feed task gets its own copy without a lifetime crossing a `tokio::spawn`.
#[derive(Clone)]
pub struct PairMetrics {
    /// The pair's label, for the one series bound per trip rather than per pair.
    label: String,
    /// The `trip_source` family, bound by (source, cause) when a trip happens.
    trip_source: GaugeVec,
    /// The `guards` family, bound per registered guard kind at adoption.
    guards: GaugeVec,
    pub feed_up: Gauge,
    pub feed_ticks: IntCounter,
    pub feed_last_tick: Gauge,
    pub feed_sample_current: Gauge,
    pub feed_mid: Gauge,
    pub feed_delta: Gauge,
    pub feed_sources_fresh: Gauge,
    pub feed_sources_min: Gauge,
    price_unusable: [IntCounter; UnusableKind::ALL.len()],
    publish_decisions: [IntCounter; 4],
    pub published_mid: Gauge,
    pub published_delta: Gauge,
    pub breaker_armed: Gauge,
    pub breaker_tripped: Gauge,
    pub breaker_trips: IntCounter,
    pub breaker_rearms: IntCounter,
    pub breaker_deviation_ratio: Gauge,
    pub breaker_window_deviation_ratio: Gauge,
    rpc_duration: [Histogram; 3],
    rpc_errors: [IntCounter; 3],
    pub sign_errors: IntCounter,
    pub target_blocks: IntCounter,
    pub signer_balance_wei: Gauge,
    pub signer_runway_updates: Gauge,
    landings: [IntCounter; 4],
    pub consecutive_landing_misses: Gauge,
    // Counted in `Service::spawn` (`service.rs`), at the call that passes `supervise` its
    // closure: see the comment there for why (a `supervise` parameter would cost seven existing
    // test call sites for one counter).
    pub pair_restarts: IntCounter,
    node_pushes: [IntCounter; 4],
}

/// One venue feed's pre-bound children, for one pair: the socket and the samples of one
/// `(pair, venue)`. The pair's own `feed_*` series describe the composite these are
/// averaged into.
#[derive(Clone)]
pub struct VenueMetrics {
    pub up: Gauge,
    pub ticks: IntCounter,
    rejected: [IntCounter; 4],
    pub last_tick: Gauge,
    pub sample_current: Gauge,
    pub mid: Gauge,
    pub delta: Gauge,
    /// `quote_updater_venue_source_lag_ms`: written at scrape time from the feed's own
    /// reading (see `Metrics::render`), and by `quiesce`.
    pub source_lag_ms: Gauge,
    /// 1 while the composite leaves this venue out of its pair's average for arriving
    /// later than `max_source_lag_ms` behind its exchange stamp.
    pub late: Gauge,
    /// Frames whose exchange stamp was too far ahead of their arrival to read a lag from.
    pub future_stamps: IntCounter,
}

impl VenueMetrics {
    pub fn rejected(&self, reason: TickReject) -> &IntCounter {
        &self.rejected[reason as usize]
    }

    /// The venue half of [`PairMetrics::quiesce`], for the same reason.
    pub fn quiesce(&self) {
        for gauge in [
            &self.up,
            &self.last_tick,
            &self.sample_current,
            &self.mid,
            &self.delta,
            &self.source_lag_ms,
            &self.late,
        ] {
            gauge.set(f64::NAN);
        }
    }
}

/// One builder connection's pre-bound children, for one pair.
// Built by `for_builder`; `BuilderTask` stores one per builder connection.
#[derive(Clone)]
pub struct BuilderMetrics {
    pub up: Gauge,
    pub ack_latency: Histogram,
    pub acks: IntCounter,
    pub rejections: IntCounter,
    pub withdrawals: IntCounter,
    pub send_errors: IntCounter,
    pub reconnects: IntCounter,
    pub seq: Gauge,
}

impl BuilderMetrics {
    /// The builder-connection half of [`PairMetrics::quiesce`], for the same reason.
    ///
    /// `up` is the one that matters: it goes to 0 whenever a connection drops, so a removed
    /// pair whose builders were torn down on the way out would leave
    /// `sum by (pair) (quote_updater_builder_up) == 0` true forever and page
    /// `PusherPairHasNoBuilder` for a lane nobody configured. Called per builder, since the
    /// children are keyed by `(pair, builder)` and only the caller knows the builder names.
    pub fn quiesce(&self) {
        self.up.set(f64::NAN);
        self.seq.set(f64::NAN);
    }
}

/// The head watcher's handles. One set per process rather than per pair, because the
/// watcher polls once on behalf of every pair (see `head.rs`) — the same reason `RpcCall`
/// does not carry the head poll any more. Cheap to clone, like [`PairMetrics`]: the poll
/// task and the subscription task each get their own copy.
#[derive(Clone)]
pub struct HeadMetrics {
    pub poll_duration: Histogram,
    pub poll_errors: IntCounter,
    pub number: Gauge,
}

/// The config reload counters. Builder mode only, like [`HeadMetrics`] and the `Health`
/// gauges: node mode has no supervised pair set to converge and no reload to count, and a
/// flat zero there would look like a service whose reloads all succeed rather than one that
/// has none.
pub struct ReloadMetrics {
    reloads: [IntCounter; 3],
    /// Incremented once per reload that actually moved every lane onto the new file, so a
    /// dashboard can mark them and an operator can tell "the config I am looking at is the
    /// one running" from "it was rejected an hour ago". Starts at 0, meaning the config the
    /// process started with.
    ///
    /// Deliberately *not* advanced by a rejected or partial reload, nor by one that found
    /// nothing to do. The whole value of this number is that reading it answers "is the file
    /// I am looking at live?", and advancing it for a reload that left a lane on its old
    /// stanza would make it answer that question wrongly in exactly the case an operator
    /// needs it most.
    pub generation: Gauge,
}

impl ReloadMetrics {
    pub fn reloads(&self, result: Reload) -> &IntCounter {
        &self.reloads[result as usize]
    }
}

/// Which of `Health::census`'s three counts one gauge reads. An enum rather than the
/// `usize` index this used to be, whose `_ => c.2` arm meant a fourth row added with
/// `pick: 3` would compile and silently publish `pairs_total` under the new gauge's
/// name — the same "a new variant quietly lands in an existing bucket" class that
/// `action_label` and `landing_label` were given exhaustive matches to prevent. There is
/// no wildcard here, so a new count has to say which one it is.
///
/// Still through `census()` rather than three separate atomic loads: it is the API
/// `BackoffGuard` exists to keep honest, and one extra relaxed load per scrape — every
/// 15 seconds — is not worth a second way to read the same state.
#[derive(Clone, Copy)]
enum Count {
    Halted,
    InBackoff,
    Total,
}

/// Builds a counter family, registers it, and hands it back.
fn counter(registry: &Registry, name: &str, help: &str, labels: &[&str]) -> Result<IntCounterVec> {
    let v = IntCounterVec::new(Opts::new(name, help), labels)?;
    registry.register(Box::new(v.clone()))?;
    Ok(v)
}

fn gauge(registry: &Registry, name: &str, help: &str, labels: &[&str]) -> Result<GaugeVec> {
    let v = GaugeVec::new(Opts::new(name, help), labels)?;
    registry.register(Box::new(v.clone()))?;
    Ok(v)
}

fn histogram(
    registry: &Registry,
    name: &str,
    help: &str,
    labels: &[&str],
    buckets: &[f64],
) -> Result<HistogramVec> {
    let v = HistogramVec::new(
        HistogramOpts::new(name, help).buckets(buckets.to_vec()),
        labels,
    )?;
    registry.register(Box::new(v.clone()))?;
    Ok(v)
}

impl Metrics {
    pub fn new() -> Result<Self> {
        let r = Registry::new();
        let pair = &["pair"][..];
        // Builder families declare `builder` before `pair`, the reverse of every other
        // family's `pair`-first order (mirrored in `for_builder`). Invisible in the
        // exposition — Prometheus sorts labels alphabetically before rendering — and
        // pinned by `a_builder_handle_carries_both_labels`, so a future swap here would
        // fail that test rather than silently reorder anything an operator sees.
        let bp = &["builder", "pair"][..];
        let pv = &["pair", "venue"][..];
        Ok(Self {
            venue_connections: Default::default(),
            feed_up: gauge(
                &r,
                "quote_updater_feed_up",
                "1 while at least min_sources venues have a fresh price.",
                pair,
            )?,
            feed_ticks: counter(
                &r,
                "quote_updater_feed_ticks_total",
                "Composite samples published to the quote loop.",
                pair,
            )?,
            feed_last_tick: gauge(
                &r,
                "quote_updater_feed_last_tick_timestamp_seconds",
                "Unix time of the last composite sample.",
                pair,
            )?,
            feed_sample_current: gauge(
                &r,
                "quote_updater_feed_sample_current",
                "1 while the held composite sample still describes fresh venue prices.",
                pair,
            )?,
            feed_mid: gauge(
                &r,
                "quote_updater_feed_mid",
                "Latest composite mid, lane orientation.",
                pair,
            )?,
            feed_delta: gauge(
                &r,
                "quote_updater_feed_delta",
                "Latest composite half-spread.",
                pair,
            )?,
            feed_sources_fresh: gauge(
                &r,
                "quote_updater_feed_sources_fresh",
                "Venues whose latest price was fresh at the last composite tick.",
                pair,
            )?,
            feed_sources_min: gauge(
                &r,
                "quote_updater_feed_sources_min",
                "Venues that must be fresh for the pair to publish (min_sources).",
                pair,
            )?,
            venue_feed_up: gauge(
                &r,
                "quote_updater_venue_feed_up",
                "1 while this venue's stream is connected.",
                pv,
            )?,
            venue_feed_ticks: counter(
                &r,
                "quote_updater_venue_feed_ticks_total",
                "Samples accepted from this venue.",
                pv,
            )?,
            venue_feed_rejected: counter(
                &r,
                "quote_updater_feed_rejected_ticks_total",
                "Venue messages refused.",
                &["pair", "venue", "reason"],
            )?,
            venue_feed_last_tick: gauge(
                &r,
                "quote_updater_venue_feed_last_tick_timestamp_seconds",
                "Unix time of the last sample accepted from this venue.",
                pv,
            )?,
            venue_feed_sample_current: gauge(
                &r,
                "quote_updater_venue_feed_sample_current",
                "1 while this venue's held sample still describes its live book.",
                pv,
            )?,
            venue_feed_mid: gauge(
                &r,
                "quote_updater_venue_feed_mid",
                "Latest mid observed at this venue, lane orientation.",
                pv,
            )?,
            venue_feed_delta: gauge(
                &r,
                "quote_updater_venue_feed_delta",
                "Latest half-spread observed at this venue.",
                pv,
            )?,
            venue_source_lag_ms: gauge(
                &r,
                "quote_updater_venue_source_lag_ms",
                "How late this venue's latest price arrived behind the exchange's own \
                 timestamp, over the venue's usual lag, in milliseconds. NaN for a venue \
                 whose frames carry no timestamp.",
                pv,
            )?,
            venue_late: gauge(
                &r,
                "quote_updater_venue_late",
                "1 while this venue is left out of its pair's average for arriving later \
                 than max_source_lag_ms behind its exchange timestamp.",
                pv,
            )?,
            venue_future_stamps: counter(
                &r,
                "quote_updater_venue_future_stamps_total",
                "Frames whose exchange timestamp was more than 2s ahead of their arrival, left \
                 out of the venue's usual lag: this host's clock or the venue's is off.",
                pv,
            )?,
            price_unusable: counter(
                &r,
                "quote_updater_price_unusable_total",
                "Ticks where no price could be published.",
                &["pair", "reason"],
            )?,
            // What a custom pricer declared in build(). Names are declared there, so the
            // cardinality is bounded by the config, not by anything a tick sees.
            diagnostic: gauge(
                &r,
                "quote_updater_diagnostic",
                "A number a custom pricer reports every tick, named in its build().",
                &["pair", "kind", "name"],
            )?,
            // Beside `breaker_tripped`, which stays the page ("a lane needs a human", whatever
            // tripped it): this says what did. `source` is the kind, `cause` is guard, panic
            // or external. Never joined into the rule: a join against a series that was
            // never recorded drops the page silently (see the rule's comment in deploy/alerts/).
            trip_source: gauge(
                &r,
                "quote_updater_trip_source",
                "1 while the lane is halted on a trip from this source, for this cause.",
                &["pair", "source", "cause"],
            )?,
            // One per registered guard a pair configured, written when the lane is
            // adopted, NaN once the pair no longer runs it (its stanza removed by the edit
            // that rebuilt the lane, or the lane removed): `breaker_armed` says the same for
            // the built-in deviation guard.
            guards: gauge(
                &r,
                "quote_updater_guards",
                "1 per registered guard kind this pair runs.",
                &["pair", "kind"],
            )?,
            // A task a component's build spawned returned on its own. Not a trip (a panic
            // is), but whatever it fed now ages until the component refuses, and the
            // operator should learn why from something other than the withdraws.
            extension_task_exits: counter(
                &r,
                "quote_updater_extension_task_exits_total",
                "Background tasks a pricer's or guard's build started that returned.",
                &["pair", "kind"],
            )?,
            // Every call into a registered pricer's or guard's code, timed. Nothing can stop
            // a component doing slow CPU work on the requote path; this makes it visible.
            extension_duration: histogram(
                &r,
                "quote_updater_extension_duration_seconds",
                "Duration of a registered pricer's or guard's call (price, check, assess).",
                &["pair", "kind"],
                EXTENSION_BUCKETS,
            )?,
            // Observers are best effort (see observe.rs): an observer that falls behind
            // loses events, and this is the only place that says so.
            observer_dropped: counter(
                &r,
                "quote_updater_observer_dropped_total",
                "Events dropped because the observer's channel was full.",
                &["observer"],
            )?,
            // 1 while the observer's task runs; 0 once it panicked and was removed, which
            // the log says once and this says for as long as the process runs.
            observer_running: gauge(
                &r,
                "quote_updater_observer_running",
                "1 while the observer is running, 0 once it panicked and was removed.",
                &["observer"],
            )?,
            publish_decisions: counter(
                &r,
                "quote_updater_publish_decisions_total",
                "Publish-gate decisions.",
                &["pair", "action"],
            )?,
            published_mid: gauge(
                &r,
                "quote_updater_published_mid",
                "Mid most recently signed and published.",
                pair,
            )?,
            published_delta: gauge(
                &r,
                "quote_updater_published_delta",
                "Delta most recently signed and published.",
                pair,
            )?,
            breaker_armed: gauge(
                &r,
                "quote_updater_breaker_armed",
                "1 if this pair configured a max_deviation.",
                pair,
            )?,
            breaker_tripped: gauge(
                &r,
                "quote_updater_breaker_tripped",
                "1 once the circuit breaker has latched.",
                pair,
            )?,
            breaker_trips: counter(
                &r,
                "quote_updater_breaker_trips_total",
                "Circuit breaker trips.",
                pair,
            )?,
            breaker_rearms: counter(
                &r,
                "quote_updater_breaker_rearms_total",
                "Circuit breaker latches cleared by a config reload.",
                pair,
            )?,
            breaker_deviation_ratio: gauge(
                &r,
                "quote_updater_breaker_deviation_ratio",
                "Last tick-to-tick move as a fraction of the configured limit.",
                pair,
            )?,
            breaker_window_deviation_ratio: gauge(
                &r,
                "quote_updater_breaker_window_deviation_ratio",
                "Last windowed move as a fraction of the configured limit.",
                pair,
            )?,
            rpc_duration: histogram(
                &r,
                "quote_updater_rpc_duration_seconds",
                "RPC call duration.",
                &["pair", "call"],
                RPC_BUCKETS,
            )?,
            rpc_errors: counter(
                &r,
                "quote_updater_rpc_errors_total",
                "Failed RPC calls.",
                &["pair", "call"],
            )?,
            sign_errors: counter(
                &r,
                "quote_updater_sign_errors_total",
                "Update signing failures.",
                pair,
            )?,
            target_blocks: counter(
                &r,
                "quote_updater_target_blocks_total",
                "Target blocks the quote loop worked on.",
                pair,
            )?,
            signer_balance_wei: gauge(
                &r,
                "quote_updater_signer_balance_wei",
                "This pair's signer balance.",
                pair,
            )?,
            signer_runway_updates: gauge(
                &r,
                "quote_updater_signer_runway_updates",
                "Updates the signer balance still covers.",
                pair,
            )?,
            landings: counter(
                &r,
                "quote_updater_landings_total",
                "Per-block landing outcomes.",
                &["pair", "result"],
            )?,
            consecutive_landing_misses: gauge(
                &r,
                "quote_updater_consecutive_landing_misses",
                "Consecutive quoted blocks whose update could not be read back (RPC failure). A block with no swap, and so no update, does not count.",
                pair,
            )?,
            pair_restarts: counter(
                &r,
                "quote_updater_pair_restarts_total",
                "Supervised restarts of this pair's loop.",
                pair,
            )?,
            builder_up: gauge(
                &r,
                "quote_updater_builder_up",
                "1 while this builder connection is live.",
                bp,
            )?,
            builder_ack_latency: histogram(
                &r,
                "quote_updater_builder_ack_latency_seconds",
                "Time from send to ack.",
                bp,
                ACK_BUCKETS,
            )?,
            builder_acks: counter(
                &r,
                "quote_updater_builder_acks_total",
                "Successful acks.",
                bp,
            )?,
            builder_rejections: counter(
                &r,
                "quote_updater_builder_rejections_total",
                "Non-empty error acks.",
                bp,
            )?,
            builder_withdrawals: counter(
                &r,
                "quote_updater_builder_withdrawals_total",
                "Acked empty-tx withdrawals.",
                bp,
            )?,
            builder_send_errors: counter(
                &r,
                "quote_updater_builder_send_errors_total",
                "Send or ack transport failures.",
                bp,
            )?,
            builder_reconnects: counter(
                &r,
                "quote_updater_builder_reconnects_total",
                "Successful redials after a drop.",
                bp,
            )?,
            builder_seq: gauge(
                &r,
                "quote_updater_builder_seq",
                // "Acked", not "sent": this is set in `on_ack`, from the sequence number
                // the response answers, so a frozen value means acks stopped arriving, not
                // that sending stopped — a different cause with a different fix. Moving the
                // recording line to right after `self.seq += 1` in `send` would match a
                // "sent" HELP string instead, but "last seq the builder accepted" is the
                // more useful signal for an operator to read, so the string was the one
                // that had to change. Now that sends do not wait for their acks, the two
                // are further apart than they were, which makes the distinction sharper
                // rather than moot.
                "Latest replacement sequence number acked.",
                bp,
            )?,
            node_pushes: counter(
                &r,
                "quote_updater_node_pushes_total",
                "Node-mode push outcomes.",
                &["pair", "result"],
            )?,
            vault_balance: gauge(
                &r,
                "quote_updater_vault_balance",
                "A vault's balance of a token, in whole tokens, re-read on chain every 12s. \
                 One series per (token, vault) across every configured pair's vault, so the \
                 whole position is visible; sum over vault for a token's total. The per-pair \
                 inventory gauges exist only on volatile pairs.",
                &["token", "vault"],
            )?,
            token_price_usd: gauge(
                &r,
                "quote_updater_token_price_usd",
                "A vault token's price in USD, from CoinGecko by contract address, refreshed \
                 every minute. Absent while no price newer than ten minutes is known.",
                &["token"],
            )?,
            vault_value_usd: gauge(
                &r,
                "quote_updater_vault_value_usd",
                "quote_updater_vault_balance times quote_updater_token_price_usd: a vault's \
                 holding of a token in USD, independent of any pair. Sum over everything for \
                 the vault's total.",
                &["token", "vault"],
            )?,
            registry: r,
        })
    }

    /// Binds one pair's children. Every child is created here, via `with_label_values`,
    /// which registers it in the exposition immediately — so a freshly-built `PairMetrics`
    /// already renders every gauge and counter at their zero value, before anything has
    /// recorded a single sample. A `0` read off one of these gauges is therefore not proof
    /// that a real zero was observed; it may just mean nothing has run yet.
    ///
    /// That distinction is safe to ignore for a counter (a real zero and an unset zero are
    /// the same fact: nothing has happened) but it is not safe for a gauge an alert rule
    /// compares against a threshold, because "never recorded" and "recorded as unhealthy"
    /// render identically. `build_live` covers the two gauges this bites in practice —
    /// `signer_runway_updates` and the three `feed_*` gauges on a pair with no feed —
    /// by setting them to a value no threshold rule can mistake for real data
    /// (`f64::INFINITY` / `f64::NAN`) before anything else runs. See its comment for why.
    /// The vault balance gauge for one token, by its on-chain symbol. Bound by the vault
    /// exporter in `volatile.rs`, not per pair: a token shared by two pairs is one series.
    /// One series per (token, vault): a token can sit in two vaults when two pairs that
    /// hold it fill from different accounts, and a dashboard sums over the vault label.
    pub fn vault_balance(&self, token: &str, vault: &str) -> Gauge {
        self.vault_balance.with_label_values(&[token, vault])
    }

    /// A vault token's USD price, by its on-chain symbol: written by the vault exporter
    /// every `vault::PRICE_REFRESH`, dropped once the price is older than it accepts.
    pub fn token_price_usd(&self, token: &str) -> Gauge {
        self.token_price_usd.with_label_values(&[token])
    }

    /// Forgets a token's USD price series, so a stale price is a gap and not a number.
    pub fn drop_token_price_usd(&self, token: &str) {
        let _ = self.token_price_usd.remove_label_values(&[token]);
    }

    /// A vault's holding of one token in USD: `vault_balance` times `token_price_usd`.
    pub fn vault_value_usd(&self, token: &str, vault: &str) -> Gauge {
        self.vault_value_usd.with_label_values(&[token, vault])
    }

    /// Forgets a (token, vault) USD value series, with its balance or with its price.
    pub fn drop_vault_value_usd(&self, token: &str, vault: &str) {
        let _ = self.vault_value_usd.remove_label_values(&[token, vault]);
    }

    /// Forgets a (token, vault) series, for a vault a pair no longer fills from. A gauge
    /// left behind would keep reporting the last balance read off an account that no
    /// longer counts, and every `sum` over the token would carry it.
    pub fn drop_vault_balance(&self, token: &str, vault: &str) {
        let _ = self.vault_balance.remove_label_values(&[token, vault]);
    }

    /// The counter for a withdraw reason a custom pricer declared, bound once at build.
    pub fn price_unusable_reason(&self, pair: &str, reason: &str) -> IntCounter {
        self.price_unusable.with_label_values(&[pair, reason])
    }

    /// The gauge for one diagnostic a custom pricer declared, bound once at build.
    pub fn diagnostic(&self, pair: &str, kind: &str, name: &str) -> prometheus::Gauge {
        self.diagnostic.with_label_values(&[pair, kind, name])
    }

    /// NaN into every `quote_updater_diagnostic` series of `pair`, whatever kind and name
    /// bound it: [`PairMetrics::quiesce`]'s "no reading" for the one family a pair's
    /// handle does not hold. The children a build binds live in the lane's pricer, out of
    /// the service's reach, and a kind change or a renamed diagnostic leaves the old
    /// build's names behind, so the family itself is read for them. Called when a lane is
    /// stopped for good and again when a lane is adopted, so a rebuild under the same
    /// names starts blank too rather than at the previous build's values, which a pricer
    /// that does not set every one on its first tick would otherwise record into that
    /// quote's terms.
    pub fn quiesce_diagnostics(&self, pair: &str) {
        use prometheus::core::Collector;
        for family in self.diagnostic.collect() {
            for metric in family.get_metric() {
                let label = |name: &str| {
                    metric
                        .get_label()
                        .iter()
                        .find(|label| label.name() == name)
                        .map(|label| label.value())
                };
                if label("pair") == Some(pair)
                    && let (Some(kind), Some(name)) = (label("kind"), label("name"))
                {
                    self.diagnostic
                        .with_label_values(&[pair, kind, name])
                        .set(f64::NAN);
                }
            }
        }
    }

    /// The counter for a component's background tasks that returned, bound at build.
    pub fn extension_task_exits(&self, pair: &str, kind: &str) -> IntCounter {
        self.extension_task_exits.with_label_values(&[pair, kind])
    }

    /// The histogram a registered kind's calls are timed into, bound at build.
    pub fn extension_duration(&self, pair: &str, kind: &str) -> Histogram {
        self.extension_duration.with_label_values(&[pair, kind])
    }

    /// The counter for an observer's dropped events, bound when its task starts.
    pub fn observer_dropped(&self, observer: &str) -> IntCounter {
        self.observer_dropped.with_label_values(&[observer])
    }

    /// Whether an observer's task is running, bound when it starts.
    pub fn observer_running(&self, observer: &str) -> Gauge {
        self.observer_running.with_label_values(&[observer])
    }

    /// The gauge saying a lane is halted on a trip from `source` for `cause`, for a test
    /// reading it back; the run binds it through `PairMetrics::trip_source`.
    #[cfg(test)]
    pub(crate) fn trip_source(&self, pair: &str, source: &str, cause: &str) -> prometheus::Gauge {
        self.trip_source.with_label_values(&[pair, source, cause])
    }

    pub fn for_pair(&self, pair: &str) -> PairMetrics {
        let p = &[pair][..];
        PairMetrics {
            label: pair.to_owned(),
            trip_source: self.trip_source.clone(),
            guards: self.guards.clone(),
            feed_up: self.feed_up.with_label_values(p),
            feed_ticks: self.feed_ticks.with_label_values(p),
            feed_last_tick: self.feed_last_tick.with_label_values(p),
            feed_sample_current: self.feed_sample_current.with_label_values(p),
            feed_mid: self.feed_mid.with_label_values(p),
            feed_delta: self.feed_delta.with_label_values(p),
            feed_sources_fresh: self.feed_sources_fresh.with_label_values(p),
            feed_sources_min: self.feed_sources_min.with_label_values(p),
            price_unusable: UnusableKind::ALL
                .map(|k| self.price_unusable.with_label_values(&[pair, k.as_str()])),
            publish_decisions: PublishAction::ALL.map(|a| {
                self.publish_decisions
                    .with_label_values(&[pair, a.as_str()])
            }),
            published_mid: self.published_mid.with_label_values(p),
            published_delta: self.published_delta.with_label_values(p),
            breaker_armed: self.breaker_armed.with_label_values(p),
            breaker_tripped: self.breaker_tripped.with_label_values(p),
            breaker_trips: self.breaker_trips.with_label_values(p),
            breaker_rearms: self.breaker_rearms.with_label_values(p),
            breaker_deviation_ratio: self.breaker_deviation_ratio.with_label_values(p),
            breaker_window_deviation_ratio: self
                .breaker_window_deviation_ratio
                .with_label_values(p),
            rpc_duration: RpcCall::ALL
                .map(|c| self.rpc_duration.with_label_values(&[pair, c.as_str()])),
            rpc_errors: RpcCall::ALL
                .map(|c| self.rpc_errors.with_label_values(&[pair, c.as_str()])),
            sign_errors: self.sign_errors.with_label_values(p),
            target_blocks: self.target_blocks.with_label_values(p),
            signer_balance_wei: self.signer_balance_wei.with_label_values(p),
            signer_runway_updates: self.signer_runway_updates.with_label_values(p),
            landings: LandingResult::ALL
                .map(|l| self.landings.with_label_values(&[pair, l.as_str()])),
            consecutive_landing_misses: self.consecutive_landing_misses.with_label_values(p),
            pair_restarts: self.pair_restarts.with_label_values(p),
            node_pushes: NodePush::ALL
                .map(|n| self.node_pushes.with_label_values(&[pair, n.as_str()])),
        }
    }

    /// Binds one venue feed's children, for one pair. Same eager-registration caveat as
    /// `for_pair`.
    pub fn for_venue(&self, pair: &str, venue: &str) -> VenueMetrics {
        let pv = &[pair, venue][..];
        VenueMetrics {
            up: self.venue_feed_up.with_label_values(pv),
            ticks: self.venue_feed_ticks.with_label_values(pv),
            rejected: TickReject::ALL.map(|r| {
                self.venue_feed_rejected
                    .with_label_values(&[pair, venue, r.as_str()])
            }),
            last_tick: self.venue_feed_last_tick.with_label_values(pv),
            sample_current: self.venue_feed_sample_current.with_label_values(pv),
            mid: self.venue_feed_mid.with_label_values(pv),
            delta: self.venue_feed_delta.with_label_values(pv),
            source_lag_ms: self.venue_source_lag_ms.with_label_values(pv),
            late: self.venue_late.with_label_values(pv),
            future_stamps: self.venue_future_stamps.with_label_values(pv),
        }
    }

    /// Binds one builder connection's children, for one pair. `run` calls this once per
    /// builder connection and hands the result to `BuilderTask::new`. Same eager-
    /// registration caveat as `for_pair`: every child renders at `0` from this call, not
    /// from its first write.
    pub fn for_builder(&self, pair: &str, builder: &str) -> BuilderMetrics {
        // `["builder", "pair"]`: the reverse of `for_pair`'s order — see the comment on
        // `bp` in `Metrics::new`.
        let b = &[builder, pair][..];
        BuilderMetrics {
            up: self.builder_up.with_label_values(b),
            ack_latency: self.builder_ack_latency.with_label_values(b),
            acks: self.builder_acks.with_label_values(b),
            rejections: self.builder_rejections.with_label_values(b),
            withdrawals: self.builder_withdrawals.with_label_values(b),
            send_errors: self.builder_send_errors.with_label_values(b),
            reconnects: self.builder_reconnects.with_label_values(b),
            seq: self.builder_seq.with_label_values(b),
        }
    }

    /// The Prometheus text exposition of everything registered right now.
    pub fn render(&self) -> Result<String> {
        // Each watched venue's `up` is what its feed task holds right now, read here, when
        // Prometheus asks, rather than whatever was last written; the lateness of its last
        // price too, from the same place.
        for ((pair, venue), connected) in self
            .venue_connections
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .iter()
        {
            self.venue_feed_up
                .with_label_values(&[pair, venue])
                .set(connected.gauge_value());
            self.venue_source_lag_ms
                .with_label_values(&[pair, venue])
                .set(connected.source_lag_ms());
        }
        Ok(TextEncoder::new().encode_to_string(&self.registry.gather())?)
    }

    /// From now on `quote_updater_venue_feed_up{pair, venue}` reads `connected`, replacing
    /// whatever generation of the pair it read before: called when a generation takes over
    /// the pair's series.
    pub fn watch_venue(&self, pair: &str, venue: &str, connected: crate::feed::Connected) {
        self.venue_connections
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert((pair.to_owned(), venue.to_owned()), connected);
    }

    /// Stops reading a venue's flag, for a pair that was removed; its series then keeps the
    /// NaN `quiesce` writes.
    pub fn unwatch_venue(&self, pair: &str, venue: &str) {
        self.venue_connections
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&(pair.to_owned(), venue.to_owned()));
    }

    /// Registers the two process-wide gauges meaningful in *every* mode: which build is
    /// running, and when it started. `run` calls this unconditionally, right after
    /// `Metrics::new()` — unlike `register_health` below, which needs a `Health` that only
    /// exists in builder mode, "which version is this" and "did it just restart" are the
    /// first two questions of any incident regardless of mode, and `--mode node` had been
    /// missing both.
    pub fn register_process(&self, version: &str) -> Result<()> {
        let build = gauge(
            &self.registry,
            "quote_updater_build_info",
            "Always 1; carries the build's version.",
            &["version"],
        )?;
        build.with_label_values(&[version]).set(1.0);

        let start = prometheus::Gauge::new(
            "quote_updater_start_time_seconds",
            "Unix time this process started.",
        )?;
        start.set(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_secs_f64())
                .unwrap_or(0.0),
        );
        self.registry.register(Box::new(start))?;
        Ok(())
    }

    /// Registers the reload counters. Called once, in builder mode, for the same reason
    /// `register_head` is: a series that belongs to one mode is absent in the other rather
    /// than zero.
    pub fn register_reload(&self) -> Result<ReloadMetrics> {
        let reloads = IntCounterVec::new(
            Opts::new(
                "quote_updater_config_reloads_total",
                "Config reloads, by whether they were applied, rejected, or only partly applied.",
            ),
            &["result"],
        )?;
        self.registry.register(Box::new(reloads.clone()))?;

        let generation = Gauge::new(
            "quote_updater_config_generation",
            "Reloads that put every configured pair on the new file; 0 is the config the \
             process started with.",
        )?;
        self.registry.register(Box::new(generation.clone()))?;

        Ok(ReloadMetrics {
            // Pre-bound like every other bounded label set, so `rejected` reads 0 rather
            // than being absent until the first one fails — a rule that has never fired is
            // exactly what `increase(...) > 0` needs to be able to see.
            reloads: Reload::ALL.map(|r| reloads.with_label_values(&[r.as_str()])),
            generation,
        })
    }

    /// Creates and registers the head watcher's three series, handing back the handles it
    /// records into.
    ///
    /// Called once, and only where a watcher exists: `--mode node` pushes `updateState`
    /// straight to the RPC and never starts one, so there these three series are *absent*
    /// rather than sitting at a zero nothing will ever move — the same reason
    /// `register_health`'s counts below are builder-mode only. Absent is the one answer a
    /// threshold rule cannot misread, which is the whole subject of `for_pair`'s doc
    /// comment.
    ///
    /// Unlabelled, deliberately. A `pair` label is a claim about which pair caused the
    /// sample, and one watcher serves all of them; the honest alternatives were a label
    /// value meaning "shared", which would then appear in the `$pair` variable every panel
    /// filters on, or no label at all.
    pub fn register_head(&self) -> Result<HeadMetrics> {
        let poll_duration = Histogram::with_opts(
            HistogramOpts::new(
                "quote_updater_head_poll_duration_seconds",
                // One poll, end to end, not just its eth_blockNumber: the poll that finds a
                // new block also fetches its header, and it is the whole round that every
                // pair waits on before it can quote the next target. Bimodal for that
                // reason — most polls find nothing and pay one call — which is what
                // makes the p95 the number to read rather than the mean.
                "Time for one head poll, including the block fetch when the head moved.",
            )
            .buckets(HEAD_BUCKETS.to_vec()),
        )?;
        self.registry.register(Box::new(poll_duration.clone()))?;

        let poll_errors = IntCounter::new(
            "quote_updater_head_poll_errors_total",
            "Head polls that failed or were abandoned at POLL_TIMEOUT.",
        )?;
        self.registry.register(Box::new(poll_errors.clone()))?;

        let number = Gauge::new(
            "quote_updater_head_number",
            // The series that answers "is the chain advancing, as far as this process can
            // tell". A frozen value here explains every lane at once: no pair can roll to a
            // new target block until this moves, so a stalled watcher shows up as every
            // lane missing its landings, with this as the one thing that says why.
            "Latest block the head watcher has published.",
        )?;
        self.registry.register(Box::new(number.clone()))?;

        Ok(HeadMetrics {
            poll_duration,
            poll_errors,
            number,
        })
    }

    /// Registers the three [`crate::supervisor::Health`] counts as `PullingGauge`s. Called
    /// once, only in builder mode, once `health` exists — node mode has no `Health` and no
    /// `supervise`, so these three gauges have no meaning there.
    ///
    /// `PullingGauge`s read `Health`'s atomics at scrape time, rather than ordinary gauges
    /// something remembers to update on every change. That is not a style choice: this
    /// exact count has already had a drift bug in this codebase — `BackoffGuard` exists
    /// because an early-return path skipped a decrement, and the count "only ever rises
    /// until `all_down` kills a process that is in fact healthy" (see
    /// `supervisor::Health::enter_backoff`). A mirrored copy here would be a second place
    /// that decrement could be forgotten; reading `Health` itself cannot drift from what
    /// `all_down` actually consults, because it consults the same atomics.
    pub fn register_health(&self, health: std::sync::Arc<crate::supervisor::Health>) -> Result<()> {
        for (name, help, pick) in [
            (
                "quote_updater_pairs_halted",
                "Pairs stopped for good on their circuit breaker.",
                Count::Halted,
            ),
            (
                "quote_updater_pairs_in_backoff",
                "Pairs waiting out a restart backoff.",
                Count::InBackoff,
            ),
            (
                "quote_updater_pairs_total",
                "Pairs this process is currently running.",
                Count::Total,
            ),
        ] {
            let h = std::sync::Arc::clone(&health);
            self.registry.register(Box::new(PullingGauge::new(
                name,
                help,
                Box::new(move || {
                    let c = h.census();
                    (match pick {
                        Count::Halted => c.0,
                        Count::InBackoff => c.1,
                        Count::Total => c.2,
                    }) as f64
                }),
            )?))?;
        }
        Ok(())
    }
}

impl PairMetrics {
    /// The gauge saying this pair runs the registered guard `kind`.
    pub(crate) fn guard(&self, kind: &str) -> Gauge {
        self.guards.with_label_values(&[&self.label, kind])
    }

    /// The gauge saying this lane is halted on a trip from `source` for `cause`.
    pub(crate) fn trip_source(&self, source: &str, cause: &str) -> Gauge {
        self.trip_source
            .with_label_values(&[&self.label, source, cause])
    }

    /// Writes the "this pair no longer exists" reading into every gauge that carries a
    /// level, for a lane a reload has removed from `pairs.toml`.
    ///
    /// A `GaugeVec` child is never reaped: once `for_pair` has bound one it is exported at
    /// whatever value was last written to it, for the life of the process. That was
    /// invisible while a pair lived as long as the process did. Now that a reload can take
    /// one away, the last value a removed lane wrote would stand forever — and the worst
    /// case is not a stale dashboard but a permanent page: a lane removed *while halted*
    /// leaves `breaker_tripped` at 1, so `PusherBreakerTripped` fires for a pair that is not
    /// configured any more and cannot be cleared by the reload the alert itself recommends.
    /// `signer_runway_updates` and `consecutive_landing_misses` latch their own rules the
    /// same way.
    ///
    /// NaN rather than 0, and rather than `remove_label_values`:
    ///
    /// - NaN fails every comparison the rules make (`== 1`, `>= 50`, `== 0`,
    ///   `time() - NaN > 30`), which is the same trick `build_live` already uses to say
    ///   "this pair has no feed task" — so this is the existing idiom for "no answer here",
    ///   not a new one. It also defeats the one rule that does not compare:
    ///   `PusherSignerNearlyDry` is a `predict_linear` over `signer_balance_wei`, and a
    ///   single NaN sample makes the whole range return nothing, so that gauge has to be in
    ///   the list below for a removed pair not to latch a page.
    /// - Removing the children would be tidier on the wire but is a footgun: a handle
    ///   already bound by `for_pair` keeps working after its child is removed, writing into
    ///   an object nothing collects. Any code holding one — including a feed task still
    ///   winding down — would silently stop being exported.
    ///
    /// Counters are deliberately left alone. They are read through `rate()`/`increase()`,
    /// and a counter that stops climbing already reads as zero. A custom pricer's
    /// `quote_updater_diagnostic` gauges are not held here: [`Metrics::quiesce_diagnostics`]
    /// blanks those, beside this.
    pub fn quiesce(&self) {
        for gauge in [
            &self.feed_up,
            &self.feed_last_tick,
            &self.feed_sample_current,
            &self.feed_mid,
            &self.feed_delta,
            &self.feed_sources_fresh,
            &self.feed_sources_min,
            &self.published_mid,
            &self.published_delta,
            &self.breaker_armed,
            &self.breaker_tripped,
            &self.breaker_deviation_ratio,
            &self.breaker_window_deviation_ratio,
            &self.signer_balance_wei,
            &self.signer_runway_updates,
            &self.consecutive_landing_misses,
        ] {
            gauge.set(f64::NAN);
        }
    }

    pub fn price_unusable(&self, kind: UnusableKind) -> &IntCounter {
        &self.price_unusable[kind as usize]
    }
    pub fn publish_decisions(&self, action: PublishAction) -> &IntCounter {
        &self.publish_decisions[action as usize]
    }
    pub fn rpc_duration(&self, call: RpcCall) -> &Histogram {
        &self.rpc_duration[call as usize]
    }
    pub fn rpc_errors(&self, call: RpcCall) -> &IntCounter {
        &self.rpc_errors[call as usize]
    }
    pub fn landings(&self, result: LandingResult) -> &IntCounter {
        &self.landings[result as usize]
    }
    pub fn node_pushes(&self, outcome: NodePush) -> &IntCounter {
        &self.node_pushes[outcome as usize]
    }
}

/// Times one RPC call, recording its duration either way and counting a failure.
///
/// A wrapper rather than a stopwatch at each site: `drive` makes five kinds of call across
/// three retry branches, and a hand-rolled timer at each is where one gets forgotten — the
/// branch that then looks fastest is the one that is not measured at all.
pub async fn timed<T, E>(
    m: &PairMetrics,
    call: RpcCall,
    fut: impl std::future::Future<Output = Result<T, E>>,
) -> Result<T, E> {
    let started = std::time::Instant::now();
    let out = fut.await;
    m.rpc_duration(call)
        .observe(started.elapsed().as_secs_f64());
    if out.is_err() {
        m.rpc_errors(call).inc();
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Reads one sample's value straight off the rendered exposition text, keyed by its
    /// exact `metric{labels}` prefix — never through the accessor that wrote it.
    ///
    /// Exists because reading back through `p.some_family(variant).get()` proves nothing
    /// about whether `variant as usize` lands on the right series: the write and that read
    /// would use the identical indexing, so a permuted `ALL` (or a `[idx]` that always
    /// returns the same child) stays self-consistent and the assertion passes either way.
    /// The rendered text carries the label the encoder actually attached, independent of
    /// how it was indexed internally, so it is the only ground truth available here.
    fn sample_value(text: &str, metric: &str, labels: &str) -> f64 {
        let prefix = format!("{metric}{{{labels}}} ");
        text.lines()
            .find(|l| l.starts_with(&prefix))
            .unwrap_or_else(|| panic!("no sample for {prefix:?} in:\n{text}"))
            .rsplit(' ')
            .next()
            .and_then(|v| v.parse().ok())
            .unwrap_or_else(|| panic!("could not parse a value from the {prefix:?} line"))
    }

    #[tokio::test]
    async fn timed_records_a_duration_and_leaves_success_alone() {
        let m = Metrics::new().unwrap();
        let p = m.for_pair("USDC/USDT");
        let out: Result<u8, ()> = timed(&p, RpcCall::GetNonce, async { Ok(7) }).await;
        assert_eq!(out, Ok(7));
        let text = m.render().unwrap();
        assert!(
            text.contains(
                r#"quote_updater_rpc_duration_seconds_count{call="get_nonce",pair="USDC/USDT"} 1"#
            ),
            "{text}"
        );
        assert!(
            !text
                .contains(r#"quote_updater_rpc_errors_total{call="get_nonce",pair="USDC/USDT"} 1"#),
            "{text}"
        );
    }

    #[tokio::test]
    async fn timed_counts_an_error_and_still_times_it() {
        let m = Metrics::new().unwrap();
        let p = m.for_pair("USDC/USDT");
        let out: Result<u8, ()> = timed(&p, RpcCall::GetNonce, async { Err(()) }).await;
        assert!(out.is_err());
        let text = m.render().unwrap();
        assert!(
            text.contains(r#"quote_updater_rpc_errors_total{call="get_nonce",pair="USDC/USDT"} 1"#),
            "{text}"
        );
        assert!(
            text.contains(
                r#"quote_updater_rpc_duration_seconds_count{call="get_nonce",pair="USDC/USDT"} 1"#
            ),
            "{text}"
        );
    }

    #[test]
    fn a_pair_handle_records_against_its_own_label() {
        let m = Metrics::new().unwrap();
        let usdc = m.for_pair("USDC/USDT");
        let weth = m.for_pair("WETH/USDC");
        usdc.feed_ticks.inc();
        usdc.feed_ticks.inc();
        weth.feed_ticks.inc();

        let out = m.render().unwrap();
        assert!(
            out.contains(r#"quote_updater_feed_ticks_total{pair="USDC/USDT"} 2"#),
            "{out}"
        );
        assert!(
            out.contains(r#"quote_updater_feed_ticks_total{pair="WETH/USDC"} 1"#),
            "{out}"
        );
    }

    #[test]
    fn every_bounded_label_variant_is_pre_bound() {
        let m = Metrics::new().unwrap();
        let p = m.for_pair("USDC/USDT");
        // Indexing by `variant as usize` is only sound if every variant has its OWN child —
        // a single shared increment (or asserting mere label presence, as this test used
        // to) cannot tell "each variant has a distinct child" apart from "every variant
        // aliases the same child", because a pre-bound-but-eagerly-registered family renders
        // every label regardless of which one (if any) actually got the write. Distinct
        // counts per variant make every position observable; `sample_value` reads them back
        // from the rendered text instead of through the accessor that wrote them, so a
        // permuted `ALL` or a broken `as usize` cast cannot mark its own homework — see
        // `sample_value`'s doc comment.
        let calls = [
            (RpcCall::GetNonce, 2u64),
            (RpcCall::GetBalance, 4),
            (RpcCall::EthCall, 5),
        ];
        for (call, n) in calls {
            for _ in 0..n {
                p.rpc_errors(call).inc();
                p.rpc_duration(call).observe(0.01);
            }
        }
        let actions = [
            (PublishAction::Publish, 1u64),
            (PublishAction::Withdraw, 2),
            (PublishAction::Halt, 3),
            (PublishAction::Nothing, 4),
        ];
        for (action, n) in actions {
            for _ in 0..n {
                p.publish_decisions(action).inc();
            }
        }
        let results = [
            (LandingResult::Landed, 1u64),
            (LandingResult::Missed, 2),
            (LandingResult::Unknown, 3),
            (LandingResult::NotQuoted, 4),
        ];
        for (result, n) in results {
            for _ in 0..n {
                p.landings(result).inc();
            }
        }

        let text = m.render().unwrap();
        for (call, n) in calls {
            let labels = format!(r#"call="{}",pair="USDC/USDT""#, call.as_str());
            assert_eq!(
                sample_value(&text, "quote_updater_rpc_errors_total", &labels),
                n as f64,
                "rpc_errors[{}]:\n{text}",
                call.as_str()
            );
            assert_eq!(
                sample_value(&text, "quote_updater_rpc_duration_seconds_count", &labels),
                n as f64,
                "rpc_duration[{}]:\n{text}",
                call.as_str()
            );
        }
        for (action, n) in actions {
            let labels = format!(r#"action="{}",pair="USDC/USDT""#, action.as_str());
            assert_eq!(
                sample_value(&text, "quote_updater_publish_decisions_total", &labels),
                n as f64,
                "publish_decisions[{}]:\n{text}",
                action.as_str()
            );
        }
        for (result, n) in results {
            let labels = format!(r#"pair="USDC/USDT",result="{}""#, result.as_str());
            assert_eq!(
                sample_value(&text, "quote_updater_landings_total", &labels),
                n as f64,
                "landings[{}]:\n{text}",
                result.as_str()
            );
        }
    }

    #[test]
    fn feed_rejected_reasons_are_pre_bound() {
        let m = Metrics::new().unwrap();
        let p = m.for_venue("USDC/USDT", "binance");
        // Same design as `every_bounded_label_variant_is_pre_bound`: distinct counts per
        // variant, read back from the rendered text rather than through `feed_rejected`
        // itself, so a permuted `ALL` or a broken `as usize` cast cannot mark its own
        // homework.
        let reasons = [
            (TickReject::OneSided, 1u64),
            (TickReject::Crossed, 2),
            (TickReject::Malformed, 3),
            (TickReject::Overflow, 4),
        ];
        for (reason, n) in reasons {
            for _ in 0..n {
                p.rejected(reason).inc();
            }
        }
        let text = m.render().unwrap();
        for (reason, n) in reasons {
            let labels = format!(
                r#"pair="USDC/USDT",reason="{}",venue="binance""#,
                reason.as_str()
            );
            assert_eq!(
                sample_value(&text, "quote_updater_feed_rejected_ticks_total", &labels),
                n as f64,
                "feed_rejected[{}]:\n{text}",
                reason.as_str()
            );
        }
    }

    #[test]
    fn price_unusable_kinds_are_pre_bound() {
        let m = Metrics::new().unwrap();
        let p = m.for_pair("USDC/USDT");
        // Same design again. `UnusableKind` is declared in `update.rs`, but its accessor,
        // its pre-bound array, and `sample_value` all live in this module, so the test does
        // too rather than splitting the write/read/ground-truth triad across two files.
        //
        // This one is more than a consistency exercise: `price_unusable(OutOfBand)` feeds
        // the series `price_unusable_total{reason="out_of_band"}` that the
        // `PusherMidOutOfBand` alert watches. `quoting.rs`'s own test reads back through
        // this same accessor, so it cannot see a permuted `ALL` (write and read share the
        // indexing); only a test that checks the rendered label independently, like this
        // one, can prove the alert's series would actually move.
        // Every kind, each with its own count, so a permuted `ALL` shows as a wrong number.
        let kinds = [
            (UnusableKind::NoSample, 1u64),
            (UnusableKind::Stale, 2),
            (UnusableKind::OutOfBand, 3),
            (UnusableKind::DeltaOverflow, 4),
            (UnusableKind::Panic, 5),
        ];
        assert_eq!(kinds.len(), UnusableKind::ALL.len());
        for (kind, n) in kinds {
            for _ in 0..n {
                p.price_unusable(kind).inc();
            }
        }
        let text = m.render().unwrap();
        for (kind, n) in kinds {
            let labels = format!(r#"pair="USDC/USDT",reason="{}""#, kind.as_str());
            assert_eq!(
                sample_value(&text, "quote_updater_price_unusable_total", &labels),
                n as f64,
                "price_unusable[{}]:\n{text}",
                kind.as_str()
            );
        }
    }

    #[test]
    fn a_builder_handle_carries_both_labels() {
        let m = Metrics::new().unwrap();
        m.for_builder("USDC/USDT", "titan-eu").acks.inc();
        let out = m.render().unwrap();
        assert!(
            out.contains(r#"{builder="titan-eu",pair="USDC/USDT"} 1"#),
            "{out}"
        );
    }

    #[test]
    fn an_unbound_family_renders_nothing() {
        // No `for_pair` call at all: nothing has bound a child, so the family has no series
        // to render. This is the only sense in which a gauge "starts absent" — once a
        // handle exists, every child in it is already present at 0 (see the next test), not
        // absent.
        let m = Metrics::new().unwrap();
        assert!(!m.render().unwrap().contains("quote_updater_feed_mid"));
    }

    /// Documents the property C1 turned out to hinge on: `for_pair` registers every child
    /// eagerly via `with_label_values`, so the child exists — and reads `0` — from the
    /// moment the handle is built, not from the moment something calls `.set`. The previous
    /// version of this test set `for_pair(...).feed_mid` and `.set(...)` on the same line
    /// and then only checked that the family's *name* appeared in the exposition, which
    /// `for_pair` alone already satisfies — so it passed with the `.set` call deleted, and
    /// its name claimed the opposite of what actually happens. Reading the *value* with
    /// `sample_value` on both sides (not `contains` on the name) is what makes "present but
    /// 0" and "changed to the set value" two separate, checked facts instead of one
    /// unchecked one.
    #[test]
    fn for_pair_alone_exposes_the_child_at_zero_and_set_changes_the_value() {
        let m = Metrics::new().unwrap();
        let p = m.for_pair("USDC/USDT");
        let labels = r#"pair="USDC/USDT""#;
        assert_eq!(
            sample_value(&m.render().unwrap(), "quote_updater_feed_mid", labels),
            0.0,
            "a freshly-bound child must already read 0, not be absent"
        );
        p.feed_mid.set(0.9998);
        assert_eq!(
            sample_value(&m.render().unwrap(), "quote_updater_feed_mid", labels),
            0.9998,
            "set() must change the value read back"
        );
    }

    #[test]
    fn vault_balance_is_one_series_per_token_and_vault() {
        let m = Metrics::new().unwrap();
        m.vault_balance("USDT", "0xa").set(293.27);
        m.vault_balance("USDC", "0xa").set(10.0);
        m.vault_balance("USDC", "0xb").set(20.0);
        let out = m.render().unwrap();
        assert_eq!(
            sample_value(
                &out,
                "quote_updater_vault_balance",
                r#"token="USDT",vault="0xa""#
            ),
            293.27
        );
        assert_eq!(
            sample_value(
                &out,
                "quote_updater_vault_balance",
                r#"token="USDC",vault="0xb""#
            ),
            20.0
        );
        assert!(
            !out.contains(r#"token="WETH""#),
            "a token nobody bound must not render"
        );
        // A pair moved off vault 0xa: its USDC series goes, the one on 0xb stays.
        m.drop_vault_balance("USDC", "0xa");
        let out = m.render().unwrap();
        assert!(!out.contains(r#"token="USDC",vault="0xa""#), "{out}");
        assert_eq!(
            sample_value(
                &out,
                "quote_updater_vault_balance",
                r#"token="USDC",vault="0xb""#
            ),
            20.0
        );
    }

    #[test]
    fn health_gauges_read_through_to_the_live_counts() {
        let health = std::sync::Arc::new(crate::supervisor::Health::new(3));
        let m = Metrics::new().unwrap();
        m.register_health(std::sync::Arc::clone(&health)).unwrap();

        assert!(m.render().unwrap().contains("quote_updater_pairs_halted 0"));
        health.halt();
        // No mirroring step: the gauge is read from the atomics at scrape time, so it cannot
        // drift from the count `all_down` actually consults.
        assert!(m.render().unwrap().contains("quote_updater_pairs_halted 1"));
        assert!(m.render().unwrap().contains("quote_updater_pairs_total 3"));

        let guard = health.enter_backoff();
        assert!(
            m.render()
                .unwrap()
                .contains("quote_updater_pairs_in_backoff 1")
        );
        drop(guard);
        assert!(
            m.render()
                .unwrap()
                .contains("quote_updater_pairs_in_backoff 0")
        );
    }

    /// The head watcher's series, and the property that makes them safe to add: they exist
    /// only where a watcher does. `--mode node` never calls `register_head`, so all three
    /// are absent there rather than reading a zero — which for `head_number` would say "the
    /// chain is at block 0", the exact class of never-recorded-reads-as-real-value the
    /// `for_pair` doc comment and four of the alert rules are about.
    #[test]
    fn head_series_exist_only_once_a_watcher_registers_them() {
        let m = Metrics::new().unwrap();
        let names = [
            "quote_updater_head_poll_duration_seconds",
            "quote_updater_head_poll_errors_total",
            "quote_updater_head_number",
        ];
        for name in names {
            assert!(
                !m.render().unwrap().contains(name),
                "{name} must not exist before a watcher does"
            );
        }

        let head = m.register_head().unwrap();
        head.poll_duration.observe(0.03);
        head.poll_errors.inc();
        head.number.set(21_000_000.0);
        let text = m.render().unwrap();
        for name in names {
            assert!(text.contains(name), "{name} missing after register_head");
        }
        // Read off the rendered line rather than through `sample_value`, which formats a
        // `name{labels}` prefix: these three carry no labels at all, which is the point of
        // them, so they render as a bare `name value`.
        assert!(
            text.contains("\nquote_updater_head_number 21000000\n"),
            "{text}"
        );
        assert!(
            text.contains("\nquote_updater_head_poll_errors_total 1\n"),
            "{text}"
        );
        // The bucket at POLL_TIMEOUT, which separates a poll abandoned at the timeout from
        // one that merely dawdled.
        assert!(
            text.contains(r#"quote_updater_head_poll_duration_seconds_bucket{le="2"}"#),
            "the POLL_TIMEOUT bucket is what an abandoned number fetch lands in: {text}"
        );
        // And a ceiling at the worst case a whole poll can cost: POLL_TIMEOUT for the number
        // plus RPC_TIMEOUT for the block fetch behind it. Below that, histogram_quantile
        // cannot interpolate past the highest finite bound, so the panel's p95 would flatten
        // at the ceiling exactly when the poll got slow enough to matter.
        assert_eq!(
            crate::head::POLL_TIMEOUT + crate::RPC_TIMEOUT,
            std::time::Duration::from_secs(5)
        );
        assert!(
            text.contains(r#"quote_updater_head_poll_duration_seconds_bucket{le="5"}"#),
            "the head histogram must reach the slowest poll it can observe: {text}"
        );
        // Same rule for the per-pair calls, whose ceiling is `bounded`'s own bound.
        let pair_text = {
            let m = Metrics::new().unwrap();
            m.for_pair("P")
                .rpc_duration(RpcCall::GetNonce)
                .observe(0.001);
            m.render().unwrap()
        };
        assert!(
            pair_text.contains(r#"le="3""#),
            "rpc_duration must reach RPC_TIMEOUT: {pair_text}"
        );
    }

    #[test]
    fn node_push_outcomes_are_pre_bound() {
        let m = Metrics::new().unwrap();
        let p = m.for_pair("USDC/USDT");
        // Distinct counts per variant, read back below from the rendered label text rather
        // than through `node_pushes` itself — see `every_bounded_label_variant_is_pre_bound`
        // and `sample_value`'s doc comment for why: the same accessor on both ends of the
        // check would validate `variant as usize` indexing against itself.
        let counts = [
            (NodePush::Landed, 1u64),
            (NodePush::Failed, 2),
            (NodePush::Reverted, 3),
            (NodePush::Mismatch, 4),
        ];
        for (outcome, n) in counts {
            for _ in 0..n {
                p.node_pushes(outcome).inc();
            }
        }
        let text = m.render().unwrap();
        for (outcome, n) in counts {
            let labels = format!(r#"pair="USDC/USDT",result="{}""#, outcome.as_str());
            assert_eq!(
                sample_value(&text, "quote_updater_node_pushes_total", &labels),
                n as f64,
                "node_pushes[{}]:\n{text}",
                outcome.as_str()
            );
        }
    }

    #[test]
    fn build_info_is_a_constant_one_carrying_the_version() {
        let m = Metrics::new().unwrap();
        m.register_process("0.1.0").unwrap();
        assert!(
            m.render()
                .unwrap()
                .contains(r#"quote_updater_build_info{version="0.1.0"} 1"#),
            "{}",
            m.render().unwrap()
        );
    }

    /// `start_time_seconds` must carry a real Unix timestamp, not a placeholder left at
    /// zero — that is the difference between "process started at this moment" and a value
    /// nobody wired up.
    #[test]
    fn start_time_seconds_is_a_real_unix_timestamp() {
        let m = Metrics::new().unwrap();
        m.register_process("0.1.0").unwrap();
        let text = m.render().unwrap();
        let line = text
            .lines()
            .find(|l| l.starts_with("quote_updater_start_time_seconds "))
            .unwrap_or_else(|| panic!("no start_time_seconds sample in:\n{text}"));
        let value: f64 = line
            .rsplit(' ')
            .next()
            .and_then(|v| v.parse().ok())
            .unwrap_or_else(|| panic!("could not parse start_time_seconds value from {line:?}"));
        // Any real Unix time is comfortably past this; a value stuck at the zero-default
        // would fail it.
        assert!(
            value > 1_700_000_000.0,
            "start_time_seconds ({value}) does not look like a real Unix timestamp"
        );
    }
}
