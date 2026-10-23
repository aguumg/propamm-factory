//! Builds, stamps and signs the updateState call both send paths (a direct RPC push and a
//! builder maker quote) publish, so the two only differ in how they get the signed bytes
//! to the chain.

use std::{
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use ethrex_common::{
    Address, U256,
    types::{BlockHeader, EIP1559Transaction, TxType},
};
use ethrex_l2_common::calldata::Value;
use ethrex_l2_rpc::signer::{Signable, Signer};
use ethrex_l2_sdk::{build_generic_tx, calldata::encode_calldata};
use ethrex_rlp::encode::RLPEncode;
use ethrex_rpc::clients::eth::{EthClient, Overrides};
use eyre::{Result, WrapErr, ensure, eyre};

use crate::{
    config::MidBand,
    feed::PriceSample,
    head::Head,
    pricing::{Diagnostics, Market, PairShape, Pricer, Refusal, TickCtx},
};
const UPDATE_STATE_SIG: &str = "updateState(address,uint256,uint32,uint256[])";

/// Updates are stamped one mainnet block time ahead of the latest block.
pub const BLOCK_TIME_SECS: u64 = 12;

/// Explicit gas limit: estimation simulates against the parent block, whose timestamp
/// is behind the update's, so the registry's freshness check would make estimation revert.
pub const UPDATE_GAS_LIMIT: u64 = 120_000;

/// How long past an update's timestamp its slot may stay empty before it is given up on.
///
/// The block for a slot is proposed at the slot's own timestamp and reaches an RPC
/// within a second or two; one that has not shown up by the attestation deadline (four
/// seconds in) is one the network is already voting against. Past this, the next block
/// will carry the *following* slot's timestamp, and the registry accepts an update only
/// in a block stamped exactly as the update is — so a quote left standing for the missed
/// slot can never land, and must be re-stamped for the slot still ahead. Read off the
/// wall clock, like node mode's own next-block check; a clock stepped far ahead would
/// re-stamp too early, which is why the slack is not smaller.
pub const MISSED_SLOT_SLACK: Duration = Duration::from_secs(4);

/// A Binance price older than this (on either the monotonic or the wall clock) is not
/// pushed: a dead feed must halt updates, not freeze the published value. The feed keeps
/// a quiet-but-alive connection fresh (it pings through quiet spells), so only a feed
/// that is actually gone ages past this.
pub const MAX_PRICE_AGE: Duration = Duration::from_secs(30);

/// Where each update is published: the registry it is sent to, and the target, lane and
/// chain it is published for. Replaces threading `&Args` into the signing path, which only
/// ever read these four fields off it.
pub struct UpdateParams {
    pub registry: Address,
    pub target: Address,
    pub lane: U256,
    pub chain_id: u64,
}

/// What each update derives from the latest (parent) block: the timestamp it is stamped
/// with, one block time ahead, and the parent's base fee its max fee is priced from.
#[derive(Clone, Copy)]
pub struct UpdateStamp {
    pub ts: u32,
    pub parent_base_fee: u64,
}

impl UpdateStamp {
    pub fn from_parent(parent: &BlockHeader) -> Result<Self> {
        Self::derive(parent.timestamp, parent.base_fee_per_gas)
    }

    /// The same, from the head watcher's view of the latest block.
    pub fn from_head(head: &Head) -> Result<Self> {
        Self::derive(head.timestamp, head.base_fee_per_gas)
    }

    fn derive(timestamp: u64, base_fee_per_gas: Option<u64>) -> Result<Self> {
        Ok(UpdateStamp {
            ts: u32::try_from(timestamp + BLOCK_TIME_SECS)
                .wrap_err("block timestamp overflows u32")?,
            parent_base_fee: base_fee_per_gas
                .ok_or_else(|| eyre!("latest block has no base fee; pre-London chain?"))?,
        })
    }

    /// The wall-clock second from which the slot this stamp is for counts as missed:
    /// its timestamp plus [`MISSED_SLOT_SLACK`].
    pub fn missed_at(&self) -> u64 {
        u64::from(self.ts) + MISSED_SLOT_SLACK.as_secs()
    }

    /// The stamp for the same target block once the wall clock says its slot was missed:
    /// the first slot still ahead of `now_secs`, priced off the same parent (the parent
    /// has not changed; only the slot the next block will land in has). A stamp whose
    /// slot is not yet missed is returned as it is.
    ///
    /// Steps over every slot `now_secs` has already passed, so a long outage advances
    /// straight to the slot that can still carry the update rather than through each
    /// missed one a block time at a time.
    pub fn after_missed_slots(&self, now_secs: u64) -> Result<Self> {
        let mut next = *self;
        while now_secs >= next.missed_at() {
            next.ts = u32::try_from(u64::from(next.ts) + BLOCK_TIME_SECS)
                .wrap_err("block timestamp overflows u32")?;
        }
        Ok(next)
    }
}

/// Seconds since the epoch on the wall clock.
pub fn wall_clock_secs() -> u64 {
    (crate::poll::wall_clock_ms() / 1_000) as u64
}

/// max_fee_per_gas for a transaction that must land in the very next block: twice the
/// parent's base fee, plus the priority fee. The next block's base fee rises at most
/// 12.5% over the parent's, so 2x always covers it, and EIP-1559 refunds the unused
/// headroom (only base fee + tip is paid), so the margin costs nothing. The SDK default
/// (eth_gasPrice = parent base fee + a tip estimate) leaves only the tip as headroom: a
/// rising base fee strands the update in the pool, and late inclusion fails the
/// registry's exact-timestamp match.
pub fn next_block_max_fee(parent_base_fee: u64, priority_fee: u64) -> u64 {
    parent_base_fee
        .saturating_mul(2)
        .saturating_add(priority_fee)
}

pub fn ensure_fits_216_bits(delta: U256) -> Result<()> {
    ensure!(
        delta >> 216 == U256::zero(),
        "delta {delta} does not fit in 216 bits: slot 0 shares its storage word with the timestamp and slot count"
    );
    Ok(())
}

/// Why no price can be published this tick. A closed set, because it is a metric label:
/// `out_of_band` is a safety event and `stale` is a feed event, and an operator paging on
/// one must not be woken by the other.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnusableKind {
    NoSample,
    Stale,
    OutOfBand,
    DeltaOverflow,
    /// The pricer panicked on this tick. The tick is withdrawn under this and the lane is
    /// tripped, so it is counted at most once per lane. Appended last: `ALL`'s order is
    /// the metric array's.
    Panic,
}

impl UnusableKind {
    /// Declaration order is load-bearing: `metrics::PairMetrics` indexes its pre-bound
    /// children by `kind as usize`.
    pub const ALL: [UnusableKind; 5] = [
        UnusableKind::NoSample,
        UnusableKind::Stale,
        UnusableKind::OutOfBand,
        UnusableKind::DeltaOverflow,
        UnusableKind::Panic,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            UnusableKind::NoSample => "no_sample",
            UnusableKind::Stale => "stale",
            UnusableKind::OutOfBand => "out_of_band",
            UnusableKind::DeltaOverflow => "delta_overflow",
            UnusableKind::Panic => "panic",
        }
    }
}

/// An unpublishable price: a machine-readable kind for the metric, and the exact operator
/// message that was previously an `eyre` error.
///
/// The message is carried rather than rebuilt from the kind because it is a documented
/// interface: `drive` renders it into the withdrawn reason on the block summary line, and
/// the README prints examples of that line. A test asserts each variant's text.
#[derive(Debug, Clone)]
pub struct Unusable {
    reason: Reason,
    message: String,
}

/// A core reason, pre-bound per pair in `PairMetrics`, or one a pricer declared in
/// `build()`, whose counter was bound then, so the hot path counts without a lookup.
#[derive(Debug, Clone)]
enum Reason {
    Core(UnusableKind),
    Declared(prometheus::IntCounter),
}

impl Unusable {
    pub fn new(kind: UnusableKind, message: String) -> Self {
        Self {
            reason: Reason::Core(kind),
            message,
        }
    }

    /// A refusal under a reason a pricer declared, counted on the counter bound for it.
    pub(crate) fn declared(counter: prometheus::IntCounter, message: String) -> Self {
        Self {
            reason: Reason::Declared(counter),
            message,
        }
    }

    /// The core reason, or `None` for one a pricer declared. Tests read it; production
    /// code counts through [`Self::count`] instead.
    #[cfg(test)]
    pub(crate) fn kind(&self) -> Option<UnusableKind> {
        match self.reason {
            Reason::Core(kind) => Some(kind),
            Reason::Declared(_) => None,
        }
    }

    /// The core's refusal as a quote guard sees it: the same kind and the same text, so a
    /// guard judging a tick the freshness gate, the backstop or the band refused reads the
    /// reason the header will carry. Only core reasons reach here; a declared one is the
    /// pricer's own `Refusal` and never comes back through this.
    pub(crate) fn as_refusal(&self) -> Refusal {
        match &self.reason {
            Reason::Core(kind) => Refusal::core(*kind, self.message.clone()),
            Reason::Declared(_) => {
                debug_assert!(
                    false,
                    "a declared refusal is the pricer's own and never comes back"
                );
                Refusal::core(UnusableKind::OutOfBand, self.message.clone())
            }
        }
    }

    /// Counts this refusal in `price_unusable_total{reason}`: read off the typed error
    /// before anything folds it into an `eyre::Report`, where the reason is lost.
    pub(crate) fn count(&self, metrics: &crate::metrics::PairMetrics) {
        match &self.reason {
            Reason::Core(kind) => metrics.price_unusable(*kind).inc(),
            Reason::Declared(counter) => counter.inc(),
        }
    }
}

impl std::fmt::Display for Unusable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for Unusable {}

/// A lane's live pricing: the pricer every supervised attempt of the lane shares, the
/// market it reads, and what its build declared.
///
/// Cloned for every attempt (`Service::spawn`) and shares the pricer rather than rebuilding
/// it, so a pricer's state (the volatile model's inventory reader, a custom model's
/// background inputs) survives a quote-loop restart. One loop runs per lane, so the lock
/// is never contended.
#[derive(Clone)]
pub struct ValueSource {
    pricer: Arc<Mutex<Box<dyn Pricer>>>,
    /// The registered name of the kind that prices the lane: what a trip, a panic and the
    /// diagnostics label the pricer by.
    kind: &'static str,
    /// The pair's reference market: `None` for a pair with no `symbol`/`sources`.
    market: Option<tokio::sync::watch::Receiver<Option<PriceSample>>>,
    pair: Arc<PairShape>,
    bound: Arc<Bound>,
    /// The lane's latch, for a quote guard's `Halt`.
    latch: crate::guard::Latch,
    /// The pair's quote guards, shared across attempts like the pricer, for the same
    /// reason: their state outlives a quote-loop restart.
    guards: Arc<Mutex<Vec<crate::guard::BoundQuoteGuard>>>,
    /// Where the quote loop reports each block's read-back, for the pricer's
    /// `BuildCtx::landings` feed. A pricer that never asked for it has no reader, and the
    /// report goes nowhere.
    landings: tokio::sync::watch::Sender<Option<crate::pricing::LandingReport>>,
}

/// What a pricer's build declared, bound to its metric series, and the tasks it started.
/// Shared by every clone of the lane's `ValueSource`, and dropped with the last one, which
/// stops those tasks.
#[derive(Default)]
pub(crate) struct Bound {
    pub(crate) diagnostics: Vec<prometheus::Gauge>,
    /// The declared names behind `diagnostics`, in order: what a quote guard's build reads
    /// the pricer's by.
    pub(crate) names: Vec<String>,
    pub(crate) refusals: Vec<prometheus::IntCounter>,
    /// Where this kind's calls are timed; `None` where nothing was bound.
    pub(crate) duration: Option<prometheus::Histogram>,
    pub(crate) _tasks: crate::tasks::Tasks,
}

impl Bound {
    /// Nothing bound: no diagnostics, no refusals, no tasks.
    pub(crate) fn none() -> Arc<Bound> {
        Arc::new(Bound::default())
    }

    /// A custom pricer's: one gauge per diagnostic and one counter per refusal its build
    /// declared, bound now so the hot path never looks a label up, and the tasks it started.
    pub(crate) fn for_build(
        ctx: &crate::pricing::BuildCtx,
        metrics: &crate::metrics::Metrics,
        kind: &str,
        tasks: crate::tasks::Tasks,
    ) -> Arc<Bound> {
        let label = &ctx.pair().label;
        Arc::new(Bound {
            diagnostics: ctx
                .diagnostic_names()
                .iter()
                .map(|name| metrics.diagnostic(label, kind, name))
                .collect(),
            names: ctx.diagnostic_names().to_vec(),
            duration: Some(metrics.extension_duration(label, kind)),
            refusals: ctx
                .refusal_names()
                .iter()
                .map(|name| metrics.price_unusable_reason(label, name))
                .collect(),
            _tasks: tasks,
        })
    }

    /// Runs one call into this kind's code, timed into its histogram when it has one.
    pub(crate) fn timed<T>(&self, call: impl FnOnce() -> T) -> T {
        let started = Instant::now();
        let result = call();
        if let Some(duration) = &self.duration {
            duration.observe(started.elapsed().as_secs_f64());
        }
        result
    }

    /// Writes this tick's declared diagnostics to their gauges; an unset one keeps its last
    /// value.
    pub(crate) fn publish(&self, out: &Diagnostics) {
        for (gauge, value) in self.diagnostics.iter().zip(out.values()) {
            if let Some(value) = value {
                gauge.set(*value);
            }
        }
    }

    /// The refusal as the quote loop handles it: a core kind, or a declared reason with
    /// the counter its build bound.
    fn unusable(&self, refusal: Refusal) -> Unusable {
        let message = refusal.message().to_owned();
        match (refusal.core_kind(), refusal.declared_index()) {
            (Some(kind), _) => Unusable::new(kind, message),
            (None, Some(index)) => match self.refusals.get(index) {
                Some(counter) => Unusable::declared(counter.clone(), message),
                // Only a handle from another lane's build gets here. The quote is still
                // withdrawn, which is what matters; it is counted as out of band.
                None => Unusable::new(UnusableKind::OutOfBand, message),
            },
            (None, None) => Unusable::new(UnusableKind::OutOfBand, message),
        }
    }
}

/// A panic inside `price()` poisons the lock; the next tick must still price, the way
/// `breaker::lock` recovers its own.
fn lock_pricer(pricer: &Mutex<Box<dyn Pricer>>) -> std::sync::MutexGuard<'_, Box<dyn Pricer>> {
    pricer
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// The feed's latest sample, or why it cannot be published from.
fn fresh_sample(
    rx: &tokio::sync::watch::Receiver<Option<PriceSample>>,
) -> Result<PriceSample, Unusable> {
    let sample = (*rx.borrow()).ok_or_else(|| {
        Unusable::new(
            UnusableKind::NoSample,
            "no usable price from the feed".to_owned(),
        )
    })?;
    let age = sample.age();
    if age > MAX_PRICE_AGE {
        return Err(Unusable::new(
            UnusableKind::Stale,
            format!("latest feed price is {age:.0?} old; refusing to push a stale value"),
        ));
    }
    Ok(sample)
}

/// What `current` decided: the `(delta, mid)` to publish, the feed mid it started from
/// (equal to `mid` unless a skew moved it), and the volatile working when there was one.
/// All in lane orientation, the scale the chain gets.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Priced {
    pub delta: U256,
    pub mid: U256,
    pub feed_mid: U256,
}

impl ValueSource {
    pub(crate) fn new(
        pricer: Box<dyn Pricer>,
        kind: &'static str,
        market: Option<tokio::sync::watch::Receiver<Option<PriceSample>>>,
        pair: Arc<PairShape>,
        bound: Arc<Bound>,
    ) -> Self {
        ValueSource {
            pricer: Arc::new(Mutex::new(pricer)),
            kind,
            market,
            pair,
            bound,
            latch: crate::guard::Latch::new(),
            guards: Arc::new(Mutex::new(Vec::new())),
            landings: tokio::sync::watch::channel(None).0,
        }
    }

    /// The sender of the build context the pricer took its landing feed from, so the
    /// loop's reports reach that feed.
    pub(crate) fn with_landings(
        mut self,
        landings: tokio::sync::watch::Sender<Option<crate::pricing::LandingReport>>,
    ) -> Self {
        self.landings = landings;
        self
    }

    /// One block's read-back, for the pricer's feed. Every clone of the source shares the
    /// channel, so whichever attempt of the quote loop reports, the feed sees it.
    pub(crate) fn report_landing(&self, report: crate::pricing::LandingReport) {
        self.landings.send_replace(Some(report));
    }

    /// The declared diagnostics as last published, for the quote's recorded terms and the
    /// block's event: what the gauges show, so an unset one carries its last value, as on
    /// the dashboard.
    pub(crate) fn diagnostics_now(&self) -> Vec<(String, f64)> {
        self.bound
            .names
            .iter()
            .zip(self.bound.diagnostics.iter())
            .map(|(name, gauge)| (name.clone(), gauge.get()))
            .collect()
    }

    /// The lane's quote guards and its latch: the one its composite and quote loop share,
    /// so a `Halt` here halts the loop, and the one that records the transition.
    pub(crate) fn with_guards(
        mut self,
        latch: crate::guard::Latch,
        guards: Vec<crate::guard::BoundQuoteGuard>,
    ) -> Self {
        self.latch = latch;
        self.guards = Arc::new(Mutex::new(guards));
        self
    }

    /// The lane's latch alone, for a source built before its guards are: a pricer's panic
    /// trips the lane whether or not a guard was ever added.
    pub(crate) fn with_latch(mut self, latch: crate::guard::Latch) -> Self {
        self.latch = latch;
        self
    }

    /// The diagnostics the pricer declared, by name, for a quote guard's build to read.
    pub(crate) fn diagnostic_names(&self) -> &[String] {
        &self.bound.names
    }

    /// The registered name of the kind that prices the lane, for a trip or a panic to name
    /// it by.
    fn kind_name(&self) -> &'static str {
        self.kind
    }

    /// Trips the lane, if nothing has yet, and records the transition once, as the
    /// composite does for its guards (`composite::Composite::trip`); the quote loop only
    /// runs once the lane owns its series, so nothing is gated here.
    fn trip(
        &self,
        source: &'static str,
        cause: crate::guard::Cause,
        reason: crate::guard::TripReason,
    ) {
        // The latch records the transition itself; a second trip changes nothing.
        self.latch.trip(source, cause, reason);
    }

    /// The quote guards' judgement of this tick, on the pricer's output or a refusal, the
    /// pricer's or the core's.
    /// Every guard runs and the most severe answer wins: a `Halt` trips the latch (the gate
    /// reads it next, so the tick is handed back as it was); a `Withdraw` on an `Ok` is the
    /// withdraw, counted under the guard's reason; on a refusal, `Allow` changes nothing and
    /// a `Withdraw` does not replace the reason already there, so a guard escalates and
    /// never reverses.
    fn judge(
        &self,
        tick: &TickCtx,
        judged: Result<crate::pricing::PricerOutput, Refusal>,
        out: &Diagnostics,
    ) -> Result<crate::pricing::PricerOutput, Unusable> {
        let mut guards = self
            .guards
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut withdraw: Option<Unusable> = None;
        let mut halted = false;
        if !guards.is_empty() {
            let candidate = crate::guard::Candidate::new(tick, judged.as_ref(), out);
            for g in guards.iter_mut() {
                let mut gout = Diagnostics::new(g.bound.diagnostics.len());
                // A guard is the binary's code: its panic is this lane's trip, not this
                // loop's death.
                let gate = match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    g.bound.timed(|| g.guard.check(&candidate, &mut gout))
                })) {
                    Ok(gate) => gate,
                    Err(payload) => {
                        let reason = crate::guard::panic_reason(g.kind, "check", &*payload);
                        self.trip(g.kind, crate::guard::Cause::Panic, reason);
                        halted = true;
                        continue;
                    }
                };
                g.bound.publish(&gout);
                match gate {
                    crate::guard::Gate::Allow => {}
                    crate::guard::Gate::Withdraw(refusal) => {
                        if withdraw.is_none() {
                            withdraw = Some(g.bound.unusable(refusal));
                        }
                    }
                    crate::guard::Gate::Halt(reason) => {
                        self.trip(g.kind, crate::guard::Cause::Guard, reason);
                        halted = true;
                    }
                }
            }
        }
        drop(guards);
        match judged {
            Err(refusal) => Err(self.bound.unusable(refusal)),
            Ok(priced) if halted => Ok(priced),
            Ok(priced) => match withdraw {
                Some(unusable) => Err(unusable),
                None => Ok(priced),
            },
        }
    }

    /// The reference market's channel, for seeding the feed gauges on adoption.
    pub(crate) fn market_rx(&self) -> Option<&tokio::sync::watch::Receiver<Option<PriceSample>>> {
        self.market.as_ref()
    }

    /// [`Self::current_detailed`] reduced to the `(delta, mid)` pair, for tests that only
    /// care what would go out.
    #[cfg(test)]
    pub fn current(&self, band: &MidBand) -> Result<(U256, U256), Unusable> {
        self.current_detailed(band).map(|p| (p.delta, p.mid))
    }

    /// The `(delta, mid)` to publish now, with the working behind it, or why there is none.
    ///
    /// The core wraps every pricer: the freshness gate before it (the same `no_sample` and
    /// `stale` refusals, with the same text, for every model), and the band after it,
    /// checked here rather than at each call site so every send path is covered by
    /// construction: an out-of-band mid becomes the same unusable-price error a stale feed
    /// produces, which the RPC path skips the push on and the builder path withdraws its
    /// quote for. The quote guards judge every tick, the ones the freshness gate refused
    /// included.
    pub fn current_detailed(&self, band: &MidBand) -> Result<Priced, Unusable> {
        let now = Instant::now();
        let market = match &self.market {
            Some(rx) => match fresh_sample(rx) {
                Ok(sample) => Some(Market::from_sample(&sample, &self.pair)),
                // Refused before the pricer runs, and still judged by the quote guards: a
                // feed gone quiet is the commonest way a lane is withdrawn, so a guard that
                // halts after so long withdrawn has to count these ticks. No market and no
                // diagnostic, since the pricer did not run. What the guards say can only
                // add a halt (a refusal is never reversed, and a withdraw does not replace
                // it), so the answer is this refusal as it was.
                Err(unusable) => {
                    let tick = TickCtx::new(now, None, &self.pair);
                    let out = Diagnostics::new(self.bound.diagnostics.len());
                    let judged = self.judge(&tick, Err(unusable.as_refusal()), &out);
                    debug_assert!(judged.is_err(), "a guard never reverses a refusal");
                    return Err(unusable);
                }
            },
            None => None,
        };
        let mut out = Diagnostics::new(self.bound.diagnostics.len());
        let tick = TickCtx::new(now, market.as_ref(), &self.pair);
        // A pricer is the binary's code, so it can panic. Caught here, as the spec's "a
        // panic counts as a trip": the tick is withdrawn under `panic`, the lane trips
        // naming the kind, and the loop halts with a reason a human can read, instead of
        // the supervisor restarting it into the same bug. The pricer's mutex is recovered
        // from the poison by `lock_pricer`, so the lane is not wedged for whatever asks next.
        let priced = match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            self.bound
                .timed(|| lock_pricer(&self.pricer).price(&tick, &mut out))
        })) {
            Ok(priced) => priced,
            Err(payload) => {
                let reason = crate::guard::panic_reason(self.kind_name(), "price", &*payload);
                let header = reason.header.clone();
                self.trip(self.kind_name(), crate::guard::Cause::Panic, reason);
                return Err(Unusable::new(UnusableKind::Panic, header));
            }
        };
        // Before the refusal is handled: what a pricer measured on a tick it then refused
        // is still worth seeing, as the volatile model's gauges always were.
        self.bound.publish(&out);
        // The core's own judgement of what the pricer published (the backstop, then the
        // band), folded in with the pricer's refusal into the one answer the quote guards
        // see: an `Ok` here is a quote the core would publish.
        let judged = match priced {
            Ok(priced) => match crate::pricing::backstop(priced, market.as_ref(), &self.pair)
                .and_then(|()| band.check(priced.mid))
            {
                Ok(()) => Ok(priced),
                Err(unusable) => Err(unusable.as_refusal()),
            },
            Err(refusal) => Err(refusal),
        };
        let priced = self.judge(&tick, judged, &out)?;
        Ok(Priced {
            delta: priced.delta,
            mid: priced.mid,
            feed_mid: market.map_or(priced.mid, |market| market.mid),
        })
    }

    /// Waits until the market publishes a sample newer than the last one this source read.
    ///
    /// A pair with no market can never change, so this never completes: callers `select!`
    /// on it alongside their other arms, where a never-ready branch simply never wins.
    pub async fn changed(&mut self) {
        match &mut self.market {
            None => std::future::pending().await,
            // Err means the feed task is gone. Returning would spin the caller's loop at
            // full speed, so treat a dead feed as "nothing will ever change again" and let
            // MAX_PRICE_AGE in `current` be what reports it.
            Some(rx) => {
                if rx.changed().await.is_err() {
                    std::future::pending().await
                }
            }
        }
    }

    /// `price_decimals` is the scale `delta` and `mid` are at: the core's backstop judges the
    /// delta against one whole unit at it, so a 6-dp pair priced on an 18-dp shape would be
    /// held to the wrong unit.
    #[cfg(test)]
    pub(crate) fn fixed_for_tests(delta: U256, mid: U256, price_decimals: u32) -> Self {
        Self::new(
            Box::new(crate::pricing::FixedPricer { delta, mid }),
            "fixed",
            None,
            Arc::new(test_shape(false, price_decimals)),
            Bound::none(),
        )
    }

    #[cfg(test)]
    pub(crate) fn pair_for_tests(&self) -> &Arc<PairShape> {
        &self.pair
    }

    /// A `feed` lane: the market's mid at `delta`, or the book's own when `None`.
    #[cfg(test)]
    pub(crate) fn feed_for_tests(
        rx: tokio::sync::watch::Receiver<Option<PriceSample>>,
        delta: Option<U256>,
        invert: bool,
        price_decimals: u32,
    ) -> Self {
        Self::new(
            Box::new(crate::pricing::FeedPricer {
                delta,
                spread_scale: crate::config::spread_scale(price_decimals),
            }),
            "feed",
            Some(rx),
            Arc::new(test_shape(invert, price_decimals)),
            Bound::none(),
        )
    }
}

/// A pair for tests that build a `ValueSource` without one: only orientation and scale
/// matter to pricing.
#[cfg(test)]
fn test_shape(inverted: bool, price_decimals: u32) -> PairShape {
    PairShape {
        label: "TEST/PAIR".into(),
        tokens: (Address::zero(), Address::zero()),
        lane: U256::zero(),
        inverted,
        price_decimals,
        target: Address::zero(),
        sources: 1,
        min_mid: None,
        max_mid: None,
    }
}

/// updateState(target, lane, ts, [delta, mid]) calldata, shared by both send paths.
pub fn update_calldata(params: &UpdateParams, ts: u32, delta: U256, mid: U256) -> Result<Vec<u8>> {
    Ok(encode_calldata(
        UPDATE_STATE_SIG,
        &[
            Value::Address(params.target),
            Value::Uint(params.lane),
            Value::Uint(ts.into()),
            Value::Array(vec![Value::Uint(delta), Value::Uint(mid)]),
        ],
    )?)
}

/// Builds and signs one updateState transaction without sending it, returning the raw
/// type-prefixed RLP bytes a builder (or eth_sendRawTransaction) expects. The priority fee
/// is zero: quote updates submitted through a builder's maker endpoint do not pay priority
/// fees. The max fee is priced off the target block's parent (see next_block_max_fee),
/// which the builder needs covered for the quote to be includable.
///
/// The nonce is passed in rather than fetched: it cannot change while we are quoting for
/// one block (nothing of ours has landed yet), so the caller fetches it once per target
/// block instead of once per re-sign, keeping the ~50ms requote budget free of an RPC
/// roundtrip. Every Overrides field is therefore supplied and building performs no RPC
/// call at all.
pub async fn sign_update_tx(
    client: &EthClient,
    signer: &Signer,
    params: &UpdateParams,
    stamp: UpdateStamp,
    nonce: u64,
    (delta, mid): (U256, U256),
) -> Result<Vec<u8>> {
    let tx = build_generic_tx(
        client,
        TxType::EIP1559,
        params.registry,
        signer.address(),
        update_calldata(params, stamp.ts, delta, mid)?.into(),
        Overrides {
            gas_limit: Some(UPDATE_GAS_LIMIT),
            max_fee_per_gas: Some(next_block_max_fee(stamp.parent_base_fee, 0)),
            max_priority_fee_per_gas: Some(0),
            nonce: Some(nonce),
            chain_id: Some(params.chain_id),
            ..Default::default()
        },
    )
    .await?;
    let mut raw: Vec<u8> = vec![TxType::EIP1559.into()];
    let signed = EIP1559Transaction::try_from(tx)
        .map_err(|err| eyre!("not an EIP-1559 transaction: {err:?}"))?
        .sign(signer)
        .await?;
    signed.encode(&mut raw);
    Ok(raw)
}

/// The timestamp a signed update carries, read back out of its raw bytes: what a builder
/// would see. Test-only, for asserting on what was actually quoted.
#[cfg(test)]
pub(crate) fn stamped_ts(raw_tx: &[u8]) -> u32 {
    use ethrex_l2_sdk::calldata::decode_calldata;
    use ethrex_rlp::decode::RLPDecode;
    let tx = EIP1559Transaction::decode(&raw_tx[1..]).expect("a type-prefixed EIP-1559 tx");
    let values = decode_calldata(UPDATE_STATE_SIG, tx.data).expect("updateState calldata");
    let Value::Uint(ts) = values[2] else {
        panic!("the third argument of updateState is the timestamp");
    };
    ts.as_u32()
}

#[cfg(test)]
mod tests {
    use std::time::SystemTime;

    use ethrex_common::Bytes;
    use ethrex_l2_sdk::calldata::decode_calldata;

    use super::*;

    mod quote_guards {
        use super::*;
        use crate::{
            guard::{BoundQuoteGuard, Candidate, Cause, Gate, Latch, QuoteGuard, TripReason},
            pricing::{BuildCtx, PricerOutput, Refusal, RefusalHandle, TickCtx},
        };

        struct Allower;
        impl QuoteGuard for Allower {
            fn check(&mut self, _: &Candidate<'_>, _: &mut Diagnostics) -> Gate {
                Gate::Allow
            }
        }

        struct Withdrawer(RefusalHandle);
        impl QuoteGuard for Withdrawer {
            fn check(&mut self, _: &Candidate<'_>, _: &mut Diagnostics) -> Gate {
                Gate::Withdraw(Refusal::new(&self.0, "too far from the oracle"))
            }
        }

        struct Halter;
        impl QuoteGuard for Halter {
            fn check(&mut self, _: &Candidate<'_>, _: &mut Diagnostics) -> Gate {
                Gate::Halt(TripReason::new("inventory cap breached"))
            }
        }

        struct Refusing;
        impl Pricer for Refusing {
            fn price(&mut self, _: &TickCtx, _: &mut Diagnostics) -> Result<PricerOutput, Refusal> {
                Err(Refusal::core(UnusableKind::NoSample, "no market"))
            }
        }

        fn withdrawer(metrics: &crate::metrics::Metrics) -> BoundQuoteGuard {
            let mut ctx = BuildCtx::new(test_shape(false, 18));
            let handle = ctx.refusal("too_far").unwrap();
            BoundQuoteGuard {
                kind: "oracle",
                guard: Box::new(Withdrawer(handle)),
                bound: Bound::for_build(&ctx, metrics, "oracle", crate::tasks::Tasks::default()),
            }
        }

        fn bare(kind: &'static str, guard: Box<dyn QuoteGuard>) -> BoundQuoteGuard {
            BoundQuoteGuard {
                kind,
                guard,
                bound: Bound::none(),
            }
        }

        fn ok_source() -> ValueSource {
            ValueSource::fixed_for_tests(U256::one(), U256::from(2u64) * U256::exp10(18), 18)
        }

        fn refusing_source() -> ValueSource {
            ValueSource::new(
                Box::new(Refusing),
                "fixed",
                None,
                Arc::new(test_shape(false, 18)),
                Bound::none(),
            )
        }

        /// A guard withdraws under the reason its build declared, counted on that reason;
        /// on a tick the pricer refused, `Allow` changes nothing and a `Withdraw` does not
        /// replace the pricer's own reason: a guard escalates, never reverses.
        #[test]
        fn a_quote_guard_can_withdraw_under_its_own_reason_and_escalate_a_refusal_but_never_allow_one()
         {
            let metrics = crate::metrics::Metrics::new().unwrap();
            let pair = metrics.for_pair("BASE/QUOTE");
            let band = MidBand::default();

            let withdrawn = ok_source().with_guards(Latch::new(), vec![withdrawer(&metrics)]);
            let err = withdrawn.current_detailed(&band).unwrap_err();
            assert_eq!(err.to_string(), "too far from the oracle");
            err.count(&pair);
            let rendered = metrics.render().unwrap();
            let counted = rendered
                .lines()
                .find(|l| {
                    l.starts_with("quote_updater_price_unusable_total")
                        && l.contains(r#"reason="too_far""#)
                })
                .unwrap_or_default();
            assert!(
                counted.ends_with(" 1"),
                "counted under the guard's declared reason: {counted:?}"
            );
            let timed = rendered
                .lines()
                .find(|l| {
                    l.starts_with("quote_updater_extension_duration_seconds_count")
                        && l.contains(r#"kind="oracle""#)
                })
                .unwrap_or_default();
            assert!(
                timed.ends_with(" 1"),
                "the guard's check is timed: {timed:?}"
            );

            let allowed =
                refusing_source().with_guards(Latch::new(), vec![bare("a", Box::new(Allower))]);
            assert_eq!(
                allowed.current_detailed(&band).unwrap_err().to_string(),
                "no market",
                "Allow on a refusal changes nothing"
            );

            let escalated = refusing_source().with_guards(Latch::new(), vec![withdrawer(&metrics)]);
            assert_eq!(
                escalated.current_detailed(&band).unwrap_err().to_string(),
                "no market",
                "the pricer's own reason stands: a withdraw over a withdraw is not more severe"
            );
        }

        /// `Halt` trips the lane's latch with the guard's kind (the latch records the
        /// transition, as the lane attached its series to it) and hands the tick back as
        /// it was: the gate reads the latch next and halts.
        #[test]
        fn a_quote_guard_halt_trips_the_latch_with_its_kind() {
            let metrics = crate::metrics::Metrics::new().unwrap();
            let pair = metrics.for_pair("BASE/QUOTE");
            let latch = Latch::new();
            let adopted = crate::feed::Adoption::new();
            adopted.adopt();
            latch.attach_metrics(crate::feed::Gated::new(pair.clone(), adopted));
            let source =
                ok_source().with_guards(latch.clone(), vec![bare("cap", Box::new(Halter))]);
            assert!(source.current_detailed(&MidBand::default()).is_ok());
            let trip = latch.tripped().expect("halted");
            assert_eq!((trip.source, trip.cause), ("cap", Cause::Guard));
            assert_eq!(
                trip.reason.alarm,
                "guard `cap` tripped: inventory cap breached"
            );
            assert_eq!(pair.breaker_tripped.get(), 1.0);
            assert_eq!(pair.breaker_trips.get(), 1);
            assert_eq!(metrics.trip_source("BASE/QUOTE", "cap", "guard").get(), 1.0);
        }

        /// What each tick showed a guard: the refusal's reason, if refused, and whether
        /// there was a market.
        type Seen = Arc<Mutex<Vec<(Option<String>, bool)>>>;

        /// Halts once the lane has been withdrawn for `limit` ticks in a row, recording
        /// what each tick showed it.
        struct WithdrawnTooLong {
            limit: usize,
            run: usize,
            seen: Seen,
        }
        impl QuoteGuard for WithdrawnTooLong {
            fn check(&mut self, candidate: &Candidate<'_>, _: &mut Diagnostics) -> Gate {
                let reason = candidate.price().err().map(|r| r.reason().to_owned());
                let market = candidate.tick().market_if_any().is_some();
                self.run = if reason.is_some() { self.run + 1 } else { 0 };
                self.seen.lock().unwrap().push((reason, market));
                if self.run >= self.limit {
                    Gate::Halt(TripReason::new(format!("withdrawn {} ticks", self.run)))
                } else {
                    Gate::Allow
                }
            }
        }

        /// "Halt after ten minutes withdrawn" has to see the commonest way a lane is
        /// withdrawn: its feed with no sample yet, or a stale one, which the core refuses
        /// before the pricer runs. The guard judges those ticks too, with no market and the
        /// core's reason, and the lane's answer is the core's refusal as it was.
        #[test]
        fn a_quote_guard_judges_a_tick_the_core_refused_for_its_feeds_freshness() {
            let (tx, rx) = tokio::sync::watch::channel(None);
            let seen = Arc::new(Mutex::new(Vec::new()));
            let latch = Latch::new();
            let source = ValueSource::feed_for_tests(rx, None, false, 18).with_guards(
                latch.clone(),
                vec![bare(
                    "withdrawn",
                    Box::new(WithdrawnTooLong {
                        limit: 2,
                        run: 0,
                        seen: Arc::clone(&seen),
                    }),
                )],
            );
            let band = MidBand::default();

            let err = source.current_detailed(&band).unwrap_err();
            assert_eq!(err.kind(), Some(UnusableKind::NoSample));
            assert_eq!(err.to_string(), "no usable price from the feed");
            assert!(latch.tripped().is_none());

            // A sample a minute old by the wall clock: stale.
            tx.send_replace(Some(PriceSample {
                delta: U256::zero(),
                mid: U256::exp10(18),
                at: Instant::now(),
                wall: std::time::SystemTime::now() - std::time::Duration::from_secs(60),
            }));
            let err = source.current_detailed(&band).unwrap_err();
            assert_eq!(
                err.kind(),
                Some(UnusableKind::Stale),
                "the core's refusal, as it was"
            );
            assert!(
                err.to_string().contains("refusing to push a stale value"),
                "{err}"
            );
            let trip = latch
                .tripped()
                .expect("withdrawn two ticks in a row: halted");
            assert_eq!((trip.source, trip.cause), ("withdrawn", Cause::Guard));
            assert_eq!(
                *seen.lock().unwrap(),
                vec![
                    (Some("no_sample".to_owned()), false),
                    (Some("stale".to_owned()), false)
                ],
                "each tick judged, with the core's reason and no market"
            );
        }

        struct Panicker;
        impl QuoteGuard for Panicker {
            fn check(&mut self, _: &Candidate<'_>, _: &mut Diagnostics) -> Gate {
                panic!("index out of range")
            }
        }

        /// A quote guard's panic is that lane's trip, named by the guard's kind, and never
        /// the quote loop's death: the tick is handed back as judged and the gate halts on
        /// the latch.
        #[test]
        fn a_quote_guard_that_panics_trips_the_lane() {
            let latch = Latch::new();
            let source =
                ok_source().with_guards(latch.clone(), vec![bare("tally", Box::new(Panicker))]);
            assert!(source.current_detailed(&MidBand::default()).is_ok());
            let trip = latch.tripped().expect("the panic trips the lane");
            assert_eq!((trip.source, trip.cause), ("tally", Cause::Panic));
            assert_eq!(
                trip.reason.alarm,
                "check of `tally` panicked: index out of range"
            );
        }

        /// Every guard runs and the most severe answer wins: a halt beside a withdraw is a
        /// halt (the withdraw is not counted), a withdraw beside an allow is a withdraw.
        #[test]
        fn the_most_severe_gate_wins_across_guards() {
            let metrics = crate::metrics::Metrics::new().unwrap();
            let latch = Latch::new();
            let source = ok_source().with_guards(
                latch.clone(),
                vec![
                    bare("a", Box::new(Allower)),
                    withdrawer(&metrics),
                    bare("cap", Box::new(Halter)),
                ],
            );
            assert!(source.current_detailed(&MidBand::default()).is_ok());
            assert_eq!(latch.tripped().map(|t| t.source), Some("cap"));

            let source = ok_source().with_guards(
                Latch::new(),
                vec![bare("a", Box::new(Allower)), withdrawer(&metrics)],
            );
            assert_eq!(
                source
                    .current_detailed(&MidBand::default())
                    .unwrap_err()
                    .to_string(),
                "too far from the oracle"
            );
        }
    }

    fn params() -> UpdateParams {
        UpdateParams {
            registry: Address::from_slice(&[0x11; 20]),
            target: Address::from_slice(&[0x22; 20]),
            lane: U256::from(7u64),
            chain_id: 1,
        }
    }

    /// The calldata carries target, lane, timestamp and both slots, in that order, and
    /// reads back through the same ABI the registry uses.
    #[test]
    fn update_calldata_round_trips_through_the_registry_abi() {
        let params = params();
        let calldata =
            update_calldata(&params, 1_755_000_012, U256::from(5u64), U256::from(9u64)).unwrap();
        let decoded = decode_calldata(UPDATE_STATE_SIG, Bytes::from(calldata)).unwrap();
        assert_eq!(
            decoded,
            vec![
                Value::Address(params.target),
                Value::Uint(params.lane),
                Value::Uint(U256::from(1_755_000_012u64)),
                Value::Array(vec![
                    Value::Uint(U256::from(5u64)),
                    Value::Uint(U256::from(9u64))
                ]),
            ]
        );
    }

    /// The registry is the transaction's `to`, not an ABI argument, so it must not appear
    /// in the calldata — a swap of the two would silently publish for the wrong target.
    #[test]
    fn update_calldata_does_not_encode_the_registry() {
        let params = params();
        let calldata = update_calldata(&params, 1, U256::one(), U256::one()).unwrap();
        assert!(
            !calldata
                .windows(20)
                .any(|w| w == params.registry.as_bytes())
        );
        assert!(calldata.windows(20).any(|w| w == params.target.as_bytes()));
    }

    #[test]
    fn update_stamp_derives_from_parent_header() {
        let parent = BlockHeader {
            timestamp: 1_755_000_000,
            base_fee_per_gas: Some(7),
            ..Default::default()
        };
        let stamp = UpdateStamp::from_parent(&parent).unwrap();
        assert_eq!(stamp.ts, 1_755_000_012);
        assert_eq!(stamp.parent_base_fee, 7);
        // A chain without a base fee cannot price the max fee.
        let no_base_fee = BlockHeader {
            base_fee_per_gas: None,
            ..parent.clone()
        };
        assert!(UpdateStamp::from_parent(&no_base_fee).is_err());
        // The registry reads the timestamp as a u32.
        let far_future = BlockHeader {
            timestamp: u64::from(u32::MAX),
            ..parent
        };
        assert!(UpdateStamp::from_parent(&far_future).is_err());
    }

    /// A missed slot leaves the target block's number where it was but moves its
    /// timestamp one block time on; the registry accepts nothing else for it.
    #[test]
    fn a_missed_slot_moves_the_stamp_one_block_time_on_and_keeps_the_base_fee() {
        let stamp = UpdateStamp {
            ts: 1_755_000_012,
            parent_base_fee: 7,
        };
        let missed_at = stamp.missed_at();
        // Up to the moment the slot counts as missed, the stamp stands.
        assert_eq!(
            stamp.after_missed_slots(missed_at - 1).unwrap().ts,
            stamp.ts
        );
        // From then on, the next slot's.
        let next = stamp.after_missed_slots(missed_at).unwrap();
        assert_eq!(u64::from(next.ts), u64::from(stamp.ts) + BLOCK_TIME_SECS);
        // Priced off the same parent, so the base fee is untouched.
        assert_eq!(next.parent_base_fee, stamp.parent_base_fee);
        // And the new stamp's own slot is not yet missed at that moment.
        assert!(next.missed_at() > missed_at);
    }

    /// After an outage several slots may have passed: step straight to the first slot
    /// still ahead rather than through each missed one a block time at a time.
    #[test]
    fn several_missed_slots_are_skipped_in_one_step() {
        let stamp = UpdateStamp {
            ts: 1_755_000_012,
            parent_base_fee: 7,
        };
        let now = stamp.missed_at() + 2 * BLOCK_TIME_SECS + 1;
        let next = stamp.after_missed_slots(now).unwrap();
        assert_eq!(
            u64::from(next.ts),
            u64::from(stamp.ts) + 3 * BLOCK_TIME_SECS
        );
        assert!(next.missed_at() > now, "the new slot must still be ahead");
        // The registry reads the timestamp as a u32, like from_parent.
        let far_future = UpdateStamp {
            ts: u32::MAX - 1,
            parent_base_fee: 7,
        };
        assert!(far_future.after_missed_slots(u64::MAX).is_err());
    }

    #[test]
    fn next_block_max_fee_covers_worst_case_base_fee_rise() {
        // Twice the parent base fee plus the tip: the next block's base fee can rise at
        // most 12.5%, so 2x always covers it.
        assert_eq!(
            next_block_max_fee(10_000_000_000, 2_000_000_000),
            22_000_000_000
        );
        // The property the doc comment claims: strictly covers a +12.5% rise.
        let base = 10_000_000_000;
        assert!(next_block_max_fee(base, 0) > base + base / 8);
        // Saturates instead of overflowing on absurd inputs.
        assert_eq!(next_block_max_fee(u64::MAX, 5), u64::MAX);
        assert_eq!(next_block_max_fee(u64::MAX / 2 + 1, u64::MAX), u64::MAX);
    }

    fn feed_of(mid: U256) -> (tokio::sync::watch::Sender<Option<PriceSample>>, ValueSource) {
        let sample = PriceSample {
            delta: U256::from(50_000_000_000_000u64), // 0.5bp at 1e18
            mid,
            at: std::time::Instant::now(),
            wall: SystemTime::now(),
        };
        // The sender is returned so the channel outlives the source.
        let (tx, rx) = tokio::sync::watch::channel(Some(sample));
        (tx, ValueSource::feed_for_tests(rx, None, false, 18))
    }

    fn feed_of_empty() -> (tokio::sync::watch::Sender<Option<PriceSample>>, ValueSource) {
        let (tx, rx) = tokio::sync::watch::channel(None);
        (tx, ValueSource::feed_for_tests(rx, None, false, 18))
    }

    /// The vault-drain case: an ETHUSDC feed wired (uninverted) onto a USDC/WETH lane
    /// publishes ~4000 where ~0.00025 belongs. The pair's band is what stops it reaching a
    /// signature, and it is enforced inside `current` so no send path can forget it.
    #[test]
    fn a_feed_mid_outside_the_pairs_band_is_refused() {
        let weth_per_usdc_band = MidBand {
            min: Some(U256::from(200_000_000_000_000u64)),
            max: Some(U256::from(300_000_000_000_000u64)),
        };
        let eth_in_usdc = U256::from(4_000u64) * U256::exp10(18);
        let (_tx, wrong_orientation) = feed_of(eth_in_usdc);
        assert!(wrong_orientation.current(&weth_per_usdc_band).is_err());

        // The correctly-oriented mid (what feed::spawn_feed delivers for an inverted
        // pair) passes the same band, delta untouched.
        let (_tx, inverted) = feed_of(U256::from(250_000_000_000_000u64));
        let (delta, mid) = inverted.current(&weth_per_usdc_band).unwrap();
        assert_eq!(mid, U256::from(250_000_000_000_000u64));
        assert_eq!(delta, U256::from(50_000_000_000_000u64));
    }

    #[tokio::test]
    async fn changed_resolves_on_a_new_sample_and_never_on_a_static_source() {
        let (tx, mut source) = feed_of(U256::exp10(18));
        // A fresh sample wakes it.
        tx.send(Some(PriceSample {
            delta: U256::from(1u64),
            mid: U256::exp10(18),
            at: std::time::Instant::now(),
            wall: SystemTime::now(),
        }))
        .unwrap();
        tokio::time::timeout(Duration::from_secs(1), source.changed())
            .await
            .expect("a new sample must wake changed()");

        // A static source never does, so a select! arm on it never wins.
        let mut static_source = ValueSource::fixed_for_tests(U256::zero(), U256::one(), 18);
        assert!(
            tokio::time::timeout(Duration::from_millis(50), static_source.changed())
                .await
                .is_err()
        );
    }

    #[test]
    fn no_sample_renders_exactly_as_before() {
        let (_tx, source) = feed_of_empty();
        let err = source.current(&MidBand::default()).unwrap_err();
        assert_eq!(err.kind(), Some(UnusableKind::NoSample));
        assert_eq!(err.to_string(), "no usable price from the feed");
    }

    /// `Stale` is the one `Unusable` message this branch retyped by hand (`ensure!` ->
    /// `format!`), and it had no test at all before this — `NoSample` and both
    /// `OutOfBand` messages were already pinned. Driven with a sample stamped 60s in the
    /// past on both clocks rather than by sleeping past `MAX_PRICE_AGE`: `at`/`wall` are
    /// plain fields on `PriceSample`, so backdating them is deterministic and instant,
    /// where a real sleep would make the test slow and (on a loaded CI runner) flaky.
    #[test]
    fn stale_renders_the_measured_age_with_the_untouched_suffix() {
        let sample = PriceSample {
            delta: U256::from(50_000_000_000_000u64),
            mid: U256::exp10(18),
            at: std::time::Instant::now() - Duration::from_secs(60),
            wall: SystemTime::now() - Duration::from_secs(60),
        };
        let (_tx, rx) = tokio::sync::watch::channel(Some(sample));
        let source = ValueSource::feed_for_tests(rx, None, false, 18);
        let err = source.current(&MidBand::default()).unwrap_err();
        assert_eq!(err.kind(), Some(UnusableKind::Stale));
        // The `{age:.0?}` prefix is a measured Duration and can differ by a millisecond
        // between the two clocks read in `sample()`, so match the invariant suffix rather
        // than the exact age — the property worth pinning is the wording, not the number.
        assert!(
            err.to_string()
                .ends_with("old; refusing to push a stale value"),
            "{err}"
        );
    }

    #[test]
    fn out_of_band_low_renders_exactly_as_before() {
        let band = MidBand {
            min: Some(U256::exp10(18)),
            max: None,
        };
        let low = U256::exp10(17);
        let err = band.check(low).unwrap_err();
        assert_eq!(err.kind(), Some(UnusableKind::OutOfBand));
        assert_eq!(
            err.to_string(),
            format!(
                "mid {low} is below this pair's min_mid {}; refusing to publish it",
                U256::exp10(18)
            )
        );
    }

    #[test]
    fn out_of_band_high_renders_exactly_as_before() {
        let band = MidBand {
            min: None,
            max: Some(U256::exp10(18)),
        };
        let high = U256::exp10(19);
        let err = band.check(high).unwrap_err();
        assert_eq!(err.kind(), Some(UnusableKind::OutOfBand));
        assert_eq!(
            err.to_string(),
            format!(
                "mid {high} is above this pair's max_mid {}; refusing to publish it",
                U256::exp10(18)
            )
        );
    }

    #[test]
    fn every_kind_has_a_distinct_label() {
        let labels: Vec<&str> = UnusableKind::ALL.iter().map(|k| k.as_str()).collect();
        assert_eq!(
            labels,
            [
                "no_sample",
                "stale",
                "out_of_band",
                "delta_overflow",
                "panic",
            ]
        );
        // A pricer may not declare a reason that is the core's: the list it is checked
        // against must be exactly these labels.
        assert_eq!(crate::pricing::RESERVED_REASONS, labels.as_slice());
        // Indexing pre-bound children by `kind as usize` requires ALL to be in declaration order.
        for (i, k) in UnusableKind::ALL.into_iter().enumerate() {
            assert_eq!(k as usize, i);
        }
    }
}
