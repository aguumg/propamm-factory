//! One pair, from its resolved config to the `Live` handle both send paths quote from:
//! dialling its feeds, reading its vault, building its `ValueSource`, and seeding its
//! metrics at the moment a service takes it on. `SendOpts` is here because it is the
//! per-process half of what a pair needs to publish.

use ethrex_common::{Address, U256};
use ethrex_rpc::clients::eth::EthClient;
use eyre::{Result, WrapErr, ensure};

use std::sync::Arc;

use crate::source_gate::{GateSource as SourceGateSource, SourceGate};
use crate::{
    composite,
    config::{self, Pair, ResolvedPair},
    feed,
    kinds::Kinds,
    metrics, preflight,
    pricing::{BuildCtx, Chain, Diagnostics, History, Market, PairShape, Pricer, TickCtx},
    record,
    tasks::Tasks,
    update::{Bound, ValueSource},
    vault, venue, volatile,
};

/// Publishes the breaker's actual state at the moment the service adopts a pair, rather
/// than assuming it is freshly armed.
///
/// The assumption used to hold: the breaker was built a line above the gauge write, so a
/// pair could not have tripped before anyone looked. It does not hold now. A pair's feed
/// runs from the moment `build_live` dials it — that is what `build_live` waits for — and
/// the breaker judges every sample the feed reads, including the ones that arrive before
/// this generation owns the pair's series. On a reload that window is every other lane's
/// withdraw.
///
/// So a pair can arrive here already tripped, and writing a flat 0 over that is not a stale
/// number: the quote loop reads the same latch on its first block and halts on it, leaving a
/// lane that is halted with `breaker_tripped` reading 0. `PusherBreakerTripped` is a *page*
/// on that gauge, and it would never fire. Nothing downstream corrects it: the latch's own
/// write was gated off when it tripped, and the composite judges a tripped lane no further,
/// so no later sample records it either.
///
/// The trip counter moves with the gauge: this is the transition being recorded at the
/// first moment anything is allowed to record it, and recorded once. `Latch::record_trip`
/// claims it, so the latch's own write, if it runs after this read, does not count it
/// again; and a trip landing after this read, before the switch, is recorded by the
/// re-check in [`adopt`].
pub(crate) fn adopt_breaker_metrics(live: &Live) {
    let m = &live.metrics;
    // Whatever the guard measured before anyone was listening; None until its first sample.
    if let Some(readout) = &live.deviation {
        let ratios = *readout.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(ratio) = ratios.tick {
            m.breaker_deviation_ratio.set(ratio);
        }
        if let Some(ratio) = ratios.window {
            m.breaker_window_deviation_ratio.set(ratio);
        }
    }
    match live.latch.tripped() {
        Some(_) => {
            live.latch.record_trip(m);
        }
        None => m.breaker_tripped.set(0.0),
    }
}

/// Hands a lane its series: seeds them ([`adopt_metrics`]), then raises the generation's
/// switch, so the feed cannot interleave a tick between the two and have it overwritten by
/// an initial value a moment later. The one way both send paths adopt a lane.
///
/// Then looks at the latch once more. A trip landing after the seed read it armed and
/// before the switch was recorded by nobody: the seed wrote 0, the latch's own write was
/// gated off, and the composite judges a tripped lane no further, so the lane would halt
/// with `breaker_tripped` reading 0. Recorded here, through the gate that is now up; the
/// latch's claim keeps a trip the seed or the latch already recorded from counting twice.
pub(crate) fn adopt(
    live: &Live,
    adopted: &feed::Adoption,
    price_decimals: u32,
    metrics: &crate::metrics::Metrics,
) {
    adopt_metrics(live, price_decimals, metrics);
    adopted.adopt();
    live.latch.record_adopted();
}

pub(crate) fn adopt_metrics(live: &Live, price_decimals: u32, metrics: &crate::metrics::Metrics) {
    let m = &live.metrics;
    // Every diagnostic series the label carries, this build's included, starts blank: an
    // earlier build's kind or names would otherwise stand beside this one's for the life of
    // the process, and this build's own would read the last build's values until its first
    // tick sets them. Before the gauges below, like every other initial value here.
    metrics.quiesce_diagnostics(&live.pair.label);
    m.breaker_armed.set(if live.armed { 1.0 } else { 0.0 });
    for kind in &live.guard_kinds {
        m.guard(kind).set(1.0);
    }
    adopt_breaker_metrics(live);
    // A new generation has judged nothing yet: whatever the last one left (withdrawn, or
    // checks this generation no longer has) is not this one's. Its first tick, if it has a
    // source gate, writes the real value.
    m.clear_source_withdrawn();
    m.signer_runway_updates.set(f64::INFINITY);
    m.signer_balance_wei.set(f64::NAN);
    match live.values.market_rx() {
        None => {
            m.feed_up.set(f64::NAN);
            m.feed_last_tick.set(f64::NAN);
            m.feed_sample_current.set(f64::NAN);
            m.feed_sources_fresh.set(f64::NAN);
            m.feed_sources_min.set(f64::NAN);
        }
        // The sample `build_live` waited for, written through as this pair's first reading.
        //
        // Not cosmetic, and not something the feed will simply supply a moment later: it
        // writes these only when it accepts a *ticker*, and bookTicker emits on book change,
        // so a quiet pair can go minutes without one. Left at the 0 `for_pair` registered
        // them at, `feed_last_tick` reads as an epoch-old sample — `time() - 0 > 30` — and
        // `feed_sample_current` as a held sample that no longer describes the book, raising
        // PusherFeedStale and PusherFeedSampleNotCurrent on a lane that was just started
        // from a price seconds old. The feed itself cannot write them, because until the
        // line below it does not own this label.
        Some(rx) => {
            m.feed_up.set(1.0);
            // Copied out rather than read through the guard: `borrow()` holds the watch's
            // read lock, and the feed task takes the write side on every tick.
            let sample = *rx.borrow();
            if let Some(sample) = sample {
                m.feed_mid
                    .set(feed::scaled_to_f64(sample.mid, price_decimals));
                m.feed_delta
                    .set(feed::scaled_to_f64(sample.delta, price_decimals));
                m.feed_sample_current.set(1.0);
                if let Ok(since_epoch) = sample.wall.duration_since(std::time::UNIX_EPOCH) {
                    m.feed_last_tick.set(since_epoch.as_secs_f64());
                }
            }
        }
    }
    // Every venue under the composite, for the same reason: what it delivered before this
    // generation owned the series was recorded nowhere. A venue with no sample yet keeps
    // its `up` flag's 0 (it has not connected, which is what 0 says) but not the registered
    // 0 on `last_tick`, where 0 would read as an epoch-old sample.
    for venue in &live.venues {
        let v = &venue.metrics;
        // From here on the venue's `up` is its feed task's own flag, read at every scrape,
        // so a connect that happened before this generation owned the series still shows.
        metrics.watch_venue(
            &live.pair.label,
            venue.venue.as_str(),
            venue.connected.clone(),
        );
        match *venue.rx.borrow() {
            Some(sample) => {
                v.mid.set(feed::scaled_to_f64(sample.mid, price_decimals));
                v.delta
                    .set(feed::scaled_to_f64(sample.delta, price_decimals));
                v.sample_current.set(1.0);
                if let Ok(since_epoch) = sample.wall.duration_since(std::time::UNIX_EPOCH) {
                    v.last_tick.set(since_epoch.as_secs_f64());
                }
            }
            None => {
                v.last_tick.set(f64::NAN);
                v.sample_current.set(f64::NAN);
            }
        }
    }
}

/// The first two lanes whose on-chain symbol labels render identically, if any.
///
/// Every metric family is keyed on that label, and `Metrics::for_pair` binds children by it,
/// so two pairs sharing one would silently share one set of series: their counters would
/// merge, two feed tasks would race on the same `feed_mid`, and — worse — the "count the
/// transition only" guard in `binance.rs` reads `breaker_tripped` off that shared handle, so
/// once the first pair tripped, the second pair's trip would increment nothing and never
/// flip its own gauge. A pair silently not counted as tripped is the opposite of what the
/// breaker is for.
///
/// Refused rather than mangled into uniqueness, for the same reason `resolve_pairs` refuses
/// two pairs that share a signer address: the config says something the operator did not
/// mean. Two lanes rendering the same symbols almost certainly means one of the four token
/// addresses is not the token they think it is — the impostor-token case — which is worth
/// stopping for, and cannot be fixed by this process renaming a series behind their back.
///
/// Pure and over the report's rows rather than the resolved pairs, so `--check` sees it too:
/// the labels are preflight's, and `--check` is the surface built to catch this class of
/// mistake before a run.
pub(crate) fn colliding_label(rows: &[preflight::Row]) -> Option<(&str, U256, U256)> {
    rows.iter().enumerate().find_map(|(i, row)| {
        rows[i + 1..]
            .iter()
            .find(|other| other.label == row.label)
            .map(|other| (row.label.as_str(), row.lane, other.lane))
    })
}

/// A pair plus the live source of its `[delta, mid]`.
#[derive(Clone)]
pub(crate) struct Live {
    pub(crate) pair: Pair,
    pub(crate) values: ValueSource,
    /// The lane's latch: the composite task trips it, the quote loop halts on it, and a
    /// reload reads what tripped it. Cloned with `Live` for a restart, so a trip that
    /// happened while no loop was running is waiting when one returns.
    pub(crate) latch: crate::guard::Latch,
    /// Whether the pair configured a deviation guard, for `breaker_armed`.
    pub(crate) armed: bool,
    /// The registered guard kinds this pair runs (market and quote), for `guards{pair,kind}`.
    pub(crate) guard_kinds: Vec<&'static str>,
    /// The deviation guard's last ratios, for adoption to seed its gauges from; `None` for
    /// a pair without one.
    pub(crate) deviation: Option<crate::guard::deviation::Readout>,
    /// This pair's pre-bound metric handles. `PairMetrics` is `Clone` and `Arc`-backed, so
    /// cloning a `Live` for a supervised restart carries the same handle forward instead of
    /// binding a fresh one: a restart that got fresh handles would reset every counter on
    /// each supervised restart, and `rate()` would under-report exactly during an incident.
    pub(crate) metrics: metrics::PairMetrics,
    /// The venue feeds under this pair's composite, for seeding their gauges on adoption
    /// and for quiescing them on removal. Empty for a static pair.
    pub(crate) venues: Vec<composite::VenueFeed>,
    /// The run's observers, for the block and landing events the quote loop emits. Set
    /// at adoption (`Service::spawn`), so empty until then, and in `--mode node`, which
    /// adopts no lane through a `Service` and tells observers nothing.
    pub(crate) observers: crate::observe::Observers,
}

/// What a pricer is told about the pair it prices, from the resolved pair.
pub(crate) fn shape_of(resolved: &ResolvedPair, price_decimals: u32, target: Address) -> PairShape {
    PairShape {
        label: resolved.pair.label.clone(),
        tokens: resolved.declared_base_quote(),
        lane: resolved.pair.lane,
        inverted: resolved.invert,
        price_decimals,
        target,
        sources: resolved.feeds.as_ref().map_or(0, |f| f.sources.len()),
        min_mid: resolved.pair.band.min,
        max_mid: resolved.pair.band.max,
    }
}

/// A custom pair's `ValueSource`: its pricer built through the kind table with the chain
/// and, for a pair with a market, that market's history to read from; the diagnostics and
/// refusals it declared bound to their series; and the tasks it asked for started, now
/// that the build succeeded, for as long as the lane lives. A task that returns on its own
/// is said out loud: whatever it fed now ages until the pricer refuses, and the operator
/// should learn why.
#[allow(clippy::too_many_arguments)] // one call site, naming each
pub(crate) async fn build_custom(
    kinds: &Kinds,
    kind: &str,
    config: &toml::Table,
    shape: PairShape,
    market: Option<tokio::sync::watch::Receiver<Option<feed::PriceSample>>>,
    metrics: &metrics::Metrics,
    chain: Chain,
    history: Option<History>,
    latch: crate::guard::Latch,
    readers: &vault::InventoryReaders,
    http: Arc<reqwest::Client>,
) -> Result<ValueSource> {
    let mut ctx = BuildCtx::new(shape.clone())
        .with_chain(chain)
        .with_latch(latch)
        .with_readers(readers.clone())
        .with_http(http);
    if let Some(history) = history {
        ctx = ctx.with_history(history);
    }
    let pricer = kinds
        .build(kind, config, &mut ctx)
        .await
        .wrap_err_with(|| format!("{}: pricing `{kind}`", shape.label))?;
    let name = kinds
        .pricer_name(kind)
        .expect("a pricer that built is registered");
    let mut tasks = Tasks::default();
    watch_tasks(&mut tasks, &mut ctx, "pricing", name, metrics);
    let bound = Bound::for_build(&ctx, metrics, kind, tasks);
    let latch = ctx.latch();
    Ok(
        ValueSource::new(pricer, name, market, Arc::new(shape), bound)
            .with_latch(latch)
            .with_landings(ctx.landings_sender()),
    )
}

/// One side's registered guards from the pair's stanzas, in file order, each built through
/// the kind table with a build context `ctx_for` makes, its declared diagnostics bound to
/// their series under its kind, and the tasks it asked for started, as a custom pricer's
/// are. Stanzas of the other side are skipped; an unregistered kind is the build's error.
async fn build_guard_side(
    kinds: &Kinds,
    stanzas: &[config::GuardStanza],
    side: crate::guard::GuardSide,
    mut ctx_for: impl FnMut() -> BuildCtx,
    metrics: &metrics::Metrics,
) -> Result<Vec<(&'static str, crate::guard::BuiltGuard, Arc<Bound>)>> {
    let mut built = Vec::new();
    for stanza in stanzas {
        if kinds.guard_side(&stanza.kind).is_some_and(|s| s != side) {
            continue;
        }
        let mut ctx = ctx_for();
        let guard = kinds.build_guard(stanza, &mut ctx).await?;
        let kind = kinds
            .guard_name(&stanza.kind)
            .expect("a guard that built is registered");
        let mut tasks = Tasks::default();
        watch_tasks(&mut tasks, &mut ctx, "guard", kind, metrics);
        let bound = Bound::for_build(&ctx, metrics, kind, tasks);
        built.push((kind, guard, bound));
    }
    Ok(built)
}

/// The pair's registered market guards, for its composite, in file order.
pub(crate) async fn build_market_guards(
    kinds: &Kinds,
    stanzas: &[config::GuardStanza],
    ctx_for: impl FnMut() -> BuildCtx,
    metrics: &metrics::Metrics,
) -> Result<Vec<crate::guard::BoundGuard>> {
    let built = build_guard_side(
        kinds,
        stanzas,
        crate::guard::GuardSide::Market,
        ctx_for,
        metrics,
    )
    .await?;
    Ok(built
        .into_iter()
        .filter_map(|(kind, guard, bound)| match guard {
            crate::guard::BuiltGuard::Market(guard) => {
                Some(crate::guard::BoundGuard { kind, guard, bound })
            }
            crate::guard::BuiltGuard::Quote(_) => None,
        })
        .collect())
}

/// The pair's registered quote guards, for its quote loop, in file order.
pub(crate) async fn build_quote_guards(
    kinds: &Kinds,
    stanzas: &[config::GuardStanza],
    ctx_for: impl FnMut() -> BuildCtx,
    metrics: &metrics::Metrics,
) -> Result<Vec<crate::guard::BoundQuoteGuard>> {
    let built = build_guard_side(
        kinds,
        stanzas,
        crate::guard::GuardSide::Quote,
        ctx_for,
        metrics,
    )
    .await?;
    Ok(built
        .into_iter()
        .filter_map(|(kind, guard, bound)| match guard {
            crate::guard::BuiltGuard::Quote(guard) => {
                Some(crate::guard::BoundQuoteGuard { kind, guard, bound })
            }
            crate::guard::BuiltGuard::Market(_) => None,
        })
        .collect())
}

/// How one of a component's background tasks ended.
#[derive(Debug, PartialEq, Eq)]
enum TaskEnd {
    /// It returned. Whatever it fed now ages until the component refuses.
    Ended,
    /// It panicked, with what it said.
    Panicked(String),
}

/// Runs one of a component's background tasks to its end and says how it ended. The panic
/// is caught here, inside the task, because the line naming the lane is the next line of
/// the same task: a panic unwinding past it would leave the operator only the default
/// hook's line, which says which thread panicked but not which pair or kind.
async fn background_ended(task: crate::pricing::LaneTask) -> TaskEnd {
    use futures_util::FutureExt;
    match std::panic::AssertUnwindSafe(task).catch_unwind().await {
        Ok(()) => TaskEnd::Ended,
        Err(payload) => TaskEnd::Panicked(crate::kinds::panic_message(&*payload).to_owned()),
    }
}

/// Starts the tasks a component's build asked for, under `tasks` so they stop with the
/// lane, and watches each to its end: a panic trips the lane (`cause = panic`, named by the
/// kind), which is what the spec's "a panic counts as a trip" means for a task; a plain
/// return is said out loud and counted in `extension_task_exits_total`, because the value
/// it fed then ages out until the component refuses, which is right, but the operator
/// should learn why.
fn watch_tasks(
    tasks: &mut Tasks,
    ctx: &mut BuildCtx,
    what: &'static str,
    kind: &'static str,
    metrics: &metrics::Metrics,
) {
    let label = ctx.pair().label.clone();
    let latch = ctx.latch();
    let exits = metrics.extension_task_exits(&label, kind);
    for task in ctx.take_tasks() {
        let (label, latch, exits) = (label.clone(), latch.clone(), exits.clone());
        tasks.spawn(async move {
            match background_ended(task).await {
                TaskEnd::Ended => {
                    tracing::warn!("[{label}] a background task of {what} `{kind}` ended");
                    exits.inc();
                }
                TaskEnd::Panicked(message) => {
                    tracing::warn!(
                        "[{label}] a background task of {what} `{kind}` panicked: {message}"
                    );
                    latch.trip(
                        kind,
                        crate::guard::Cause::Panic,
                        crate::guard::TripReason::new(format!(
                            "a background task of `{kind}` panicked: {message}"
                        )),
                    );
                }
            }
        });
    }
}

/// Starts a pair's price source. Its venues are dialled with the pair's orientation and
/// the composite is waited on once, so a wrong symbol or blocked endpoint fails at startup
/// rather than every tick. Consumes the `ResolvedPair`: past this point the source exists
/// only as a live `ValueSource`, so nothing downstream can read a stale copy of it.
///
/// A volatile pair also needs the chain: both tokens' `decimals()` are read once here, then
/// `target.vaultFor(base, quote)` and its balances are polled for the life of the pair. The
/// market's price history comes from `histories`, which outlives any one pair generation.
#[allow(clippy::too_many_arguments)] // like quoting::run: one call site per mode, each naming them
pub(crate) async fn build_live(
    resolved: ResolvedPair,
    endpoints: &venue::Endpoints,
    price_decimals: u32,
    metrics: &metrics::Metrics,
    chain: (&EthClient, Address),
    histories: &volatile::Histories,
    readers: &vault::InventoryReaders,
    recorder: Option<&record::Recorder>,
    kinds: &Kinds,
) -> Result<Built> {
    // The latch and the guards are built before the feeds, because the composite is what
    // judges every sample; the quote loop only reads the latch. Spawned once here and never
    // rebuilt, so the guards run through an RPC outage or a restart backoff that stops the
    // loop. See `composite::spawn`.
    let latch = crate::guard::Latch::new();
    let pair_metrics = metrics.for_pair(&resolved.pair.label);
    let adopted = feed::Adoption::new();
    latch.attach_metrics(feed::Gated::new(pair_metrics.clone(), adopted.clone()));
    let armed = resolved.breaker.is_some();
    let deviation = resolved.breaker.map(|config| {
        crate::guard::deviation::DeviationGuard::new(
            config,
            Some(feed::Gated::new(pair_metrics.clone(), adopted.clone())),
        )
    });
    let readout = deviation.as_ref().map(|guard| guard.readout());
    // What every pricer and guard, built-in or not, is told about the pair it judges.
    let shape = Arc::new(shape_of(&resolved, price_decimals, chain.1));
    let chain_handle = Chain::new(chain.0.clone(), chain.1);
    // The deviation guard first, then the pair's registered market guards in file order.
    // Built before the feeds, like the deviation guard: they judge from the first sample.
    let mut guards: Vec<crate::guard::BoundGuard> = deviation
        .map(|guard| crate::guard::BoundGuard::built_in("deviation", Box::new(guard)))
        .into_iter()
        .collect();
    guards.extend(
        build_market_guards(
            kinds,
            &resolved.guards,
            || {
                BuildCtx::new((*shape).clone())
                    .with_chain(chain_handle.clone())
                    .with_latch(latch.clone())
                    .with_http(endpoints.http())
            },
            metrics,
        )
        .await?,
    );
    let guard_kinds: Vec<&'static str> = guards
        .iter()
        .map(|g| g.kind)
        .filter(|kind| *kind != "deviation")
        .collect();
    // `for_pair` above already registered every gauge at 0 — see its doc comment. Two of
    // them would then be indistinguishable from a real, alertable zero until something
    // gets around to recording them for real, which for these two can take a while or
    // never happen at all:
    //
    // - `signer_runway_updates` is set only from inside `drive`'s main loop, on the
    //   RUNWAY_CHECK_BLOCKS cadence. If `drive`'s first `get_block_by_number` keeps
    //   failing, it retries internally forever and never reaches that check. No alert rule
    //   compares this gauge any more — `PusherSignerNearlyDry` moved onto the balance below
    //   — but the "Runway" panel still draws it, and a registered 0 there reads as a key
    //   with nothing left. +Inf is "never recorded", rendered as ∞.
    // - `feed_up`/`feed_last_tick`/`feed_sample_current` are set only inside
    //   `composite::spawn`, which a `Static`-source pair never spawns (see the match
    //   below) — a supported configuration (see config.example.toml), not an edge case.
    //   Left at 0 they would permanently fire `PusherFeedDown`, `PusherFeedStale` and
    //   `PusherFeedSampleNotCurrent` on a pair that is working exactly as configured. NaN
    //   fails every comparison those rules make, including `time() - NaN > 30`.
    // - `signer_balance_wei` rides with the runway: same recording site, same cadence, same
    //   never-recorded-until-then. NaN, and it now carries the weight the runway's +Inf used
    //   to: `PusherSignerNearlyDry` is a `predict_linear` over this series, and one NaN
    //   sample anywhere in its window makes the whole range return nothing, so a pair
    //   stalled on an RPC outage cannot page an operator to top up a key that is fine. It
    //   also keeps the "Signer balance" panel honest, where a registered 0 reads as an empty
    //   key and NaN draws a gap. The pair goes NaN together for the same reason
    //   `runway_check` writes them together: a balance with no runway beside it invites
    //   exactly the arithmetic an operator should not be doing by hand.
    //
    // Written by `Service::adopt` rather than here, and that is the second half of the
    // reason this function touches no metrics at all: a reload dials the replacement feed
    // before stopping the pair it replaces, so at this point the gauges below still belong
    // to a lane that is quoting to takers. Writing them here would reach across into that
    // lane's series — and if this build then failed, the reload would leave the numbers
    // there, on a pair it had just promised to leave completely alone.
    // The guards go to whichever arm connects the composite. A pair that streams nothing
    // connects none, and has no market guard to lose: `Kinds::check` refused the file for
    // one, as `config.rs` refuses a breaker on a fixed mid. The closure takes them by value and so is called once, on one arm: a `Cell` in its place
    // would be held across the arm's await and make the run's future `!Send`, which
    // `Updater::start` spawns.
    let params = |history| composite::Params {
        endpoints,
        decimals: price_decimals,
        invert: resolved.invert,
        latch: latch.clone(),
        guards,
        adopted: adopted.clone(),
        metrics,
        pair_metrics: pair_metrics.clone(),
        label: resolved.pair.label.clone(),
        recorder: recorder.cloned(),
        history,
    };
    let quote_shape = Arc::clone(&shape);
    // The reference market first, if the pair has one, so a wrong symbol fails before the
    // pricer is built. Its σ history is the process-level store's, so `ctx.history()` reads
    // what a reload keeps: one f64 per second per market, whether or not the kind reads it.
    let (market, venues, history) = match &resolved.feeds {
        Some(feeds) => {
            let history = histories.for_symbol(&feeds.history_key());
            let spawned = composite::connect(feeds, params(Some(history.clone()))).await?;
            (Some(spawned.rx), spawned.venues, Some(History(history)))
        }
        None => (None, Vec::new(), None),
    };
    let (client, target) = chain;
    let values = build_custom(
        kinds,
        &resolved.pricing.kind,
        &resolved.pricing.config,
        (*shape).clone(),
        market,
        metrics,
        Chain::new(client.clone(), target),
        history,
        latch.clone(),
        readers,
        endpoints.http(),
    )
    .await?;
    // The quote guards last, after the pricer, whose diagnostics a quote guard's build may
    // bind by name: the spec's build order (market, pricer, guards), so a failure here keeps
    // the old lane and reports the reload partial like any other build failure.
    let names = values.diagnostic_names().to_vec();
    let quote_guards = build_quote_guards(
        kinds,
        &resolved.guards,
        || {
            BuildCtx::new((*quote_shape).clone())
                .with_chain(chain_handle.clone())
                .with_latch(latch.clone())
                .with_http(endpoints.http())
                .with_pricer_diagnostics(names.clone())
        },
        metrics,
    )
    .await?;
    // Both sides' kinds, for `guards{pair,kind}` and the removal that retires it.
    let mut guard_kinds = guard_kinds;
    guard_kinds.extend(quote_guards.iter().map(|guard| guard.kind));
    let mut values = values.with_guards(latch.clone(), quote_guards);
    // The checks on the primary source, read off the venues the composite dialled: a pair
    // that sets neither gets no gate and quotes exactly as it did.
    if let Some(feeds) = &resolved.feeds {
        let sources = venues.iter().map(SourceGateSource::of).collect();
        if let Some(gate) = SourceGate::new(
            &resolved.pair.label,
            feeds,
            sources,
            price_decimals,
            Some(pair_metrics.clone()),
        ) {
            values = values.with_source_gate(gate);
        }
    }
    Ok(Built {
        live: Live {
            pair: resolved.pair,
            values,
            latch,
            armed,
            guard_kinds,
            deviation: readout,
            metrics: pair_metrics,
            venues,
            // None yet: the lane is not the observers' to hear of until `Service::spawn`
            // adopts it, which is where its latch is told of them too.
            observers: Default::default(),
        },
        adopted,
    })
}

/// A pair dialled and ready to start, with the switch that lets its feeds report.
///
/// The switch is separate from `Live` because `Live` is cloned once per `supervise`
/// invocation while there is exactly one of these per pair *generation*: it is raised when
/// [`Service::spawn`] adopts the pair and lowered when [`Service::stop`] gives it up. See
/// [`feed::Gated`] for what that buys.
pub(crate) struct Built {
    pub(crate) live: Live,
    pub(crate) adopted: feed::Adoption,
}

/// Resolves what a volatile pair's inventory is read from and starts reading it: both tokens'
/// decimals, then a first reading of `target.vaultFor(base, quote)` and its two balances.
/// The first reading is awaited here for the same reason `composite::connect` waits for a sample: a
/// pair the target does not list or a token with no `decimals()` is a
/// config mistake, and it fails at startup with the call that failed rather than as a lane
/// that never prices.
pub(crate) async fn connect_inventory(
    readers: &vault::InventoryReaders,
    client: &EthClient,
    target: Address,
    base: Address,
    quote: Address,
    label: &str,
) -> Result<crate::pricing::InventoryFeed> {
    // A lane already reading this vault, on this process, shares its poller: one read of
    // the chain per refresh however many lanes price off the vault.
    let key = (target, base, quote);
    if let Some(reader) = readers.get(key) {
        return Ok(crate::pricing::InventoryFeed(reader));
    }
    let (base_decimals, quote_decimals) = tokio::join!(
        vault::read_decimals(client, base),
        vault::read_decimals(client, quote),
    );
    let base = vault::Side {
        token: base,
        decimals: base_decimals?,
    };
    let quote = vault::Side {
        token: quote,
        decimals: quote_decimals?,
    };
    let first = volatile::read_inventory(client, target, base, quote)
        .await
        .wrap_err_with(|| format!("could not read {label}'s vault inventory from {target:#x}"))?;
    let rx = volatile::spawn_inventory(
        client.clone(),
        target,
        base,
        quote,
        label.to_owned(),
        first,
        readers.refresh(),
    );
    Ok(crate::pricing::InventoryFeed(readers.register(key, rx)))
}

/// Collects per-pair startup results, or fails naming **every** pair that failed rather
/// than only the first.
///
/// One symbol that never delivers costs a 15s timeout, so reporting them one at a time
/// hides the second behind the first: an operator would fix one typo, wait, and meet the
/// next. Generic over the payload so the aggregation rule is testable on its own.
pub(crate) fn all_or_none<T>(results: Vec<(String, Result<T>)>) -> Result<Vec<T>> {
    let mut values = Vec::with_capacity(results.len());
    let mut failures = Vec::new();
    for (label, result) in results {
        match result {
            Ok(value) => values.push(value),
            Err(err) => failures.push(format!("  [{label}] {err:#}")),
        }
    }
    ensure!(
        failures.is_empty(),
        "{} pair(s) could not start:\n{}",
        failures.len(),
        failures.join("\n")
    );
    Ok(values)
}

/// One `--check` row from a pricer's `preview`: the `(delta, mid)` it would publish or why
/// not, and the note it left for the row. `sample` is the pair's market, if it has one;
/// `diagnostics` is how many its build declared, so the buffer has the slots the run's has
/// and a pricer reading back what it set sees the same here.
///
/// The preview goes through the core's backstop with the same market and shape the run's
/// every tick does, so a row cannot pass what the run would withdraw.
pub(crate) fn preview_row(
    pricer: &mut dyn Pricer,
    sample: Option<&feed::PriceSample>,
    shape: &PairShape,
    diagnostics: usize,
) -> (Result<(U256, U256), String>, Option<String>) {
    let market = sample.map(|sample| Market::from_sample(sample, shape));
    let tick = TickCtx::new(std::time::Instant::now(), market.as_ref(), shape);
    let mut out = Diagnostics::new(diagnostics);
    // A preview is the binary's code too: its panic is this row's error, and the check
    // goes on to the next pair. There is no lane to trip under `--check`.
    let row = match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        pricer.preview(&tick, &mut out)
    })) {
        Ok(preview) => preview
            .map_err(|refusal| refusal.to_string())
            .and_then(|priced| {
                crate::pricing::backstop(priced, market.as_ref(), shape)
                    .map(|()| (priced.delta, priced.mid))
                    .map_err(|unusable| unusable.to_string())
            }),
        Err(payload) => Err(format!(
            "preview panicked: {}",
            crate::kinds::panic_message(&*payload)
        )),
    };
    (row, out.note)
}

/// `--check`'s second table: connects every feed, waits for one sample, and reports what
/// each pair would publish right now.
///
/// Concurrent for the same reason startup is: three pairs cost one timeout rather than
/// three, and a second dead symbol is reported beside the first rather than behind it.
/// Needs no keys — a feed is a symbol and an orientation — so it still runs on the
/// bootstrap config where nothing is configured yet.
///
/// A volatile pair's spread needs a minute of history, which `--check` does not wait for:
/// its row shows the spread at σ = 0 (the competition term alone) and says so, and it
/// reads the vault the way the run will, so a target or token the run would refuse fails
/// here first.
pub(crate) async fn collect_prices(
    config: &config::Config,
    report: &preflight::Report,
    endpoints: &venue::Endpoints,
    price_decimals: u32,
    metrics: &metrics::Metrics,
    chain: (&EthClient, Address),
    kinds: &Kinds,
) -> Vec<preflight::PriceRow> {
    futures_util::future::join_all(config.pairs.iter().map(|spec| {
        // The on-chain symbol pair preflight resolved, so both tables name a pair the
        // same way; the lane fallback carries through when it could not be read.
        let label = report
            .rows
            .iter()
            .find(|row| row.lane == spec.lane)
            .map_or_else(|| config::lane_label(spec.lane), |row| row.label.clone());
        // `--check` never serves the registry this is drawn from (see
        // `should_serve_metrics`), so there is nobody to scrape it — but the composite
        // still needs somewhere for its counters to land. Adopted straight away: there is
        // no other generation to lose a race with here, and an unadopted feed would
        // silently record nothing at all.
        let adopted = feed::Adoption::new();
        adopted.adopt();
        async move {
            let params = || composite::Params {
                endpoints,
                decimals: price_decimals,
                invert: spec.invert,
                // No guards: `--check` prices a pair once and publishes nothing, so there
                // is nothing to guard.
                latch: crate::guard::Latch::new(),
                guards: Vec::new(),
                adopted: adopted.clone(),
                metrics,
                pair_metrics: metrics.for_pair(&label),
                label: label.clone(),
                recorder: None,
                history: None,
            };
            // What each venue delivered, for the lines under a multi-source row: a venue
            // that disagrees with the others is exactly what this table exists to show
            // before anything is published.
            let venue_lines = |spawned: &composite::Spawned| -> Vec<(String, Option<U256>)> {
                if spawned.venues.len() < 2 {
                    return Vec::new();
                }
                spawned
                    .venues
                    .iter()
                    .map(|v| (v.name(), v.rx.borrow().map(|s| s.mid)))
                    .collect()
            };
            let shape = PairShape {
                label: label.clone(),
                tokens: spec.declared_base_quote(),
                lane: spec.lane,
                inverted: spec.invert,
                price_decimals,
                target: chain.1,
                sources: spec.feeds.as_ref().map_or(0, |f| f.sources.len()),
                min_mid: spec.band.min,
                max_mid: spec.band.max,
            };
            let mut venues = Vec::new();
            // The diagnostics the pair's pricer declared in its build, for its quote guards
            // to bind to below, as in the run.
            let mut pricer_diagnostics: Vec<String> = Vec::new();
            // Whether there is a pricer for a quote guard to bind to: only once its build
            // ran and succeeded.
            let mut pricer_built = false;
            // Every row goes through the pair's pricer's `preview` and the core's backstop,
            // so the report and the run cannot disagree about what a pair would publish. No
            // freshness gate and no band here, as before: the row shows the price, the
            // report judges it.
            // The stanza against this pair first, as the run and a reload judge it, so
            // `--check` fails a file the run would refuse; and before the feed, so a refused
            // stanza does not wait out a market it will never price. The guard stanzas are
            // judged below.
            let kind = &spec.pricing.kind;
            let validated = kinds.validate_pricer(&shape, &spec.pricing);
            let sample = match (validated, &spec.feeds) {
                (Err(err), _) => Err(format!("{err:#}")),
                (Ok(()), Some(feeds)) => match composite::connect(feeds, params()).await {
                    Ok(spawned) => {
                        venues = venue_lines(&spawned);
                        let sample = *spawned.rx.borrow();
                        sample
                            .map(Some)
                            .ok_or_else(|| "the feed reported a sample and then lost it".to_owned())
                    }
                    Err(err) => Err(format!("{err:#}")),
                },
                (Ok(()), None) => Ok(None),
            };
            let (sample, note) = match sample {
                // Built and never run: `--check` sends nothing and starts none of the
                // pricer's tasks, which drop with its build context here. The chain is the
                // run's, so a vault the run would refuse fails here first; the history is
                // empty, since the check does not wait the minute σ needs.
                Ok(sample) => {
                    let (client, target) = chain;
                    let mut ctx = BuildCtx::new(shape.clone())
                        .with_chain(Chain::new(client.clone(), target))
                        .with_http(endpoints.http());
                    if spec.feeds.is_some() {
                        ctx = ctx.with_history(History::empty());
                    }
                    match kinds.build(kind, &spec.pricing.config, &mut ctx).await {
                        Ok(mut pricer) => {
                            pricer_built = true;
                            pricer_diagnostics = ctx.diagnostic_names().to_vec();
                            preview_row(
                                pricer.as_mut(),
                                sample.as_ref(),
                                &shape,
                                pricer_diagnostics.len(),
                            )
                        }
                        Err(err) => (Err(format!("{err:#}")), None),
                    }
                }
                Err(err) => (Err(err), None),
            };
            // Every guard stanza, validated against the pair and built as the run does both
            // before the lane quotes, whatever prices the pair, with the pricer's declared
            // diagnostics for a quote guard to bind to: a guard the run would refuse fails
            // here first, which is what the check is for. Built and dropped: nothing quotes,
            // so nothing is guarded, and a task a build started ends with its context, as a
            // pricer's does above.
            let guard_error = if spec.guards.is_empty() {
                None
            } else if let Err(err) = kinds.validate_guards(&shape, &spec.guards) {
                Some(format!("{err:#}"))
            } else {
                let (client, target) = chain;
                let ctx_for = || {
                    BuildCtx::new(shape.clone())
                        .with_chain(Chain::new(client.clone(), target))
                        .with_http(endpoints.http())
                };
                let market = build_market_guards(kinds, &spec.guards, &ctx_for, metrics).await;
                // A quote guard binds to what the pricer declared. With no pricer built
                // (its build failed, its stanza was refused, its feed never delivered) the
                // row already says why, and a line about diagnostics "declared: none"
                // would only repeat it in other words.
                let quote = if pricer_built {
                    build_quote_guards(
                        kinds,
                        &spec.guards,
                        || ctx_for().with_pricer_diagnostics(pricer_diagnostics.clone()),
                        metrics,
                    )
                    .await
                } else {
                    Ok(Vec::new())
                };
                match (market, quote) {
                    (Ok(_), Ok(_)) => None,
                    (Err(err), _) | (_, Err(err)) => Some(format!("{err:#}")),
                }
            };
            preflight::PriceRow {
                label,
                invert: spec.invert,
                sample,
                note,
                venues,
                guard_error,
            }
        }
    }))
    .await
}

/// Options every pair shares: where to write and how to behave, with nothing
/// pair-specific.
#[derive(Clone)]
pub(crate) struct SendOpts {
    pub(crate) registry: Address,
    pub(crate) target: Address,
    /// Fetched once at startup: the chain a signer signs for cannot change under a running
    /// process, and leaving it unset costs an eth_chainId inside every signing.
    pub(crate) chain_id: u64,
    pub(crate) no_pin: bool,
    pub(crate) mine: bool,
    pub(crate) once: bool,
    pub(crate) requote_ms: u64,
    /// The global default; a builder's own `disable_cross_region` overrides it.
    pub(crate) disable_cross_region: bool,
    /// The scale a published `(delta, mid)` is stamped in. `drive` needs it to render the
    /// `published_mid`/`published_delta` gauges as the same human-readable price the
    /// registry and preflight report already use, rather than a raw on-chain integer.
    /// Carried here rather than threaded into `drive` as its own parameter: `drive` already
    /// takes eight, and every other value it needs is reached through `opts` the same way.
    pub(crate) price_decimals: u32,
    /// Where the quote loop records every update it signs, when recording is on. Carried
    /// here for the same reason as `price_decimals`.
    pub(crate) recorder: Option<record::Recorder>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use eyre::eyre;
    use url::Url;

    /// Two pairs cannot share a metrics label. The mechanism that makes this more than
    /// cosmetic: `binance.rs` counts a breaker trip only on the transition, and reads
    /// `breaker_tripped` off the shared handle to find it — so on a merged label the second
    /// pair's trip increments nothing and its gauge never flips.
    #[test]
    fn two_lanes_rendering_the_same_symbols_are_refused() {
        let row = |label: &str, lane: u64| preflight::Row {
            label: label.to_owned(),
            lane: U256::from(lane),
            orientation: String::new(),
            stream: String::new(),
            pricing: String::new(),
            vault: None,
            breaker: "-".to_owned(),
            window: "-".to_owned(),

            guards: "-".to_owned(),
            key_env: String::new(),
            updater: None,
            authorized: None,
            runway: None,
            failures: Vec::new(),
            warnings: Vec::new(),
        };

        assert_eq!(
            colliding_label(&[row("WETH/USDC", 1), row("USDC/USDT", 2)]),
            None,
            "distinct labels are the normal case"
        );
        // The fallback label is `lane 0x…`, which is derived from the lane and so cannot
        // collide between distinct lanes — worth pinning, because it means a pair whose
        // symbols preflight could not read never trips this refusal.
        assert_eq!(
            colliding_label(&[row("lane 0x1", 1), row("lane 0x2", 2)]),
            None
        );
        // Bound to a local: the returned label borrows the rows it was found in, which is
        // what lets the production site name it without cloning.
        let rows = [
            row("WETH/USDC", 1),
            row("USDC/USDT", 2),
            row("WETH/USDC", 3),
        ];
        let (label, a, b) =
            colliding_label(&rows).expect("two lanes with one label must be refused");
        assert_eq!(label, "WETH/USDC");
        // Both lanes named, in declaration order: the operator has to know which two of
        // four token addresses to look at.
        assert_eq!((a, b), (U256::from(1), U256::from(3)));
    }

    #[test]
    fn a_startup_failure_names_every_pair_that_failed_not_just_the_first() {
        let results = vec![
            ("WETH/USDC".to_owned(), Ok(1u32)),
            ("WBTC/USDC".to_owned(), Err(eyre!("no price for BTCUSDC"))),
            ("USDC/USDT".to_owned(), Err(eyre!("no price for USDCUSDT"))),
        ];
        let err = format!("{:#}", all_or_none(results).unwrap_err());
        assert!(err.contains("WBTC/USDC"), "unexpected error: {err}");
        assert!(
            err.contains("USDC/USDT"),
            "the second failure costs another 15s timeout to rediscover: {err}"
        );
        assert!(err.contains('2'), "should count the failures: {err}");
    }

    #[test]
    fn all_or_none_returns_every_value_in_order_when_none_failed() {
        let results = vec![
            ("a".to_owned(), Ok(1u32)),
            ("b".to_owned(), Ok(2)),
            ("c".to_owned(), Ok(3)),
        ];
        assert_eq!(all_or_none(results).unwrap(), vec![1, 2, 3]);
    }

    /// The other half of `adopt_metrics`, and the one that is easy to get wrong: a feed pair
    /// must not be adopted into a state the feed rules fire on.
    ///
    /// A feed writes `feed_last_tick`/`feed_sample_current` only when it accepts a *ticker*,
    /// and bookTicker emits on book change — so a quiet pair can go minutes without one, and
    /// a lane cannot be left sitting on the `0` those gauges are registered at while it
    /// waits. `time() - 0 > 30` is every timestamp there has ever been, so PusherFeedStale
    /// would raise a ticket a minute after every start and every reload, on a lane priced
    /// from a sample seconds old. `build_live` already holds that sample; this is it being
    /// written through.
    #[tokio::test]
    async fn a_feed_pair_is_adopted_from_the_sample_it_was_built_on() {
        use ethrex_l2_rpc::signer::{LocalSigner, Signer};

        let secret = secp256k1::SecretKey::from_slice(&[0x22; 32]).unwrap();
        let metrics = metrics::Metrics::new().unwrap();
        let wall = std::time::SystemTime::now();
        let (_tx, rx) = tokio::sync::watch::channel(Some(feed::PriceSample {
            delta: U256::from(100_000_000_000_000u64), // 0.0001 at 1e18
            mid: U256::from(2u64) * U256::exp10(18),
            at: std::time::Instant::now(),
            wall,
        }));
        let live = Live {
            pair: Pair {
                tokens: (Address::zero(), Address::zero()),
                lane: U256::from(3),
                signer: Signer::from(LocalSigner::new(secret)),
                label: "FEED/TEST".to_owned(),
                band: config::MidBand::default(),
            },
            values: ValueSource::feed_for_tests(rx, None, false, 18),
            latch: crate::guard::Latch::new(),
            armed: false,
            guard_kinds: Vec::new(),
            deviation: None,
            metrics: metrics.for_pair("FEED/TEST"),
            venues: Vec::new(),
            observers: Default::default(),
        };

        adopt_metrics(&live, 18, &metrics);

        let m = metrics.for_pair("FEED/TEST");
        assert_eq!(m.feed_up.get(), 1.0, "PusherFeedDown (== 0) must not fire");
        assert_eq!(
            m.feed_sample_current.get(),
            1.0,
            "PusherFeedSampleNotCurrent (== 0) must not fire"
        );
        let age = std::time::SystemTime::now()
            .duration_since(wall)
            .unwrap()
            .as_secs_f64();
        assert!(
            (unix_seconds(wall) - m.feed_last_tick.get()).abs() < 1.0 && age < 30.0,
            "PusherFeedStale (age > 30s) must not fire on a lane just built from this \
             sample; feed_last_tick reads {}",
            m.feed_last_tick.get()
        );
        assert_eq!(
            m.feed_mid.get(),
            2.0,
            "the panel shows the price it started on"
        );
    }

    /// A pair reloaded while its source checks held it withdrawn, into a config that drops
    /// both keys: the new generation has no gate to write the gauge, so adoption lowers it,
    /// or `PusherSourceWithdrawn` would raise a ticket for a pair that is quoting.
    #[tokio::test]
    async fn adoption_lowers_a_source_withdrawal_the_last_generation_left() {
        use crate::source_gate::SourceFault;
        use ethrex_l2_rpc::signer::{LocalSigner, Signer};

        let secret = secp256k1::SecretKey::from_slice(&[0x24; 32]).unwrap();
        let metrics = metrics::Metrics::new().unwrap();
        let before = metrics.for_pair("SRC/TEST");
        before.source_withdrawn(SourceFault::Stale).set(1.0);
        before.source_withdrawn(SourceFault::Divergence).set(1.0);
        let (_tx, rx) = tokio::sync::watch::channel(None);
        let live = Live {
            pair: Pair {
                tokens: (Address::zero(), Address::zero()),
                lane: U256::from(5),
                signer: Signer::from(LocalSigner::new(secret)),
                label: "SRC/TEST".to_owned(),
                band: config::MidBand::default(),
            },
            values: ValueSource::feed_for_tests(rx, None, false, 18),
            latch: crate::guard::Latch::new(),
            armed: false,
            guard_kinds: Vec::new(),
            deviation: None,
            metrics: metrics.for_pair("SRC/TEST"),
            venues: Vec::new(),
            observers: Default::default(),
        };

        adopt_metrics(&live, 18, &metrics);

        for fault in SourceFault::ALL {
            assert_eq!(before.source_withdrawn(fault).get(), 0.0, "{fault:?}");
        }
    }

    fn unix_seconds(at: std::time::SystemTime) -> f64 {
        at.duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs_f64()
    }

    /// A venue's `up` is its feed task's own flag, read when Prometheus scrapes, not a value
    /// written ahead of time. The OKX case on prod: its feed connected before the restarted
    /// pair owned its series, and the one write that said so was dropped, so `up` read 0 for
    /// as long as the socket stayed up. Read at scrape time, there is nothing to drop.
    #[tokio::test]
    async fn a_venues_up_is_read_from_its_feeds_own_flag() {
        use ethrex_l2_rpc::signer::{LocalSigner, Signer};

        let secret = secp256k1::SecretKey::from_slice(&[0x23; 32]).unwrap();
        let metrics = metrics::Metrics::new().unwrap();
        let (_tx, rx) = tokio::sync::watch::channel(None);
        let venue = |name: &str, connected: feed::Connected| {
            let (_tx, rx) = tokio::sync::watch::channel(None);
            crate::composite::VenueFeed {
                venue: name.parse().unwrap(),
                symbol: "ETHUSDT".to_owned(),
                rx,
                metrics: metrics.for_venue("UP/TEST", name),
                connected,
            }
        };
        let live = |okx: bool| Live {
            pair: Pair {
                tokens: (Address::zero(), Address::zero()),
                lane: U256::from(4),
                signer: Signer::from(LocalSigner::new(secret)),
                label: "UP/TEST".to_owned(),
                band: config::MidBand::default(),
            },
            values: ValueSource::feed_for_tests(rx.clone(), None, false, 18),
            latch: crate::guard::Latch::new(),
            armed: false,
            guard_kinds: Vec::new(),
            deviation: None,
            metrics: metrics.for_pair("UP/TEST"),
            venues: vec![
                venue("okx", feed::Connected::with(okx)),
                venue("gate", feed::Connected::with(false)),
            ],
            observers: Default::default(),
        };
        let up = |venue: &str| {
            let text = metrics.render().unwrap();
            let key = format!(r#"quote_updater_venue_feed_up{{pair="UP/TEST",venue="{venue}"}} "#);
            text.lines()
                .find_map(|l| l.strip_prefix(key.as_str()))
                .map(|v| v.parse::<f64>().unwrap())
        };

        adopt_metrics(&live(true), 18, &metrics);
        assert_eq!(
            up("okx"),
            Some(1.0),
            "connected: PusherVenueFeedDown must not fire"
        );
        assert_eq!(up("gate"), Some(0.0));

        // A reload's new generation takes over: its own flags are what is read from then on.
        adopt_metrics(&live(false), 18, &metrics);
        assert_eq!(up("okx"), Some(0.0));
    }

    /// A pair can arrive at adoption already tripped, because its feed has been reading and
    /// its breaker judging since `build_live` dialled it — on a reload, for as long as every
    /// other lane takes to withdraw. Writing a flat 0 over that latch leaves a lane that
    /// halts on its first block with `breaker_tripped` reading 0, and PusherBreakerTripped
    /// is a page on exactly that gauge.
    #[tokio::test]
    async fn a_pair_that_tripped_before_anyone_was_listening_is_adopted_as_tripped() {
        use ethrex_l2_rpc::signer::{LocalSigner, Signer};

        use crate::guard::{Cause, CompositeSample, MarketGuard, Verdict};

        let secret = secp256k1::SecretKey::from_slice(&[0x33; 32]).unwrap();
        let metrics = metrics::Metrics::new().unwrap();
        let latch = crate::guard::Latch::new();
        let mut guard = crate::guard::deviation::DeviationGuard::new(
            crate::breaker::BreakerConfig {
                threshold_scaled: Some(feed::parse_decimal_scaled("0.02", 18).unwrap()),
                window: None,
                decimals: 18,
            },
            None,
        );
        let readout = guard.readout();
        // What the composite does while nobody owns the series: the first sample arms the
        // guard, the second is a 10% jump, and its verdict trips the latch.
        for mid in [100u64, 110] {
            let sample = CompositeSample::new(
                std::time::Instant::now(),
                U256::from(mid) * U256::exp10(18),
                U256::zero(),
                false,
                18,
                Vec::new(),
            );
            if let Verdict::Trip(reason) =
                guard.assess(&sample, &mut crate::pricing::Diagnostics::new(0))
            {
                latch.trip("deviation", Cause::Guard, reason);
            }
        }
        assert!(latch.tripped().is_some(), "the fixture must actually latch");

        let (_tx, rx) = tokio::sync::watch::channel(Some(feed::PriceSample {
            delta: U256::from(100_000_000_000_000u64),
            mid: U256::from(110u64) * U256::exp10(18),
            at: std::time::Instant::now(),
            wall: std::time::SystemTime::now(),
        }));
        let live = Live {
            pair: Pair {
                tokens: (Address::zero(), Address::zero()),
                lane: U256::from(9),
                signer: Signer::from(LocalSigner::new(secret)),
                label: "TRIP/TEST".to_owned(),
                band: config::MidBand::default(),
            },
            values: ValueSource::feed_for_tests(rx, None, false, 18),
            latch,
            armed: true,
            guard_kinds: Vec::new(),
            deviation: Some(readout),
            metrics: metrics.for_pair("TRIP/TEST"),
            venues: Vec::new(),
            observers: Default::default(),
        };

        adopt_metrics(&live, 18, &metrics);

        let m = metrics.for_pair("TRIP/TEST");
        assert_eq!(
            m.breaker_tripped.get(),
            1.0,
            "the lane will halt on its first block; PusherBreakerTripped has to be able to \
             say so"
        );
        assert_eq!(
            m.breaker_trips.get(),
            1,
            "and the transition is counted once, here, since the feed was gated when it \
             happened"
        );
        assert_eq!(
            metrics.trip_source("TRIP/TEST", "deviation", "guard").get(),
            1.0,
            "and the source series says what tripped it"
        );
        assert!(
            m.breaker_deviation_ratio.get() > 1.0,
            "the move that tripped it is 10% against a 2% limit"
        );
    }

    /// The window between the seed and the switch: the seed reads an armed latch and writes
    /// `breaker_tripped` 0, the composite trips the latch before the switch is thrown, so
    /// the latch's own write is still gated off, and the composite judges a tripped lane no
    /// further, so no later sample records it either. The lane then halts on its first block
    /// with `breaker_tripped` reading 0, and PusherBreakerTripped never fires.
    ///
    /// Driven deterministically through `adopt` itself: the seed reads the market's channel
    /// after it reads the latch, so the test holds that channel's write lock, waits for the
    /// seed to have passed the latch (its next write, the runway's +Inf, says so), and trips
    /// the latch there.
    #[test]
    fn a_trip_between_the_seed_and_the_switch_is_recorded_once_the_lane_owns_its_series() {
        use ethrex_l2_rpc::signer::{LocalSigner, Signer};

        let secret = secp256k1::SecretKey::from_slice(&[0x44; 32]).unwrap();
        let metrics = std::sync::Arc::new(metrics::Metrics::new().unwrap());
        let pair_metrics = metrics.for_pair("RACE/TEST");
        let adopted = feed::Adoption::new();
        let latch = crate::guard::Latch::new();
        latch.attach_metrics(feed::Gated::new(pair_metrics.clone(), adopted.clone()));
        let (tx, rx) = tokio::sync::watch::channel(Some(feed::PriceSample {
            delta: U256::zero(),
            mid: U256::exp10(18),
            at: std::time::Instant::now(),
            wall: std::time::SystemTime::now(),
        }));
        let live = Live {
            pair: Pair {
                tokens: (Address::zero(), Address::zero()),
                lane: U256::from(9),
                signer: Signer::from(LocalSigner::new(secret)),
                label: "RACE/TEST".to_owned(),
                band: config::MidBand::default(),
            },
            values: ValueSource::feed_for_tests(rx, None, false, 18),
            latch: latch.clone(),
            armed: true,
            guard_kinds: Vec::new(),
            deviation: None,
            metrics: pair_metrics.clone(),
            venues: Vec::new(),
            observers: Default::default(),
        };

        let mut adopting = None;
        tx.send_modify(|_| {
            adopting = Some({
                let (live, adopted, metrics) = (live.clone(), adopted.clone(), metrics.clone());
                std::thread::spawn(move || adopt(&live, &adopted, 18, &metrics))
            });
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
            while pair_metrics.signer_runway_updates.get() != f64::INFINITY {
                assert!(
                    std::time::Instant::now() < deadline,
                    "the seed never got past the latch"
                );
                std::thread::yield_now();
            }
            assert_eq!(
                pair_metrics.breaker_tripped.get(),
                0.0,
                "the seed read it armed"
            );
            assert!(latch.trip(
                "deviation",
                crate::guard::Cause::Guard,
                crate::guard::TripReason::new("moved 4%")
            ));
            // The channel's lock is released when this returns: the seed reads the sample,
            // finishes, and the switch is thrown.
        });
        adopting.unwrap().join().expect("adopt ran");
        assert_eq!(
            pair_metrics.breaker_tripped.get(),
            1.0,
            "a trip between the seed and the switch was recorded by nobody: the lane halts \
             with breaker_tripped reading 0 and PusherBreakerTripped never fires"
        );
        assert_eq!(
            pair_metrics.breaker_trips.get(),
            1,
            "one trip, one transition"
        );
        assert_eq!(
            metrics.trip_source("RACE/TEST", "deviation", "guard").get(),
            1.0
        );
    }

    /// The C1 regression test: a `Static`-source pair has no feed task and never runs
    /// `drive`'s runway check, so before this fix `signer_runway_updates` and the three
    /// `feed_*` gauges would sit at the `0` `for_pair` pre-bound them to forever — a value
    /// three ticket rules and one page rule would fire on, on a pair that is working
    /// exactly as `config.example.toml` documents it can. `adopt_metrics` now initializes
    /// them to values no threshold comparison in `quote-updater.rules.yml` can mistake for a
    /// recorded reading; this reads the real exposition back to prove it, rather than
    /// asserting against `PairMetrics` fields directly (see `sample_value`'s doc comment in
    /// `metrics.rs` for why that distinction matters).
    ///
    /// Driven through `adopt_metrics` rather than `build_live` because that is where these
    /// writes live now: a reload dials a replacement feed before stopping the pair it
    /// replaces, so seeding a lane's gauges at build time would write them onto whatever is
    /// still quoting under that label. Same values, written at the moment the service takes
    /// the pair on.
    #[tokio::test]
    async fn a_static_pairs_live_carries_no_value_the_feed_or_signer_rules_would_fire_on() {
        use ethrex_l2_rpc::signer::{LocalSigner, Signer};

        let secret = secp256k1::SecretKey::from_slice(&[0x11; 32]).unwrap();
        let resolved = ResolvedPair {
            pair: Pair {
                tokens: (
                    Address::from_slice(&[0xa0; 20]),
                    Address::from_slice(&[0xda; 20]),
                ),
                lane: U256::from(7),
                signer: Signer::from(LocalSigner::new(secret)),
                label: "STATIC/TEST".to_owned(),
                band: config::MidBand {
                    min: None,
                    max: None,
                },
            },
            pricing: config::PricingSpec::fixed_for_tests("1", "0"),
            feeds: None,
            invert: false,
            breaker: None,
            guards: Vec::new(),
        };
        let metrics = metrics::Metrics::new().unwrap();
        // binance_ws is never dialled for a pair without feeds: build_live only connects
        // a market when the pair has one.
        let client = EthClient::new(Url::parse("http://127.0.0.1:1").unwrap()).unwrap();
        let built = build_live(
            resolved,
            &venue::Endpoints::single(venue::VenueId::Binance, "ws://unused".to_owned()),
            18,
            &metrics,
            (&client, Address::zero()),
            &volatile::Histories::default(),
            &vault::InventoryReaders::default(),
            None,
            &crate::kinds::Kinds::shipped(),
        )
        .await
        .unwrap();
        // What an earlier build on this label left: a custom kind's reading at the value it
        // last published. This lane is another kind now; a rebuild under a renamed
        // diagnostic, or under the same names, must not export the old value as its own
        // either, nor record it into its first quote's terms.
        let earlier = metrics.diagnostic("STATIC/TEST", "skewed", "funding");
        earlier.set(0.25);
        adopt_metrics(&built.live, 18, &metrics);
        assert!(
            earlier.get().is_nan(),
            "an earlier build's diagnostic stands at {}",
            earlier.get()
        );
        let text = metrics.render().unwrap();
        assert!(
            // The `prometheus` crate's text encoder renders `f64::INFINITY` as `inf`
            // (lowercase, no sign) rather than the exposition format's own `+Inf`; `promtool`
            // accepts either spelling on the way in, which is the direction that matters.
            text.contains(r#"quote_updater_signer_runway_updates{pair="STATIC/TEST"} inf"#),
            "the Runway panel must read ∞ here, not a key with nothing left:\n{text}"
        );
        assert!(
            text.contains(r#"quote_updater_signer_balance_wei{pair="STATIC/TEST"} NaN"#),
            "PusherSignerNearlyDry (predict_linear over this series) must not be able to \
             fire on this pair, and the panel must not read an empty key: {text}"
        );
        assert!(
            text.contains(r#"quote_updater_feed_up{pair="STATIC/TEST"} NaN"#),
            "PusherFeedDown (== 0) must not be able to fire on this pair:\n{text}"
        );
        assert!(
            text.contains(
                r#"quote_updater_feed_last_tick_timestamp_seconds{pair="STATIC/TEST"} NaN"#
            ),
            "PusherFeedStale (time() - x > 30) must not be able to fire on this pair:\n{text}"
        );
        assert!(
            text.contains(r#"quote_updater_feed_sample_current{pair="STATIC/TEST"} NaN"#),
            "PusherFeedSampleNotCurrent (== 0) must not be able to fire on this pair:\n{text}"
        );
        // Keep `live` alive through the assertions above rather than dropping it early —
        // its `metrics` field is the same handle `metrics.render()` reads back through.
        drop(built);
    }

    /// `--check` judges a pricer's preview by the core's backstop, with the same market and
    /// shape the run's every tick gets, so its row cannot pass what the run would withdraw:
    /// a pricer whose preview spreads a whole unit is refused there as it would be here.
    #[test]
    fn a_preview_the_backstop_would_withdraw_is_refused_in_the_check_row() {
        use crate::pricing::{PricerOutput, Refusal};

        struct WholeUnit;
        impl Pricer for WholeUnit {
            fn price(
                &mut self,
                tick: &TickCtx,
                _: &mut Diagnostics,
            ) -> Result<PricerOutput, Refusal> {
                let market = tick.market()?;
                Ok(PricerOutput::new(U256::exp10(18), market.mid))
            }
        }
        let shape = PairShape {
            label: "WETH/USDC".to_owned(),
            tokens: (Address::repeat_byte(1), Address::repeat_byte(2)),
            lane: U256::one(),
            inverted: false,
            price_decimals: 18,
            target: Address::zero(),
            sources: 1,
            min_mid: None,
            max_mid: None,
        };
        let sample = feed::PriceSample {
            mid: U256::from(4000u64) * U256::exp10(18),
            delta: U256::exp10(14),
            at: std::time::Instant::now(),
            wall: std::time::SystemTime::now(),
        };
        let (row, _) = preview_row(&mut WholeUnit, Some(&sample), &shape, 0);
        let refused = row.unwrap_err();
        assert!(refused.contains("not below one whole unit"), "{refused}");
    }

    /// A pricer's background task that panics is reported with its panic, so the lane's
    /// warning says what happened and not only that the task is gone.
    #[tokio::test]
    async fn a_background_task_reports_how_it_ended() {
        assert_eq!(background_ended(Box::pin(async {})).await, TaskEnd::Ended);
        assert_eq!(
            background_ended(Box::pin(async { panic!("lost the socket") })).await,
            TaskEnd::Panicked("lost the socket".to_owned())
        );
    }

    mod custom {
        use std::sync::{
            Arc,
            atomic::{AtomicBool, AtomicUsize, Ordering},
        };

        use super::*;
        use crate::{
            kinds::{Kinds, fn_factory},
            pricing::{
                BuildCtx, DiagHandle, Diagnostics, PairShape, Pricer, PricerOutput, Refusal,
                RefusalHandle, TickCtx,
            },
        };

        fn shape() -> PairShape {
            PairShape {
                sources: 1,
                min_mid: None,
                max_mid: None,
                label: "CUSTOM/TEST".into(),
                tokens: (Address::repeat_byte(1), Address::repeat_byte(2)),
                lane: U256::from(9),
                inverted: false,
                price_decimals: 18,
                target: Address::repeat_byte(3),
            }
        }

        fn stanza(kind: &str) -> toml::Table {
            toml::from_str(&format!("kind = \"{kind}\"")).unwrap()
        }

        /// Market guards are built from the pair's stanzas in file order, on the market
        /// side only: a quote-side kind in the list is left for the quote guards, and an
        /// unregistered kind fails the build. `build_live` puts the deviation guard first.
        #[tokio::test]
        async fn market_guard_stanzas_build_in_file_order_on_their_own_side() {
            use crate::{
                config::GuardStanza,
                kinds::{market_guard_fn, quote_guard_fn},
            };
            struct Pass;
            impl crate::guard::MarketGuard for Pass {
                fn assess(
                    &mut self,
                    _: &crate::guard::CompositeSample,
                    _: &mut Diagnostics,
                ) -> crate::guard::Verdict {
                    crate::guard::Verdict::Pass
                }
            }
            struct Allow;
            impl crate::guard::QuoteGuard for Allow {
                fn check(
                    &mut self,
                    _: &crate::guard::Candidate<'_>,
                    _: &mut Diagnostics,
                ) -> crate::guard::Gate {
                    crate::guard::Gate::Allow
                }
            }
            let mut kinds = Kinds::default();
            kinds.insert_guard(
                "cap",
                market_guard_fn(|_: NoConfig, _: &mut BuildCtx| Ok(Pass)),
            );
            kinds.insert_guard(
                "dispersion",
                market_guard_fn(|_: NoConfig, _: &mut BuildCtx| Ok(Pass)),
            );
            kinds.insert_guard(
                "tally",
                quote_guard_fn(|_: NoConfig, _: &mut BuildCtx| Ok(Allow)),
            );
            let stanzas: Vec<GuardStanza> = ["dispersion", "tally", "cap"]
                .iter()
                .map(|kind| GuardStanza {
                    kind: kind.to_string(),
                    config: toml::from_str(&format!("kind = \"{kind}\"")).unwrap(),
                })
                .collect();
            let metrics = metrics::Metrics::new().unwrap();
            let market = build_market_guards(&kinds, &stanzas, || BuildCtx::new(shape()), &metrics)
                .await
                .unwrap();
            assert_eq!(
                market.iter().map(|g| g.kind).collect::<Vec<_>>(),
                ["dispersion", "cap"]
            );
            let quote = build_quote_guards(&kinds, &stanzas, || BuildCtx::new(shape()), &metrics)
                .await
                .unwrap();
            assert_eq!(quote.iter().map(|g| g.kind).collect::<Vec<_>>(), ["tally"]);
            let unknown = vec![GuardStanza {
                kind: "nope".to_owned(),
                config: toml::from_str("kind = \"nope\"").unwrap(),
            }];
            let err =
                match build_market_guards(&kinds, &unknown, || BuildCtx::new(shape()), &metrics)
                    .await
                {
                    Err(err) => format!("{err:#}"),
                    Ok(_) => panic!("an unregistered kind must not build"),
                };
            assert!(err.contains("unknown guard kind `nope`"), "{err}");
        }

        /// A background task that panics trips the lane with `cause = panic`, naming the
        /// pricer's kind: whatever it fed is not coming back, and the operator gets a halt
        /// with a reason rather than a value quietly ageing out.
        #[tokio::test]
        async fn a_background_task_that_panics_trips_the_lane() {
            use crate::guard::Cause;
            let mut kinds = Kinds::default();
            kinds.insert(
                "fragile",
                fn_factory(|_: NoConfig, ctx: &mut BuildCtx| {
                    ctx.spawn(async {
                        tokio::task::yield_now().await;
                        panic!("lost the socket");
                    });
                    Ok(Idle)
                }),
            );
            let metrics = metrics::Metrics::new().unwrap();
            let latch = crate::guard::Latch::new();
            let values = build_custom(
                &kinds,
                "fragile",
                &stanza("fragile"),
                shape(),
                None,
                &metrics,
                no_chain(),
                None,
                latch.clone(),
                &Default::default(),
                Arc::new(reqwest::Client::new()),
            )
            .await
            .unwrap();
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            let trip = latch.tripped().expect("the task's panic trips the lane");
            assert_eq!((trip.source, trip.cause), ("fragile", Cause::Panic));
            assert_eq!(
                trip.reason.alarm,
                "a background task of `fragile` panicked: lost the socket"
            );
            drop(values);
        }

        /// A component trips its own lane through the latch its build got: a model that
        /// sees its own inputs go bad stops the lane itself, with `cause = external`. (A
        /// kill switch is a halt in the file, never this: a reload re-arms a latch.) A task
        /// that then returns is counted, since it fed nothing after.
        #[tokio::test]
        async fn a_component_can_trip_its_own_lane() {
            use crate::guard::{Cause, TripReason};
            let mut kinds = Kinds::default();
            kinds.insert(
                "watch",
                fn_factory(|_: NoConfig, ctx: &mut BuildCtx| {
                    let latch = ctx.latch();
                    ctx.spawn(async move {
                        latch.trip(
                            "watch",
                            Cause::External,
                            TripReason::new("the oracle disagreed for 5 blocks"),
                        );
                    });
                    Ok(Idle)
                }),
            );
            let metrics = metrics::Metrics::new().unwrap();
            let latch = crate::guard::Latch::new();
            let values = build_custom(
                &kinds,
                "watch",
                &stanza("watch"),
                shape(),
                None,
                &metrics,
                no_chain(),
                None,
                latch.clone(),
                &Default::default(),
                Arc::new(reqwest::Client::new()),
            )
            .await
            .unwrap();
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            let trip = latch.tripped().expect("tripped by the component");
            assert_eq!((trip.source, trip.cause), ("watch", Cause::External));
            assert_eq!(trip.reason.alarm, "the oracle disagreed for 5 blocks");
            assert!(
                metrics.render().unwrap().contains(
                    r#"quote_updater_extension_task_exits_total{kind="watch",pair="CUSTOM/TEST"} 1"#
                ),
                "a task that returned is counted"
            );
            drop(values);
        }

        /// A chain nothing answers on, for a pricer that never reads it.
        fn no_chain() -> Chain {
            Chain::new(
                EthClient::new(url::Url::parse("http://127.0.0.1:1").unwrap()).unwrap(),
                Address::repeat_byte(3),
            )
        }

        /// Reads the vault in its build and publishes the base balance as its mid: what
        /// comes out is what went through `ctx.inventory()`, the chain and the mock.
        struct Vaulted {
            inventory: crate::pricing::InventoryFeed,
        }

        impl Pricer for Vaulted {
            fn price(&mut self, _: &TickCtx, _: &mut Diagnostics) -> Result<PricerOutput, Refusal> {
                let reading = self.inventory.latest();
                Ok(PricerOutput {
                    delta: U256::one(),
                    mid: U256::from(reading.base as u64) * U256::exp10(18),
                })
            }
        }

        struct VaultedFactory;

        impl crate::pricing::Factory for VaultedFactory {
            type Config = NoConfig;
            type Pricer = Vaulted;

            fn build<'a>(
                &'a self,
                _: &'a NoConfig,
                ctx: &'a mut BuildCtx,
            ) -> crate::pricing::BoxFuture<'a, Result<Vaulted>> {
                Box::pin(async move {
                    Ok(Vaulted {
                        inventory: ctx.inventory().await?,
                    })
                })
            }
        }

        /// A custom pricer reads the vault through the build context the way the volatile
        /// model does: decimals, `vaultFor`, both balances, through the run's chain. The
        /// base token answers 6 decimals, so the mock's 1e24 raw is 1e18 whole tokens: the
        /// scaling by decimals went through as well.
        #[tokio::test]
        async fn a_custom_pricer_reads_the_vault_through_its_build_context() {
            let rpc = crate::rpc_mock::MockRpc::spawn(100).await;
            rpc.set_vault(Address::repeat_byte(0xaa));
            rpc.set_balance(Address::repeat_byte(0xaa), U256::exp10(24));
            rpc.set_decimals(Address::repeat_byte(1), 6);
            let client = EthClient::new(url::Url::parse(&rpc.url).unwrap()).unwrap();
            let mut kinds = Kinds::default();
            kinds.insert("vaulted", Arc::new(VaultedFactory));
            let metrics = metrics::Metrics::new().unwrap();
            let values = build_custom(
                &kinds,
                "vaulted",
                &stanza("vaulted"),
                shape(),
                None,
                &metrics,
                Chain::new(client, Address::repeat_byte(3)),
                None,
                crate::guard::Latch::new(),
                &Default::default(),
                Arc::new(reqwest::Client::new()),
            )
            .await
            .unwrap();
            let priced = values
                .current_detailed(&config::MidBand::default())
                .unwrap();
            assert_eq!(priced.mid, U256::exp10(18) * U256::exp10(18));
            assert!(
                rpc.calls("eth_call").len() >= 5,
                "decimals twice, vaultFor, balanceOf twice: {} calls",
                rpc.calls("eth_call").len()
            );
        }

        /// Two lanes reading one vault share one poller: the second build finds the
        /// first's reader alive and takes a handle to it, so the chain is read once per
        /// refresh, not twice. Dropping one lane leaves the reader polling for the other;
        /// dropping both stops it, since nothing holds a receiver. On the real clock, at a
        /// 100ms refresh, judged by rate: a refresh is three reads (`vaultFor` and two
        /// `balanceOf`), so two pollers would read at twice the rate.
        #[tokio::test]
        async fn two_lanes_on_one_vault_share_one_poller() {
            let rpc = crate::rpc_mock::MockRpc::spawn(100).await;
            rpc.set_vault(Address::repeat_byte(0xaa));
            rpc.set_balance(Address::repeat_byte(0xaa), U256::exp10(24));
            let client = EthClient::new(url::Url::parse(&rpc.url).unwrap()).unwrap();
            let readers = crate::vault::InventoryReaders::default()
                .with_refresh(std::time::Duration::from_millis(100));
            let mut kinds = Kinds::default();
            kinds.insert("vaulted", Arc::new(VaultedFactory));
            let metrics = metrics::Metrics::new().unwrap();
            let mut lanes = Vec::new();
            for _ in 0..2 {
                lanes.push(
                    build_custom(
                        &kinds,
                        "vaulted",
                        &stanza("vaulted"),
                        shape(),
                        None,
                        &metrics,
                        Chain::new(client.clone(), Address::repeat_byte(3)),
                        None,
                        crate::guard::Latch::new(),
                        &readers,
                        Arc::new(reqwest::Client::new()),
                    )
                    .await
                    .unwrap(),
                );
            }
            // The second build found the first's reader: one connect (two decimals, a
            // vaultFor, two balanceOf), not two.
            assert_eq!(
                rpc.calls("eth_call").len(),
                5,
                "the second lane took the first's reader"
            );
            let calls = || rpc.calls("eth_call").len();
            let window = std::time::Duration::from_secs(1);

            let before = calls();
            tokio::time::sleep(window).await;
            let per_second = calls() - before;
            assert!(
                (15..=45).contains(&per_second),
                "one poller at 100ms is ~30 reads a second, two would be ~60: got {per_second}"
            );

            drop(lanes.remove(0));
            let before = calls();
            tokio::time::sleep(window / 2).await;
            assert!(
                calls() - before >= 6,
                "the survivor keeps the shared reader polling"
            );

            drop(lanes);
            // The poller sees no receiver on its next tick and ends; whatever was in flight
            // lands first.
            tokio::time::sleep(std::time::Duration::from_millis(300)).await;
            let before = calls();
            tokio::time::sleep(window / 2).await;
            assert_eq!(calls(), before, "no lane left: the poller stopped");
        }

        /// Every lane's build context offers the run's one HTTP client (`ctx.http()`), so
        /// pricers and the polled venues share a connection pool: the client two lanes get
        /// is the same client, not two. A bare context, a test's, offers a default one.
        #[tokio::test]
        async fn the_build_context_offers_one_http_client_to_every_lane() {
            struct Flat;
            impl Pricer for Flat {
                fn price(
                    &mut self,
                    _: &TickCtx,
                    _: &mut Diagnostics,
                ) -> Result<PricerOutput, Refusal> {
                    Ok(PricerOutput::new(U256::exp10(14), U256::exp10(18)))
                }
            }
            let grabbed: Arc<std::sync::Mutex<Vec<Arc<reqwest::Client>>>> = Arc::default();
            let sink = Arc::clone(&grabbed);
            let mut kinds = Kinds::default();
            kinds.insert(
                "http",
                crate::kinds::fn_factory(move |_: NoConfig, ctx: &mut BuildCtx| {
                    sink.lock().unwrap().push(
                        ctx.http_shared()
                            .expect("a run's context carries the run's client"),
                    );
                    // The public accessor is what a pricer keeps: a client of its own to hold.
                    let _client: reqwest::Client = ctx.http();
                    Ok(Flat)
                }),
            );
            let rpc = crate::rpc_mock::MockRpc::spawn(100).await;
            let client = EthClient::new(url::Url::parse(&rpc.url).unwrap()).unwrap();
            let metrics = metrics::Metrics::new().unwrap();
            let http = Arc::new(reqwest::Client::new());
            for _ in 0..2 {
                build_custom(
                    &kinds,
                    "http",
                    &stanza("http"),
                    shape(),
                    None,
                    &metrics,
                    Chain::new(client.clone(), Address::repeat_byte(3)),
                    None,
                    crate::guard::Latch::new(),
                    &crate::vault::InventoryReaders::default(),
                    Arc::clone(&http),
                )
                .await
                .unwrap();
            }
            let grabbed = grabbed.lock().unwrap();
            assert_eq!(grabbed.len(), 2);
            assert!(
                grabbed.iter().all(|got| Arc::ptr_eq(got, &http)),
                "both lanes were handed the run's client"
            );
            let bare = BuildCtx::new(shape());
            assert!(bare.http_shared().is_none(), "a test's context has no run");
            let _default: reqwest::Client = bare.http();
        }

        /// The chain a build context offers answers `eth_call` through the run's client and
        /// its timeout: the mock's `symbol()` comes back ABI-encoded, undecoded.
        #[tokio::test]
        async fn a_build_context_offers_the_chain() {
            let rpc = crate::rpc_mock::MockRpc::spawn(100).await;
            let token = Address::repeat_byte(1);
            rpc.set_symbol(token, "WETH");
            let client = EthClient::new(url::Url::parse(&rpc.url).unwrap()).unwrap();
            let ctx =
                BuildCtx::new(shape()).with_chain(Chain::new(client, Address::repeat_byte(3)));
            let calldata = ethrex_l2_sdk::calldata::encode_calldata("symbol()", &[]).unwrap();
            let answer = ctx.chain().unwrap().call(token, calldata).await.unwrap();
            assert!(
                answer.windows(4).any(|w| w == b"WETH"),
                "{}",
                hex::encode(&answer)
            );
            assert!(
                BuildCtx::new(shape()).chain().is_err(),
                "a bare context has no chain"
            );
        }

        /// The chain encodes a call from its signature and the re-exported `Value`s, so a
        /// downstream needs no ethrex pin of its own to read a contract.
        #[tokio::test]
        async fn the_chain_encodes_a_call_from_its_signature() {
            let rpc = crate::rpc_mock::MockRpc::spawn(100).await;
            let token = Address::repeat_byte(1);
            rpc.set_symbol(token, "WETH");
            let client = EthClient::new(url::Url::parse(&rpc.url).unwrap()).unwrap();
            let chain = Chain::new(client, Address::repeat_byte(3));
            let answer = chain.call_sig(token, "symbol()", &[]).await.unwrap();
            assert!(
                answer.windows(4).any(|w| w == b"WETH"),
                "{}",
                hex::encode(&answer)
            );
            let err = chain
                .call_sig(
                    token,
                    "not a signature",
                    &[crate::calldata::Value::Uint(U256::one())],
                )
                .await
                .unwrap_err();
            assert!(format!("{err:#}").contains("not a signature"), "{err:#}");
        }

        /// `--check` builds every guard as the run would, so a guard the run refuses (a
        /// quote guard reading a diagnostic the pair's pricer never declares) fails the
        /// check instead of passing it and refusing the start.
        #[tokio::test]
        async fn the_check_builds_the_guards_and_reports_one_that_cannot_be() {
            use crate::kinds::quote_guard_fn;
            struct Allow;
            impl crate::guard::QuoteGuard for Allow {
                fn check(
                    &mut self,
                    _: &crate::guard::Candidate<'_>,
                    _: &mut Diagnostics,
                ) -> crate::guard::Gate {
                    crate::guard::Gate::Allow
                }
            }
            let mut kinds = Kinds::shipped();
            kinds.insert_guard(
                "share_band",
                quote_guard_fn(|_: NoConfig, ctx: &mut BuildCtx| {
                    ctx.read_diagnostic("base_share")?;
                    Ok(Allow)
                }),
            );
            let toml = "target = \"0x70997970C51812dc3A010C7d01b50e0d17dc79C8\"\n\n\
                        [[pairs]]\n\
                        tokens = [\"0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48\", \
                        \"0xdAC17F958D2ee523a2206206994597C13D831ec7\"]\n\
                        key_env = \"K\"\npricing = { kind = \"fixed\", mid = \"1.0001\", delta = \"0\" }\n\n\
                        [[pairs.guards]]\nkind = \"share_band\"\n";
            let config = crate::config::parse_config(toml, 18).unwrap();
            let metrics = metrics::Metrics::new().unwrap();
            let client = EthClient::new(Url::parse("http://127.0.0.1:1").unwrap()).unwrap();
            let report = preflight::Report {
                target: Address::zero(),
                registry: Address::zero(),
                base_fee: None,
                warnings: Vec::new(),
                rows: Vec::new(),
            };
            let rows = collect_prices(
                &config,
                &report,
                &venue::Endpoints::single(venue::VenueId::Binance, "ws://unused".to_owned()),
                18,
                &metrics,
                (&client, Address::zero()),
                &kinds,
            )
            .await;
            assert!(
                rows[0].sample.is_ok(),
                "a static pair prices without a chain: {:?}",
                rows[0].sample
            );
            let err = rows[0]
                .guard_error
                .as_deref()
                .expect("the guard that cannot build is reported");
            assert!(
                err.contains("share_band") && err.contains("base_share"),
                "{err}"
            );
            assert!(!preflight::prices_ok(&rows));
        }

        /// `--check` validates every pair's guard stanzas against the pair, as the run does
        /// before it builds a lane, whatever prices the pair: a guard whose `validate`
        /// refuses a fixed or feed pair fails the check under its row, rather than passing
        /// it and refusing the start.
        #[tokio::test]
        async fn the_check_validates_the_guards_of_a_pair_that_is_not_custom() {
            struct Allow;
            impl crate::guard::QuoteGuard for Allow {
                fn check(
                    &mut self,
                    _: &crate::guard::Candidate<'_>,
                    _: &mut Diagnostics,
                ) -> crate::guard::Gate {
                    crate::guard::Gate::Allow
                }
            }
            /// Refuses every pair in `validate`, and would build if asked.
            struct InvertedOnly;
            impl crate::pricing::QuoteGuardFactory for InvertedOnly {
                type Config = NoConfig;
                type Guard = Allow;
                fn validate(&self, _: &NoConfig, pair: &PairShape) -> eyre::Result<()> {
                    eyre::ensure!(pair.inverted, "this guard judges inverted lanes only");
                    Ok(())
                }
                fn build<'a>(
                    &'a self,
                    _: &'a NoConfig,
                    _: &'a mut BuildCtx,
                ) -> futures_util::future::BoxFuture<'a, eyre::Result<Allow>> {
                    Box::pin(async { Ok(Allow) })
                }
            }
            let mut kinds = Kinds::shipped();
            kinds.insert_guard(
                "inverted_only",
                crate::kinds::quote_guard_factory(InvertedOnly),
            );
            let toml = "target = \"0x70997970C51812dc3A010C7d01b50e0d17dc79C8\"\n\n\
                        [[pairs]]\n\
                        tokens = [\"0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48\", \
                        \"0xdAC17F958D2ee523a2206206994597C13D831ec7\"]\n\
                        key_env = \"K\"\npricing = { kind = \"fixed\", mid = \"1.0001\", delta = \"0\" }\n\n\
                        [[pairs.guards]]\nkind = \"inverted_only\"\n";
            let config = crate::config::parse_config(toml, 18).unwrap();
            let metrics = metrics::Metrics::new().unwrap();
            let client = EthClient::new(Url::parse("http://127.0.0.1:1").unwrap()).unwrap();
            let report = preflight::Report {
                target: Address::zero(),
                registry: Address::zero(),
                base_fee: None,
                warnings: Vec::new(),
                rows: Vec::new(),
            };
            let rows = collect_prices(
                &config,
                &report,
                &venue::Endpoints::single(venue::VenueId::Binance, "ws://unused".to_owned()),
                18,
                &metrics,
                (&client, Address::zero()),
                &kinds,
            )
            .await;
            assert!(
                rows[0].sample.is_ok(),
                "the price is fine: {:?}",
                rows[0].sample
            );
            let err = rows[0]
                .guard_error
                .as_deref()
                .expect("the stanza the run would refuse is reported");
            assert!(
                err.contains("inverted_only") && err.contains("inverted lanes only"),
                "{err}"
            );
            assert!(!preflight::prices_ok(&rows));
        }

        /// A quote guard binds to what the pair's pricer declared, so when the pricer's
        /// own build failed the row says that and nothing else: a second line about a
        /// diagnostic "declared: none" would repeat the first failure in other words.
        #[tokio::test]
        async fn a_pricer_that_does_not_build_leaves_its_quote_guards_unjudged() {
            use crate::kinds::quote_guard_fn;
            struct Never;
            impl Pricer for Never {
                fn price(
                    &mut self,
                    _: &TickCtx,
                    _: &mut Diagnostics,
                ) -> Result<PricerOutput, Refusal> {
                    unreachable!("never built")
                }
            }
            struct Allow;
            impl crate::guard::QuoteGuard for Allow {
                fn check(
                    &mut self,
                    _: &crate::guard::Candidate<'_>,
                    _: &mut Diagnostics,
                ) -> crate::guard::Gate {
                    crate::guard::Gate::Allow
                }
            }
            let mut kinds = Kinds::default();
            kinds.insert(
                "broken",
                fn_factory(|_: NoConfig, _: &mut BuildCtx| -> eyre::Result<Never> {
                    Err(eyre::eyre!("no oracle answers"))
                }),
            );
            kinds.insert_guard(
                "share_band",
                quote_guard_fn(|_: NoConfig, ctx: &mut BuildCtx| {
                    ctx.read_diagnostic("base_share")?;
                    Ok(Allow)
                }),
            );
            let toml = "target = \"0x70997970C51812dc3A010C7d01b50e0d17dc79C8\"\n\n\
                        [[pairs]]\n\
                        tokens = [\"0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48\", \
                        \"0xdAC17F958D2ee523a2206206994597C13D831ec7\"]\n\
                        key_env = \"K\"\n\n\
                        [pairs.pricing]\nkind = \"broken\"\n\n\
                        [[pairs.guards]]\nkind = \"share_band\"\npricing = { kind = \"feed\", delta = \"0.0002\" }\n";
            let config = crate::config::parse_config(toml, 18).unwrap();
            let metrics = metrics::Metrics::new().unwrap();
            let client = EthClient::new(Url::parse("http://127.0.0.1:1").unwrap()).unwrap();
            let report = preflight::Report {
                target: Address::zero(),
                registry: Address::zero(),
                base_fee: None,
                warnings: Vec::new(),
                rows: Vec::new(),
            };
            let rows = collect_prices(
                &config,
                &report,
                &venue::Endpoints::single(venue::VenueId::Binance, "ws://unused".to_owned()),
                18,
                &metrics,
                (&client, Address::zero()),
                &kinds,
            )
            .await;
            let err = rows[0].sample.as_ref().unwrap_err();
            assert!(err.contains("no oracle answers"), "{err}");
            assert_eq!(
                rows[0].guard_error, None,
                "the pricer's failure is the row's one problem"
            );
        }

        fn two() -> U256 {
            U256::from(2u64) * U256::exp10(18)
        }

        #[derive(Clone, serde::Deserialize)]
        struct NoConfig {}

        struct Const {
            calls: DiagHandle,
            n: f64,
        }

        impl Pricer for Const {
            fn price(
                &mut self,
                _: &TickCtx,
                out: &mut Diagnostics,
            ) -> Result<PricerOutput, Refusal> {
                self.n += 1.0;
                out.set(&self.calls, self.n);
                Ok(PricerOutput {
                    delta: U256::one(),
                    mid: two(),
                })
            }
        }

        #[tokio::test]
        async fn a_custom_pair_quotes_through_its_pricer_and_reports_its_diagnostics() {
            let mut kinds = Kinds::default();
            kinds.insert(
                "const",
                fn_factory(|_: NoConfig, ctx: &mut BuildCtx| {
                    Ok(Const {
                        calls: ctx.diagnostic("calls")?,
                        n: 0.0,
                    })
                }),
            );
            let metrics = metrics::Metrics::new().unwrap();
            let values = build_custom(
                &kinds,
                "const",
                &stanza("const"),
                shape(),
                None,
                &metrics,
                no_chain(),
                None,
                crate::guard::Latch::new(),
                &Default::default(),
                Arc::new(reqwest::Client::new()),
            )
            .await
            .unwrap();
            let priced = values
                .current_detailed(&config::MidBand::default())
                .unwrap();
            assert_eq!(
                (priced.delta, priced.mid, priced.feed_mid),
                (U256::one(), two(), two())
            );
            let text = metrics.render().unwrap();
            assert!(
                text.contains(
                    r#"quote_updater_diagnostic{kind="const",name="calls",pair="CUSTOM/TEST"} 1"#
                ),
                "{text}"
            );
            // Nothing can stop a component doing slow CPU work on the 50ms path, but a
            // histogram makes it visible: every call into the binary's code is timed.
            assert!(
                text.contains(
                    r#"quote_updater_extension_duration_seconds_count{kind="const",pair="CUSTOM/TEST"} 1"#
                ),
                "{text}"
            );
        }

        struct Closed(RefusalHandle);

        impl Pricer for Closed {
            fn price(&mut self, _: &TickCtx, _: &mut Diagnostics) -> Result<PricerOutput, Refusal> {
                Err(Refusal::new(&self.0, "market closed"))
            }
        }

        #[tokio::test]
        async fn a_declared_refusal_withdraws_and_counts_under_its_own_name() {
            let mut kinds = Kinds::default();
            kinds.insert(
                "closed",
                fn_factory(|_: NoConfig, ctx: &mut BuildCtx| Ok(Closed(ctx.refusal("closed")?))),
            );
            let metrics = metrics::Metrics::new().unwrap();
            let values = build_custom(
                &kinds,
                "closed",
                &stanza("closed"),
                shape(),
                None,
                &metrics,
                no_chain(),
                None,
                crate::guard::Latch::new(),
                &Default::default(),
                Arc::new(reqwest::Client::new()),
            )
            .await
            .unwrap();
            let err = values
                .current_detailed(&config::MidBand::default())
                .unwrap_err();
            assert_eq!(
                (err.kind(), err.to_string()),
                (None, "market closed".to_owned())
            );
            err.count(&metrics.for_pair("CUSTOM/TEST"));
            let text = metrics.render().unwrap();
            assert!(
                text.contains(
                    r#"quote_updater_price_unusable_total{pair="CUSTOM/TEST",reason="closed"} 1"#
                ),
                "{text}"
            );
        }

        struct Flaky(bool);

        impl Pricer for Flaky {
            fn price(&mut self, _: &TickCtx, _: &mut Diagnostics) -> Result<PricerOutput, Refusal> {
                if !std::mem::replace(&mut self.0, true) {
                    panic!("first tick");
                }
                Ok(PricerOutput {
                    delta: U256::one(),
                    mid: two(),
                })
            }
        }

        /// Review Focus 4: a panic poisons the pricer's mutex; the next tick must still
        /// price, or one bad tick would wedge the lane until a restart.
        #[tokio::test]
        async fn a_pricer_that_panics_trips_the_lane_and_the_next_tick_is_not_wedged() {
            use crate::guard::Cause;
            let mut kinds = Kinds::default();
            kinds.insert(
                "flaky",
                fn_factory(|_: NoConfig, _: &mut BuildCtx| Ok(Flaky(false))),
            );
            let metrics = metrics::Metrics::new().unwrap();
            let latch = crate::guard::Latch::new();
            let values = build_custom(
                &kinds,
                "flaky",
                &stanza("flaky"),
                shape(),
                None,
                &metrics,
                no_chain(),
                None,
                latch.clone(),
                &Default::default(),
                Arc::new(reqwest::Client::new()),
            )
            .await
            .unwrap();
            let band = config::MidBand::default();
            // The panic never leaves `current_detailed`: the tick is withdrawn under
            // `panic` and the lane is tripped, so the loop halts with a human-readable reason
            // instead of the supervisor restarting it into the same bug.
            let first = values
                .current_detailed(&band)
                .expect_err("the first tick's panic is a withdrawn tick");
            assert_eq!(first.kind(), Some(crate::update::UnusableKind::Panic));
            assert_eq!(first.to_string(), "price of `flaky` panicked: first tick");
            let trip = latch.tripped().expect("tripped by the panic");
            assert_eq!((trip.source, trip.cause), ("flaky", Cause::Panic));
            assert_eq!(trip.reason.alarm, "price of `flaky` panicked: first tick");
            // The pricer's mutex is not left poisoned: whatever asks next (the halt reads
            // nothing more, but a caller that kept going must not be wedged) prices again.
            assert!(values.current_detailed(&band).is_ok());
        }

        struct Idle;

        impl Pricer for Idle {
            fn price(&mut self, _: &TickCtx, _: &mut Diagnostics) -> Result<PricerOutput, Refusal> {
                Ok(PricerOutput {
                    delta: U256::one(),
                    mid: two(),
                })
            }
        }

        struct FlagOnDrop(Arc<AtomicBool>);

        impl Drop for FlagOnDrop {
            fn drop(&mut self) {
                self.0.store(true, Ordering::SeqCst);
            }
        }

        #[tokio::test]
        async fn a_spawned_task_starts_after_the_build_and_stops_with_the_lane() {
            let started = Arc::new(AtomicUsize::new(0));
            let stopped = Arc::new(AtomicBool::new(false));
            let (s, t) = (started.clone(), stopped.clone());
            let mut kinds = Kinds::default();
            kinds.insert(
                "ticker",
                fn_factory(move |_: NoConfig, ctx: &mut BuildCtx| {
                    let (s, t) = (s.clone(), t.clone());
                    let during_build = s.clone();
                    ctx.spawn(async move {
                        let _guard = FlagOnDrop(t);
                        s.fetch_add(1, Ordering::SeqCst);
                        std::future::pending::<()>().await;
                    });
                    assert_eq!(
                        during_build.load(Ordering::SeqCst),
                        0,
                        "nothing runs during the build"
                    );
                    Ok(Idle)
                }),
            );
            let metrics = metrics::Metrics::new().unwrap();
            let values = build_custom(
                &kinds,
                "ticker",
                &stanza("ticker"),
                shape(),
                None,
                &metrics,
                no_chain(),
                None,
                crate::guard::Latch::new(),
                &Default::default(),
                Arc::new(reqwest::Client::new()),
            )
            .await
            .unwrap();
            let settle = || tokio::time::sleep(std::time::Duration::from_millis(50));
            settle().await;
            assert_eq!(
                started.load(Ordering::SeqCst),
                1,
                "started once the build succeeded"
            );
            let clone = values.clone();
            drop(values);
            settle().await;
            assert!(
                !stopped.load(Ordering::SeqCst),
                "a clone still holds the lane"
            );
            drop(clone);
            settle().await;
            assert!(
                stopped.load(Ordering::SeqCst),
                "stopped with the last clone"
            );
        }
    }
}
