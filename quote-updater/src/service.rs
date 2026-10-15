//! The builder-mode service: every pair the process is running, and the convergence of
//! that set onto the config file on each reload. `reload::plan` decides what differs; this
//! is what acts on it, with the rule that a lane whose stanza did not change is left
//! completely alone.

use std::{
    path::PathBuf,
    sync::Arc,
    time::{Duration, SystemTime},
};

use ethrex_common::{Address, U256};
use ethrex_rpc::clients::eth::EthClient;
use eyre::{Result, ensure};

use crate::{
    backoffice,
    config::{self, ResolvedPair},
    feed, head, metrics,
    pair::{Built, SendOpts, build_live, colliding_label},
    preflight, quoting, record, reload, supervisor, venue, volatile,
};

/// One pair the service is running, as the reload path needs to see it.
pub(crate) struct RunningPair {
    /// Preflight's label for this lane, e.g. `WETH/USDC`, for the lines a reload prints
    /// about it. Kept here because a removed pair has to be named after its `Live` is gone.
    label: String,
    /// The stanza this pair was built from. What a reload diffs against: equal means the
    /// operator did not touch this lane and it can keep quoting untouched.
    spec: config::PairSpec,
    /// This lane's own stop signal. Raising it takes the pair through the same withdraw
    /// path a ctrl-c does — see [`Service::stop`].
    stop: tokio::sync::watch::Sender<bool>,
    /// Set by `record_outcome` when this pair halts on its latch. Read by a reload to
    /// decide which lanes to bring back.
    halted: Arc<std::sync::atomic::AtomicBool>,
    /// The lane's latch, which outlives the pair's `Live`: read when a reload re-arms the
    /// lane, so the line announcing that says exactly what it cleared.
    latch: crate::guard::Latch,
    /// Held so a reload can wait for this lane to actually be gone before starting its
    /// replacement; see [`Service::stop`].
    pub(crate) task: tokio::task::JoinHandle<()>,
    /// This generation's claim on the pair's metric series, lowered by [`Service::stop`].
    /// See [`feed::FeedMetrics`].
    adopted: feed::Adoption,
    /// The pair's already-bound series. Kept so a reload can record against them without
    /// going back through `for_pair`, which re-binds all ~30 children under the registry
    /// lock to reach one.
    metrics: metrics::PairMetrics,
    /// The registered guard kinds the lane runs, for retiring their gauges with it.
    guard_kinds: Vec<&'static str>,
    /// Whether the observers were told this lane's loop ended (`reap` says so when the
    /// loop ends on its own; `stop` and `drain` say so otherwise, once).
    told_stopped: bool,
    /// Every `(source, cause)` this lane tripped on in an earlier generation, carried
    /// across its rebuilds, re-arms or not: each is a `trip_source` child (lowered to 0
    /// when the generation that tripped on it was replaced) that only the lane's removal
    /// can retire, and the current generation's latch does not know them.
    trip_sources: Vec<(&'static str, crate::guard::Cause)>,
    /// The venues this pair reads, so a removal can quiesce their series too.
    venues: Vec<venue::VenueId>,
    /// Distinguishes this generation of the lane from the next one. A pair task announces
    /// itself on the `done` channel as it ends, and by the time the run loop reads that a
    /// reload may already have started a replacement on the same lane — so the lane alone
    /// cannot say which pair the message is about. See [`Service::reap`].
    id: u64,
}

impl RunningPair {
    fn is_halted(&self) -> bool {
        self.halted.load(std::sync::atomic::Ordering::SeqCst)
    }
}

/// The running pairs, and everything needed to build another one.
///
/// Exists because the pair set is no longer fixed at startup: a reload adds, removes and
/// rebuilds lanes while the process runs, and every one of those needs the same nine
/// process-wide handles the startup loop used to clone inline. Keyed by lane — the pair's
/// identity, and the one thing preflight, the registry and `PropAMM._pairKey` all agree on.
pub(crate) struct Service {
    pub(crate) client: EthClient,
    pub(crate) opts: SendOpts,
    pub(crate) builders: Arc<config::BuildersConfig>,
    pub(crate) builders_path: String,
    /// Whether the connected builders came from the config file, so a reload may compare
    /// the file's list against them.
    pub(crate) builders_from_config: bool,
    /// Whether this process may run with no pairs, i.e. whether a backoffice is bound.
    /// Startup and every reload have to agree.
    pub(crate) backoffice_bound: bool,
    /// As it was at startup, so a reload can refuse a changed key by name.
    pub(crate) file_settings: config::FileSettings,
    pub(crate) head: tokio::sync::watch::Receiver<head::Head>,
    pub(crate) metrics: Arc<metrics::Metrics>,
    pub(crate) health: Arc<supervisor::Health>,
    pub(crate) endpoints: venue::Endpoints,
    pub(crate) price_decimals: u32,
    pub(crate) config_path: PathBuf,
    pub(crate) registry: Address,
    pub(crate) reload_metrics: metrics::ReloadMetrics,
    /// Every symbol's price history, kept here so a reload that rebuilds a volatile pair
    /// hands the new feed the samples the old one collected.
    pub(crate) histories: volatile::Histories,
    /// Every vault's inventory reader, shared by the lanes that read it; see
    /// `vault::InventoryReaders`.
    pub(crate) readers: crate::vault::InventoryReaders,
    /// The feed recorder's handle, `None` when `--record-db-url` is unset. Handed to every
    /// pair a reload builds, the same one startup handed the first generation.
    pub(crate) recorder: Option<record::Recorder>,
    /// The process-wide ctrl-c. Every pair gets a forwarder from it (see [`Self::spawn`]),
    /// so a shutdown reaches a quoting lane even while this struct is busy inside a reload.
    pub(crate) global: supervisor::Shutdown,
    pub(crate) running: std::collections::BTreeMap<U256, RunningPair>,
    /// Pair tasks still running. Only `--once` reads it — a service exits on its shutdown
    /// signal, not on running out of pairs, because a reload can always bring more back.
    pub(crate) outstanding: usize,
    /// Hands out [`RunningPair::id`]. Monotonic for the life of the process, so an id is
    /// never reused and a late `done` message can always be told from a current pair.
    pub(crate) next_id: u64,
    /// The vault exporter's pair set, replaced on every reload the file passes.
    pub(crate) vault_pairs: tokio::sync::watch::Sender<Vec<(Address, Address)>>,
    /// The run's observers: told of every lane transition and reload here.
    pub(crate) observers: crate::observe::Observers,
    /// The backoffice's view of `running`, rebuilt by [`Self::publish_states`] whenever a
    /// lane is added or dropped.
    pub(crate) pair_states: backoffice::PairStates,
    /// Where `candidate()` reads updater keys: the process environment, or what the run
    /// was handed (see `config::KeyLookup`).
    pub(crate) key_lookup: config::KeyLookup,
    /// The pricing kinds the binary registered, for checking and building custom pairs.
    pub(crate) kinds: Arc<crate::kinds::Kinds>,
}

/// `run` returning early (an error after the pairs started) must not leave their supervised
/// tasks quoting inside a caller's runtime. A normal shutdown has already drained
/// `running`, so this finds it empty.
impl Drop for Service {
    fn drop(&mut self) {
        for pair in self.running.values() {
            pair.task.abort();
        }
    }
}

/// What stopping a lane found on the way out.
///
/// Both halves are read after the pair's task has ended rather than before it is asked to
/// stop, because both can be written by the pair itself during its own teardown.
#[derive(Debug, Default)]
struct Stopped {
    /// Whether the lane had halted on its latch.
    halted: bool,
    /// The trip it took, as the latch holds it: the one it halted on, or one that landed
    /// while it was being stopped.
    trip: Option<crate::guard::Trip>,
    /// Every source the lane tripped on, this generation's included: what a restart
    /// carries and a removal retires.
    trip_sources: Vec<(&'static str, crate::guard::Cause)>,
    /// The registered guard kinds the lane ran, for a rebuild to retire the ones its
    /// replacement does not run.
    guard_kinds: Vec<&'static str>,
}

/// What one reload did, for the operator line and the counters.
#[derive(Debug, Default, PartialEq, Eq)]
struct Applied {
    added: Vec<String>,
    removed: Vec<String>,
    /// Lanes stopped because the file now halts them: kept, with every setting, and not
    /// run until the flag is cleared.
    halted: Vec<String>,
    restarted: Vec<String>,
    /// Lanes whose feed would not come up. They keep running on their old config (or stay
    /// absent, if they were being added), and this is what says so.
    failed: Vec<String>,
}

impl Applied {
    fn is_empty(&self) -> bool {
        self.added.is_empty()
            && self.removed.is_empty()
            && self.halted.is_empty()
            && self.restarted.is_empty()
            && self.failed.is_empty()
    }

    /// The one line a reload prints. Names counts rather than every lane, because the
    /// per-lane detail is already printed as each action happens.
    fn summary(&self) -> String {
        let mut parts = Vec::new();
        for (n, what) in [
            (self.added.len(), "added"),
            (self.restarted.len(), "restarted"),
            (self.removed.len(), "removed"),
            (self.halted.len(), "halted"),
            (self.failed.len(), "failed"),
        ] {
            if n > 0 {
                parts.push(format!("{n} {what}"));
            }
        }
        if parts.is_empty() {
            "reload: no change".to_owned()
        } else {
            format!("reload: {}", parts.join(", "))
        }
    }
}

impl Service {
    /// Starts one pair, records it, and counts it into `Health`.
    ///
    /// Every pair gets its own `Shutdown` rather than sharing the process-wide one, which is
    /// what makes a lane stoppable on its own. Nothing else had to change for that:
    /// `quoting::run` already withdraws at every builder when its `cancel` is raised — the
    /// path ctrl-c has always taken — so a per-lane signal reuses the whole teardown,
    /// acks included, instead of inventing a second way out.
    pub(crate) fn spawn(
        &mut self,
        built: Built,
        spec: config::PairSpec,
        done: tokio::sync::mpsc::UnboundedSender<(U256, u64)>,
    ) {
        let Built { mut live, adopted } = built;
        let lane = spec.lane;
        let label = live.pair.label.clone();
        let pair_metrics = live.metrics.clone();
        let venues: Vec<venue::VenueId> = live.venues.iter().map(|v| v.venue).collect();
        let guard_kinds = live.guard_kinds.clone();
        let id = self.next_id;
        self.next_id += 1;
        // Everything this generation is about to publish, written before the switch is
        // thrown; see `pair::adopt`.
        crate::pair::adopt(&live, &adopted, self.price_decimals, &self.metrics);
        // The observers hear of the lane from its adoption, as its series do, and before
        // its task is spawned, so `LaneStarted` comes before anything the lane says: its
        // blocks, and a trip, which its latch tells from here (one in the build window, at
        // once). Not at the build, which can trip, run beside the generation it replaces,
        // or fail and never run.
        live.observers = self.observers.clone();
        self.observers.emit(crate::observe::Event::LaneStarted {
            pair: label.clone(),
        });
        live.latch
            .attach_observers(self.observers.clone(), label.clone());

        let (stop, cancel) = supervisor::shutdown_channel();
        // A reload that lands while ctrl-c is being handled must not start a pair that
        // outlives it. The global signal is a value, not an event, so asking now is enough.
        if self.global.is_set() {
            stop.send_replace(true);
        }
        let _forwarder = forward_shutdown(self.global.clone(), stop.clone());
        let halted = Arc::new(std::sync::atomic::AtomicBool::new(false));
        // Taken before `live` moves into the closure: the latch has to outlive the pair, so
        // the trip can still be read after the halt has dropped everything else.
        let latch = live.latch.clone();

        let (client, opts, builders, builders_path, head, metrics, health) = (
            self.client.clone(),
            self.opts.clone(),
            self.builders.clone(),
            self.builders_path.clone(),
            self.head.clone(),
            self.metrics.clone(),
            self.health.clone(),
        );
        let supervised = supervisor::supervise(
            label.clone(),
            health.clone(),
            supervisor::INITIAL_BACKOFF,
            cancel.clone(),
            {
                let (flag, latch) = (halted.clone(), latch.clone());
                // Taken before `live` moves into the closure below, since every
                // invocation re-clones `live` for its own async block.
                let restarts = live.metrics.pair_restarts.clone();
                let mut attempts = Attempts(0);
                move || {
                    // Counted at this call site rather than inside `supervise` itself:
                    // a `metrics` parameter there would cost seven existing test call
                    // sites for one counter, and `supervisor.rs` is deliberately kept
                    // free of metrics code — instrumentation belongs at the boundary
                    // that consumes a verdict, not inside the unit that produces it.
                    // See `attempt_is_restart` for the counting rule itself.
                    if attempts.record() {
                        restarts.inc();
                    }
                    let (
                        client,
                        live,
                        opts,
                        builders,
                        builders_path,
                        head,
                        cancel,
                        health,
                        metrics,
                        flag,
                        latch,
                    ) = (
                        client.clone(),
                        live.clone(),
                        opts.clone(),
                        builders.clone(),
                        builders_path.clone(),
                        head.clone(),
                        cancel.clone(),
                        health.clone(),
                        metrics.clone(),
                        flag.clone(),
                        latch.clone(),
                    );
                    async move {
                        let ended = quoting::run(
                            client,
                            live,
                            opts,
                            builders,
                            builders_path,
                            head,
                            cancel,
                            metrics,
                        )
                        .await?;
                        record_outcome(ended, &health, &flag, &latch);
                        Ok(())
                    }
                }
            },
        );

        let task = tokio::spawn(async move {
            // Told rather than polled: the run loop selects on this, so a pair that halts or
            // finishes is noticed the moment it does, without a handle to await.
            //
            // A guard rather than a send at the end, so it also fires if `supervise` itself
            // unwinds. `supervise` catches the *pair loop's* panics as a JoinError, but a
            // panic in `supervise` would escape it — and an ending that went unannounced
            // would leave the lane counted as running forever, hanging a `--once` run that
            // waits for every task and quietly overstating `pairs_total` in a service.
            struct Announce(tokio::sync::mpsc::UnboundedSender<(U256, u64)>, U256, u64);
            impl Drop for Announce {
                fn drop(&mut self) {
                    let _ = self.0.send((self.1, self.2));
                }
            }
            let _announce = Announce(done, lane, id);

            if let Err(err) = supervised.await {
                tracing::error!("pair task failed: {err:#}");
            }
        });

        self.health.add_pair();
        self.outstanding += 1;
        self.running.insert(
            lane,
            RunningPair {
                label: label.clone(),
                spec,
                stop,
                halted,
                latch,
                task,
                adopted,
                metrics: pair_metrics,
                guard_kinds,
                told_stopped: false,
                trip_sources: Vec::new(),
                venues,
                id,
            },
        );
        self.publish_states();
    }

    /// Stops one lane and waits for it to be gone, returning whether it had halted.
    ///
    /// Waits, rather than signalling and moving on, because the caller's next move may be to
    /// start a replacement on the same lane: two pairs quoting one lane, even briefly, would
    /// have them overwrite each other's updates at every builder with neither one wrong.
    ///
    /// Never `abort()`. A pair that is aborted rather than asked leaves its quote standing at
    /// every builder until it ages out, priced by nobody — the failure `KillSignal=SIGINT` in
    /// the systemd unit exists to avoid. The wait is bounded by the withdraw path itself,
    /// which stops waiting for cancel acks before giving up on the log line, and by `supervise`
    /// treating the signal as a reason to abandon a restart backoff rather than sleep it out.
    async fn stop(&mut self, lane: U256) -> Stopped {
        let Some(pair) = self.running.remove(&lane) else {
            return Stopped::default();
        };
        self.publish_states();
        // Cloned out before the handle is awaited, which moves it out of `pair`.
        let (halted, latch, adopted) = (
            Arc::clone(&pair.halted),
            pair.latch.clone(),
            pair.adopted.clone(),
        );
        pair.stop.send_replace(true);
        if let Err(err) = pair.task.await {
            tracing::error!(
                "[{}] pair task did not stop cleanly: {err}",
                config::lane_label(lane)
            );
        }
        // This generation is done writing. Its feed task may still be winding down and its
        // teardown would otherwise land on the series the replacement is about to claim.
        adopted.release();
        // Likewise its latch, which a task still winding down can trip: a trip told after
        // the `LaneStopped` below would be news of a generation that is gone.
        latch.detach_observers();

        // Read *after* the task is gone, never before. The flag is set-only and is raised by
        // the pair itself on its way out, so a pair that trips during its own teardown —
        // the stop is raised, the loop finishes its tick, the breaker latches — sets it
        // between a read taken up front and the task actually ending. `remove_pair` would
        // then take the lane out of `total` while leaving it in `halted`, and nothing can
        // ever bring that count back down: `any_halted` stays true for the life of the
        // process, parking a pusher whose operator has already dealt with the pair.
        let was_halted = halted.load(std::sync::atomic::Ordering::SeqCst);
        let trip = latch.tripped().cloned();
        let mut trip_sources = pair.trip_sources.clone();
        // Once per source: a lane re-armed on the same source for months carries one entry.
        if let Some(trip) = &trip
            && !trip_sources.contains(&(trip.source, trip.cause))
        {
            trip_sources.push((trip.source, trip.cause));
        }
        if let (true, Some(trip)) = (was_halted, &trip) {
            self.health.forget_trip(trip.source, trip.cause);
        }
        // `outstanding` is deliberately not touched here. Every pair task reports itself on
        // the `done` channel as it ends, this one included, and the run loop decrements
        // there — so decrementing here as well would count the same ending twice and, under
        // `--once`, could take the loop to zero while pairs were still quoting.
        self.health.remove_pair(was_halted);
        // Unless `reap` already said so, when the loop ended before this stop; a halted
        // lane stopped by a reload before its ending was reaped is said here.
        if !pair.told_stopped {
            self.observers.emit(crate::observe::Event::LaneStopped {
                pair: pair.label.clone(),
            });
        }
        Stopped {
            halted: was_halted,
            trip,
            trip_sources,
            guard_kinds: pair.guard_kinds.clone(),
        }
    }

    /// Handles a pair task that ended on its own, rather than because [`Self::stop`] asked.
    ///
    /// Three ways a lane's task can end and only one of them is a surprise. A lane `stop`
    /// took is already out of the map, and so is one whose id no longer matches — a reload
    /// can have started a replacement between the old task ending and this message being
    /// read, and reaping on the lane alone would then tear down the pair that just replaced
    /// it. A halted lane stays: that is exactly the state a reload exists to find and clear.
    ///
    /// What is left ended without being asked and without halting. `supervise` unwinding is
    /// the case the `Announce` guard was written for, and leaving the record behind is the
    /// worst of both worlds: `reload::plan` reads it as "running, and matching the file", so
    /// no reload ever brings the lane back, while `pairs_total` and `all_down` keep counting
    /// a pair that is not quoting. Dropped instead, so the next reload sees a lane the file
    /// asks for and nothing serving it, and adds it.
    pub(crate) fn reap(&mut self, lane: U256, id: u64) {
        // Two ways a task ends cleanly with nothing to report, both of which would otherwise
        // land here looking like the surprise this method exists for. Under `--once` a pair
        // publishes and returns `Ended::Finished`, which is the run doing exactly what it
        // was asked; and a pair started into a shutdown, or caught by one, ends the same way
        // — the run loop breaks out on its own signal rather than reading these messages,
        // but a pair `spawn` stopped for an already-raised ctrl-c can beat it here.
        if self.opts.once || self.global.is_set() {
            return;
        }
        let Some(pair) = self.running.get_mut(&lane) else {
            return;
        };
        if pair.id != id {
            return;
        }
        let label = pair.label.clone();
        let halted = pair.is_halted();
        // Whether it halted or died: the loop is over. Noted on the pair, so a reload that
        // later replaces a halted lane does not say it twice; and its latch tells nothing
        // after it, as at a stop.
        pair.told_stopped = true;
        pair.latch.detach_observers();
        self.observers.emit(crate::observe::Event::LaneStopped {
            pair: label.clone(),
        });
        if halted {
            return;
        }
        tracing::warn!(
            "[{label}] pair task ended without halting and without being asked; it is no \
             longer running. Reload to start it again"
        );
        self.forget(lane);
    }

    /// Rebuilds the backoffice's view from `running`: each running pair's `key_env` and the
    /// flag its breaker raises on a trip. Shared rather than copied, so a trip shows on the
    /// page as it happens, not at the next reload.
    fn publish_states(&self) {
        let states = self
            .running
            .values()
            .map(|pair| (pair.spec.key_env.clone(), Arc::clone(&pair.halted)))
            .collect();
        *self
            .pair_states
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = states;
    }

    /// Drops a lane's record and stops counting it, without asking it to stop — for a task
    /// that has already ended. Quiesces its series for the reason [`metrics::PairMetrics::quiesce`]
    /// gives.
    fn forget(&mut self, lane: U256) {
        let Some(pair) = self.running.remove(&lane) else {
            return;
        };
        self.publish_states();
        pair.adopted.release();
        if let (true, Some(trip)) = (pair.is_halted(), pair.latch.tripped()) {
            self.health.forget_trip(trip.source, trip.cause);
        }
        self.health.remove_pair(pair.is_halted());
        let mut trip_sources = pair.trip_sources.clone();
        if let Some(trip) = pair.latch.tripped() {
            trip_sources.push((trip.source, trip.cause));
        }
        self.quiesce(&pair.label, &pair.venues, &pair.guard_kinds, &trip_sources);
    }

    /// Writes "this pair is gone" into every series a removed lane leaves behind.
    ///
    /// Called only once the lane's generation has been un-adopted, so nothing still winding
    /// down can write over the NaNs afterwards.
    ///
    /// The presence gauge of each registered guard the lane ran and the `trip_source`
    /// child of every trip it took, this generation's and the re-armed ones', go with
    /// it: both are `GaugeVec` children keyed by kind and by source, so they need the
    /// lane's own facts rather than its label alone, which is why this takes them.
    fn quiesce(
        &self,
        label: &str,
        venues: &[venue::VenueId],
        guard_kinds: &[&'static str],
        trip_sources: &[(&'static str, crate::guard::Cause)],
    ) {
        let pair = self.metrics.for_pair(label);
        pair.quiesce();
        self.metrics.quiesce_diagnostics(label);
        for kind in guard_kinds {
            pair.guard(kind).set(f64::NAN);
        }
        for (source, cause) in trip_sources {
            pair.trip_source(source, cause.as_str()).set(f64::NAN);
        }
        for venue in venues {
            self.metrics.unwatch_venue(label, venue.as_str());
            self.metrics.for_venue(label, venue.as_str()).quiesce();
        }
        for builder in &self.builders.builders {
            self.metrics.for_builder(label, &builder.name).quiesce();
        }
    }

    /// Raises every lane's stop, for the one ctrl-c that has to reach all of them.
    pub(crate) fn stop_all(&self) {
        for pair in self.running.values() {
            pair.stop.send_replace(true);
        }
    }

    /// Waits for every lane asked to stop by [`Self::stop_all`] to actually end, so the
    /// withdraws go out, and tells the observers of each loop that ended here.
    pub(crate) async fn drain(&mut self) {
        for (_, pair) in std::mem::take(&mut self.running) {
            let told = pair.told_stopped;
            let _ = pair.task.await;
            pair.latch.detach_observers();
            // Unless `reap` already said so.
            if !told {
                self.observers.emit(crate::observe::Event::LaneStopped {
                    pair: pair.label.clone(),
                });
            }
        }
    }

    /// The lanes currently halted on their circuit breaker.
    fn halted_lanes(&self) -> std::collections::BTreeSet<U256> {
        self.running
            .iter()
            .filter(|(_, pair)| pair.is_halted())
            .map(|(lane, _)| *lane)
            .collect()
    }

    /// The stanza each running lane was built from, for the diff.
    fn running_specs(&self) -> std::collections::BTreeMap<U256, config::PairSpec> {
        self.running
            .iter()
            .map(|(lane, pair)| (*lane, pair.spec.clone()))
            .collect()
    }

    /// Parses and checks a candidate config without touching anything that is running.
    ///
    /// All-or-nothing, and deliberately every check startup makes. A reload that applied the
    /// half of a file it liked would leave the running pairs matching neither the old config
    /// nor the new one, which is the one state this design must never produce: the whole
    /// promise is that after a reload the file describes what is running.
    ///
    /// Returns each stanza beside the pair it resolves to, in file order, and the token
    /// pairs whose vaults the exporter should watch (halted pairs included).
    async fn candidate(
        &self,
    ) -> Result<(
        Vec<(config::PairSpec, ResolvedPair)>,
        Vec<(Address, Address)>,
        Vec<config::PairSpec>,
    )> {
        let config = config::load_config(
            &self.config_path,
            self.price_decimals,
            self.backoffice_bound,
        )?;
        // A reload naming an unregistered kind, or a stanza its kind cannot read, changes
        // nothing: refused here, before anything is stopped.
        self.kinds.check(&config)?;

        refuse_target_change(config.target, self.opts.target, &self.config_path)?;
        refuse_settings_change(
            &config::load_file_settings(&self.config_path)?,
            &self.file_settings,
            &self.config_path,
        )?;
        // No `refuse_breakers_in_node_mode` here: a `Service` only exists in builder mode
        // (node mode leaves `builders` as None and never reaches the loop that builds one),
        // so there is no node-mode reload for it to catch. Startup still makes the check.
        refuse_unresolvable_keys(&config, |name: &str| (self.key_lookup)(name))?;

        let specs = config.pairs.clone();
        let vault_pairs = config.vault_pairs();
        // The pairs the file keeps but halts: a running one among them is stopped as a
        // removed one is, and the line says which of the two it was.
        let halted_in_file = config.halted.clone();
        let report = preflight::build_report(
            &self.client,
            &config,
            self.registry,
            |name: &str| (self.key_lookup)(name),
            &self.kinds,
        )
        .await?;
        if let Some((label, a, b)) = colliding_label(&report.rows) {
            eyre::bail!(
                "pairs {a} and {b} would both report metrics as {label}; every per-pair \
                 series would merge silently"
            );
        }
        ensure!(
            report.ok(),
            "the new config failed its checks, so nothing was changed: {}",
            report.failure_lines().join("; ")
        );

        let mut resolved = config::resolve_pairs(config, |name: &str| (self.key_lookup)(name))?;
        // The same label adoption startup does, so a reloaded pair's logs read like every
        // other pair's rather than falling back to `lane 0x…`.
        for pair in &mut resolved {
            if let Some(row) = report.rows.iter().find(|row| row.lane == pair.pair.lane) {
                pair.pair.label = row.label.clone();
            }
        }
        // Every custom stanza against the pair it would price, before anything is touched:
        // an invalid one rejects the whole file, like any other refusal here.
        let shapes: Vec<_> = resolved
            .iter()
            .map(|pair| {
                (
                    crate::pair::shape_of(pair, self.price_decimals, self.opts.target),
                    &pair.pricing,
                    pair.guards.as_slice(),
                )
            })
            .collect();
        self.kinds.validate(&shapes)?;
        Ok((
            specs.into_iter().zip(resolved).collect(),
            vault_pairs,
            halted_in_file,
        ))
    }

    /// The file's builder list, if it differs from the connected one and at least one of
    /// the new entries can be reached. `Ok(None)` means nothing to do.
    ///
    /// The dial is real. Every lane is about to be rebuilt against this list, and a lane
    /// opens its builders *after* it starts, unlike its feed, so a list that connects to
    /// nothing would take them all down. One connection is the bar, the same bar startup
    /// clears.
    async fn reloadable_builders(&self) -> Result<Option<Vec<config::BuilderConfig>>> {
        // `--builders` means the connected list came from somewhere the config file does
        // not control, so the file's own list is not what produced it and must not replace
        // it either.
        // No mode check: a `Service` only exists in builder mode, so reaching here means
        // there are builder connections to rebuild.
        if !self.builders_from_config {
            return Ok(None);
        }
        let desired = config::load_raw(&self.config_path)?.builders;
        if desired == self.builders.builders {
            return Ok(None);
        }
        config::validate_builders(&desired)?;

        let live = quoting::connect_all(&desired)
            .await
            .into_iter()
            .filter(|result| result.is_ok())
            .count();
        ensure!(
            live > 0,
            "the [[builder]] list in {} changed, but none of its {} builders would connect, \
             and every pair has to be rebuilt to pick up a new list. Nothing was changed; \
             check the endpoints and API keys.",
            self.config_path.display(),
            desired.len(),
        );
        tracing::info!(
            "reload: builder list changed, {live}/{} connected; every pair will be restarted \
             onto it",
            desired.len()
        );
        Ok(Some(desired))
    }

    /// Shared by SIGHUP and the backoffice, so the journal and the counters cannot drift
    /// apart. The return value is for the backoffice and the handle, which have to know
    /// whether to keep the edit they wrote: see [`ReloadFailure`].
    pub(crate) async fn reload_and_report(
        &mut self,
        done: &tokio::sync::mpsc::UnboundedSender<(U256, u64)>,
        config_path: &std::path::Path,
    ) -> std::result::Result<String, ReloadFailure> {
        // Held across the converge, so the watchdog does not read a count between two pair
        // sets. Cloned out first because the guard borrows it.
        let health = Arc::clone(&self.health);
        let outcome = {
            let _reloading = health.reloading();
            self.reload(done).await
        };
        match outcome {
            Ok(applied) => {
                let result = reload_result(&applied);
                self.reload_metrics.reloads(result).inc();
                self.observers.emit(crate::observe::Event::Reload {
                    outcome: match result {
                        metrics::Reload::Partial => crate::observe::ReloadOutcome::Partial,
                        _ => crate::observe::ReloadOutcome::Applied,
                    },
                });
                // A partial reload leaves a pair on its old config, so the generation must
                // not claim the file is live.
                if result == metrics::Reload::Applied && !applied.is_empty() {
                    self.reload_metrics.generation.inc();
                }
                // Every applied reload, whether or not a lane moved: the recorder keeps a
                // row only when the file differs from the last one it kept, so this costs
                // nothing when nothing changed and catches a change the last record missed.
                if result == metrics::Reload::Applied {
                    record_config(self.recorder.as_ref(), config_path, "reload");
                }
                let line = if applied.is_empty() {
                    format!(
                        "reload: {} matches what is running; nothing to do",
                        config_path.display()
                    )
                } else {
                    applied.summary()
                };
                tracing::info!("{line}");
                // A failure to the caller even though it applied: the backoffice must not
                // say a change is live while a pair is still on its old config. Its own
                // kind, because unlike a rejection it must not put the file back.
                if result == metrics::Reload::Partial {
                    return Err(ReloadFailure::Partial(format!(
                        "{line} — some pairs kept their previous config, see the journal"
                    )));
                }
                Ok(line)
            }
            // Nothing was touched: every lane is still quoting whatever it was quoting
            // before. Said at this altitude because it is the one moment the operator's
            // belief about the running config can go wrong.
            Err(err) => {
                self.reload_metrics.reloads(metrics::Reload::Rejected).inc();
                self.observers.emit(crate::observe::Event::Reload {
                    outcome: crate::observe::ReloadOutcome::Rejected,
                });
                let line = format!("{err:#}");
                tracing::warn!(
                    "reload REJECTED, nothing changed, the pairs are still running the \
                     previous config: {line}"
                );
                Err(ReloadFailure::Rejected(line))
            }
        }
    }

    /// Re-reads the config file and converges the running pairs onto it.
    ///
    /// An error here means nothing was touched — see [`Self::candidate`]. Past that point the
    /// feeds are dialled **before** anything is stopped, so a stanza that turns out to be
    /// wrong costs the lane nothing: it keeps quoting on the config it already had, and only
    /// that lane is reported as failed. That is the one place a reload cannot be
    /// all-or-nothing, because whether a feed comes up is not knowable without dialling it,
    /// and the startup rule of failing everything is exactly wrong here — at startup nothing
    /// is serving anyone yet, while here the other lanes are quoting to takers.
    async fn reload(
        &mut self,
        done: &tokio::sync::mpsc::UnboundedSender<(U256, u64)>,
    ) -> Result<Applied> {
        let (candidate, vault_pairs, halted_in_file) = self.candidate().await?;
        let specs: Vec<config::PairSpec> = candidate.iter().map(|(spec, _)| spec.clone()).collect();

        // Before anything is stopped, and fatal if nothing connects: every lane is about to
        // be rebuilt against this list.
        let new_builders = self.reloadable_builders().await?;
        let builders_changed = new_builders.is_some();

        // Past the last check that refuses the whole file, and before the early return
        // below: a halted pair added to the file changes no running lane but still has a
        // vault to watch.
        self.vault_pairs.send_replace(vault_pairs);

        let actions = reload::plan(
            &self.running_specs(),
            &specs,
            &self.halted_lanes(),
            builders_changed,
        );
        if actions.is_empty() {
            return Ok(Applied::default());
        }

        // Before anything is spawned. Each running pair holds the `Arc` it was built with,
        // which is why they all have to be restarted.
        if let Some(builders) = new_builders {
            self.builders = Arc::new(config::BuildersConfig { builders });
        }

        let mut by_lane: std::collections::BTreeMap<U256, ResolvedPair> = candidate
            .into_iter()
            .map(|(spec, resolved)| (spec.lane, resolved))
            .collect();

        let mut removals = Vec::new();
        let mut starts = Vec::new();
        for action in actions {
            let lane = action.lane();
            match action {
                reload::Action::Remove(lane) => removals.push(lane),
                reload::Action::Add(spec) | reload::Action::Restart(spec) => {
                    let resolved = by_lane
                        .remove(&lane)
                        .expect("plan only emits lanes that came from the candidate config");
                    starts.push((spec, resolved, self.running.contains_key(&lane)));
                }
            }
        }

        // Every feed at once, so a reload touching three lanes costs one FIRST_PRICE_TIMEOUT
        // rather than three — and so a second bad symbol is reported beside the first rather
        // than behind its wait. Nothing has been stopped yet.
        let built = futures_util::future::join_all(starts.into_iter().map(
            |(spec, resolved, was_running)| {
                let readers = self.readers.clone();
                let (endpoints, metrics, decimals, client, target, histories, recorder) = (
                    self.endpoints.clone(),
                    self.metrics.clone(),
                    self.price_decimals,
                    self.client.clone(),
                    self.opts.target,
                    self.histories.clone(),
                    self.recorder.clone(),
                );
                let kinds = Arc::clone(&self.kinds);
                async move {
                    let label = resolved.pair.label.clone();
                    let live = build_live(
                        resolved,
                        &endpoints,
                        decimals,
                        &metrics,
                        (&client, target),
                        &histories,
                        &readers,
                        recorder.as_ref(),
                        &kinds,
                    )
                    .await;
                    (spec, was_running, label, live)
                }
            },
        ))
        .await;

        let mut applied = Applied::default();
        for lane in removals {
            let (label, venues, guard_kinds) = self
                .running
                .get(&lane)
                .map(|pair| {
                    (
                        pair.label.clone(),
                        pair.venues.clone(),
                        pair.guard_kinds.clone(),
                    )
                })
                .unwrap_or_else(|| (config::lane_label(lane), Vec::new(), Vec::new()));
            let stopped = self.stop(lane).await;
            // Nothing reaps a `GaugeVec` child, so without this the lane's last readings
            // stand for the life of the process — and a lane removed while halted would
            // leave `breaker_tripped` at 1, paging under `PusherBreakerTripped` for a pair
            // that is not configured any more, clearable only by the restart this whole
            // feature exists to avoid.
            self.quiesce(&label, &venues, &guard_kinds, &stopped.trip_sources);
            // A pair the file keeps but halts is stopped exactly as a removed one, and said
            // apart: it is coming back on a resume, and the reason (a binary's kill switch
            // writes one) is what whoever resumes it will want to know.
            match halted_in_file.iter().find(|spec| spec.lane == lane) {
                Some(spec) => {
                    let reason = spec
                        .halt_reason
                        .as_deref()
                        .map(|reason| format!(": {reason}"))
                        .unwrap_or_default();
                    tracing::info!(
                        "reload: [{label}] halted{reason}; its quote was withdrawn at every \
                         builder"
                    );
                    applied.halted.push(label);
                }
                None => {
                    tracing::info!(
                        "reload: [{label}] removed; its quote was withdrawn at every builder"
                    );
                    applied.removed.push(label);
                }
            }
        }

        // Every replacement is stopped before any is started, rather than each lane being
        // swapped in turn. Interleaving lets two *running* pairs briefly share one signer
        // address whenever an edit moves a `key_env` from one lane to another: the lane
        // started first would be signing from the address the lane not yet stopped is still
        // using, so the two would share a nonce and only one of them could land — the exact
        // starvation `resolve_pairs` refuses a config for. The cost is that a lane's gap now
        // spans its neighbours' withdraws as well as its own, which is bounded by the same
        // ack wait and is the cheaper of the two.
        let mut starting = Vec::with_capacity(built.len());
        for (spec, was_running, label, built) in built {
            let built = match built {
                Ok(built) => built,
                Err(err) => {
                    let kept = if was_running {
                        "leaving it quoting on the config it already had"
                    } else {
                        "so it was not started"
                    };
                    tracing::warn!("reload: [{label}] {err:#}; {kept}");
                    applied.failed.push(label);
                    continue;
                }
            };
            let stopped = self.stop(spec.lane).await;
            starting.push((spec, was_running, label, built, stopped));
        }
        for (spec, was_running, label, built, stopped) in starting {
            let lane = spec.lane;
            // What the generation just stopped leaves on series the new one does not write
            // for itself. Before `spawn`, so the new generation's seed and latch write
            // over these and never the other way round: a re-armed lane that trips again
            // at once on the same source reads 1 there, not this 0.
            retire_stopped(&built.live, &stopped);
            self.spawn(built, spec, done.clone());
            // The sources it tripped on, carried into the new generation for its removal
            // to retire, halted or not: a trip can land while a lane is being stopped.
            if let Some(pair) = self.running.get_mut(&lane) {
                pair.trip_sources = stopped.trip_sources.clone();
            }
            if was_running {
                tracing::info!("reload: [{label}] restarted");
                applied.restarted.push(label.clone());
            } else {
                tracing::info!("reload: [{label}] added");
                applied.added.push(label.clone());
            }
            if stopped.halted {
                // Never silent. A reload run for an unrelated edit also re-arms any halted
                // lane, which is the price of one verb — so the line says which guard it
                // cleared and what tripped it, rather than letting a latch a human was meant
                // to look at disappear into a summary count.
                tracing::info!(
                    "reload: [{label}] re-armed{}",
                    rearm_detail(stopped.trip.as_ref())
                );
                self.observers.emit(crate::observe::Event::Rearmed {
                    pair: label.clone(),
                });
                // The handle the pair just adopted, rather than a fresh `for_pair`: that
                // re-binds every one of the pair's ~30 children through the registry lock to
                // reach one counter.
                self.running[&lane].metrics.breaker_rearms.inc();
            }
        }
        Ok(applied)
    }
}

/// Why a reload did not leave every lane on the file, as the service answers a request
/// for one. Two kinds, because the one who wrote the file has to do opposite things.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum ReloadFailure {
    /// Refused whole: nothing changed and every pair is still quoting its previous config,
    /// so a file written for this reload is put back.
    Rejected(String),
    /// Applied to every lane it reached, but at least one kept its previous config (or was
    /// not started) because its feed would not come up. The file stays: the change is live
    /// on the lanes it reached, a halt among them, and a put-back file would describe
    /// neither what runs nor what was asked for, and have the next reload undo the rest.
    Partial(String),
}

impl ReloadFailure {
    /// The line the service gave, whichever kind it is.
    pub(crate) fn line(&self) -> &str {
        match self {
            ReloadFailure::Rejected(line) | ReloadFailure::Partial(line) => line,
        }
    }
}

/// Writes what a stopped generation leaves on series its replacement does not write for
/// itself, before the replacement adopts them.
///
/// - The `trip_source` child of the trip it took, halted on it or tripped while it was
///   being stopped: "1 while the lane is halted on a trip from this source", and it is not
///   now. The new generation has a fresh latch, so nothing else lowers it; and lowered
///   before the new one adopts, so a new trip on the same source, recorded by its seed or
///   its latch, writes 1 over this 0 rather than under it.
/// - The presence gauge of each registered guard it ran that the new one does not (a
///   stanza removed in the edit that rebuilt it), retired as a removed lane's are. The ones
///   the new generation runs are set to 1 by its seed.
fn retire_stopped(live: &crate::pair::Live, stopped: &Stopped) {
    let m = &live.metrics;
    // The stopped generation's source-check verdict: the replacement may not run the
    // checks at all (a reload that removed both keys), and nothing else would lower it.
    m.clear_source_withdrawn();
    if let Some(trip) = &stopped.trip {
        m.trip_source(trip.source, trip.cause.as_str()).set(0.0);
    }
    for kind in &stopped.guard_kinds {
        if !live.guard_kinds.contains(kind) {
            m.guard(kind).set(f64::NAN);
        }
    }
}

/// Reads the config file, hands it to `change`, writes it back and asks the service to
/// reload, restoring the previous file if the reload is refused. The one path a change to
/// the running config takes, whether the backoffice or an `UpdaterHandle` asks, so neither
/// can forget the revert; `by` names the asker in the journal. The caller serializes
/// changes (the `edits` lock the backoffice and the handle share): two at once would each
/// read the file and write it back, and the second would drop the first's change.
///
/// `Ok` is a change that is live everywhere. [`ReloadFailure::Partial`] is one that is in
/// the file and applied to every lane the reload reached, kept rather than reverted;
/// [`ReloadFailure::Rejected`] means the file is as it was (or, if it could not be put
/// back, says so).
pub(crate) async fn change_config(
    path: &std::path::Path,
    reloads: &tokio::sync::mpsc::Sender<crate::backoffice::ReloadRequest>,
    by: &str,
    what: &str,
    change: impl FnOnce(&mut config::RawConfig) -> Result<()>,
) -> std::result::Result<String, ReloadFailure> {
    // Refused before the write: the file is untouched.
    let refused = ReloadFailure::Rejected;
    let before = std::fs::read_to_string(path)
        .map_err(|err| refused(format!("could not read {}: {err}", path.display())))?;
    let mut raw = config::load_raw(path).map_err(|err| refused(format!("{err:#}")))?;
    change(&mut raw).map_err(|err| refused(format!("{err:#}")))?;
    config::write_raw_atomically(path, &raw).map_err(|err| refused(format!("{err:#}")))?;

    match request_reload(reloads).await {
        Ok(summary) => {
            tracing::info!("{by} {what}; {summary}");
            Ok(format!("{what}. {summary}"))
        }
        // Not put back: the reload stopped, started and restarted what it could, so the
        // file already describes those lanes, and reverting it would have the next reload
        // undo them (a halt, say, lifted by an unrelated edit).
        Err(ReloadFailure::Partial(summary)) => {
            tracing::warn!("{by} {what}; applied in part, the change is kept: {summary}");
            Err(ReloadFailure::Partial(format!(
                "{what}, but the reload applied in part: {summary}. The config file keeps the \
                 change, which is live on every pair the reload reached; the next reload \
                 retries the rest."
            )))
        }
        Err(ReloadFailure::Rejected(err)) => {
            // Put back before the rejection is reported, so by the time it is read the file
            // already matches what is running again.
            let restored = std::fs::write(path, &before);
            tracing::warn!("{by} {what} REJECTED, reverted: {err}");
            Err(ReloadFailure::Rejected(match restored {
                Ok(()) => format!(
                    "{what} was rejected and nothing changed: {err}. The config file has \
                     been put back as it was."
                ),
                Err(write_err) => format!(
                    "{what} was rejected ({err}) AND the config file could not be put back \
                     ({write_err}). The file on disk no longer matches what is running: fix \
                     it by hand before the next reload."
                ),
            }))
        }
    }
}

/// Asks the service to reload and waits for its verdict. A service that is gone, or went
/// before it answered, counts as a rejection: nothing says it applied anything.
pub(crate) async fn request_reload(
    reloads: &tokio::sync::mpsc::Sender<crate::backoffice::ReloadRequest>,
) -> std::result::Result<String, ReloadFailure> {
    let (reply, answer) = tokio::sync::oneshot::channel();
    reloads
        .send(crate::backoffice::ReloadRequest { reply })
        .await
        .map_err(|_| {
            ReloadFailure::Rejected("the pusher is shutting down and is not reloading".to_owned())
        })?;
    answer.await.map_err(|_| {
        ReloadFailure::Rejected("the pusher stopped before it answered the reload".to_owned())
    })?
}

/// Opens the SIGHUP stream, whatever mode this process is about to run in.
///
/// Above the mode branch, and held for the life of the run, because *not* installing a
/// handler is not the same as ignoring the signal: SIGHUP's default disposition is to
/// terminate. The shipped unit declares `ExecReload=/bin/kill -HUP $MAINPID` and takes its
/// mode from the environment file, so a pusher an operator has put in `--mode node` and then
/// reloads would be killed by the reload rather than reloading — and a `--once` run would
/// die mid-flight on a stray one.
///
/// Only builder mode has a pair set to converge, so only builder mode reads the stream. The
/// other modes still hold it, and holding it is the whole point: an installed handler nobody
/// polls coalesces the signal and drops it, which is exactly the no-op wanted.
pub(crate) fn install_hangup(config: &std::path::Path) -> Option<tokio::signal::unix::Signal> {
    match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::hangup()) {
        Ok(stream) => Some(stream),
        // Said out loud rather than left to be discovered by an operator whose reload
        // silently does nothing at 3am. The default disposition is still in place here, so a
        // reload will end the process — which is exactly what the message warns of.
        Err(err) => {
            tracing::warn!(
                "could not listen for SIGHUP ({err}); config reloads are unavailable and a \
                 reload will terminate this process, so changing {} needs a restart",
                config.display()
            );
            None
        }
    }
}

/// Raises one lane's stop when the process-wide ctrl-c is raised.
///
/// Each pair has its own stop so a reload can end one lane without touching the others, and
/// the run loop turns ctrl-c into all of them through `Service::stop_all`. That is enough
/// only while the loop is at its `select!` — and it is not there during a reload, which
/// costs a preflight's worth of RPC round-trips plus up to `FIRST_PRICE_TIMEOUT` of feed
/// dialling. A lane with no other way to hear the signal would quote through all of it and
/// then be SIGKILLed at the unit's `TimeoutStopSec` with its quote still standing at every
/// builder — the failure `KillSignal=SIGINT` exists to prevent. Before per-lane stops the
/// pairs held the global signal directly and had no such gap.
///
/// Ends with the pair rather than outliving it: `closed()` resolves once the pair has
/// dropped its receiver, so a service that reloads for months does not accumulate one
/// parked task per pair generation. The handle is returned only so that property has a
/// test; `spawn` drops it.
fn forward_shutdown(
    mut global: supervisor::Shutdown,
    stop: tokio::sync::watch::Sender<bool>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        tokio::select! {
            _ = global.wait() => {
                stop.send_replace(true);
            }
            // The pair ended on its own; there is nothing left to signal.
            _ = stop.closed() => {}
        }
    })
}

/// Writes the initial reading of every gauge a pair does not record for itself, at the
/// moment the service adopts that pair.
///
/// This used to live in `build_live`, which is where the values are known — but a reload
/// dials a replacement feed long before it stops the pair it replaces, and the series are
/// keyed by the pair's label, so writing them at build time reached into a lane that was
/// still quoting. Here they land after [`Service::stop`] has let the previous generation
/// go, which makes the last write the new pair's rather than the old one's teardown.
///
/// `for_pair` registers every gauge at 0, and for these a registered 0 is indistinguishable
/// from a real, alertable zero until something gets around to recording them for real —
/// which for some of them can take a while, or never happen at all:
///
/// - `signer_runway_updates` is set only from inside `drive`'s main loop, on the
///   RUNWAY_CHECK_BLOCKS cadence. If `drive`'s first `get_block_by_number` keeps failing, it
///   retries internally forever and never reaches that check. No alert rule compares this
///   gauge any more — `PusherSignerNearlyDry` moved onto the balance below when it started
///   asking how long the key lasts rather than how many updates it buys — but the "Runway"
///   panel still draws it, and a registered 0 there reads as a key with nothing left. +Inf
///   is "never recorded", which that panel renders as ∞.
/// - `feed_up`/`feed_last_tick`/`feed_sample_current` are set only inside
///   `feed::spawn_feed`, which a `Static`-source pair never spawns — a supported
///   configuration (see pairs.example.toml), not an edge case. Left at 0 they would
///   permanently fire `PusherFeedDown`, `PusherFeedStale` and `PusherFeedSampleNotCurrent`
///   on a pair that is working exactly as configured. NaN fails every comparison those rules
///   make, including `time() - NaN > 30`.
/// - `signer_balance_wei` rides with the runway: same recording site, same cadence, same
///   never-recorded-until-then. NaN, and it now carries the weight the runway's +Inf used to.
///   `PusherSignerNearlyDry` is a `predict_linear` over this series, and one NaN sample
///   anywhere in its window makes the whole range return nothing — so a pair stalled on an
///   RPC outage cannot page an operator to top up a key that is fine. That suppression is
///   why this write is not a cosmetic one. It also keeps the "Signer balance" panel honest,
///   which divides it by 1e18 and draws it: a registered 0 there reads as an empty key,
///   where NaN draws a gap.
/// - `breaker_armed` says whether the pair is guarded at all, so a pair with no breaker is
///   visibly unguarded rather than absent and indistinguishable from one that has not
///   sampled yet.
///
/// A feed pair's `feed_up` is asserted here too, and it is true by construction: `build_live`
/// does not return until the feed has delivered a sample. If the socket has dropped in the
/// meantime the feed's own loop corrects it within RECONNECT_DELAY — worth that, because the
/// alternative is a re-armed lane inheriting the NaN its halted predecessor left behind and
/// never writing `feed_up` again until it happens to redial.
///
/// Everything else the feed writes while it is not yet the owner is accounted for here or
/// deliberately not. Every *gauge* it writes is seeded: `feed_last_tick`,
/// `feed_sample_current`, `feed_mid` and `feed_delta` from the sample below, and
/// `breaker_tripped`/`breaker_deviation_ratio` in `adopt_breaker_metrics`. The two
/// *counters* `feed_ticks` and `feed_rejected` undercount by whatever landed in that
/// window, which is the first tick at startup and one lane's dial time on a reload. They
/// are read through `rate()` and no rule thresholds them, so the alternative (replaying
/// counts nobody recorded) would buy nothing and could only be wrong. `breaker_trips` does
/// not undercount: a trip in that window is the transition itself, recorded once by the
/// seed or by `pair::adopt`'s re-check after the switch.
/// Waits for the recorder's last flush, bounded: the writer reacts to the shutdown signal
/// at once, so this is normally a few milliseconds, and a database that will not answer
/// must not hold the exit hostage past the withdraws that already went out.
pub(crate) async fn finish_recording(recording: Option<tokio::task::JoinHandle<()>>) {
    let Some(handle) = recording else { return };
    if tokio::time::timeout(Duration::from_secs(5), handle)
        .await
        .is_err()
    {
        tracing::warn!("[record] the last flush did not finish in 5s; its rows are lost");
    }
}

/// Hands the recorder the configuration the pusher now runs from, as the file says it:
/// redacted for the row (`record::redacted_config`), and whole, in memory only, to judge
/// whether it changed. A file that does not read is not a row: the caller has already
/// loaded and validated it, so this is a race with an editor, and the next reload records.
pub(crate) fn record_config(
    recorder: Option<&record::Recorder>,
    path: &std::path::Path,
    source: &str,
) {
    let Some(recorder) = recorder else { return };
    match config::load_raw(path) {
        Ok(raw) => recorder.config(record::ConfigSnapshot::new(
            SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_secs() as i64)
                .unwrap_or(0),
            source,
            &raw,
        )),
        Err(err) => tracing::warn!(
            "[record] could not read {} to record it: {err:#}",
            path.display()
        ),
    }
}

/// Refuses a reload whose file names a different PropAMM than the process is running.
///
/// The target is not a per-pair setting a reload can converge: every lane's `QuoteIdentity`
/// is built from it and it is baked into `SendOpts`, so a process that adopted a new one
/// would be a different service wearing the same pid. Refused whole rather than ignored,
/// because the operator who edited that line believes it took effect.
///
/// Separate and pure so its message has a test: this is one of the few errors whose only
/// job is to send someone to the right remedy.
fn refuse_target_change(desired: Address, running: Address, path: &std::path::Path) -> Result<()> {
    ensure!(
        desired == running,
        "target is {desired:#x} in {} but this process is running {running:#x}; changing the \
         target needs a restart, not a reload",
        path.display(),
    );
    Ok(())
}

/// Every setting is consumed once, at startup, so applying one here would change the file
/// without changing the process. Refused, with the key named.
fn refuse_settings_change(
    desired: &config::FileSettings,
    running: &config::FileSettings,
    path: &std::path::Path,
) -> Result<()> {
    let changed = running.changed_keys(desired);
    ensure!(
        changed.is_empty(),
        "[settings] {} changed in {} since this process started; these are read once, at \
         startup, so applying them needs a restart rather than a reload{}. Nothing was \
         changed.",
        changed.join(", "),
        path.display(),
        crate::hints::restart_note(),
    );
    Ok(())
}

/// Refuses a reload whose file names an updater key this process cannot see.
///
/// Checked ahead of `resolve_pairs`, which reports the same fact as a bare "not set in the
/// environment". True, and useless here: the operator has almost certainly just written that
/// key into `.env` and expects the reload to pick it up. Nothing re-reads that file — systemd
/// hands the environment over once, at exec — so the answer is a restart, and saying so is
/// the whole point of checking separately.
///
/// Names every missing variable rather than the first, for the same reason `all_or_none`
/// does: fixing them one restart at a time is the worst version of this.
fn refuse_unresolvable_keys(
    config: &config::Config,
    lookup: impl Fn(&str) -> Option<String>,
) -> Result<()> {
    let missing: Vec<&str> = config
        .pairs
        .iter()
        .map(|pair| pair.key_env.as_str())
        .filter(|name| lookup(name).is_none())
        .collect();
    ensure!(
        missing.is_empty(),
        "{} not set in this process's environment, so that pair cannot sign. A new updater \
         key needs a restart rather than a reload: systemd reads EnvironmentFile only when it \
         starts the service.",
        missing.join(", "),
    );
    Ok(())
}

/// Which result one applied reload counts as.
///
/// The distinction the counter exists for. A lane whose feed would not come up is left on
/// its old stanza while every other lane moved, so `pairs.toml` now describes only *part* of
/// what is running — and that is a worse thing for an operator to be wrong about than a
/// rejection, which at least leaves every lane on the previous config together. Counting it
/// as a clean apply would leave nothing at all able to see it: the pairs stay healthy, no
/// other series moves, and the one line saying so scrolls past in the journal.
///
/// A free function so the rule has a test, and because the caller gates
/// `ReloadMetrics::generation` on the same answer — see that field's doc comment.
fn reload_result(applied: &Applied) -> metrics::Reload {
    if applied.failed.is_empty() {
        metrics::Reload::Applied
    } else {
        metrics::Reload::Partial
    }
}

/// The tail of the line announcing that a reload cleared a lane's latch.
///
/// A latch is the one thing in this service that exists to make a human look, and a reload
/// clears it as a side effect of converging the file, so the clearing has to be at least as
/// loud as the trip was, and has to carry the same numbers. Rendered from the reason the
/// trip was latched with, so the move reported here is the move that was reported when it
/// fired: for the deviation guard, `on a 4.10% move from previous 1.00 to 1.04 (limit
/// 2.00%)`, exactly as it always read.
///
/// A free function so the wording has a test; separate from the caller so it can also say
/// nothing sensible when there is nothing recorded, which is what a halt with no trip
/// (impossible today) would leave.
fn rearm_detail(cleared: Option<&crate::guard::Trip>) -> String {
    let Some(trip) = cleared else {
        return ", clearing a circuit breaker that recorded no trip".to_owned();
    };
    format!(
        ", clearing the halt it took {} ago {}",
        format_elapsed(trip.at.elapsed()),
        trip.reason.rearm,
    )
}

/// A coarse "how long ago", for an operator line rather than a measurement: a halted pusher
/// is designed to sit for days, so seconds of precision on a two-day-old trip is noise.
fn format_elapsed(elapsed: Duration) -> String {
    let secs = elapsed.as_secs();
    match secs {
        ..60 => format!("{secs}s"),
        60..3600 => format!("{}m", secs / 60),
        3600..86400 => format!("{}h", secs / 3600),
        _ => format!("{}d", secs / 86400),
    }
}

/// Adapts a finished quote loop's outcome to what `supervise` understands, which is only
/// `Ok` (stop) and `Err` (restart).
///
/// Both outcomes here are `Ok` — a halted pair must not be restarted, and a pair that
/// simply finished has nothing to restart — so the distinction between them survives only
/// if it is recorded here, on the way past, while the reason is still known. Getting this
/// wrong in either direction is quiet: halting unconditionally would make every clean
/// `--once` completion park the process, and never halting would let the all-down watchdog
/// exit and clear the breakers it exists to protect.
///
/// A free function rather than an inline closure body so that property has a test.
///
/// `halted` is this one lane's flag, raised beside `Health`'s process-wide count. The count
/// answers "should the process park", which is all it ever had to; a reload also has to
/// know *which* lanes to bring back, and deriving that from a total is not possible. Both
/// are set here, in the one place the outcome is known, so they cannot disagree.
fn record_outcome(
    ended: quoting::Ended,
    health: &supervisor::Health,
    halted: &std::sync::atomic::AtomicBool,
    latch: &crate::guard::Latch,
) {
    if ended == quoting::Ended::Halted {
        health.halt();
        // What the watchdog's notice names. Before the flag, like the count: a reload that
        // reads the flag and forgets the trip must find it noted.
        if let Some(trip) = latch.tripped() {
            health.note_trip(trip.source, trip.cause);
        }
        halted.store(true, std::sync::atomic::Ordering::SeqCst);
    }
}

/// Whether one invocation of a `supervise` `spawn` closure counts as a restart.
///
/// `supervise` calls `spawn()` once per attempt (supervisor.rs:173), so the first
/// invocation is the initial start and every later one follows a failure — the same rule
/// `supervise` uses for its own internal `restarts` counter, including the clean-exit case
/// (one invocation, zero restarts). `attempts` is owned by the caller's closure and
/// advanced here, so repeated calls against the same counter track its own invocation
/// history.
///
/// A named thing rather than two inline lines at each call site: this exact rule was
/// duplicated verbatim at three places in this file (the production closure `run` passes
/// to `supervise`, and two tests exercising it) with nothing keeping the copies in sync —
/// a future edit to only the production gate (say, `>=` instead of `>`) would go
/// undetected, since the tests would still be asserting their own separate copy of the
/// rule rather than this one.
///
/// A type rather than `attempt_is_restart(&mut u32)`, which read as a predicate and mutated
/// its argument. Consulting that twice in one `spawn()` invocation — logging the restart and
/// then counting it, say — advanced the counter twice and reported the *initial* start as a
/// restart, which is `PusherPairRestarting` firing on a pusher that has never restarted.
/// `attempts.record()` cannot be misread as a question.
struct Attempts(u32);

impl Attempts {
    /// Records one `spawn()` invocation, returning whether it was a restart rather than the
    /// initial start.
    fn record(&mut self) -> bool {
        self.0 += 1;
        self.0 > 1
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use url::Url;

    /// Every setting is consumed once, at startup, so a reload that quietly ignored a
    /// changed one would leave the file saying something the process is not doing.
    #[test]
    fn a_reload_refuses_a_changed_setting_and_names_it() {
        let path = std::path::Path::new("config.toml");
        let running = config::FileSettings {
            requote_ms: Some(50),
            ..Default::default()
        };
        refuse_settings_change(&running, &running, path).expect("an unchanged table reloads");

        let desired = config::FileSettings {
            requote_ms: Some(25),
            ..Default::default()
        };
        let err = format!(
            "{:#}",
            refuse_settings_change(&desired, &running, path).unwrap_err()
        );
        assert!(err.contains("requote_ms"), "{err}");
        assert!(err.contains("restart"), "{err}");
    }

    /// Ctrl-c has to reach a quoting lane even while the run loop is stuck inside a reload,
    /// which is where it cannot poll its own shutdown arm. Before per-lane stops the pairs
    /// held the global signal directly; the forwarder is what gives that back.
    #[tokio::test]
    async fn a_lane_hears_ctrl_c_without_the_run_loop_relaying_it() {
        let (global_tx, global) = supervisor::shutdown_channel();
        let (stop, lane) = supervisor::shutdown_channel();
        forward_shutdown(global, stop);
        assert!(!lane.is_set());

        global_tx.send_replace(true);

        tokio::time::timeout(Duration::from_secs(5), {
            let mut lane = lane.clone();
            async move { lane.wait().await }
        })
        .await
        .expect("the lane must hear a ctrl-c nobody relayed to it");
        assert!(lane.is_set());
    }

    /// And the forwarder must not outlive the pair it was started for: a service that
    /// reloads for months would otherwise hold one parked task per pair generation, each
    /// waiting on a ctrl-c that may never come.
    #[tokio::test]
    async fn the_forwarder_ends_with_its_pair() {
        let (_global_tx, global) = supervisor::shutdown_channel();
        let (stop, lane) = supervisor::shutdown_channel();
        let forwarder = forward_shutdown(global, stop);

        // What the end of a pair looks like from here: its receiver is gone.
        drop(lane);

        tokio::time::timeout(Duration::from_secs(5), forwarder)
            .await
            .expect("the forwarder must not outlive the pair")
            .expect("and must not panic on the way out");
    }

    /// A `Service` with nothing running and no chain behind it.
    ///
    /// Every field the apply path actually reads is real — `health`, `metrics`, `builders`,
    /// `running` — and the rest are placeholders for a client nothing in these tests calls.
    /// That is the point: the bugs this exercises are all in the bookkeeping between
    /// `spawn`, `stop` and `reap`, which is the one part of the reload path that never
    /// needed a chain and never had a test.
    fn test_service(builders: Vec<config::BuilderConfig>) -> Service {
        test_service_with(builders, false)
    }

    fn test_service_with(builders: Vec<config::BuilderConfig>, once: bool) -> Service {
        let metrics = Arc::new(metrics::Metrics::new().unwrap());
        let reload_metrics = metrics.register_reload().unwrap();
        let (_, global) = supervisor::shutdown_channel();
        Service {
            client: EthClient::new(Url::parse("http://127.0.0.1:1").unwrap()).unwrap(),
            opts: SendOpts {
                registry: Address::zero(),
                target: Address::zero(),
                chain_id: 1,
                no_pin: false,
                mine: false,
                once,
                requote_ms: 50,
                disable_cross_region: false,
                price_decimals: 18,
                recorder: None,
            },
            builders: Arc::new(config::BuildersConfig { builders }),
            builders_path: "config.toml".to_owned(),
            // The reload path these tests drive never re-reads the file, so the two flags
            // that only matter to `candidate` are at their defaults.
            builders_from_config: true,
            backoffice_bound: false,
            file_settings: config::FileSettings::default(),
            head: tokio::sync::watch::channel(head::Head::default()).1,
            metrics: Arc::clone(&metrics),
            health: Arc::new(supervisor::Health::new(0)),
            reload_metrics,
            histories: volatile::Histories::default(),
            readers: crate::vault::InventoryReaders::default(),
            recorder: None,
            endpoints: venue::Endpoints::single(venue::VenueId::Binance, "ws://unused".to_owned()),
            observers: Default::default(),
            price_decimals: 18,
            config_path: PathBuf::from("config.toml"),
            registry: Address::zero(),
            global,
            running: std::collections::BTreeMap::new(),
            outstanding: 0,
            next_id: 0,
            vault_pairs: tokio::sync::watch::channel(Vec::new()).0,
            pair_states: backoffice::PairStates::default(),
            key_lookup: Arc::new(config::env_lookup),
            kinds: Arc::new(crate::kinds::Kinds::shipped()),
        }
    }

    /// Registers a lane whose task the test drives by hand, standing in for a pair loop.
    ///
    /// `spawn` would need a `Live`, a chain and a builder connection to reach the same
    /// state; none of that is what is under test here. The task holds until `release` is
    /// dropped, so a test can decide exactly when the pair "ends" — including *after* the
    /// stop has been raised, which is the window the halt-flag ordering turns on.
    /// The handles a test needs on a pair it registered: the flag the pair would raise for
    /// itself on a halt, the slot it would record the trip in, and the switch that decides
    /// when its task ends.
    struct FakePair {
        halted: Arc<std::sync::atomic::AtomicBool>,
        latch: crate::guard::Latch,
        release: tokio::sync::oneshot::Sender<()>,
    }

    fn register_fake_pair(service: &mut Service, lane: U256, label: &str) -> FakePair {
        let (stop, _cancel) = supervisor::shutdown_channel();
        let halted = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let latch = crate::guard::Latch::new();
        let (release, held) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(async move {
            let _ = held.await;
        });
        let id = service.next_id;
        service.next_id += 1;
        service.health.add_pair();
        service.outstanding += 1;
        service.running.insert(
            lane,
            RunningPair {
                label: label.to_owned(),
                spec: config::PairSpec {
                    tokens: (Address::zero(), Address::zero()),
                    lane,
                    invert: false,
                    pricing: config::PricingSpec::fixed_for_tests("0.000000000000000001", "0"),
                    feeds: None,
                    band: config::MidBand::default(),
                    breaker: None,
                    guards: Vec::new(),
                    key_env: "UPDATER_KEY_TEST".to_owned(),
                    allow_symbol_mismatch: false,
                    halt_reason: None,
                },
                stop,
                halted: Arc::clone(&halted),
                latch: latch.clone(),
                task,
                adopted: feed::Adoption::new(),
                metrics: service.metrics.for_pair(label),
                guard_kinds: Vec::new(),
                told_stopped: false,
                trip_sources: Vec::new(),
                venues: Vec::new(),
                id,
            },
        );
        FakePair {
            halted,
            latch,
            release,
        }
    }

    /// The window that made a halt permanent. `stop` used to sample the halt flag before
    /// raising the stop and before awaiting the task — but the flag is raised by the pair
    /// *itself*, on its way out, so a lane that trips while it is being stopped set it after
    /// the read. `remove_pair(false)` then took the lane out of `total` and left it in
    /// `halted`, and nothing can ever bring that back down: `any_halted` stays true for the
    /// life of the process, parking a pusher whose operator has already dealt with the pair.
    #[tokio::test]
    async fn a_pair_that_halts_while_it_is_being_stopped_is_not_left_counted_as_halted() {
        let mut service = test_service(Vec::new());
        let FakePair {
            halted,
            latch,
            release,
        } = register_fake_pair(&mut service, U256::from(1), "A/B");

        // Exactly the order a real pair takes: the stop is raised, the loop finishes its
        // tick, the lane trips, and only then does the task end.
        tokio::spawn(async move {
            halted.store(true, std::sync::atomic::Ordering::SeqCst);
            service_health_halt(&latch);
            let _ = release.send(());
        });
        // The count the pair raises for itself, from the same place `record_outcome` does.
        service.health.halt();

        let stopped = service.stop(U256::from(1)).await;

        assert!(
            stopped.halted,
            "the halt has to be seen, or the re-arm is never announced"
        );
        assert_eq!(
            service.health.census(),
            (0, 0, 0),
            "both counts come down together"
        );
        assert!(
            !service.health.any_halted(),
            "a halt that outlived its pair would park the process forever"
        );
    }

    /// Tripping the latch beside the flag, so the re-arm line has the numbers it exists to
    /// repeat. `stop` reads both after the task has ended for the same reason.
    fn service_health_halt(latch: &crate::guard::Latch) {
        latch.trip(
            "deviation",
            crate::guard::Cause::Guard,
            crate::guard::TripReason {
                header: "price deviation 4.10% exceeds the 2.00% limit".to_owned(),
                alarm: "circuit breaker tripped: mid moved 4.10% > 2.00% (previous 0.000251, new 0.000261)".to_owned(),
                rearm: "on a 4.10% move from previous 0.000251 to 0.000261 (limit 2.00%)".to_owned(),
            },
        );
    }

    /// A trip as the latch holds it, `at` ago.
    fn trip_at(rearm: &str, at: std::time::Instant) -> crate::guard::Trip {
        crate::guard::Trip {
            source: "deviation",
            cause: crate::guard::Cause::Guard,
            reason: crate::guard::TripReason {
                header: String::new(),
                alarm: String::new(),
                rearm: rearm.to_owned(),
            },
            at,
        }
    }

    /// A pair task that ends without halting and without being asked ended for a reason
    /// nothing else reports — `supervise` unwinding is the case the `Announce` guard exists
    /// for. Leaving the record behind is the worst outcome available: `reload::plan` reads
    /// it as "running, and matching the file", so no reload ever brings the lane back, while
    /// `pairs_total` keeps counting a pair that is not quoting.
    #[tokio::test]
    async fn a_pair_that_ends_on_its_own_is_dropped_so_a_reload_can_add_it_again() {
        let mut service = test_service(Vec::new());
        let pair = register_fake_pair(&mut service, U256::from(1), "A/B");
        drop(pair.release);

        service.reap(U256::from(1), 0);

        assert!(
            service.running.is_empty(),
            "the lane is gone, so the file now asks for a pair nothing is serving"
        );
        assert_eq!(service.health.census(), (0, 0, 0));
    }

    /// Under `--once` a pair that publishes and returns is the run succeeding, not a lane
    /// falling over. Reaping it would print an alarming line on every clean batch run and
    /// quiesce the series a script may be about to scrape.
    #[tokio::test]
    async fn a_once_run_finishing_its_pairs_is_not_treated_as_a_surprise() {
        let mut service = test_service_with(Vec::new(), true);
        let pair = register_fake_pair(&mut service, U256::from(1), "A/B");
        drop(pair.release);

        service.reap(U256::from(1), 0);

        assert_eq!(
            service.running.len(),
            1,
            "--once ends by every pair finishing; that is the run working"
        );
        assert_eq!(service.health.census(), (0, 0, 1));
    }

    /// A halted lane is the one ending that must survive being reaped: it is exactly what a
    /// reload has to find in order to re-arm it.
    #[tokio::test]
    async fn a_halted_pair_stays_in_the_running_set_for_a_reload_to_find() {
        let mut service = test_service(Vec::new());
        let pair = register_fake_pair(&mut service, U256::from(1), "A/B");
        pair.halted.store(true, std::sync::atomic::Ordering::SeqCst);
        drop(pair.release);

        service.reap(U256::from(1), 0);

        assert_eq!(service.running.len(), 1, "still there to be re-armed");
        assert_eq!(
            service.halted_lanes(),
            std::collections::BTreeSet::from([U256::from(1)])
        );
    }

    /// The generation check. A pair task announces itself as it ends, and by the time the
    /// run loop reads that, a reload may already have started a replacement on the same
    /// lane — so reaping on the lane alone would tear down the pair that just replaced it.
    #[tokio::test]
    async fn a_late_ending_does_not_reap_the_pair_that_replaced_it() {
        let mut service = test_service(Vec::new());
        let first = register_fake_pair(&mut service, U256::from(1), "A/B");
        drop(first.release);
        // The replacement, on the same lane, with the next id.
        let _second = register_fake_pair(&mut service, U256::from(1), "A/B");

        // The message the first generation left behind, read after the swap.
        service.reap(U256::from(1), 0);

        assert_eq!(
            service.running.len(),
            1,
            "the replacement is still running; only its predecessor ended"
        );
        assert_eq!(service.running[&U256::from(1)].id, 1);
    }

    /// A removed lane must stop being able to fire anything. Nothing reaps a `GaugeVec`
    /// child, so before this the last reading a removed pair wrote stood for the life of the
    /// process — and a lane removed *while halted* left `breaker_tripped` at 1, paging under
    /// PusherBreakerTripped for a pair that is not configured any more and cannot be cleared
    /// by the reload the alert itself recommends.
    #[tokio::test]
    async fn a_removed_pair_leaves_no_reading_any_rule_can_fire_on() {
        let mut service = test_service(vec![config::BuilderConfig {
            name: "titan".to_owned(),
            endpoint: "ws://unused".to_owned(),
            api_key: "unused".to_owned(),
            disable_cross_region: None,
        }]);
        let pair_handles = register_fake_pair(&mut service, U256::from(1), "A/B");

        // The readings a halted, nearly-dry, disconnected pair leaves behind — each one the
        // exact level a page-severity rule matches on.
        let pair = service.metrics.for_pair("A/B");
        pair.breaker_tripped.set(1.0);
        pair.signer_runway_updates.set(4.0);
        // The balance beside it, draining hard enough that a `predict_linear` over the
        // window would put it under PusherSignerNearlyDry's week: since that rule moved off
        // the runway, this is the series a removed pair could latch the page with.
        pair.signer_balance_wei.set(8.4e13);
        pair.consecutive_landing_misses.set(60.0);
        service.metrics.for_builder("A/B", "titan").up.set(0.0);
        // A custom pricer's declared reading, bound by its build and held nowhere the
        // service can reach.
        service
            .metrics
            .diagnostic("A/B", "skewed", "funding")
            .set(0.25);
        // What a registered guard's trip leaves: the source series the alarm annotation
        // points at, and the guard's own presence gauge.
        pair_handles.latch.trip(
            "kill",
            crate::guard::Cause::Guard,
            crate::guard::TripReason::new("kill switch"),
        );
        pair.trip_source("kill", "guard").set(1.0);
        pair.guard("kill").set(1.0);

        // A venue whose feed still says connected: once the lane is gone, no scrape may
        // read that flag into the series again.
        service
            .metrics
            .watch_venue("A/B", "binance", crate::feed::Connected::with(true));

        drop(pair_handles.release);
        let stopped = service.stop(U256::from(1)).await;
        service.quiesce(
            "A/B",
            &[venue::VenueId::Binance],
            &["kill"],
            &stopped.trip_sources,
        );

        let text = service.metrics.render().unwrap();
        for (series, rule) in [
            ("quote_updater_breaker_tripped", "PusherBreakerTripped"),
            ("quote_updater_signer_balance_wei", "PusherSignerNearlyDry"),
            // No rule reads this one any more, but the "Runway" panel does, and a removed
            // pair frozen at 4 updates left is a dashboard that lies about a lane nobody
            // configured.
            ("quote_updater_signer_runway_updates", "the Runway panel"),
            (
                "quote_updater_consecutive_landing_misses",
                "PusherLandingUnverified",
            ),
            ("quote_updater_builder_up", "PusherPairHasNoBuilder"),
            // No rule either, but a pricer's own panel and the next build's first
            // `feed.quotes.terms` would read the removed lane's last value as live.
            ("quote_updater_diagnostic", "a custom pricer's panel"),
            (
                "quote_updater_trip_source",
                "the PusherBreakerTripped annotation",
            ),
            ("quote_updater_guards", "the guards panel"),
            ("quote_updater_venue_feed_up", "PusherVenueFeedDown"),
        ] {
            let line = text
                .lines()
                .find(|line| line.starts_with(series) && line.contains(r#"pair="A/B""#))
                .unwrap_or_else(|| panic!("{series} is not exported at all:\n{text}"));
            assert!(
                line.ends_with(" NaN"),
                "{rule} can still fire on a pair that no longer exists: {line}"
            );
        }
    }

    /// A lane that halts while a reload is converging is stopped by the reload before its
    /// own ending is reaped, so `stop` is the only site that can say it stopped; and a
    /// lane `reap` already spoke for is not announced again when it is later replaced.
    #[tokio::test]
    async fn a_lane_that_halted_while_a_reload_was_converging_still_says_it_stopped() {
        use crate::observe::{Event, Observer, Observers};
        struct Recorder(Arc<std::sync::Mutex<Vec<Event>>>);
        impl Observer for Recorder {
            fn name(&self) -> &'static str {
                "recorder"
            }
            fn on_event<'a>(&'a mut self, event: &'a Event) -> crate::pricing::BoxFuture<'a, ()> {
                let seen = Arc::clone(&self.0);
                Box::pin(async move { seen.lock().unwrap().push(event.clone()) })
            }
        }
        let stops = |seen: &Arc<std::sync::Mutex<Vec<Event>>>, pair: &str| {
            seen.lock()
                .unwrap()
                .iter()
                .filter(|event| matches!(event, Event::LaneStopped { pair: p } if p == pair))
                .count()
        };
        let mut service = test_service(Vec::new());
        let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
        service.observers = Observers::spawn(
            vec![Box::new(Recorder(Arc::clone(&seen)))],
            &service.metrics,
        )
        .0;

        // Halted, and stopped by a reload before the loop's ending was reaped.
        let first = register_fake_pair(&mut service, U256::from(1), "A/B");
        first
            .halted
            .store(true, std::sync::atomic::Ordering::SeqCst);
        drop(first.release);
        service.stop(U256::from(1)).await;
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        assert_eq!(stops(&seen, "A/B"), 1, "{:?}", seen.lock().unwrap());

        // Halted, reaped (the usual order), then replaced: said once.
        let second = register_fake_pair(&mut service, U256::from(2), "C/D");
        let id = service.next_id - 1;
        second
            .halted
            .store(true, std::sync::atomic::Ordering::SeqCst);
        drop(second.release);
        service.reap(U256::from(2), id);
        service.stop(U256::from(2)).await;
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        assert_eq!(stops(&seen, "C/D"), 1, "{:?}", seen.lock().unwrap());
    }

    /// A generation's latch tells its trip only while the generation is the lane's: once
    /// `stop` (a reload's removal or restart) or the shutdown's drain has said the lane
    /// stopped, a trip from a task of its own still winding down (the composite judging a
    /// last sample) is not told after its `LaneStopped`.
    #[tokio::test]
    async fn a_stopped_generations_latch_tells_no_trip() {
        use crate::{
            guard::{Cause, TripReason},
            observe::{Event, Observers, Recorder},
        };
        let mut service = test_service(Vec::new());
        let (recorder, seen) = Recorder::new();
        service.observers = Observers::spawn(vec![Box::new(recorder)], &service.metrics).0;

        let stopped = register_fake_pair(&mut service, U256::from(1), "A/B");
        stopped
            .latch
            .attach_observers(service.observers.clone(), "A/B".to_owned());
        drop(stopped.release);
        service.stop(U256::from(1)).await;
        stopped
            .latch
            .trip("deviation", Cause::Guard, TripReason::new("late"));

        let drained = register_fake_pair(&mut service, U256::from(2), "C/D");
        drained
            .latch
            .attach_observers(service.observers.clone(), "C/D".to_owned());
        drop(drained.release);
        service.stop_all();
        service.drain().await;
        drained
            .latch
            .trip("deviation", Cause::Guard, TripReason::new("late"));
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;

        let seen = seen.lock().unwrap().clone();
        assert_eq!(
            seen,
            [
                Event::LaneStopped {
                    pair: "A/B".to_owned()
                },
                Event::LaneStopped {
                    pair: "C/D".to_owned()
                },
            ]
        );
    }

    /// The counter's whole reason for existing. A reload that left one lane on its old
    /// stanza while every other lane moved is the one state nothing else in the process can
    /// see — the pairs stay healthy and no other series moves — so it must not be counted as
    /// a clean apply, and it must not advance the generation an operator reads as "the file
    /// I am looking at is live".
    #[test]
    fn a_reload_that_could_not_start_every_pair_is_not_counted_as_applied() {
        let clean = Applied {
            added: vec!["A/B".to_owned()],
            ..Applied::default()
        };
        assert_eq!(reload_result(&clean), metrics::Reload::Applied);

        let mixed = Applied {
            restarted: vec!["A/B".to_owned()],
            failed: vec!["C/D".to_owned()],
            ..Applied::default()
        };
        assert_eq!(
            reload_result(&mixed),
            metrics::Reload::Partial,
            "one lane left behind is not an applied reload"
        );
    }

    /// A reload's refusals exist to send an operator to the right remedy, so the remedy is
    /// what these assert. Both of these are reached with the pairs still quoting on the old
    /// config, which is the fact the message has to leave them believing.
    #[test]
    fn a_reload_that_changes_the_target_is_refused_and_says_to_restart() {
        let path = std::path::Path::new("pairs.toml");
        let running = Address::from_low_u64_be(1);

        refuse_target_change(running, running, path).expect("an unchanged target is fine");

        let err = refuse_target_change(Address::from_low_u64_be(2), running, path)
            .expect_err("a changed target must not be adopted by a running process")
            .to_string();
        assert!(err.contains("restart"), "must name the remedy: {err}");
        assert!(err.contains("pairs.toml"), "and the file it read: {err}");
    }

    /// The diagnostic the deferred `.env` re-read makes necessary: without it this surfaces
    /// as a bare "not set in the environment" to someone who has just set it, in the file
    /// they set it in.
    #[test]
    fn a_reload_naming_an_unknown_key_says_why_writing_it_to_env_was_not_enough() {
        let toml = r#"
            target = "0x0000000000000000000000000000000000000001"
            [[pairs]]
            tokens = ["0x000000000000000000000000000000000000000A",
                      "0x000000000000000000000000000000000000000B"]
            symbol = "ETHUSDC"
            key_env = "UPDATER_KEY_PRESENT"
pricing = { kind = "feed" }
            [[pairs]]
            tokens = ["0x000000000000000000000000000000000000000C",
                      "0x000000000000000000000000000000000000000D"]
            symbol = "BTCUSDC"
            key_env = "UPDATER_KEY_MISSING"
pricing = { kind = "feed" }
        "#;
        let config = config::parse_config(toml, 18).expect("the fixture must parse");

        refuse_unresolvable_keys(&config, |_| Some("set".to_owned()))
            .expect("every key resolvable is the ordinary case");

        let err = refuse_unresolvable_keys(&config, |name| {
            (name == "UPDATER_KEY_PRESENT").then(|| "set".to_owned())
        })
        .expect_err("a pair that cannot sign must not be started")
        .to_string();

        assert!(err.contains("UPDATER_KEY_MISSING"), "names it: {err}");
        assert!(!err.contains("UPDATER_KEY_PRESENT"), "and only it: {err}");
        assert!(err.contains("restart"), "names the remedy: {err}");
        assert!(
            err.contains("EnvironmentFile"),
            "and why a reload cannot be it: {err}"
        );
    }

    /// Clearing a latch has to be at least as loud as the trip was, and carry the same
    /// numbers — it is the whole mitigation for a reload re-arming a lane the operator was
    /// not thinking about when they ran it.
    #[test]
    fn the_rearm_line_repeats_the_trip_it_cleared() {
        let at = std::time::Instant::now() - Duration::from_secs(14 * 60);
        let trip = trip_at(
            "on a 4.10% move from previous 0.000251 to 0.000261 (limit 2.00%)",
            at,
        );

        let line = rearm_detail(Some(&trip));

        assert_eq!(
            line,
            ", clearing the halt it took 14m ago on a 4.10% move from previous 0.000251 to \
             0.000261 (limit 2.00%)"
        );
        assert_eq!(
            rearm_detail(None),
            ", clearing a circuit breaker that recorded no trip"
        );
    }

    /// The counterpart above pins the tick-trip wording; this pins the windowed one, so a
    /// windowed halt does not print as though the tick check had fired: the span survives
    /// into the rearm line, and the reference reads as the window's anchor rather than as
    /// an unlabelled "previous" mid.
    #[test]
    fn the_rearm_line_carries_a_windowed_trips_span_and_label() {
        let at = std::time::Instant::now() - Duration::from_secs(14 * 60);
        let trip = trip_at(
            "on a 11.00% move over 5 blocks from anchor 100.000000 to 111.000000 (limit 10.00%)",
            at,
        );

        let line = rearm_detail(Some(&trip));

        for expected in [
            "11.00%",
            "10.00%",
            "100.000000",
            "111.000000",
            "14m",
            "over 5 blocks",
            "anchor",
        ] {
            assert!(line.contains(expected), "missing {expected}: {line}");
        }
    }

    /// A halted pusher is designed to sit for days, so the elapsed time has to stay readable
    /// across that whole range rather than reporting six figures of seconds.
    #[test]
    fn elapsed_is_rendered_at_a_scale_an_operator_reads() {
        for (secs, expected) in [
            (0, "0s"),
            (59, "59s"),
            (60, "1m"),
            (3599, "59m"),
            (3600, "1h"),
            (86_399, "23h"),
            (86_400, "1d"),
            (10 * 86_400, "10d"),
        ] {
            assert_eq!(format_elapsed(Duration::from_secs(secs)), expected);
        }
    }

    #[test]
    fn a_reload_summary_names_every_kind_that_happened_and_no_others() {
        assert_eq!(Applied::default().summary(), "reload: no change");

        let applied = Applied {
            added: vec!["A/B".to_owned()],
            restarted: vec!["C/D".to_owned(), "E/F".to_owned()],
            removed: Vec::new(),
            halted: vec!["I/J".to_owned()],
            failed: vec!["G/H".to_owned()],
        };
        assert_eq!(
            applied.summary(),
            "reload: 1 added, 2 restarted, 1 halted, 1 failed"
        );
        assert!(!applied.is_empty());
    }

    /// The distinction `supervise` cannot make for itself. It stops on `Ok` either way, so
    /// if this mapping is wrong nothing else catches it: halting on a clean finish would
    /// park the process at the end of every `--once` run, and failing to halt would let the
    /// watchdog exit and hand a supervisor the restart that clears the breakers.
    #[test]
    fn only_a_halted_pair_is_recorded_as_halted() {
        use std::sync::atomic::{AtomicBool, Ordering};
        let health = supervisor::Health::new(2);
        let halted = AtomicBool::new(false);
        let latch = crate::guard::Latch::new();
        latch.trip(
            "funding",
            crate::guard::Cause::Panic,
            crate::guard::TripReason::new("price of `funding` panicked: boom"),
        );

        record_outcome(quoting::Ended::Finished, &health, &halted, &latch);
        assert!(
            !health.any_halted(),
            "a pair that simply finished (--once, shutdown) is not a halt"
        );
        assert_eq!(health.census(), (0, 0, 2));
        assert!(
            !halted.load(Ordering::SeqCst),
            "and a reload has nothing to bring back"
        );

        assert!(health.trips().is_empty(), "nothing halted, nothing to name");

        record_outcome(quoting::Ended::Halted, &health, &halted, &latch);
        assert!(health.any_halted());
        assert_eq!(health.census(), (1, 0, 2));
        assert!(
            halted.load(Ordering::SeqCst),
            "the lane's own flag rises with the count, so a reload knows which one to re-arm"
        );
        assert_eq!(
            health.trips(),
            [("funding", crate::guard::Cause::Panic)],
            "and the watchdog can name what tripped it"
        );
        health.forget_trip("funding", crate::guard::Cause::Panic);
        assert!(
            health.trips().is_empty(),
            "removed or re-armed, it is no longer named"
        );
    }

    /// Drives the real `supervise` end to end with a closure shaped exactly like the one
    /// `run` passes it — calling the same `attempt_is_restart` helper the production
    /// closure calls, so the counting rule cannot drift between the two. Not a
    /// pure-function test of the rule in isolation: the thing worth pinning down here is
    /// that it stays correct against `supervise`'s actual invocation pattern (one
    /// `spawn()` call per attempt, per supervisor.rs:173), which a hand-written stand-in
    /// for that pattern could silently drift from.
    ///
    /// Two separate `supervise` runs sharing one `PairMetrics` handle, so the counter's
    /// value can be checked after exactly one invocation and again after exactly two.
    #[tokio::test]
    async fn pair_restarts_counts_every_invocation_after_the_first() {
        let metrics = metrics::Metrics::new().unwrap();
        let pair_metrics = metrics.for_pair("TEST/PAIR");

        // One invocation, no failure: the initial start, not a restart.
        {
            let (_stop, cancel) = supervisor::shutdown_channel();
            let restarts = pair_metrics.pair_restarts.clone();
            let mut attempts = Attempts(0);
            supervisor::supervise(
                "test-pair".to_owned(),
                Arc::new(supervisor::Health::new(1)),
                Duration::from_millis(1),
                cancel,
                move || {
                    if attempts.record() {
                        restarts.inc();
                    }
                    async move { Ok(()) }
                },
            )
            .await
            .unwrap();
        }
        assert_eq!(
            pair_metrics.pair_restarts.get(),
            0,
            "a clean first attempt is not a restart"
        );

        // A second run that fails once then succeeds: two invocations, one restart.
        {
            let (_stop, cancel) = supervisor::shutdown_channel();
            let restarts = pair_metrics.pair_restarts.clone();
            let mut attempts = Attempts(0);
            let failed_once = Arc::new(std::sync::atomic::AtomicBool::new(false));
            supervisor::supervise(
                "test-pair".to_owned(),
                Arc::new(supervisor::Health::new(1)),
                Duration::from_millis(1),
                cancel,
                move || {
                    if attempts.record() {
                        restarts.inc();
                    }
                    let failed_once = failed_once.clone();
                    async move {
                        if !failed_once.swap(true, std::sync::atomic::Ordering::SeqCst) {
                            eyre::bail!("transient")
                        }
                        Ok(())
                    }
                },
            )
            .await
            .unwrap();
        }
        assert_eq!(
            pair_metrics.pair_restarts.get(),
            1,
            "one restart after one failure and one success"
        );
    }

    mod custom_reload {
        use std::collections::BTreeMap;

        use super::*;
        use crate::{
            kinds::{Kinds, fn_factory},
            pricing::{
                BuildCtx, Diagnostics, Factory, PairShape, Pricer, PricerOutput, Refusal, TickCtx,
            },
        };

        #[derive(serde::Deserialize)]
        #[serde(deny_unknown_fields)]
        struct ACfg {
            #[serde(default)]
            broken: bool,
            #[serde(default)]
            invalid: bool,
            #[serde(default)]
            panics_building: bool,
            #[serde(default)]
            panics_validating: bool,
            /// Trips the lane's latch from the build, `external` from `a`: a lane that
            /// trips before the service adopts it.
            #[serde(default)]
            trips: bool,
        }

        #[derive(Clone, serde::Deserialize)]
        struct NoConfig {}

        struct One;

        impl Pricer for One {
            fn price(&mut self, _: &TickCtx, _: &mut Diagnostics) -> Result<PricerOutput, Refusal> {
                Ok(PricerOutput {
                    delta: U256::one(),
                    mid: U256::exp10(18),
                })
            }
        }

        /// Guard kinds `m` and `q`, a market and a quote guard that never object.
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

        /// Kind `a`: its build fails when the stanza says `broken`, and `validate` refuses
        /// it when the stanza says `invalid`. The `panics_*` flags panic instead, as a
        /// factory's `unwrap()` on a missing variable would.
        struct A;

        impl Factory for A {
            type Config = ACfg;
            type Pricer = One;

            fn validate(&self, cfg: &ACfg, _: &PairShape) -> eyre::Result<()> {
                assert!(!cfg.panics_validating, "validate blew up");
                eyre::ensure!(!cfg.invalid, "invalid is set");
                Ok(())
            }

            fn build<'a>(
                &'a self,
                cfg: &'a ACfg,
                ctx: &'a mut BuildCtx,
            ) -> futures_util::future::BoxFuture<'a, eyre::Result<One>> {
                Box::pin(async move {
                    assert!(!cfg.panics_building, "build blew up");
                    eyre::ensure!(!cfg.broken, "broken is set");
                    if cfg.trips {
                        ctx.latch().trip(
                            "a",
                            crate::guard::Cause::External,
                            crate::guard::TripReason::new("tripped at build"),
                        );
                    }
                    Ok(One)
                })
            }
        }

        fn file(a_extra: &str, builder: &str) -> String {
            format!(
                r#"target = "0x70997970C51812dc3A010C7d01b50e0d17dc79C8"

[[pairs]]
tokens  = ["0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48", "0xdAC17F958D2ee523a2206206994597C13D831ec7"]
key_env = "KEY_A"

[pairs.pricing]
kind = "a"
{a_extra}

[[pairs]]
tokens  = ["0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2", "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"]
key_env = "KEY_B"

[pairs.pricing]
kind = "b"

[[builder]]
name = "mock"
endpoint = "{builder}"
api_key = "k"
"#
            )
        }

        async fn setup(
            tag: &str,
        ) -> (
            Service,
            crate::rpc_mock::MockRpc,
            crate::builder::mock::Mock,
            std::path::PathBuf,
        ) {
            let rpc = crate::rpc_mock::MockRpc::spawn(100).await;
            for (token, symbol) in [
                ("0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48", "USDC"),
                ("0xdAC17F958D2ee523a2206206994597C13D831ec7", "USDT"),
                ("0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2", "WETH"),
            ] {
                rpc.set_symbol(token.parse().unwrap(), symbol);
            }
            rpc.set_vault(Address::repeat_byte(0xaa));
            rpc.set_balance(Address::repeat_byte(0xaa), U256::exp10(24));
            let builder = crate::builder::mock::spawn(crate::builder::mock::Behaviour::Ack).await;
            let dir = std::env::temp_dir().join(format!("qu-{tag}-{}", std::process::id()));
            std::fs::create_dir_all(&dir).unwrap();
            let path = dir.join("config.toml");

            let mut kinds = Kinds::default();
            kinds.insert("a", Arc::new(A));
            kinds.insert("b", fn_factory(|_: NoConfig, _: &mut BuildCtx| Ok(One)));
            kinds.insert_guard(
                "m",
                crate::kinds::market_guard_fn(|_: NoConfig, _: &mut BuildCtx| Ok(Pass)),
            );
            kinds.insert_guard(
                "q",
                crate::kinds::quote_guard_fn(|_: NoConfig, _: &mut BuildCtx| Ok(Allow)),
            );
            let mut service = test_service(vec![]);
            service.client = EthClient::new(Url::parse(&rpc.url).unwrap()).unwrap();
            service.config_path = path.clone();
            service.opts.target = "0x70997970C51812dc3A010C7d01b50e0d17dc79C8"
                .parse()
                .unwrap();
            service.kinds = Arc::new(kinds);
            service.key_lookup = Arc::new(|name: &str| match name {
                "KEY_A" => Some(
                    "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80".to_owned(),
                ),
                "KEY_B" => Some(
                    "0x5de4111afa1a4b94908f83103eb1f1706367c2e68ca870fc3fb9a804cdab365a".to_owned(),
                ),
                _ => None,
            });
            (service, rpc, builder, path)
        }

        /// [`file`] without the first pair: lane A removed.
        fn file_without_a(builder: &str) -> String {
            let full = file("", builder);
            let first = full.find("[[pairs]]").unwrap();
            let second = first + 1 + full[first + 1..].find("[[pairs]]").unwrap();
            format!("{}{}", &full[..first], &full[second..])
        }

        /// Lane A, the one priced by kind `a`, and its label.
        fn lane_a(service: &Service) -> (U256, String) {
            service
                .running
                .iter()
                .find(|(_, pair)| pair.spec.pricing.kind == "a")
                .map(|(lane, pair)| (*lane, pair.label.clone()))
                .expect("lane A is running")
        }

        /// Halts a running lane on a trip from `a`, as its quote loop and `record_outcome`
        /// leave it: latched, counted, noted, and flagged.
        fn halt_on_a(service: &Service, lane: U256) {
            let pair = &service.running[&lane];
            assert!(pair.latch.trip(
                "a",
                crate::guard::Cause::External,
                crate::guard::TripReason::new("first")
            ));
            service.health.halt();
            service.health.note_trip("a", crate::guard::Cause::External);
            pair.halted.store(true, std::sync::atomic::Ordering::SeqCst);
        }

        fn specs(service: &Service) -> BTreeMap<U256, config::PairSpec> {
            service
                .running
                .iter()
                .map(|(lane, pair)| (*lane, pair.spec.clone()))
                .collect()
        }

        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn a_failed_custom_build_keeps_the_old_lane() {
            let (mut service, _rpc, builder, path) = setup("reload-build").await;
            let (done, _done_rx) = tokio::sync::mpsc::unbounded_channel();

            std::fs::write(&path, file("", &builder.url)).unwrap();
            let first = service
                .reload(&done)
                .await
                .expect("the first reload adds both lanes");
            assert_eq!(first.added.len(), 2, "{first:?}");
            let before = specs(&service);

            std::fs::write(&path, file("broken = true", &builder.url)).unwrap();
            let second = service
                .reload(&done)
                .await
                .expect("one failed lane does not fail the file");
            assert_eq!(second.failed.len(), 1, "A's build failed: {second:?}");
            assert!(
                second.restarted.is_empty(),
                "B did not change and was not touched: {second:?}"
            );
            assert_eq!(specs(&service), before, "A still runs on its old stanza");
        }

        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn a_stanza_that_fails_validate_rejects_the_whole_reload() {
            let (mut service, _rpc, builder, path) = setup("reload-validate").await;
            let (done, _done_rx) = tokio::sync::mpsc::unbounded_channel();

            std::fs::write(&path, file("", &builder.url)).unwrap();
            service
                .reload(&done)
                .await
                .expect("the first reload adds both lanes");
            let before = specs(&service);

            std::fs::write(&path, file("invalid = true", &builder.url)).unwrap();
            let err = format!("{:#}", service.reload(&done).await.expect_err("rejected"));
            assert!(err.contains("invalid is set"), "{err}");
            assert_eq!(specs(&service), before, "nothing changed");
        }

        /// A panic in the user's `build` is that lane's failed build, as an `Err` is: the
        /// reload is partial and the old lane keeps quoting. Unwinding out of `reload` would
        /// end `run()`, drop every lane and, through the restart, clear every latched breaker.
        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn a_panicking_custom_build_is_a_failed_lane_not_a_crash() {
            let (mut service, _rpc, builder, path) = setup("reload-build-panic").await;
            let (done, _done_rx) = tokio::sync::mpsc::unbounded_channel();

            std::fs::write(&path, file("", &builder.url)).unwrap();
            service
                .reload(&done)
                .await
                .expect("the first reload adds both lanes");
            let before = specs(&service);

            std::fs::write(&path, file("panics_building = true", &builder.url)).unwrap();
            let second = service
                .reload(&done)
                .await
                .expect("a panicking build fails its lane, not the reload");
            assert_eq!(second.failed.len(), 1, "{second:?}");
            assert_eq!(specs(&service), before, "A still runs on its old stanza");
        }

        /// A market guard on a pair that streams nothing is refused on a reload as at
        /// startup: it would never run, and the lane would read as guarded. Nothing changes.
        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn a_market_guard_on_a_pair_with_no_market_rejects_the_reload() {
            let (mut service, _rpc, builder, path) = setup("reload-market-guard").await;
            let (done, _done_rx) = tokio::sync::mpsc::unbounded_channel();

            std::fs::write(&path, file("", &builder.url)).unwrap();
            service
                .reload(&done)
                .await
                .expect("the first reload adds both lanes");
            let before = specs(&service);

            std::fs::write(
                &path,
                file("\n[[pairs.guards]]\nkind = \"m\"", &builder.url),
            )
            .unwrap();
            let err = format!("{:#}", service.reload(&done).await.expect_err("rejected"));
            assert!(
                err.contains("pair 1") && err.contains("`m` is a market guard"),
                "{err}"
            );
            assert_eq!(specs(&service), before, "nothing changed");
        }

        /// A re-armed lane that trips again at once, on the same source, before or as the
        /// service adopts it, is halted on it, and its `trip_source` must say so. The
        /// re-arm's lowering of the old trip's source has to land before the new
        /// generation's seed, not after it, where it wrote 0 over the new trip's 1.
        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn a_rearmed_lane_that_trips_again_at_once_reads_tripped_on_its_source() {
            let (mut service, _rpc, builder, path) = setup("reload-retrip").await;
            let (done, _done_rx) = tokio::sync::mpsc::unbounded_channel();

            std::fs::write(&path, file("", &builder.url)).unwrap();
            service
                .reload(&done)
                .await
                .expect("the first reload adds both lanes");
            let (lane, label) = lane_a(&service);
            halt_on_a(&service, lane);

            // A changed stanza whose build trips the new latch on the same source and cause.
            std::fs::write(&path, file("trips = true", &builder.url)).unwrap();
            let applied = service.reload(&done).await.expect("applied");
            assert_eq!(applied.restarted, vec![label.clone()], "{applied:?}");
            assert!(service.running[&lane].latch.tripped().is_some());
            assert_eq!(
                service.metrics.trip_source(&label, "a", "external").get(),
                1.0,
                "the lane is halted on a trip from `a` again; its source must not read 0"
            );
            assert_eq!(service.metrics.for_pair(&label).breaker_tripped.get(), 1.0);
        }

        /// A lane rebuilt without having halted can still carry a trip: one that landed while
        /// it was being stopped, recorded as 1 by its latch. The new generation is quoting, so
        /// that source reads 0 now, and the lane keeps the source in its record, so its
        /// removal sets it to NaN rather than leaving a 0 or a 1 behind for good.
        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn a_lane_rebuilt_without_halting_lowers_and_later_retires_its_trip_source() {
            let (mut service, _rpc, builder, path) = setup("reload-teardown-trip").await;
            let (done, _done_rx) = tokio::sync::mpsc::unbounded_channel();

            std::fs::write(&path, file("", &builder.url)).unwrap();
            service
                .reload(&done)
                .await
                .expect("the first reload adds both lanes");
            let (lane, label) = lane_a(&service);
            // Tripped, and recorded by the adopted latch, but not halted: the stop got there
            // first.
            assert!(service.running[&lane].latch.trip(
                "a",
                crate::guard::Cause::External,
                crate::guard::TripReason::new("during teardown")
            ));
            assert_eq!(
                service.metrics.trip_source(&label, "a", "external").get(),
                1.0
            );

            std::fs::write(&path, file("broken = false", &builder.url)).unwrap();
            let applied = service.reload(&done).await.expect("applied");
            assert_eq!(applied.restarted, vec![label.clone()], "{applied:?}");
            assert_eq!(
                service.metrics.trip_source(&label, "a", "external").get(),
                0.0,
                "the lane quoting now is not tripped on `a`"
            );
            assert_eq!(service.metrics.for_pair(&label).breaker_tripped.get(), 0.0);

            std::fs::write(&path, file_without_a(&builder.url)).unwrap();
            let applied = service.reload(&done).await.expect("applied");
            assert_eq!(applied.removed, vec![label.clone()], "{applied:?}");
            assert!(
                service
                    .metrics
                    .trip_source(&label, "a", "external")
                    .get()
                    .is_nan(),
                "a removed lane leaves no trip source behind"
            );
        }

        /// A guard stanza removed from a lane rebuilds it without the guard, so the guard's
        /// presence gauge goes with it, as a removed lane's do; the guards it still runs
        /// stay at 1.
        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn a_removed_guard_stanza_retires_its_presence_gauge() {
            let (mut service, _rpc, builder, path) = setup("reload-guard-removed").await;
            let (done, _done_rx) = tokio::sync::mpsc::unbounded_channel();

            std::fs::write(
                &path,
                file("\n[[pairs.guards]]\nkind = \"q\"", &builder.url),
            )
            .unwrap();
            service
                .reload(&done)
                .await
                .expect("the first reload adds both lanes");
            let (_, label) = lane_a(&service);
            assert_eq!(service.metrics.for_pair(&label).guard("q").get(), 1.0);

            std::fs::write(&path, file("", &builder.url)).unwrap();
            let applied = service.reload(&done).await.expect("applied");
            assert_eq!(applied.restarted, vec![label.clone()], "{applied:?}");
            assert!(
                service.metrics.for_pair(&label).guard("q").get().is_nan(),
                "the lane no longer runs `q`"
            );
        }

        /// A panic in `validate` rejects the file, as its `Err` does: nothing changes.
        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn a_panicking_validate_rejects_the_reload() {
            let (mut service, _rpc, builder, path) = setup("reload-validate-panic").await;
            let (done, _done_rx) = tokio::sync::mpsc::unbounded_channel();

            std::fs::write(&path, file("", &builder.url)).unwrap();
            service
                .reload(&done)
                .await
                .expect("the first reload adds both lanes");
            let before = specs(&service);

            std::fs::write(&path, file("panics_validating = true", &builder.url)).unwrap();
            let err = format!("{:#}", service.reload(&done).await.expect_err("rejected"));
            assert!(
                err.contains("panicked") && err.contains("validate blew up"),
                "{err}"
            );
            assert_eq!(specs(&service), before, "nothing changed");
        }
    }
}
