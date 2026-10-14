//! Startup: read the config, build the client and the metrics, run preflight, then either
//! the builder-mode `Service` with its reload loop or the node-mode ticker. This is the
//! composition root today and becomes the `Pusher` builder when the extension API lands.

use std::{net::SocketAddr, sync::Arc, time::Duration};

use ethrex_rpc::{
    clients::eth::EthClient,
    types::block_identifier::{BlockIdentifier, BlockTag},
};
use eyre::{Result, WrapErr, ensure, eyre};
use url::Url;

use crate::{
    RUNWAY_CHECK_BLOCKS, backoffice,
    cli::{
        Args, Mode, apply_file_settings, refuse_backoffice_in_node_mode,
        refuse_breakers_in_node_mode, removed_vars_error, removed_vars_present,
        should_serve_metrics, validate_mode,
    },
    config, exporter, head, metrics,
    node::push_update,
    pair::{Live, SendOpts, all_or_none, build_live, collect_prices, colliding_label},
    preflight, record,
    service::{Service, finish_recording, install_hangup, record_config},
    supervisor,
    tasks::Tasks,
    updater::{Outcome, Parts},
    vault, venue, volatile,
    watchdog::{DOWN_CHECK_INTERVAL, Watchdog, halt_notice, watchdog_verdict},
};

/// The supervisor restarts one pair's quote loop when it panics, which only works if the
/// panic unwinds: built with `panic = "abort"`, one pair's bug would end every pair's
/// quoting. Profiles are the one build setting a library cannot choose for its users
/// (Cargo reads them from the final binary's workspace only), so the library checks what
/// it was compiled with, and refuses at startup, the way every misconfigured safety
/// feature here is refused rather than warned about.
fn refuse_panic_abort(aborts: bool) -> Result<()> {
    ensure!(
        !aborts,
        "this binary was built with panic = \"abort\", so a panic in one pair would end \
         every pair's quoting instead of restarting that pair; build it with the default \
         panic = \"unwind\" (see the release profile in this repository's Cargo.toml)"
    );
    Ok(())
}

/// `--check`'s verdict: it passes only when the chain report, every pair's price preview
/// and the label check all do. Returned rather than exited on, so a caller can run a check
/// inside its own process.
pub(crate) fn check_outcome(report_ok: bool, prices_ok: bool, no_collision: bool) -> Outcome {
    if report_ok && prices_ok && no_collision {
        Outcome::Success
    } else {
        Outcome::CheckFailed
    }
}

/// Runs until shut down, `--once` completes or `--check` has reported, and honours a
/// shutdown asked for during startup too: preflight and every pair's first price come
/// before anything quotes, and the RPC client has no timeout, so an RPC that accepts and
/// never answers would otherwise hold `shutdown(); wait()` for ever. Startup is dropped
/// where it stands then, which is safe because nothing is quoting yet: there is no quote
/// to withdraw, and the run's own `Tasks` take what it had spawned. Once a pair may quote
/// the run's loop takes the shutdown instead, withdrawing every quote on the way out.
pub(crate) async fn run(args: Args, parts: Parts) -> Result<Outcome> {
    // Neither is a `[settings]` key, so the flags already say what the run will do.
    let batch = if args.flags.check {
        Some("--check")
    } else if args.flags.once {
        Some("--once")
    } else {
        None
    };
    let (stop, cancel) = supervisor::shutdown_channel();
    let mut asked = cancel.clone();
    let quoting = Arc::new(std::sync::atomic::AtomicBool::new(false));
    tokio::select! {
        // The run first, so a step that finished in the same poll as the shutdown is seen
        // to have raised `quoting` before the arm below reads it.
        biased;
        outcome = run_inner(args, parts, (stop, cancel), Arc::clone(&quoting)) => outcome,
        () = async {
            asked.wait().await;
            if quoting.load(std::sync::atomic::Ordering::SeqCst) {
                std::future::pending::<()>().await;
            }
        } => match batch {
            // A batch run that never finished did not do what it was asked, and a script
            // waiting on it must not read a pass.
            Some(what) => Err(eyre!("shut down during startup, before {what} finished")),
            None => Ok(Outcome::Success),
        },
    }
}

/// [`run`], minus the startup race: `shutdown_channel` is the one the race watches, and
/// `quoting` is raised just before the first pair may quote, which ends it.
async fn run_inner(
    mut args: Args,
    parts: Parts,
    shutdown_channel: (tokio::sync::watch::Sender<bool>, supervisor::Shutdown),
    quoting: Arc<std::sync::atomic::AtomicBool>,
) -> Result<Outcome> {
    refuse_panic_abort(cfg!(panic = "abort"))?;
    // A kind registered twice, a guard under the breaker's name, or two observers under one
    // name, is the binary's mistake: refused before anything else, whatever the config says.
    parts.registration_errors()?;
    let Parts {
        shutdown,
        key_lookup,
        kinds,
        control,
        observers,
        // Consumed by `run_from_env` before the run; a run inside a binary's own runtime
        // has that binary's subscriber.
        log_targets: _,
        unit,
    } = parts;
    // Before anything prints a hint: the unit is a fact about the process (see hints.rs).
    if let Some(unit) = unit {
        crate::hints::set_unit(unit);
    }
    let kinds = Arc::new(kinds);
    let key_lookup = key_lookup.unwrap_or_else(|| Arc::new(config::env_lookup));
    // Every long-lived task below is spawned through this, so none outlives `run`, on any
    // way out of it; see tasks.rs.
    let mut tasks = Tasks::default();
    // Refused before anything connects: a leftover key is a live credential, and the run
    // it would have authenticated must not start.
    let removed = removed_vars_present();
    ensure!(removed.is_empty(), "{}", removed_vars_error(&removed));

    // Before every check below: each reads a setting the file may supply.
    let file_settings = config::load_file_settings(&args.flags.config)?;
    let given = args.given.clone();
    apply_file_settings(&mut args, &|id| given.contains(id), &file_settings)?;
    // The handle's halt and resume are refused in node mode, which only now is known.
    if let Some(control) = &control {
        control.mode.send_replace(Some(args.flags.mode));
    }

    ensure!(
        args.flags.interval > 0,
        "interval must be at least 1 second"
    );
    // `--binance-ws` has a default, and the default must lose to `[settings.endpoints]`:
    // only a flag, a variable or the file's `binance_ws` (which `apply_file_settings` put
    // in `args` above) is a Binance endpoint somebody chose.
    let binance_ws = (args.given("binance_ws") || file_settings.binance_ws.is_some())
        .then_some(args.flags.binance_ws.as_str());
    let endpoints = venue::Endpoints::resolve(binance_ws, &file_settings.endpoints)?;
    validate_mode(
        args.flags.mode,
        args.flags.mine,
        args.flags.no_pin,
        args.flags.builders.is_some(),
    )?;
    refuse_backoffice_in_node_mode(args.flags.mode, file_settings.backoffice_addr.as_deref())?;

    // Loaded before anything connects, though nothing here needs the chain: the file is
    // local, cheap and deterministic, while the RPC calls below are slow and can fail
    // transiently. Behind them this error would be unreachable by the only person it is
    // written for — whoever has no builders configured would first have to get an RPC up,
    // the registry deployed and the updaters authorized to be told so.
    // With a backoffice an empty config is a fresh server about to be configured through
    // the page. Without one it is the mistake it has always been.
    let backoffice_bound = file_settings.backoffice_addr.is_some() && !args.flags.once;
    let config = config::load_config(
        &args.flags.config,
        args.flags.price_decimals,
        backoffice_bound,
    )?;
    // Every stanza names a registered kind and fits its config: checked with the file, before
    // anything connects.
    kinds.check(&config)?;
    // Taken before `config` is consumed below: halted pairs never become running pairs, but
    // their vaults are watched all the same.
    let vault_pairs_at_start = config.vault_pairs();

    // `--builders` replaces the config file's list rather than adding to it, so one list
    // is in force and a stale entry cannot survive in the other.
    let builders = match args.flags.mode {
        Mode::Builder => {
            let (source, list) = match args.flags.builders.as_deref() {
                Some(path) => {
                    let loaded = config::BuildersConfig::load(std::path::Path::new(path))
                        .wrap_err_with(|| format!("no usable builders config at {path}"))?;
                    (path.to_owned(), loaded.builders)
                }
                None => (
                    args.flags.config.display().to_string(),
                    config.builders.clone(),
                ),
            };
            // Same exception: the first thing anyone does on a fresh server is add a
            // builder, and they need the page up to do it.
            ensure!(
                !list.is_empty() || backoffice_bound,
                "no builders configured in {source}; builder mode has nothing to quote to \
                 without at least one [[builder]] entry (see config.example.toml), or pass \
                 --mode node to submit updateState transactions to --rpc-url instead"
            );
            Some((source, Arc::new(config::BuildersConfig { builders: list })))
        }
        Mode::Node => None,
    };

    refuse_breakers_in_node_mode(args.flags.mode, &config)?;
    let target = config.target;

    // One ctrl-c listener for the whole process, raised into a value every pair observes
    // whenever it next looks — including a pair between restarts, which has no future of
    // its own registered anywhere. See supervisor::Shutdown for what that fixes. Up here,
    // before the first step that waits on anything: a shutdown asked for during startup
    // ends the run there (see `run`), so it has to be heard from the start.
    let (stop, mut cancel) = shutdown_channel;
    tasks.spawn(async move {
        // A binary's own trigger, if it set one, takes the same path as ctrl-c.
        let signal = match shutdown {
            Some(signal) => {
                signal.await;
                Ok(())
            }
            None => tokio::signal::ctrl_c().await,
        };
        match signal {
            Ok(()) => {
                tracing::info!("shutting down");
                stop.send_replace(true);
            }
            // Said out loud rather than left to be discovered by pressing ctrl-c: with no
            // handler installed the default kill-on-SIGINT disposition still applies, so
            // ctrl-c terminates the process abruptly instead of withdrawing each pair's
            // live quote first.
            Err(err) => tracing::warn!(
                "could not listen for ctrl-c ({err}); a SIGINT will kill the process without \
                 cancelling live quotes"
            ),
        }
    });

    // Built whether or not it is exported: the handles are threaded into every pair, so a run
    // with no listener records into a registry nobody scrapes rather than carrying an Option
    // down every call path.
    let metrics = Arc::new(metrics::Metrics::new()?);
    // Unconditional, and in both modes: which build is running and when it started are
    // process-wide facts, not something only builder mode's `Health` gates. See
    // `Metrics::register_process`'s doc comment for why this is split from
    // `register_health` below rather than being one call gated on builder mode.
    metrics.register_process(env!("CARGO_PKG_VERSION"))?;
    if let Some(addr) =
        should_serve_metrics(args.flags.metrics_addr, args.flags.check, args.flags.once)
    {
        // Fatal on failure, and deliberately before any pair starts quoting: see
        // exporter::bind.
        let (local, task) = exporter::bind(addr, Arc::clone(&metrics)).await?;
        tasks.adopt(&task);
        tracing::info!("metrics listening on http://{local}/metrics");
    }

    let url = Url::parse(&args.flags.rpc_url).wrap_err("invalid RPC url")?;
    if let Some(ws_url) = &args.flags.rpc_ws_url {
        // Checked before anything connects: a typo here would otherwise surface as a
        // redial loop in the logs, with polling quietly carrying the head.
        let scheme = Url::parse(ws_url)
            .wrap_err("invalid --rpc-ws-url")?
            .scheme()
            .to_owned();
        ensure!(
            scheme == "ws" || scheme == "wss",
            "--rpc-ws-url must be a ws:// or wss:// endpoint, got {scheme}://"
        );
    }
    let client = EthClient::new(url).wrap_err("failed to create RPC client")?;
    let code = client
        .get_code(args.flags.registry, BlockIdentifier::Tag(BlockTag::Latest))
        .await
        .wrap_err("failed to connect to RPC")?;
    ensure!(
        !code.is_empty(),
        "no code at registry {:#x}",
        args.flags.registry
    );
    // Once per process, for both send paths: see SendOpts::chain_id.
    let chain_id: u64 = client
        .get_chain_id()
        .await
        .wrap_err("failed to read the chain id")?
        .try_into()
        .map_err(|_| eyre!("chain id does not fit a u64"))?;

    // Printed on every run, not just --check: the operator sees exactly which lanes, which
    // orientations and which keys are about to publish, before anything is sent.
    let report = preflight::build_report(
        &client,
        &config,
        args.flags.registry,
        |name: &str| key_lookup(name),
        &kinds,
    )
    .await?;
    // The report ends in a newline and the output layer adds one per event.
    let rendered = report.render();
    tracing::info!("{}", rendered.strip_suffix('\n').unwrap_or(&rendered));
    // Before `--check` branches, so the one surface built to catch a config mistake reports
    // it as well as the run refusing it.
    let collision = colliding_label(&report.rows);
    if let Some((label, a, b)) = collision {
        tracing::warn!(
            "lanes {a:#x} and {b:#x} both resolve to the symbol label {label:?}; every metric \
             series for them would merge, including the breaker gauge one pair's trip would \
             then hide. Check the four token addresses: two lanes reporting the same symbols \
             usually means one of them is not the token it claims to be."
        );
    }
    if args.flags.check {
        // Feeds are connected only under --check. A normal run connects them a few lines
        // below on its way to quoting, and doing it twice would double every symbol's
        // startup wait for nothing.
        let prices = collect_prices(
            &config,
            &report,
            &endpoints,
            args.flags.price_decimals,
            &metrics,
            (&client, target),
            &kinds,
        )
        .await;
        let rendered = preflight::render_prices(&prices, args.flags.price_decimals);
        tracing::info!("{}", rendered.strip_suffix('\n').unwrap_or(&rendered));
        tracing::info!("\nnothing sent (--check)");
        return Ok(check_outcome(
            report.ok(),
            preflight::prices_ok(&prices),
            collision.is_none(),
        ));
    }
    ensure!(
        report.ok(),
        "preflight failed; fix the failures above (re-run with --check to re-inspect)"
    );
    ensure!(
        collision.is_none(),
        "two pairs share a metrics label; see the line above"
    );

    // Cloned before `resolve_pairs` consumes the config: these are what a reload diffs the
    // new file against, so the running pairs have to keep the stanzas they were built from.
    let specs = config.pairs.clone();
    let mut pairs = config::resolve_pairs(config, |name: &str| key_lookup(name))?;
    // Adopt preflight's on-chain symbol label (e.g. "USDC/USDT") for the rest of the run,
    // so push/quote logs read the same way the report above did. A pair whose symbols
    // preflight could not resolve simply keeps the "lane 0x…" fallback resolve_pairs
    // already gave it.
    for resolved in &mut pairs {
        if let Some(row) = report
            .rows
            .iter()
            .find(|row| row.lane == resolved.pair.lane)
        {
            resolved.pair.label = row.label.clone();
        }
    }
    // Every custom stanza against the pair it would price, before any lane is built: an
    // invalid one refuses the start, naming the pair and the kind.
    let shapes: Vec<_> = pairs
        .iter()
        .map(|pair| {
            (
                crate::pair::shape_of(pair, args.flags.price_decimals, target),
                &pair.pricing,
                pair.guards.as_slice(),
            )
        })
        .collect();
    kinds.validate(&shapes)?;
    // The feed recorder's handle, if recording is on: the feeds, the vault exporter and
    // every quote loop write into it from here on, and the writer that drains it into
    // Postgres is spawned below, once the shutdown signal it flushes on exists. A pusher
    // that is not a long-running service (--once) records nothing, like it serves no
    // metrics. The configuration this run starts from is its first row.
    let recorder = args
        .flags
        .record_db_url
        .as_ref()
        .filter(|_| !args.flags.once)
        .map(|_| record::Recorder::new());
    record_config(recorder.as_ref(), &args.flags.config, "startup");

    let opts = SendOpts {
        registry: args.flags.registry,
        target,
        chain_id,
        no_pin: args.flags.no_pin,
        mine: args.flags.mine,
        once: args.flags.once,
        requote_ms: args.flags.requote_ms,
        disable_cross_region: args.flags.disable_cross_region,
        price_decimals: args.flags.price_decimals,
        recorder: recorder.clone(),
        quote_id: args.flags.quote_id,
    };

    // A feed that never delivers aborts the whole process rather than starting the pairs
    // that did work. The per-pair isolation argument is about failures *during* a run, where the
    // other pairs are already serving takers and killing them would turn one broken pair
    // into three; at startup nothing is serving anyone yet, so there is no working state to
    // protect, and quoting two of three lanes while the operator believes three are live is
    // exactly the quiet drop-out the design exists to prevent. `--check` fails on the same
    // condition, so the surprise lands at preflight rather than at 3am.
    //
    // Concurrently, so three pairs cost one FIRST_PRICE_TIMEOUT rather than three, and so a
    // second broken symbol is reported beside the first instead of behind its wait.
    // Every pair's vault balances, as one `quote_updater_vault_balance` series per (vault,
    // token). Spawned before `pairs` is consumed below; see `vault.rs` for why it is not
    // part of any pair.
    // Every reload replaces the set through `vault_pairs`, so a pair added after startup
    // is watched too.
    let (vault_pairs, vault_pairs_rx) = tokio::sync::watch::channel(vault_pairs_at_start);
    vault::spawn_vault_exporter(
        &mut tasks,
        client.clone(),
        target,
        vault_pairs_rx,
        Arc::clone(&metrics),
        recorder.clone(),
        // USD prices for the vault's tokens, from CoinGecko on the chain the tokens live on,
        // over the run's one HTTP client.
        match vault::UsdPricing::platform(chain_id) {
            Some(platform) => Some(vault::UsdPricing {
                http: endpoints.http(),
                // `[settings.endpoints] coingecko`, for a mock; CoinGecko's own API otherwise.
                endpoint: file_settings
                    .endpoints
                    .get("coingecko")
                    .cloned()
                    .unwrap_or_else(|| crate::venues::coingecko::DEFAULT_ENDPOINT.to_owned()),
                platform,
            }),
            None => {
                tracing::warn!("[vault] no CoinGecko prices for chain {chain_id}; no USD values");
                None
            }
        },
    );

    let endpoints_ref = &endpoints;
    let metrics_ref: &metrics::Metrics = &metrics;
    let histories = volatile::Histories::default();
    let histories_ref = &histories;
    let readers = vault::InventoryReaders::default();
    let readers_ref = &readers;
    let client_ref = &client;
    let recorder_ref = recorder.as_ref();
    let kinds_ref: &crate::kinds::Kinds = &kinds;
    let lives = all_or_none(
        futures_util::future::join_all(pairs.into_iter().map(|resolved| {
            let label = resolved.pair.label.clone();
            async move {
                (
                    label,
                    build_live(
                        resolved,
                        endpoints_ref,
                        args.flags.price_decimals,
                        metrics_ref,
                        (client_ref, target),
                        histories_ref,
                        readers_ref,
                        recorder_ref,
                        kinds_ref,
                    )
                    .await,
                )
            }
        }))
        .await,
    )?;

    // The recorder's writer: drains what the feeds and the exporter left into Postgres
    // once a second, and flushes one last time on shutdown. Failures stay inside it.
    // Adopted like every other task, so an error return below aborts it. The clean exits
    // drain it first through `finish_recording`, and the abort then finds it finished, or
    // stops one whose last flush outlasted that wait.
    let recording = match (&recorder, &args.flags.record_db_url) {
        (Some(recorder), Some(url)) => {
            tracing::info!("recording the feed and vault balances to postgres");
            let writer = record::spawn(url.clone(), recorder.clone(), cancel.clone());
            tasks.adopt(&writer);
            Some(writer)
        }
        _ => None,
    };

    // Installed here, above the mode branch, and held for the whole run. See
    // `install_hangup`: not installing it is not the same as ignoring the signal.
    let mut reloads = install_hangup(&args.flags.config);

    // The reloads that wait for a verdict: the backoffice's, and an `UpdaterHandle`'s when
    // the run was `start`ed, which then supplies the channel so both share one. Bounded at
    // one in flight: the page (or the caller) waits for its answer, so a second click is
    // better made to wait than buffered into a convergence nobody asked for. Above the mode
    // branch so every mode reads it: a request nobody answers would hang its sender.
    let (backoffice_tx, mut backoffice_reloads, edits) = match control {
        Some(crate::updater::Control {
            reloads,
            requests,
            edits,
            mode: _,
        }) => (reloads, requests, edits),
        None => {
            let (tx, rx) = tokio::sync::mpsc::channel(1);
            (tx, rx, Arc::new(tokio::sync::Mutex::new(())))
        }
    };

    // Builder mode: one supervised task per pair, each fanning its signed update out to
    // every configured builder (see quoting.rs).
    if let Some((builders_path, builders)) = builders {
        // Zero, not the pair count: `Service::spawn` counts each pair as it starts it, so
        // startup and a later reload go through the one path and cannot disagree.
        let health = Arc::new(supervisor::Health::new(0));
        // A fully halted pusher deliberately stays up rather than exiting (see the comment
        // below on `any_halted`), so it is otherwise a green, `Restart=on-failure` process
        // quoting nothing that no liveness probe would catch. These three gauges are what
        // make that state visible from the outside.
        metrics.register_health(Arc::clone(&health))?;

        // A pusher with a halted or dead pair is worse than a dead one you notice. What to
        // do about it depends on why: see `watchdog_verdict`.
        {
            let health = health.clone();
            // Named in the notice, so it says where to go and fix it. The configured
            // string, not the bound one, since the listener is opened further down.
            let backoffice_addr = if backoffice_bound {
                file_settings.backoffice_addr.clone()
            } else {
                None
            };
            tasks.spawn(async move {
                let mut last_notice: Option<tokio::time::Instant> = None;
                loop {
                    tokio::time::sleep(DOWN_CHECK_INTERVAL).await;
                    let now = tokio::time::Instant::now();
                    match watchdog_verdict(&health, last_notice, now) {
                        Watchdog::Quiet => {}
                        Watchdog::Notice => {
                            last_notice = Some(now);
                            tracing::warn!(
                                "{}",
                                halt_notice(
                                    health.census(),
                                    backoffice_addr.as_deref(),
                                    &health.trips(),
                                    &crate::hints::reload()
                                )
                            );
                        }
                    }
                }
            });
        }

        // One head watcher for every pair: the chain has one head, and polling it from
        // inside each pair's loop cost a round-trip per tick per pair (see head.rs).
        let head = head::spawn(
            &mut tasks,
            client.clone(),
            head::poll_interval(args.flags.requote_ms),
            head::POLL_TIMEOUT,
            args.flags.rpc_ws_url.clone(),
            // Registered here and not in `Metrics::new`, so the watcher's three series exist
            // only in the mode that has a watcher: see `register_head`.
            metrics.register_head()?,
        );

        // Told when a pair's task ends rather than joining a fixed set of handles: the set
        // is no longer fixed. A reload adds and removes lanes while the loop below runs, and
        // a halt ends one task while the others carry on.
        let (done, mut finished) = tokio::sync::mpsc::unbounded_channel();
        let pair_states = backoffice::PairStates::default();
        // The observers' tasks, owned by the run like every other. Started here and not
        // above the mode branch: every event is about a lane the service adopts or a
        // reload it answers, so this is the one mode with anything to tell them.
        let (observers, observer_tasks) = crate::observe::Observers::spawn(observers, &metrics);
        for handle in observer_tasks.handles() {
            tasks.adopt(handle);
        }
        let mut service = Service {
            client: client.clone(),
            opts: opts.clone(),
            builders,
            builders_path,
            builders_from_config: args.flags.builders.is_none(),
            backoffice_bound,
            file_settings: file_settings.clone(),
            head,
            metrics: metrics.clone(),
            health: health.clone(),
            reload_metrics: metrics.register_reload()?,
            endpoints: endpoints.clone(),
            price_decimals: args.flags.price_decimals,
            config_path: args.flags.config.clone(),
            registry: args.flags.registry,
            histories,
            readers,
            recorder: recorder.clone(),
            global: cancel.clone(),
            running: std::collections::BTreeMap::new(),
            outstanding: 0,
            next_id: 0,
            vault_pairs,
            pair_states: pair_states.clone(),
            key_lookup: Arc::clone(&key_lookup),
            kinds: Arc::clone(&kinds),
            observers: observers.clone(),
        };
        // From here a shutdown goes through the loop below, which withdraws every quote.
        quoting.store(true, std::sync::atomic::Ordering::SeqCst);
        for (built, spec) in lives.into_iter().zip(specs) {
            service.spawn(built, spec, done.clone());
        }

        // SIGHUP, the signal whose meaning is "re-read your config file" — which since
        // `pairs.toml` is the desired state is the whole of this service's control surface.
        // No socket, no port, nothing to authenticate: `systemctl --user reload` and
        // `kill -HUP` both reach it, and the file's own permissions are the only access
        // control there is, exactly as for the keys beside it.
        //
        // This is the only mode with a supervised pair set to converge, so it is the only
        // one that reads the stream `install_hangup` opened above.
        if reloads.is_some() && !args.flags.once {
            tracing::info!(
                "reload {} with {}; check it first with --check",
                args.flags.config.display(),
                crate::hints::reload_command()
            );
        }

        // Not under `--once`: there is nothing to converge, and binding a port from a batch
        // run is a surprise.
        let _backoffice = match file_settings.backoffice_addr.as_deref() {
            Some(addr) if !args.flags.once => {
                let addr: SocketAddr = addr.parse().wrap_err_with(|| {
                    format!("[settings] backoffice_addr {addr:?} is not host:port")
                })?;
                let (local, task) = backoffice::bind(
                    addr,
                    args.flags.config.clone(),
                    backoffice_tx.clone(),
                    client.clone(),
                    pair_states.clone(),
                    edits.clone(),
                    Arc::clone(&kinds),
                )
                .await?;
                tracing::info!("backoffice listening on http://{local}/");
                tasks.adopt(&task);
                Some(task)
            }
            _ => None,
        };

        loop {
            tokio::select! {
                // Ctrl-c reaches every lane through its own stop, so each withdraws at its
                // builders on the way out instead of leaving a quote standing.
                _ = cancel.wait() => {
                    service.stop_all();
                    break;
                }
                // A pair's task ended: it halted on its breaker, finished cleanly under
                // `--once`, or ended for a reason nothing else reports. A halted lane
                // deliberately stays in the registry — it is exactly what a reload has to
                // find in order to bring it back — and `reap` sorts out which this was.
                Some((lane, id)) = finished.recv() => {
                    service.outstanding = service.outstanding.saturating_sub(1);
                    service.reap(lane, id);
                    if args.flags.once && service.outstanding == 0 {
                        break;
                    }
                }
                Some(_) = async { match reloads.as_mut() {
                    Some(stream) => stream.recv().await,
                    // Never fires. `select!` still polls this arm, so it must be a future
                    // that does not resolve rather than one that resolves to None, which
                    // would spin the loop.
                    None => std::future::pending().await,
                } } => {
                    if args.flags.once {
                        // `--once` is a batch invocation: the pairs publish once and the run
                        // ends. There is no desired state to converge onto and nothing that
                        // would still be running to converge, so the signal is acknowledged
                        // and dropped rather than silently ignored — see the handler above
                        // for why it is installed at all.
                        tracing::warn!("SIGHUP ignored: --once has no running pair set to reload");
                        continue;
                    }
                    let _ = service.reload_and_report(&done, &args.flags.config).await;
                }
                // The backoffice or the handle, which unlike SIGHUP wait for the verdict:
                // a rejection means putting the operator's file back.
                Some(request) = backoffice_reloads.recv() => {
                    let outcome = if args.flags.once {
                        // As for SIGHUP above: a batch run has no pair set to converge.
                        Err(crate::service::ReloadFailure::Rejected(
                            "--once has no running pair set to reload".to_owned(),
                        ))
                    } else {
                        service.reload_and_report(&done, &args.flags.config).await
                    };
                    let _ = request.reply.send(outcome);
                }
            }
        }

        // Every lane has been asked to stop; wait for the withdraws to actually go out.
        // Draining rather than aborting, for the reason `Service::stop` gives.
        service.drain().await;
        // Every lane has ended and said so. What the observers still hold (those
        // `LaneStopped`s, a last block and its landing) is delivered before the run
        // returns and aborts their tasks, bounded by `DELIVERY_GRACE`. Before the `--once`
        // verdict below: that error is the run's own, and the trip behind it is the one
        // event an observer most needs to have been told.
        observer_tasks.finish().await;

        // A pair that halted on its circuit breaker used to park the process here, because
        // returning would end it and *any* exit status invites the restart the halt exists to
        // prevent: zero reads as success to a shell loop or `Restart=always`, and non-zero is
        // precisely what `Restart=on-failure` acts on. The loop above is now what keeps the
        // process up — it exits only when the operator asks it to — and it does something the
        // park could not: a reload brings the halted lane back without the restart that would
        // have cleared every other lane's breaker too.
        //
        // `--once` is unchanged. It is a batch invocation, not a service: a script or CI job
        // is waiting on it, and a trip means the run did not do what it was asked, so it fails
        // rather than parking or reporting success.
        ensure!(
            !(args.flags.once && health.any_halted()),
            "a pair halted on its circuit breaker, so --once did not publish every \
             pair; see the trip reported above"
        );
        finish_recording(recording).await;
        return Ok(Outcome::Success);
    }

    // Said once, before anything is pushed: a binary's observers are not started here,
    // and quiet ones would otherwise look like a broken observer. See observe.rs.
    if let Some(notice) = crate::observe::node_mode_notice(&observers) {
        tracing::warn!("{notice}");
    }
    drop(observers);

    // Node mode has no `Service`, no reload, and so only ever one generation of each pair:
    // every lane adopts its own series here, where the builder path's `Service::spawn` does
    // it. Without this the gauges `adopt_metrics` seeds would stay at the registered 0 that
    // its doc comment explains is unsafe, and the feed would report nothing at all.
    // As in builder mode: the pushes below are the run's to finish.
    quoting.store(true, std::sync::atomic::Ordering::SeqCst);
    let lives: Vec<Live> = lives
        .into_iter()
        .map(|built| {
            crate::pair::adopt(
                &built.live,
                &built.adopted,
                args.flags.price_decimals,
                &metrics,
            );
            built.live
        })
        .collect();

    // RPC path: pairs are pushed sequentially within a tick. Distinct keys mean nonces
    // cannot collide, but --mine (anvil-only) would interleave anvil_mine across
    // concurrent tasks, and latency does not matter on a test path.
    if args.flags.once {
        for live in &lives {
            push_update(&client, live, &opts, true).await?;
        }
        return Ok(Outcome::Success);
    }

    tracing::info!(
        "pushing updates for {} pair(s) every {}s; ctrl-c to stop",
        lives.len(),
        args.flags.interval
    );
    let mut ticker = tokio::time::interval(Duration::from_secs(args.flags.interval));
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut pushes = 0u64;
    // Lanes whose halt was announced: once, not every tick (see `node::push_update`).
    let mut halted_said = std::collections::BTreeSet::new();
    loop {
        tokio::select! {
            // A value, not a signal future: recreating this arm on every iteration cannot
            // miss a SIGINT that arrived while a push was in flight, which a fresh
            // ctrl_c() per select! would (tokio's installed handler has already replaced
            // the default kill-on-SIGINT disposition, so nothing else would notice).
            _ = cancel.wait() => {
                finish_recording(recording).await;
                return Ok(Outcome::Success);
            }
            // Node mode runs no supervised pair set, so there is nothing for a reload to
            // converge — but the signal must still be handled rather than left to its
            // default disposition, which is to kill this process. See `install_hangup`.
            Some(_) = async { match reloads.as_mut() {
                Some(stream) => stream.recv().await,
                None => std::future::pending().await,
            } } => {
                tracing::warn!(
                    "SIGHUP ignored: --mode node has no supervised pair set to reload, so \
                     changing {} needs a restart",
                    args.flags.config.display()
                );
            }
            // The handle's reload, answered rather than left waiting: the backoffice is
            // refused in this mode, so only a `start`ed run can ask.
            Some(request) = backoffice_reloads.recv() => {
                let _ = request.reply.send(Err(crate::service::ReloadFailure::Rejected(format!(
                    "--mode node has no supervised pair set to reload, so changing {} needs \
                     a restart",
                    args.flags.config.display()
                ))));
            }
            _ = ticker.tick() => {
                // 0 is a multiple of everything, so the first tick prices the runway, just
                // as `drive`'s `blocks_seen` starting at 0 makes a lane's first block check
                // it. All pairs share the tick, so one counter serves them all.
                let check_runway = pushes.is_multiple_of(RUNWAY_CHECK_BLOCKS);
                pushes += 1;
                for live in &lives {
                    // A tripped lane is halted here too, and there is no reload to re-arm
                    // it: said once, then skipped rather than refused on every tick.
                    if let Some(trip) = live.latch.tripped() {
                        if halted_said.insert(live.pair.lane) {
                            tracing::warn!(
                                "[{}] {}; halted: node mode has no reload, so this pair will \
                                 not push again until a restart",
                                live.pair.label,
                                trip.reason.alarm
                            );
                        }
                        continue;
                    }
                    // A failed push (RPC blip, stale feed, dropped tx) must not stop the
                    // other pairs or kill the service; the next tick samples fresh values.
                    if let Err(err) = push_update(&client, live, &opts, check_runway).await {
                        tracing::warn!(
                            "[{}] push from {:#x} failed (will retry next tick): {err:#}",
                            live.pair.label,
                            live.pair.signer.address(),
                        );
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use ethrex_common::{Address, U256};

    use super::*;

    #[test]
    fn a_binary_that_aborts_on_panic_is_refused() {
        let err = refuse_panic_abort(true).expect_err("refused");
        assert!(format!("{err}").contains("panic = \"abort\""), "{err}");
        refuse_panic_abort(false).expect("unwinding is what the supervisor needs");
    }

    #[test]
    fn check_passes_only_when_the_report_the_prices_and_the_labels_all_do() {
        assert_eq!(check_outcome(true, true, true), Outcome::Success);
        assert_eq!(check_outcome(false, true, true), Outcome::CheckFailed);
        assert_eq!(check_outcome(true, false, true), Outcome::CheckFailed);
        assert_eq!(check_outcome(true, true, false), Outcome::CheckFailed);
    }

    const ANVIL_KEY_0: &str = "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80";

    /// What every in-process run test starts from: a mock RPC serving one USDC/USDT pair
    /// with a vault, an acking mock builder, and a config file naming both.
    struct Fixture {
        rpc: crate::rpc_mock::MockRpc,
        /// Shared, so a `shutdown_on` future can wait on it.
        builder: std::sync::Arc<crate::builder::mock::Mock>,
        dir: std::path::PathBuf,
        config: std::path::PathBuf,
        /// The pair's tokens, as a handle names it.
        tokens: (Address, Address),
    }

    impl Fixture {
        async fn spawn(tag: &str) -> Self {
            Self::spawn_with(tag, "").await
        }

        /// `spawn`, with `pair_tail` appended to the pair's stanza: a `[[pairs.guards]]`
        /// table, say.
        async fn spawn_with(tag: &str, pair_tail: &str) -> Self {
            let usdc: Address = "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"
                .parse()
                .unwrap();
            let usdt: Address = "0xdAC17F958D2ee523a2206206994597C13D831ec7"
                .parse()
                .unwrap();
            let rpc = crate::rpc_mock::MockRpc::spawn(100).await;
            rpc.set_symbol(usdc, "USDC");
            rpc.set_symbol(usdt, "USDT");
            rpc.set_vault(Address::repeat_byte(0xaa));
            rpc.set_balance(Address::repeat_byte(0xaa), U256::exp10(24));
            let builder = crate::builder::mock::spawn(crate::builder::mock::Behaviour::Ack).await;

            let dir = std::env::temp_dir().join(format!("qu-run-{tag}-{}", std::process::id()));
            std::fs::create_dir_all(&dir).unwrap();
            let config = dir.join("config.toml");
            std::fs::write(
                &config,
                format!(
                    r#"target = "0x70997970C51812dc3A010C7d01b50e0d17dc79C8"

[settings]
rpc_url = "{rpc}"
# A port nothing listens on: no test here streams a market, and one that named a symbol by
# mistake must fail to dial rather than reach the real venue.
binance_ws = "ws://127.0.0.1:1/ws"

[[pairs]]
tokens  = ["{usdc:#x}", "{usdt:#x}"]
key_env = "QU_TEST_KEY"
pricing = {{ kind = "fixed", mid = "1.0001", delta = "0.0002" }}
{pair_tail}
[[builder]]
name = "mock"
endpoint = "{ws}"
api_key = "k"
"#,
                    rpc = rpc.url,
                    ws = builder.url,
                ),
            )
            .unwrap();
            Self {
                rpc,
                builder: std::sync::Arc::new(builder),
                dir,
                config,
                tokens: (usdc, usdt),
            }
        }

        /// Waits until the builder's latest frame is a withdraw (a cancel carries no tx):
        /// the lane stopped, whether halted, tripped or removed.
        async fn wait_for_withdraw(&self, timeout: std::time::Duration) -> bool {
            tokio::time::timeout(timeout, async {
                while !self
                    .builder
                    .received()
                    .last()
                    .is_some_and(|r| r.tx.is_empty())
                {
                    tokio::time::sleep(std::time::Duration::from_millis(5)).await;
                }
            })
            .await
            .is_ok()
        }

        /// The pair as the file has it now.
        fn raw_pair(&self) -> crate::config::RawPair {
            crate::config::load_raw(&self.config)
                .unwrap()
                .pairs
                .remove(0)
        }

        /// The command line for this fixture's config, the metrics port left to the OS,
        /// plus `extra`.
        fn args(&self, extra: &[&str]) -> crate::Args {
            self.args_at("127.0.0.1:0", extra)
        }

        /// [`Self::args`] with the exporter on `metrics_addr`, for a test that scrapes it.
        fn args_at(&self, metrics_addr: &str, extra: &[&str]) -> crate::Args {
            let mut argv = vec![
                "quote-updater",
                "--config",
                self.config.to_str().unwrap(),
                "--metrics-addr",
                metrics_addr,
            ];
            argv.extend_from_slice(extra);
            crate::Args::try_parse_from(argv).unwrap()
        }

        /// The builder every run test uses: the fixture's key and the shipped pricing kinds,
        /// nothing else registered.
        fn updater(&self) -> crate::UpdaterBuilder {
            shipped(crate::Updater::builder())
                .key_lookup(|name| (name == "QU_TEST_KEY").then(|| ANVIL_KEY_0.to_owned()))
        }
    }

    /// `builder` with the three pricing kinds this crate ships registered, as a binary does.
    fn shipped(builder: crate::UpdaterBuilder) -> crate::UpdaterBuilder {
        builder
            .pricer("fixed", crate::pricing::Fixed)
            .pricer("feed", crate::pricing::Feed)
            .pricer("volatile", crate::pricing::Volatile)
    }

    impl Drop for Fixture {
        fn drop(&mut self) {
            std::fs::remove_dir_all(&self.dir).ok();
        }
    }

    /// `run` inside a caller's runtime: it starts, quotes to the builder, stops when
    /// `shutdown_on` fires, and leaves no task of its own behind: the head watcher, the
    /// exporter, the watchdog, the shutdown listener and the pair's own tasks all went
    /// with it.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn run_leaves_no_task_behind_in_the_callers_runtime() {
        use std::time::Duration;
        let fx = Fixture::spawn("owns").await;

        let before = tokio::runtime::Handle::current()
            .metrics()
            .num_alive_tasks();
        let builder = std::sync::Arc::clone(&fx.builder);
        let run = fx
            .updater()
            .shutdown_on(async move {
                // Stop once the builder has seen a quote: the run really started.
                builder.wait_for(1, Duration::from_secs(10)).await;
            })
            .run(fx.args(&[]));
        let outcome = tokio::time::timeout(Duration::from_secs(20), run)
            .await
            .expect("stops within 20s")
            .expect("runs and stops cleanly");
        assert_eq!(outcome, Outcome::Success);
        // Aborted tasks finish on their next poll; give them a moment.
        tokio::time::sleep(Duration::from_millis(300)).await;
        let after = tokio::runtime::Handle::current()
            .metrics()
            .num_alive_tasks();
        assert!(
            after <= before,
            "run left {} task(s) running",
            after - before
        );
    }

    /// `run` failing after the recorder's writer started (here the backoffice cannot bind
    /// its port) takes the writer with it, as it takes every other task: a writer left
    /// behind would keep draining a recorder nobody feeds into a database, inside the
    /// caller's runtime, for as long as that runtime lives.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_run_that_fails_after_recording_started_leaves_no_writer_behind() {
        use std::time::Duration;
        let fx = Fixture::spawn("record-owned").await;
        // Taken, so the backoffice's bind fails once the pairs and the writer are running.
        let taken = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let text = std::fs::read_to_string(&fx.config).unwrap();
        std::fs::write(
            &fx.config,
            text.replacen(
                "[settings]\n",
                &format!(
                    "[settings]\nbackoffice_addr = \"{}\"\n",
                    taken.local_addr().unwrap()
                ),
                1,
            ),
        )
        .unwrap();

        let before = tokio::runtime::Handle::current()
            .metrics()
            .num_alive_tasks();
        let run = fx
            .updater()
            .run(fx.args(&["--record-db-url", "postgresql://u:p@127.0.0.1:1/db"]));
        let err = tokio::time::timeout(Duration::from_secs(20), run)
            .await
            .expect("fails within 20s")
            .expect_err("the backoffice cannot bind");
        assert!(format!("{err:#}").contains("backoffice"), "{err:#}");
        // Aborted tasks finish on their next poll; give them a moment.
        tokio::time::sleep(Duration::from_millis(300)).await;
        let after = tokio::runtime::Handle::current()
            .metrics()
            .num_alive_tasks();
        assert!(
            after <= before,
            "run left {} task(s) running",
            after - before
        );
    }

    /// A shutdown asked for during startup ends the run there: nothing quotes yet, so there
    /// is nothing to withdraw, and waiting for startup to finish could mean waiting for
    /// ever, since the RPC client has no timeout. Here the RPC accepts every connection
    /// and never answers. Paused time, so the test's own bound costs nothing.
    #[tokio::test(start_paused = true)]
    async fn a_shutdown_during_startup_ends_the_run_where_it_is() {
        use std::time::Duration;
        let fx = Fixture::spawn("startup-stop").await;
        let silent = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let silent_url = format!("http://{}", silent.local_addr().unwrap());
        tokio::spawn(async move {
            let mut held = Vec::new();
            while let Ok((stream, _)) = silent.accept().await {
                held.push(stream);
            }
        });
        let text = std::fs::read_to_string(&fx.config).unwrap();
        std::fs::write(&fx.config, text.replace(&fx.rpc.url, &silent_url)).unwrap();

        let handle = fx.updater().start(fx.args(&[]));
        // Long enough for the run to be stuck on its first RPC call.
        tokio::time::sleep(Duration::from_secs(5)).await;
        handle.shutdown();
        let outcome = tokio::time::timeout(Duration::from_secs(60), handle.wait())
            .await
            .expect("a shutdown during startup is not held up by the RPC")
            .expect("a service asked to stop before it quoted stops cleanly");
        assert_eq!(outcome, Outcome::Success);
    }

    /// A batch run stopped before it finished did not do what it was asked: `--check`
    /// stopped during startup is an error, not a passed check a script would act on.
    #[tokio::test(start_paused = true)]
    async fn a_check_stopped_during_startup_is_not_a_pass() {
        use std::time::Duration;
        let fx = Fixture::spawn("startup-stop-check").await;
        let silent = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let silent_url = format!("http://{}", silent.local_addr().unwrap());
        tokio::spawn(async move {
            let mut held = Vec::new();
            while let Ok((stream, _)) = silent.accept().await {
                held.push(stream);
            }
        });
        let text = std::fs::read_to_string(&fx.config).unwrap();
        std::fs::write(&fx.config, text.replace(&fx.rpc.url, &silent_url)).unwrap();

        let handle = fx.updater().start(fx.args(&["--check"]));
        tokio::time::sleep(Duration::from_secs(5)).await;
        handle.shutdown();
        let err = tokio::time::timeout(Duration::from_secs(60), handle.wait())
            .await
            .expect("a shutdown during startup is not held up by the RPC")
            .expect_err("an unfinished check did not pass");
        assert!(format!("{err:#}").contains("--check"), "{err:#}");
    }

    /// `start` inside a caller's runtime: the handle's `reload()` is answered by the run
    /// itself (an unchanged file is "nothing to do"), `shutdown()` stops it the graceful
    /// way and asks nothing of a second call, and `wait()` hands back how it ended.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_handle_reloads_and_shuts_down_a_run_in_the_callers_runtime() {
        use std::time::Duration;
        let fx = Fixture::spawn("handle").await;

        let handle = fx.updater().start(fx.args(&[]));
        fx.builder.wait_for(1, Duration::from_secs(10)).await;

        let line = tokio::time::timeout(Duration::from_secs(10), handle.reload())
            .await
            .expect("the run answers a reload")
            .expect("an unchanged file is accepted");
        assert!(
            line.contains("matches what is running; nothing to do"),
            "{line}"
        );

        handle.shutdown();
        handle.shutdown();
        let outcome = tokio::time::timeout(Duration::from_secs(20), handle.wait())
            .await
            .expect("stops within 20s")
            .expect("runs and stops cleanly");
        assert_eq!(outcome, Outcome::Success);
    }

    /// The handle is shared, never consumed: a caller races `wait()` against a stop trigger
    /// of its own and still calls `shutdown()` from the other arm. The `wait` the select
    /// dropped took nothing with it, so the next one hands back how the run ended, and one
    /// after that answers at once that the run is over rather than waiting forever.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_handle_races_its_wait_against_a_trigger_and_still_shuts_down() {
        use std::time::Duration;
        let fx = Fixture::spawn("handle-select").await;

        let handle = fx.updater().start(fx.args(&[]));
        let builder = std::sync::Arc::clone(&fx.builder);
        let trigger = async move {
            builder.wait_for(1, Duration::from_secs(10)).await;
        };
        let ended = tokio::time::timeout(Duration::from_secs(30), async {
            tokio::select! {
                ended = handle.wait() => ended,
                _ = trigger => {
                    handle.shutdown();
                    handle.wait().await
                }
            }
        })
        .await
        .expect("stops within 30s");
        assert_eq!(ended.expect("runs and stops cleanly"), Outcome::Success);

        let again = tokio::time::timeout(Duration::from_secs(1), handle.wait())
            .await
            .expect("a wait after the end answers at once");
        assert!(
            format!(
                "{:#}",
                again.expect_err("the outcome was already handed back")
            )
            .contains("already"),
        );
    }

    /// A run with no pair set to converge answers a reload with why, rather than leaving
    /// the caller waiting: `--check` here, which reports and returns.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_run_with_nothing_to_reload_says_so() {
        use std::time::Duration;
        let fx = Fixture::spawn("handle-check").await;

        let handle = fx.updater().start(fx.args(&["--check"]));
        let err = tokio::time::timeout(Duration::from_secs(10), handle.reload())
            .await
            .expect("the run answers a reload")
            .expect_err("--check has nothing to reload");
        assert!(err.contains("has stopped"), "{err}");
        let outcome = tokio::time::timeout(Duration::from_secs(20), handle.wait())
            .await
            .expect("ends within 20s")
            .expect("checks cleanly");
        assert_eq!(outcome, Outcome::Success);
    }

    /// Node mode set by the file rather than the flag is node mode all the same: the handle
    /// refuses to halt in it before touching the file, rather than writing the halt and
    /// leaving the run to reject the reload it cannot perform.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_handle_refuses_a_halt_in_node_mode_the_file_chose() {
        use std::time::Duration;
        let fx = Fixture::spawn("handle-node").await;
        let text = std::fs::read_to_string(&fx.config).unwrap();
        std::fs::write(
            &fx.config,
            text.replacen("[settings]\n", "[settings]\nmode = \"node\"\n", 1),
        )
        .unwrap();

        let handle = fx.updater().start(fx.args(&[]));
        let err = tokio::time::timeout(Duration::from_secs(10), handle.halt(fx.tokens, "desk"))
            .await
            .expect("answered")
            .expect_err("node mode has nothing to halt");
        assert!(err.contains("node mode has no service to halt"), "{err}");
        assert!(!fx.raw_pair().halted);

        handle.shutdown();
        let outcome = tokio::time::timeout(Duration::from_secs(20), handle.wait())
            .await
            .expect("stops within 20s")
            .expect("runs and stops cleanly");
        assert_eq!(outcome, Outcome::Success);
    }

    /// A quote guard that halts the lane while its flag is up: a kill switch built on the
    /// latch, which is exactly what §3.4 says not to do, and the test shows why.
    mod kill {
        use std::sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
        };

        use crate::{
            guard::{Candidate, Gate, QuoteGuard, TripReason},
            pricing::{BuildCtx, Diagnostics},
        };

        #[derive(Clone, serde::Deserialize)]
        #[serde(deny_unknown_fields)]
        pub(super) struct Cfg {}

        pub(super) struct Kill(pub(super) Arc<AtomicBool>);

        impl QuoteGuard for Kill {
            fn check(&mut self, _: &Candidate<'_>, _: &mut Diagnostics) -> Gate {
                if self.0.load(Ordering::SeqCst) {
                    Gate::Halt(TripReason::new("kill switch"))
                } else {
                    Gate::Allow
                }
            }
        }

        /// Registers the guard on `builder`, sharing `flag` with the caller.
        pub(super) fn register(
            builder: crate::UpdaterBuilder,
            flag: &Arc<AtomicBool>,
        ) -> crate::UpdaterBuilder {
            let flag = Arc::clone(flag);
            builder.quote_guard_fn("kill", move |_: Cfg, _: &mut BuildCtx| {
                Ok(Kill(Arc::clone(&flag)))
            })
        }
    }

    /// A quote guard whose build trips its own lane's latch, the way a component that
    /// finds something wrong while building would, and then fails if its stanza says
    /// `fail = true`: a trip in the build window, for a lane that starts or for one whose
    /// rebuild is refused.
    mod tripwire {
        use crate::{
            guard::{Candidate, Cause, Gate, QuoteGuard, TripReason},
            pricing::{BuildCtx, Diagnostics},
        };

        #[derive(Clone, serde::Deserialize)]
        #[serde(deny_unknown_fields)]
        pub(super) struct Cfg {
            #[serde(default)]
            fail: bool,
        }

        pub(super) struct Allow;

        impl QuoteGuard for Allow {
            fn check(&mut self, _: &Candidate<'_>, _: &mut Diagnostics) -> Gate {
                Gate::Allow
            }
        }

        pub(super) fn register(builder: crate::UpdaterBuilder) -> crate::UpdaterBuilder {
            builder.quote_guard_fn("tripwire", |cfg: Cfg, ctx: &mut BuildCtx| {
                ctx.latch().trip(
                    "tripwire",
                    Cause::External,
                    TripReason::new("tripped while building"),
                );
                eyre::ensure!(!cfg.fail, "broken after tripping its latch");
                Ok(Allow)
            })
        }
    }

    /// Waits until `seen` holds an event `wanted` matches.
    async fn wait_for_event(
        seen: &std::sync::Mutex<Vec<crate::observe::Event>>,
        wanted: impl Fn(&crate::observe::Event) -> bool,
    ) -> bool {
        tokio::time::timeout(std::time::Duration::from_secs(10), async {
            while !seen.lock().unwrap().iter().any(&wanted) {
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            }
        })
        .await
        .is_ok()
    }

    /// A trip in the build window (a guard's build tripping its own lane, at startup) is
    /// told after the lane's `LaneStarted`, not before: an observer never hears of a trip
    /// on a lane it has not been told started.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_trip_during_the_build_is_told_after_the_lane_started() {
        use crate::observe::{Event, Recorder};
        let fx =
            Fixture::spawn_with("trip-build", "\n[[pairs.guards]]\nkind = \"tripwire\"\n").await;
        let (recorder, seen) = Recorder::new();
        let handle = tripwire::register(fx.updater())
            .observer(recorder)
            .start(fx.args(&[]));
        assert!(
            wait_for_event(&seen, |event| matches!(event, Event::LaneStopped { .. })).await,
            "the tripped lane halts: {:?}",
            seen.lock().unwrap()
        );
        handle.shutdown();
        tokio::time::timeout(std::time::Duration::from_secs(20), handle.wait())
            .await
            .expect("stops")
            .expect("cleanly");

        let seen = seen.lock().unwrap().clone();
        let transitions: Vec<&str> = seen
            .iter()
            .filter_map(|event| match event {
                Event::LaneStarted { .. } => Some("started"),
                Event::Tripped { .. } => Some("tripped"),
                Event::LaneStopped { .. } => Some("stopped"),
                _ => None,
            })
            .collect();
        assert_eq!(transitions, ["started", "tripped", "stopped"], "{seen:?}");
    }

    /// A rebuild whose guard trips the new generation's latch and then fails is refused,
    /// and the lane keeps quoting on its previous config: no trip is told, since the lane
    /// that is quoting never tripped and the latch that did never ran.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_rebuild_that_tripped_and_failed_tells_no_trip() {
        use crate::observe::{Event, Recorder, ReloadOutcome};
        let fx = Fixture::spawn("trip-failed-build").await;
        let (recorder, seen) = Recorder::new();
        let handle = tripwire::register(fx.updater())
            .observer(recorder)
            .start(fx.args(&[]));
        let ten = std::time::Duration::from_secs(10);
        assert!(fx.builder.wait_for(1, ten).await, "the lane quotes first");

        let text = std::fs::read_to_string(&fx.config).unwrap().replacen(
            "\n[[builder]]",
            "\n[[pairs.guards]]\nkind = \"tripwire\"\nfail = true\n\n[[builder]]",
            1,
        );
        std::fs::write(&fx.config, text).unwrap();
        // Partial: the rest of the file applied and stays, so the handle answers `Ok` with
        // the line that says which pair kept its previous config.
        let line = tokio::time::timeout(ten, handle.reload())
            .await
            .expect("answered")
            .expect("the rebuild fails, so the reload is partial, and the file stays");
        assert!(line.contains("1 failed"), "{line}");
        let quotes = fx.builder.count();
        fx.rpc.set_head(101);
        assert!(
            fx.builder.wait_for(quotes + 1, ten).await,
            "the lane still quotes on its previous config"
        );
        // In order, so a trip told during the build would be in by now.
        assert!(
            wait_for_event(&seen, |event| matches!(
                event,
                Event::Reload {
                    outcome: ReloadOutcome::Partial,
                    ..
                }
            ))
            .await,
            "{:?}",
            seen.lock().unwrap()
        );
        handle.shutdown();
        tokio::time::timeout(std::time::Duration::from_secs(20), handle.wait())
            .await
            .expect("stops")
            .expect("cleanly");
        let seen = seen.lock().unwrap().clone();
        assert!(
            !seen
                .iter()
                .any(|event| matches!(event, Event::Tripped { .. })),
            "{seen:?}"
        );
    }

    /// A run that ends normally (a shutdown, `--once`) lets each observer deliver what is
    /// already queued before it returns: the lane's `LaneStopped` from the drain on the
    /// way out, which `Event::LaneStopped` promises, reaches even an observer that takes a
    /// webhook's round trip over every event.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_run_that_ends_normally_delivers_the_lanes_last_word() {
        use std::{
            sync::{Arc, Mutex},
            time::Duration,
        };

        use crate::{
            observe::{Event, Observer},
            pricing::BoxFuture,
        };

        struct Webhook(Arc<Mutex<Vec<Event>>>);
        impl Observer for Webhook {
            fn name(&self) -> &'static str {
                "webhook"
            }
            fn on_event<'a>(&'a mut self, event: &'a Event) -> BoxFuture<'a, ()> {
                let seen = Arc::clone(&self.0);
                Box::pin(async move {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    seen.lock().unwrap().push(event.clone());
                })
            }
        }

        let ten = Duration::from_secs(10);
        for once in [false, true] {
            let fx = Fixture::spawn(if once { "last-word-once" } else { "last-word" }).await;
            let seen = Arc::new(Mutex::new(Vec::new()));
            let handle = fx
                .updater()
                .observer(Webhook(Arc::clone(&seen)))
                .start(fx.args(if once { &["--once"] } else { &[] }));
            assert!(fx.builder.wait_for(1, ten).await, "the lane quotes");
            if once {
                // `--once` ends on the block it quoted passing.
                fx.rpc.set_head(101);
            } else {
                handle.shutdown();
            }
            let outcome = tokio::time::timeout(Duration::from_secs(20), handle.wait())
                .await
                .expect("ends")
                .expect("cleanly");
            assert_eq!(outcome, Outcome::Success);
            // Read at once: what the run did not deliver before it returned, it never will.
            let seen = seen.lock().unwrap().clone();
            assert!(
                matches!(seen.last(), Some(Event::LaneStopped { pair }) if pair == "USDC/USDT"),
                "once = {once}: {seen:?}"
            );
        }
    }

    /// `--mode node` adopts no lane through a `Service`, so it tells observers nothing: a
    /// binary that registered one is told so once, at startup, rather than left to wonder
    /// why its observer is quiet on an anvil or a fork.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn node_mode_tells_observers_nothing_and_says_so_once() {
        use std::time::Duration;

        use crate::observe::Recorder;
        // The run is awaited on this thread, so the lines its own body says come through
        // this thread's subscriber.
        let (said, subscriber) = crate::output::capturing();
        let _said = tracing::subscriber::set_default(subscriber);
        let fx = Fixture::spawn("node-observers").await;
        let (recorder, seen) = Recorder::new();
        let run = fx
            .updater()
            .observer(recorder)
            .shutdown_on(tokio::time::sleep(Duration::from_millis(300)))
            .run(fx.args(&["--mode", "node"]));
        let outcome = tokio::time::timeout(Duration::from_secs(20), run)
            .await
            .expect("stops within 20s")
            .expect("runs and stops cleanly");
        assert_eq!(outcome, Outcome::Success);

        let notices: Vec<String> = said
            .lines()
            .into_iter()
            .map(|(_, line)| line)
            .filter(|line| line.contains("`recorder`"))
            .collect();
        assert_eq!(notices.len(), 1, "{notices:?}");
        assert!(notices[0].contains("--mode node"), "{notices:?}");
        assert_eq!(*seen.lock().unwrap(), [], "told nothing");
    }

    /// A halt is an intent, in the file: a reload leaves it alone and only `resume` clears
    /// it. A trip is a judgement, on the latch: the next reload re-arms the lane.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_halt_survives_a_reload_and_a_trip_does_not() {
        use std::{
            sync::{Arc, atomic::AtomicBool, atomic::Ordering},
            time::Duration,
        };
        let fx = Fixture::spawn_with("halt", "\n[[pairs.guards]]\nkind = \"kill\"\n").await;
        let killed = Arc::new(AtomicBool::new(false));
        let handle = kill::register(fx.updater(), &killed).start(fx.args(&[]));
        let ten = Duration::from_secs(10);
        assert!(fx.builder.wait_for(1, ten).await, "the lane quotes first");

        // The halt: written to the file, applied by a reload, the quote withdrawn.
        let line = tokio::time::timeout(ten, handle.halt(fx.tokens, "risk desk says so"))
            .await
            .expect("the run answers")
            .expect("the halt is applied");
        assert!(line.contains("halted pair QU_TEST_KEY"), "{line}");
        let pair = fx.raw_pair();
        assert!(pair.halted, "the file carries the halt");
        assert_eq!(pair.halt_reason.as_deref(), Some("risk desk says so"));
        assert!(
            fx.wait_for_withdraw(ten).await,
            "the quote is withdrawn at the builder"
        );

        // A reload finds the file as it is and changes nothing: the halt stands.
        let line = tokio::time::timeout(ten, handle.reload())
            .await
            .expect("the run answers")
            .expect("an unchanged file reloads");
        assert!(line.contains("nothing to do"), "{line}");
        assert!(fx.raw_pair().halted, "a reload does not clear a halt");

        // Only a resume does, and resuming what is not halted is refused.
        let quotes = fx.builder.count();
        let line = tokio::time::timeout(ten, handle.resume(fx.tokens))
            .await
            .expect("the run answers")
            .expect("the resume is applied");
        assert!(line.contains("resumed pair QU_TEST_KEY"), "{line}");
        let pair = fx.raw_pair();
        assert!(!pair.halted && pair.halt_reason.is_none(), "{pair:?}");
        assert!(
            fx.builder.wait_for(quotes + 1, ten).await,
            "the lane quotes again"
        );
        let err = tokio::time::timeout(ten, handle.resume(fx.tokens))
            .await
            .expect("the run answers")
            .expect_err("nothing to resume");
        assert!(err.contains("not halted"), "{err}");

        // The trip: the guard halts the lane on the next block, and a reload re-arms it.
        killed.store(true, Ordering::SeqCst);
        fx.rpc.set_head(101);
        assert!(
            fx.wait_for_withdraw(ten).await,
            "the tripped lane withdraws"
        );
        killed.store(false, Ordering::SeqCst);
        let quotes = fx.builder.count();
        let line = tokio::time::timeout(ten, handle.reload())
            .await
            .expect("the run answers")
            .expect("the reload re-arms");
        assert!(line.contains("1 restarted"), "{line}");
        assert!(
            fx.builder.wait_for(quotes + 1, ten).await,
            "the re-armed lane quotes"
        );

        handle.shutdown();
        let outcome = tokio::time::timeout(Duration::from_secs(20), handle.wait())
            .await
            .expect("stops within 20s")
            .expect("runs and stops cleanly");
        assert_eq!(outcome, Outcome::Success);
    }

    /// A halt whose reload applies only in part, because another lane added beside it would
    /// not build, is still a halt: the reload stopped the pair, so the file keeps saying
    /// so, the answer says the change applied rather than "nothing changed", and the next
    /// reload leaves the pair stopped instead of reading a put-back file and restarting it.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_halt_sticks_when_another_lane_fails_its_reload() {
        use std::time::Duration;
        const ANVIL_KEY_1: &str =
            "0x59c6995e998f97a5a0044966f0945389dc9e86dae88c7a8412f4603b6b78690d";
        let fx = Fixture::spawn("halt-partial").await;
        let ten = Duration::from_secs(10);
        // A kind whose stanza checks out and whose build never does: the one way a lane can
        // fail its reload after the file was accepted, with no market dialled and no chain
        // read.
        #[derive(Clone, serde::Deserialize)]
        struct NoKeys {}
        let handle = shipped(crate::Updater::builder())
            .pricer_fn(
                "unbuildable",
                |_: NoKeys, _: &mut crate::pricing::BuildCtx| {
                    Err::<crate::pricing::FixedPricer, _>(eyre::eyre!("this kind never builds"))
                },
            )
            .key_lookup(|name| match name {
                "QU_TEST_KEY" => Some(ANVIL_KEY_0.to_owned()),
                "QU_TEST_KEY_B" => Some(ANVIL_KEY_1.to_owned()),
                _ => None,
            })
            .start(fx.args(&[]));
        assert!(fx.builder.wait_for(1, ten).await, "the lane quotes first");

        // A lane added by hand on that kind, so its build fails at once and the reload the
        // halt asks for applies everywhere but there.
        let weth: Address = "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2"
            .parse()
            .unwrap();
        fx.rpc.set_symbol(weth, "WETH");
        let text = std::fs::read_to_string(&fx.config).unwrap();
        let usdc = format!("{:#x}", fx.tokens.0);
        std::fs::write(
            &fx.config,
            text.replacen(
                "[[builder]]",
                &format!(
                    "[[pairs]]\ntokens = [\"{weth:#x}\", \"{usdc}\"]\n\
                     key_env = \"QU_TEST_KEY_B\"\npricing = {{ kind = \"unbuildable\" }}\n\n[[builder]]"
                ),
                1,
            ),
        )
        .unwrap();

        let line = tokio::time::timeout(ten, handle.halt(fx.tokens, "desk"))
            .await
            .expect("answered")
            .expect("the halt applied, though not every lane did");
        assert!(!line.contains("nothing changed"), "{line}");
        assert!(
            line.contains("1 halted") && line.contains("1 failed"),
            "{line}"
        );
        assert!(fx.raw_pair().halted, "the file keeps the halt");
        assert!(fx.wait_for_withdraw(ten).await, "the lane was stopped");

        // And stays stopped: the next reload reads `halted = true`, not a put-back file.
        let quoted = fx.builder.received().len();
        let _ = tokio::time::timeout(ten, handle.reload())
            .await
            .expect("answered");
        tokio::time::sleep(Duration::from_millis(500)).await;
        assert!(
            fx.builder.received()[quoted..]
                .iter()
                .all(|frame| frame.tx.is_empty()),
            "a reload restarted the halted lane"
        );

        handle.shutdown();
        tokio::time::timeout(Duration::from_secs(20), handle.wait())
            .await
            .expect("stops within 20s")
            .expect("runs and stops cleanly");
    }

    /// An observer registered on the builder sees the run's transitions in order (a lane
    /// started, tripped and stopped, re-armed by a reload, halted by the handle) and one
    /// `Block` per block that passed, never one per tick, with the block's landing. None
    /// for the block a halt broke out of: it has not passed, and the generation the reload
    /// re-arms is the one that sees it pass and says so.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn events_arrive_at_transitions_and_once_per_block_never_per_tick() {
        use std::{
            collections::BTreeSet,
            sync::{Arc, Mutex, atomic::AtomicBool, atomic::Ordering},
            time::Duration,
        };

        use crate::{
            guard::Cause,
            observe::{BlockOutcome, Event, Observer, ReloadOutcome},
            pricing::BoxFuture,
        };

        struct Recorder(Arc<Mutex<Vec<Event>>>);
        impl Observer for Recorder {
            fn name(&self) -> &'static str {
                "recorder"
            }
            fn on_event<'a>(&'a mut self, event: &'a Event) -> BoxFuture<'a, ()> {
                let seen = Arc::clone(&self.0);
                Box::pin(async move { seen.lock().unwrap().push(event.clone()) })
            }
        }

        let fx = Fixture::spawn_with("observe", "\n[[pairs.guards]]\nkind = \"kill\"\n").await;
        let killed = Arc::new(AtomicBool::new(false));
        let seen = Arc::new(Mutex::new(Vec::new()));
        let handle = kill::register(fx.updater(), &killed)
            .observer(Recorder(Arc::clone(&seen)))
            .start(fx.args(&[]));
        let ten = Duration::from_secs(10);
        assert!(fx.builder.wait_for(1, ten).await, "block 100 is quoted");
        fx.rpc.set_head(101);
        assert!(fx.builder.wait_for(2, ten).await, "block 101 is quoted");

        // The trip, on the next block; then a reload re-arms the lane.
        killed.store(true, Ordering::SeqCst);
        fx.rpc.set_head(102);
        assert!(
            fx.wait_for_withdraw(ten).await,
            "the tripped lane withdraws"
        );
        killed.store(false, Ordering::SeqCst);
        let quotes = fx.builder.count();
        let line = tokio::time::timeout(ten, handle.reload())
            .await
            .expect("answered")
            .expect("re-arms");
        assert!(line.contains("1 restarted"), "{line}");
        assert!(
            fx.builder.wait_for(quotes + 1, ten).await,
            "the lane quotes again"
        );
        // Block 103, the one the trip broke out of (the head stayed at 102 since), now
        // passes for the re-armed lane. Waited for on the observer: the builder's count
        // includes its own re-sends, so it says nothing about a block.
        fx.rpc.set_head(103);
        assert!(
            wait_for_event(&seen, |event| matches!(
                event,
                Event::Block {
                    block: 103,
                    outcome: BlockOutcome::Quoted { .. },
                    ..
                }
            ))
            .await,
            "block 103 passes quoted: {:?}",
            seen.lock().unwrap()
        );

        // A halt through the handle, then shutdown.
        tokio::time::timeout(ten, handle.halt(fx.tokens, "desk"))
            .await
            .expect("answered")
            .expect("halts");
        assert!(fx.wait_for_withdraw(ten).await, "the halted lane withdraws");
        handle.shutdown();
        tokio::time::timeout(Duration::from_secs(20), handle.wait())
            .await
            .expect("stops")
            .expect("cleanly");

        let seen = seen.lock().unwrap().clone();
        let transitions: Vec<&str> = seen
            .iter()
            .filter_map(|event| match event {
                Event::LaneStarted { .. } => Some("started"),
                Event::LaneStopped { .. } => Some("stopped"),
                Event::Tripped { source, cause, .. } => {
                    assert_eq!((*source, *cause), ("kill", Cause::Guard));
                    Some("tripped")
                }
                Event::Rearmed { .. } => Some("rearmed"),
                Event::Reload { outcome } => {
                    assert_eq!(*outcome, ReloadOutcome::Applied);
                    Some("reload")
                }
                _ => None,
            })
            .collect();
        assert_eq!(
            transitions,
            [
                "started", "tripped", "stopped", "started", "rearmed", "reload", "stopped",
                "reload"
            ],
            "{seen:?}"
        );

        // Blocks: one event per block, none per tick, and the quoted ones say what.
        let blocks: Vec<(u64, &BlockOutcome)> = seen
            .iter()
            .filter_map(|event| match event {
                Event::Block { block, outcome, .. } => Some((*block, outcome)),
                _ => None,
            })
            .collect();
        let distinct: BTreeSet<u64> = blocks.iter().map(|(block, _)| *block).collect();
        assert_eq!(
            blocks.len(),
            distinct.len(),
            "one Block per block: {blocks:?}"
        );
        // The lane quotes for the head's successor, so block 101 is the first that passes
        // with a quote standing. The kill switch tripped on 103 before it passed, so the
        // tripped generation said nothing of it (the trip is the `Tripped` above) and the
        // re-armed one, which quoted it, said it passed: once, which the count above holds
        // to.
        assert!(
            blocks
                .iter()
                .any(|(block, outcome)| *block == 101
                    && matches!(outcome, BlockOutcome::Quoted { .. })),
            "{blocks:?}"
        );
        assert!(
            !blocks
                .iter()
                .any(|(_, outcome)| matches!(outcome, BlockOutcome::Withdrawn { .. })),
            "no block passed withdrawn: {blocks:?}"
        );
        assert!(
            seen.iter()
                .any(|event| matches!(event, Event::Landing { .. })),
            "each block's read-back is reported"
        );
    }

    /// `trip_source{pair,source,cause}` is what the breaker page's annotation points the
    /// operator at, so it must say what is tripped *now*: 1 from the trip, 0 once a reload
    /// re-arms the lane, and no reading at all once the lane is gone, exactly as every
    /// other gauge of a removed pair. The guard's presence gauge goes with the lane too.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_rearm_lowers_trip_source_and_a_removal_retires_it() {
        use std::{
            sync::{Arc, atomic::AtomicBool, atomic::Ordering},
            time::Duration,
        };
        let fx = Fixture::spawn_with("trip-source", "\n[[pairs.guards]]\nkind = \"kill\"\n").await;
        // A port of this test's own: the exporter's address is not read back from the run.
        let metrics_addr = std::net::TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap()
            .to_string();
        let killed = Arc::new(AtomicBool::new(false));
        let handle = kill::register(fx.updater(), &killed).start(fx.args_at(&metrics_addr, &[]));
        let ten = Duration::from_secs(10);
        assert!(fx.builder.wait_for(1, ten).await, "the lane quotes first");
        let series = |text: &str, name: &str| -> Option<String> {
            text.lines()
                .find(|line| line.starts_with(name) && line.contains(r#"pair="USDC/USDT""#))
                .map(str::to_owned)
        };
        let scrape = || async {
            reqwest::get(format!("http://{metrics_addr}/metrics"))
                .await
                .expect("the exporter answers")
                .text()
                .await
                .unwrap()
        };

        killed.store(true, Ordering::SeqCst);
        fx.rpc.set_head(101);
        assert!(
            fx.wait_for_withdraw(ten).await,
            "the tripped lane withdraws"
        );
        let text = scrape().await;
        let tripped = series(&text, "quote_updater_trip_source").expect("recorded at the trip");
        assert!(
            tripped.contains(r#"source="kill""#)
                && tripped.contains(r#"cause="guard""#)
                && tripped.ends_with(" 1"),
            "{tripped}"
        );

        killed.store(false, Ordering::SeqCst);
        tokio::time::timeout(ten, handle.reload())
            .await
            .expect("answered")
            .expect("re-arms");
        let text = scrape().await;
        let rearmed = series(&text, "quote_updater_trip_source").unwrap();
        assert!(
            rearmed.ends_with(" 0"),
            "re-armed, so nothing is tripped now: {rearmed}"
        );
        assert!(
            series(&text, "quote_updater_guards")
                .unwrap()
                .ends_with(" 1")
        );

        tokio::time::timeout(ten, handle.halt(fx.tokens, "desk"))
            .await
            .expect("answered")
            .expect("halts");
        assert!(fx.wait_for_withdraw(ten).await, "the halted lane withdraws");
        let text = scrape().await;
        for name in ["quote_updater_trip_source", "quote_updater_guards"] {
            let line = series(&text, name).unwrap();
            assert!(line.ends_with(" NaN"), "the lane is gone: {line}");
        }

        handle.shutdown();
        tokio::time::timeout(Duration::from_secs(20), handle.wait())
            .await
            .expect("stops")
            .expect("cleanly");
    }

    /// A halt and a reload issued together: whichever the service takes first, the halt is
    /// applied exactly once, both are answered, and neither claims what the other did.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_halt_and_a_reload_serialize_through_the_service() {
        use std::time::Duration;
        let fx = Fixture::spawn("halt-race").await;
        let handle = fx.updater().start(fx.args(&[]));
        let ten = Duration::from_secs(10);
        assert!(fx.builder.wait_for(1, ten).await, "the lane quotes first");

        let both = tokio::join!(handle.halt(fx.tokens, "desk"), handle.reload());
        let (halt, reload) = tokio::time::timeout(ten, async { both })
            .await
            .expect("both answered");
        let halt = halt.expect("the halt is applied whichever ran first");
        let reload = reload.expect("the reload is answered whichever ran first");
        let applied = [&halt, &reload]
            .iter()
            .filter(|line| line.contains("1 halted"))
            .count();
        assert_eq!(
            applied, 1,
            "exactly one reload stopped the lane: {halt} / {reload}"
        );
        assert!(fx.raw_pair().halted);
        assert!(
            fx.wait_for_withdraw(ten).await,
            "the quote is withdrawn at the builder"
        );

        handle.shutdown();
        let outcome = tokio::time::timeout(Duration::from_secs(20), handle.wait())
            .await
            .expect("stops within 20s")
            .expect("runs and stops cleanly");
        assert_eq!(outcome, Outcome::Success);
    }

    /// A pricing kind registered through the public builder, as a downstream binary does it,
    /// and judged the same way by `--check` and by the run.
    mod custom_through_the_builder {
        use std::{sync::Arc, time::Duration};

        use super::*;
        use crate::pricing::{
            BoxFuture, BuildCtx, DiagHandle, Diagnostics, Factory, PairShape, Pricer, PricerOutput,
            Refusal, RefusalHandle, TickCtx,
        };

        #[derive(Clone, serde::Deserialize)]
        #[serde(deny_unknown_fields)]
        struct FlatCfg {
            #[serde(default)]
            refuse: bool,
        }

        /// A fixed [delta, mid], so the pair needs no feed and the test no Binance.
        struct Flat;

        impl Pricer for Flat {
            fn price(&mut self, _: &TickCtx, _: &mut Diagnostics) -> Result<PricerOutput, Refusal> {
                Ok(PricerOutput::new(U256::exp10(14), U256::exp10(18)))
            }
        }

        /// `Flat`, after setting a diagnostic and reading it back: it refuses when the tick's
        /// buffer had no slot for what its build declared.
        struct Echo {
            seen: DiagHandle,
            unseen: RefusalHandle,
        }

        impl Pricer for Echo {
            fn price(
                &mut self,
                tick: &TickCtx,
                out: &mut Diagnostics,
            ) -> Result<PricerOutput, Refusal> {
                out.set(&self.seen, 1.0);
                if out.get(&self.seen).is_none() {
                    return Err(Refusal::new(&self.unseen, "the diagnostic it set is gone"));
                }
                Flat.price(tick, out)
            }
        }

        /// `Flat` behind a `validate` that refuses a stanza saying `refuse`.
        struct Picky;

        impl Factory for Picky {
            type Config = FlatCfg;
            type Pricer = Flat;

            fn validate(&self, cfg: &FlatCfg, _: &PairShape) -> eyre::Result<()> {
                eyre::ensure!(!cfg.refuse, "refuse is set");
                Ok(())
            }

            fn build<'a>(
                &'a self,
                _: &'a FlatCfg,
                _: &'a mut BuildCtx,
            ) -> BoxFuture<'a, eyre::Result<Flat>> {
                Box::pin(async { Ok(Flat) })
            }
        }

        /// The mock chain and builder, and a one-pair file whose `[pairs.pricing]` is
        /// `stanza`.
        async fn setup(
            name: &str,
            stanza: &str,
        ) -> (
            crate::rpc_mock::MockRpc,
            crate::builder::mock::Mock,
            std::path::PathBuf,
        ) {
            let usdc: Address = "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"
                .parse()
                .unwrap();
            let usdt: Address = "0xdAC17F958D2ee523a2206206994597C13D831ec7"
                .parse()
                .unwrap();
            let rpc = crate::rpc_mock::MockRpc::spawn(100).await;
            rpc.set_symbol(usdc, "USDC");
            rpc.set_symbol(usdt, "USDT");
            rpc.set_vault(Address::repeat_byte(0xaa));
            rpc.set_balance(Address::repeat_byte(0xaa), U256::exp10(24));
            let builder = crate::builder::mock::spawn(crate::builder::mock::Behaviour::Ack).await;
            let dir = std::env::temp_dir().join(format!("qu-{name}-{}", std::process::id()));
            std::fs::create_dir_all(&dir).unwrap();
            std::fs::write(
                dir.join("config.toml"),
                format!(
                    r#"target = "0x70997970C51812dc3A010C7d01b50e0d17dc79C8"

[settings]
rpc_url = "{rpc}"

[[pairs]]
tokens  = ["{usdc:#x}", "{usdt:#x}"]
key_env = "QU_TEST_KEY"

[pairs.pricing]
{stanza}

[[builder]]
name = "mock"
endpoint = "{ws}"
api_key = "k"
"#,
                    rpc = rpc.url,
                    ws = builder.url,
                ),
            )
            .unwrap();
            (rpc, builder, dir)
        }

        fn args(dir: &std::path::Path, extra: &[&str]) -> crate::Args {
            let config = dir.join("config.toml");
            let mut argv = vec![
                "quote-updater",
                "--config",
                config.to_str().unwrap(),
                "--metrics-addr",
                "127.0.0.1:0",
            ];
            argv.extend_from_slice(extra);
            crate::Args::try_parse_from(argv).unwrap()
        }

        fn key(name: &str) -> Option<String> {
            (name == "QU_TEST_KEY").then(|| ANVIL_KEY_0.to_owned())
        }

        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn a_kind_registered_with_pricer_fn_quotes_through_run() {
            let (_rpc, builder, dir) = setup("custom-run", r#"kind = "flat""#).await;
            let builder = Arc::new(builder);
            let seen = Arc::clone(&builder);
            let run = crate::Updater::builder()
                .pricer_fn("flat", |_: FlatCfg, ctx: &mut BuildCtx| {
                    ctx.diagnostic("seen")?;
                    Ok(Flat)
                })
                .key_lookup(key)
                .shutdown_on(async move {
                    seen.wait_for(1, Duration::from_secs(10)).await;
                })
                .run(args(&dir, &[]));
            let outcome = tokio::time::timeout(Duration::from_secs(20), run)
                .await
                .expect("stops within 20s")
                .expect("runs and stops cleanly");
            assert_eq!(outcome, Outcome::Success);
            assert!(
                builder.count() >= 1,
                "the custom lane quoted to the builder"
            );
            std::fs::remove_dir_all(&dir).ok();
        }

        /// `--check` previews with a diagnostics buffer sized for what the build declared, as
        /// the run does, so a pricer reading back its own diagnostic sees the same there.
        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn check_previews_with_the_diagnostics_the_build_declared() {
            let (_rpc, _builder, dir) = setup("custom-check-diag", r#"kind = "echo""#).await;
            let checked = crate::Updater::builder()
                .pricer_fn("echo", |_: FlatCfg, ctx: &mut BuildCtx| {
                    Ok(Echo {
                        seen: ctx.diagnostic("seen")?,
                        unseen: ctx.refusal("unseen")?,
                    })
                })
                .key_lookup(key)
                .run(args(&dir, &["--check"]))
                .await
                .expect("--check reports, it does not error");
            assert_eq!(checked, Outcome::Success);
            std::fs::remove_dir_all(&dir).ok();
        }

        /// `--check` exists to catch what the run would refuse: a stanza its kind's
        /// `validate` rejects fails the check, not only the start.
        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn check_refuses_a_stanza_the_runs_validate_refuses() {
            let (_rpc, _builder, dir) =
                setup("custom-check", "kind = \"picky\"\nrefuse = true").await;
            let updater = || {
                crate::Updater::builder()
                    .pricer("picky", Picky)
                    .key_lookup(key)
            };
            let checked = updater()
                .run(args(&dir, &["--check"]))
                .await
                .expect("--check reports, it does not error");
            assert_eq!(checked, Outcome::CheckFailed);
            let run = tokio::time::timeout(Duration::from_secs(20), updater().run(args(&dir, &[])))
                .await
                .expect("refused, not left running");
            let err = format!("{:#}", run.expect_err("the run refuses it"));
            assert!(err.contains("refuse is set"), "{err}");
            std::fs::remove_dir_all(&dir).ok();
        }
    }
}
