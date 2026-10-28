//! The entry point a binary assembles: `Updater::builder()`, then `run` or `run_from_env`.
//!
//! `run` belongs to the caller's process: it runs on the caller's tokio runtime and returns
//! how it ended instead of exiting. `run_from_env` is the one place allowed to own a
//! process: it parses the command line, builds the runtime and turns the result into an
//! exit code, so a binary's `main` is one line.

use std::{path::PathBuf, process::ExitCode, sync::Arc};

use tokio::{
    sync::{mpsc, oneshot},
    task::JoinHandle,
};

use crate::{
    Address, backoffice::ReloadRequest, cli::Args, config::RawPair, service::ReloadFailure,
};

/// A quote updater, assembled with [`Updater::builder`].
pub struct Updater;

impl Updater {
    /// Starts assembling an updater. Nothing is registered yet, the three pricing kinds
    /// this crate ships (`pricers::{Fixed, Feed, Volatile}`) included: register the kinds
    /// the config may name on the builder, then run it.
    pub fn builder() -> UpdaterBuilder {
        UpdaterBuilder {
            parts: Parts::default(),
        }
    }
}

/// Collects what a binary adds to the library, then runs it. See [`Updater::builder`].
pub struct UpdaterBuilder {
    parts: Parts,
}

/// What the builder hands `run`. Its own type so `pusher::run` takes one value however
/// many things a binary can register.
#[derive(Default)]
pub(crate) struct Parts {
    /// What stops the run instead of ctrl-c; see [`UpdaterBuilder::shutdown_on`].
    pub(crate) shutdown: Option<std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>>>,
    /// Where updater keys are read from; `None` is the process environment.
    pub(crate) key_lookup: Option<crate::config::KeyLookup>,
    /// The pricing kinds this binary registered.
    pub(crate) kinds: crate::kinds::Kinds,
    /// The channel an [`UpdaterHandle`] reloads through, when the run was `start`ed.
    pub(crate) control: Option<Control>,
    /// The observers this binary registered, in order.
    pub(crate) observers: Vec<Box<dyn crate::observe::Observer>>,
    /// The binary's own `tracing` targets, rendered by the operator layer `run_from_env`
    /// installs; see [`UpdaterBuilder::log_target`].
    pub(crate) log_targets: Vec<&'static str>,
    /// The systemd unit the operator hints name; see [`UpdaterBuilder::systemd_unit`].
    pub(crate) unit: Option<&'static str>,
}

impl Parts {
    /// What the binary got wrong registering, all of it: a kind twice, a guard under the
    /// deviation breaker's name, two observers under one name. One check for `run` and
    /// `run_from_env`, so both refuse the same things before reading anything.
    pub(crate) fn registration_errors(&self) -> eyre::Result<()> {
        self.kinds.errors()?;
        crate::observe::refuse_duplicate_names(&self.observers)
    }
}

/// The reload channel a `start`ed run reads beside the backoffice's: the requests are
/// the backoffice's own type, so the loop that answers a page's click answers the handle
/// with the same verdict, and the backoffice, if bound, sends on `reloads` too.
pub(crate) struct Control {
    pub(crate) reloads: mpsc::Sender<ReloadRequest>,
    pub(crate) requests: mpsc::Receiver<ReloadRequest>,
    /// Serializes the handle's edits of the config file with the backoffice's.
    pub(crate) edits: Arc<tokio::sync::Mutex<()>>,
    /// The mode the run settled on, published once `[settings]` has been applied: the flag
    /// alone does not say, since `[settings] mode` decides when no flag or variable did.
    pub(crate) mode: tokio::sync::watch::Sender<Option<crate::cli::Mode>>,
}

/// How a run ended, for the caller to turn into an exit status.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum Outcome {
    /// Shut down on request, finished (`--once`), or `--check` found nothing wrong.
    Success,
    /// `--check` found a problem; the report it printed says which.
    CheckFailed,
}

impl Outcome {
    /// The exit status a process should end with: 0 for success, 1 for a failed check.
    pub fn exit_code(self) -> ExitCode {
        match self {
            Outcome::Success => ExitCode::SUCCESS,
            Outcome::CheckFailed => ExitCode::FAILURE,
        }
    }
}

impl UpdaterBuilder {
    /// Names a `tracing` target of the binary's own, so the operator layer
    /// [`Self::run_from_env`] installs renders its lines beside this crate's: the crate's
    /// name (`env!("CARGO_CRATE_NAME")`) covers every module under it. Without it, the
    /// layer renders this crate's events only, and an observer's `tracing::info!` goes
    /// nowhere: a layer that rendered every crate would print ethrex's and hyper's too. A
    /// binary that installs a subscriber of its own before `run_from_env` needs none of
    /// this; `run_from_env` then leaves that subscriber alone.
    pub fn log_target(mut self, target: &'static str) -> Self {
        self.parts.log_targets.push(target);
        self
    }

    /// Names the systemd user unit this binary runs as, so the operator hints the library
    /// prints (how to reload after a halt, how to restart after a settings change) say
    /// `systemctl --user reload <unit>` rather than `SIGHUP`. Process-wide, once: the
    /// first run (`run`, `start` or `run_from_env`) in a process decides for any other,
    /// and a different unit named by a later one is refused with a warning.
    pub fn systemd_unit(mut self, unit: &'static str) -> Self {
        self.parts.unit = Some(unit);
        self
    }

    /// Registers a pricing kind: a pair whose `[pairs.pricing]` stanza says `kind = "<kind>"`
    /// is priced by what `factory` builds from the rest of that stanza. The kinds this crate
    /// ships are registered the same way (`pricers::{Fixed, Feed, Volatile}`), under
    /// whatever names the binary picks. Registering a name twice is the binary's bug: `run`
    /// refuses it before reading any config, and `run_from_env` before parsing the command
    /// line, so it shows on every invocation, `--help` included.
    pub fn pricer<F: crate::pricing::Factory>(mut self, kind: &'static str, factory: F) -> Self {
        self.parts.kinds.insert(kind, std::sync::Arc::new(factory));
        self
    }

    /// Registers a pricing kind built by a closure, for a pricer that needs no async build
    /// and no `validate`: `make` gets the stanza's config (its own copy, to keep) and the
    /// build's context, and returns the pricer. Background work goes through `ctx.spawn`.
    ///
    /// `make` runs synchronously, on a runtime thread, inside the lane's build: a blocking
    /// call there (an HTTP fetch, a file on a slow mount) stalls startup or the reload, and
    /// the 30s build limit cannot interrupt it, since a timeout fires only when the build
    /// yields. A pricer that must fetch before it can price implements
    /// [`crate::pricing::Factory`], whose `build` is async.
    pub fn pricer_fn<C, P, F>(mut self, kind: &'static str, make: F) -> Self
    where
        C: serde::de::DeserializeOwned + Clone + Send + Sync + 'static,
        P: crate::pricing::Pricer,
        F: Fn(C, &mut crate::pricing::BuildCtx) -> eyre::Result<P> + Send + Sync + 'static,
    {
        self.parts
            .kinds
            .insert(kind, crate::kinds::fn_factory(make));
        self
    }

    /// Registers a market guard kind: a pair whose `[[pairs.guards]]` stanza says
    /// `kind = "<kind>"` runs, in its composite task, what `factory` builds from the rest of
    /// that stanza. Market and quote guards share one namespace, and `deviation` is the
    /// breaker's, configured by its own keys; registering a name twice is the binary's bug,
    /// refused as a pricer's is.
    /// A pair that streams no price has no composite to judge, so a file giving it a market
    /// guard is refused, as one giving it a breaker is.
    pub fn market_guard<F: crate::pricing::MarketGuardFactory>(
        mut self,
        kind: &'static str,
        factory: F,
    ) -> Self {
        self.parts
            .kinds
            .insert_guard(kind, crate::kinds::market_guard_factory(factory));
        self
    }

    /// Registers a market guard kind built by a closure, as [`Self::pricer_fn`] does for a
    /// pricer: no async build, the default `validate`, no `summary`.
    pub fn market_guard_fn<C, G, F>(mut self, kind: &'static str, make: F) -> Self
    where
        C: serde::de::DeserializeOwned + Clone + Send + Sync + 'static,
        G: crate::guard::MarketGuard,
        F: Fn(C, &mut crate::pricing::BuildCtx) -> eyre::Result<G> + Send + Sync + 'static,
    {
        self.parts
            .kinds
            .insert_guard(kind, crate::kinds::market_guard_fn(make));
        self
    }

    /// Registers a quote guard kind: a pair whose `[[pairs.guards]]` stanza says
    /// `kind = "<kind>"` runs, in its quote loop after the pricer, what `factory` builds
    /// from the rest of that stanza.
    pub fn quote_guard<F: crate::pricing::QuoteGuardFactory>(
        mut self,
        kind: &'static str,
        factory: F,
    ) -> Self {
        self.parts
            .kinds
            .insert_guard(kind, crate::kinds::quote_guard_factory(factory));
        self
    }

    /// Registers a quote guard kind built by a closure; see [`Self::market_guard_fn`].
    pub fn quote_guard_fn<C, G, F>(mut self, kind: &'static str, make: F) -> Self
    where
        C: serde::de::DeserializeOwned + Clone + Send + Sync + 'static,
        G: crate::guard::QuoteGuard,
        F: Fn(C, &mut crate::pricing::BuildCtx) -> eyre::Result<G> + Send + Sync + 'static,
    {
        self.parts
            .kinds
            .insert_guard(kind, crate::kinds::quote_guard_fn(make));
        self
    }

    /// Registers the NYSE calendar guard, `us_market_hours`, under that name: the alerts on
    /// a stock pair's calendar (`PusherStockCalendarEnding`, `PusherStockCalendarEnded`)
    /// match on it, so a binary that registers [`crate::stocks::hours::UsMarketHoursFactory`]
    /// through [`Self::quote_guard`] under another name loses them. Separate from
    /// [`Self::stock_issuer`] because the calendar is the exchange's, not an issuer's.
    pub fn us_market_hours(self) -> Self {
        self.quote_guard(
            "us_market_hours",
            crate::stocks::hours::UsMarketHoursFactory,
        )
    }

    /// Registers a tokenized-stock issuer's two kinds, its status guard and its token
    /// guard, under the names the issuer declares ([`crate::stocks::Issuer`]): `ondo` and
    /// `ondo_token` for [`crate::stocks::issuers::ondo::Ondo`]. A binary names the issuers
    /// it integrates, one call each, and nothing registers one implicitly. The explicit
    /// [`Self::quote_guard`] call remains for other names. The issuer's price is a venue
    /// (`ondo` in a pair's `sources`), which needs no registering.
    pub fn stock_issuer<I: crate::stocks::Issuer>(self, issuer: I) -> Self {
        let token_guard = issuer.token_guard();
        self.quote_guard(I::STATUS_GUARD_KIND, issuer)
            .quote_guard(I::TOKEN_GUARD_KIND, token_guard)
    }

    /// Registers an observer: a binary's code that is told what the run does
    /// ([`crate::observe::Event`]: a lane started, stopped, tripped or re-armed, a block
    /// passed and what stood for it, its landing, a reload's outcome) and can do nothing
    /// to it. Code-only, unlike a pricer or guard: it lives for the process and takes no
    /// part in a reload. Best effort: it runs in a task of its own behind a channel of
    /// 1024 events, a full channel drops and counts, and a panic removes it; see
    /// [`crate::observe`]. Its [`crate::observe::Observer::name`] labels its series, so
    /// two under one name are the binary's bug, refused as a kind registered twice is.
    /// Builder mode only: `--mode node` runs no lane an observer is told of, so it starts
    /// none and says so once at startup.
    ///
    /// An observer that logs: its events reach the terminal only when the binary named its
    /// crate with [`Self::log_target`] or installed a subscriber of its own; `run_from_env`'s
    /// layer renders this crate's events otherwise.
    pub fn observer(mut self, observer: impl crate::observe::Observer) -> Self {
        self.parts.observers.push(Box::new(observer));
        self
    }

    /// Stops the updater when `signal` completes, instead of on ctrl-c: the same graceful
    /// path (every live quote cancelled at every builder, the pairs drained) for a binary
    /// that has its own idea of when to stop.
    pub fn shutdown_on(
        mut self,
        signal: impl std::future::Future<Output = ()> + Send + 'static,
    ) -> Self {
        self.parts.shutdown = Some(Box::pin(signal));
        self
    }

    /// Reads updater keys from `lookup` instead of the process environment, so a test can
    /// hand a run its keys without `set_var`, which is unsafe and racy across test threads.
    #[cfg(test)]
    pub(crate) fn key_lookup(
        mut self,
        lookup: impl Fn(&str) -> Option<String> + Send + Sync + 'static,
    ) -> Self {
        self.parts.key_lookup = Some(std::sync::Arc::new(lookup));
        self
    }

    /// Runs until shut down (ctrl-c, or [`Self::shutdown_on`]), `--once` completes, or
    /// `--check` has reported. Must be awaited on a multi-thread tokio runtime. A shutdown
    /// during startup, before any pair quotes, ends the run where it is, however long the
    /// RPC or a feed is taking: [`Outcome::Success`] for a service, an error for `--check`
    /// and `--once`, which did not finish what they were asked.
    ///
    /// Parsing is the caller's, and so is what `--help` does: [`Args::from_env`] returns
    /// `--help` and a usage error as a `clap::Error`, to print rather than propagate, whose
    /// exit code is the process's (0 for help). This is [`Self::run_from_env`] in a runtime
    /// of the binary's own:
    ///
    /// ```no_run
    /// use std::process::ExitCode;
    ///
    /// use quote_updater::{Args, Updater};
    ///
    /// #[tokio::main]
    /// async fn main() -> quote_updater::eyre::Result<ExitCode> {
    ///     let args = match Args::from_env() {
    ///         Ok(args) => args,
    ///         Err(err) => {
    ///             err.print()?;
    ///             return Ok(ExitCode::from(u8::try_from(err.exit_code()).unwrap_or(2)));
    ///         }
    ///     };
    ///     quote_updater::output::install();
    ///     Ok(Updater::builder().run(args).await?.exit_code())
    /// }
    /// ```
    pub async fn run(self, args: Args) -> eyre::Result<Outcome> {
        crate::pusher::run(args, self.parts).await
    }

    /// Spawns the run on the current tokio runtime and hands back the handle that reloads
    /// it, stops it and waits for it: [`Self::run`] for a binary that has other work to do
    /// in the meantime. Like `tokio::spawn`, it panics outside a runtime; the runtime must
    /// be multi-thread, as for `run`.
    ///
    /// The run stops on [`UpdaterHandle::shutdown`], or on the [`Self::shutdown_on`] signal
    /// if one was set; ctrl-c is the caller's to wire to `shutdown`, as it is with
    /// `shutdown_on`. Dropping the handle does not stop the run: it is detached, as a
    /// dropped tokio `JoinHandle` detaches its task.
    pub fn start(mut self, args: Args) -> UpdaterHandle {
        let (stop, stopped) = oneshot::channel();
        let signal = self.parts.shutdown.take();
        self.parts.shutdown = Some(Box::pin(async move {
            // A dropped handle closes `stopped` with an error, which must not read as a
            // shutdown: the run goes on as if the handle had never existed.
            let handle = async move {
                if stopped.await.is_err() {
                    std::future::pending::<()>().await;
                }
            };
            match signal {
                Some(signal) => tokio::select! {
                    _ = handle => {}
                    _ = signal => {}
                },
                None => handle.await,
            }
        }));
        // Bounded at one in flight, as the backoffice's is: the caller awaits its answer.
        let (reloads, requests) = mpsc::channel(1);
        let edits = Arc::new(tokio::sync::Mutex::new(()));
        let (mode, settled) = tokio::sync::watch::channel(None);
        self.parts.control = Some(Control {
            reloads: reloads.clone(),
            requests,
            edits: Arc::clone(&edits),
            mode,
        });
        let config_path = args.config_path().to_owned();
        let task = tokio::spawn(crate::pusher::run(args, self.parts));
        UpdaterHandle {
            reloads,
            shutdown: std::sync::Mutex::new(Some(stop)),
            task: tokio::sync::Mutex::new(Some(task)),
            config_path,
            mode: settled,
            edits,
        }
    }

    /// Parses this process's command line and environment, runs, and returns the exit
    /// code `main` should return:
    ///
    /// ```no_run
    /// fn main() -> std::process::ExitCode {
    ///     quote_updater::Updater::builder().run_from_env()
    /// }
    /// ```
    ///
    /// Installs the operator layer ([`crate::output`]) for this crate's events and the
    /// targets named with [`Self::log_target`], unless the binary installed a subscriber
    /// of its own first, which is then left as it is.
    pub fn run_from_env(self) -> ExitCode {
        // Before the command line: a registration error is a bug in the binary, not in how
        // it was invoked, and `--help` succeeding would hide it until the first real run.
        if let Err(err) = self.parts.registration_errors() {
            report_error(&err);
            return ExitCode::FAILURE;
        }
        let args = match Args::from_env() {
            Ok(args) => args,
            // What clap's own `exit()` would have printed, on the stream it would have
            // chosen, with its exit code (0 for --help and --version, 2 for a usage error).
            Err(err) => {
                let _ = err.print();
                return ExitCode::from(u8::try_from(err.exit_code()).unwrap_or(2));
            }
        };
        // The operator's lines, unless the binary already installed a subscriber of its own.
        crate::output::install_for(&self.parts.log_targets);
        let runtime = match tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
        {
            Ok(runtime) => runtime,
            Err(err) => {
                report_error(&eyre::Report::new(err).wrap_err("failed to build the tokio runtime"));
                return ExitCode::FAILURE;
            }
        };
        match runtime.block_on(self.run(args)) {
            Ok(outcome) => outcome.exit_code(),
            Err(err) => {
                report_error(&err);
                ExitCode::FAILURE
            }
        }
    }
}

/// A run started with [`UpdaterBuilder::start`]: the caller's way to reload it, stop it and
/// learn how it ended, from inside the same process.
///
/// Every method takes `&self`, so the handle can be shared (behind an `Arc`, or borrowed
/// by two arms of one `select!`) and a wait can be raced against the caller's own stop:
///
/// ```no_run
/// # async fn example(handle: quote_updater::UpdaterHandle) -> quote_updater::eyre::Result<()> {
/// let outcome = tokio::select! {
///     outcome = handle.wait() => outcome?,
///     _ = tokio::signal::ctrl_c() => {
///         handle.shutdown();
///         handle.wait().await?
///     }
/// };
/// # let _ = outcome;
/// # Ok(())
/// # }
/// ```
pub struct UpdaterHandle {
    reloads: mpsc::Sender<ReloadRequest>,
    /// `None` once `shutdown` has been called. A std mutex: it is held for a `take` only,
    /// never across an await, so `shutdown` stays a plain call usable from any arm.
    shutdown: std::sync::Mutex<Option<oneshot::Sender<()>>>,
    /// The run, `None` once a [`Self::wait`] has handed back how it ended. A tokio mutex,
    /// because a wait holds it across the run's whole life: a second wait queues behind the
    /// first rather than polling the same `JoinHandle` from two places.
    task: tokio::sync::Mutex<Option<JoinHandle<eyre::Result<Outcome>>>>,
    /// The file `halt` and `resume` edit: the run's `--config`.
    config_path: PathBuf,
    /// The run's mode once its settings are applied, `None` until then; node mode has no
    /// service to halt a pair in. See [`Control::mode`].
    mode: tokio::sync::watch::Receiver<Option<crate::cli::Mode>>,
    /// The edit lock the backoffice holds for its own writes of the same file.
    edits: Arc<tokio::sync::Mutex<()>>,
}

impl UpdaterHandle {
    /// Re-reads the config file and converges the running pairs onto it, exactly as SIGHUP
    /// and the backoffice do, and answers with the reload's one-line summary, or with why
    /// it was rejected, in which case nothing changed and every pair is still quoting its
    /// previous config. A reload that applied in part (a pair whose feed would not come up
    /// keeps its previous config, or is not started) answers `Ok`, since it did change
    /// what runs: its line counts the pairs that failed and says to see the journal. A run
    /// with no pair set to converge (`--once`, `--mode node`) says so; one that has already
    /// ended (`--check` reports and returns) says it has stopped. Reloads are serialized
    /// with the backoffice's: one in flight at a time.
    pub async fn reload(&self) -> Result<String, String> {
        let (reply, answer) = oneshot::channel();
        self.reloads
            .send(ReloadRequest { reply })
            .await
            .map_err(|_| "the updater has stopped".to_owned())?;
        // A dropped reply is the run ending with the request unread, which only the paths
        // that never read the channel do (`--check`); the loops always answer.
        match answer.await {
            Ok(Ok(line) | Err(ReloadFailure::Partial(line))) => Ok(line),
            Ok(Err(ReloadFailure::Rejected(line))) => Err(line),
            Err(_) => Err("the updater has stopped".to_owned()),
        }
    }

    /// Stops the run the graceful way, as ctrl-c would: every live quote withdrawn at
    /// every builder, the pairs drained; during startup, before anything quotes, the run
    /// ends where it is (see [`UpdaterBuilder::run`]). A second call does nothing;
    /// [`Self::wait`] for the run to actually end.
    pub fn shutdown(&self) {
        // A poisoned lock is a panic elsewhere mid-`take`, which leaves the slot as it was;
        // the stop is still ours to send.
        let stop = self
            .shutdown
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        if let Some(stop) = stop {
            // An error here means the run has already ended, which is what was asked.
            let _ = stop.send(());
        }
    }

    /// Stops the pair quoting and keeps it, with every setting, in the config file:
    /// `halted = true` with `reason` beside it as `halt_reason`, then a reload, which
    /// withdraws its quote at every builder exactly as removing it would. A halt is an
    /// intent, not a judgement: no reload clears it, only [`Self::resume`] (or that pair's
    /// own Resume in the backoffice, whose "Restart halted pairs" leaves a halt with a
    /// reason as it is), and it survives a restart. That is what a kill switch is built on;
    /// a [`crate::guard::Latch`] trip is not, since the next reload re-arms it.
    ///
    /// Answers with what changed and the reload's summary (`Ok` too when the reload applied
    /// in part because another pair's feed would not come up: the halt is in the file and
    /// in force, and the line says which pairs kept their previous config), or with why it
    /// was refused, in which case the file is as it was: no pair has these tokens, it is
    /// already halted, the reload was rejected, or the run is in node mode (by flag,
    /// variable or `[settings] mode`), which has no service to halt a pair in. Serialized
    /// with the backoffice's edits of the same file, and written as the backoffice writes
    /// it: the file is rewritten in full, in its canonical form, so comments in it are not
    /// kept, keys are sorted and arrays re-wrapped; `config.example.toml` documents every
    /// key.
    pub async fn halt(&self, tokens: (Address, Address), reason: &str) -> Result<String, String> {
        self.edit(tokens, "halt", |pair| halt_line(pair, reason))
            .await
    }

    /// Clears a halt, whoever set it, and reloads: the pair quotes again at every builder.
    /// Refused when the pair is not halted, and in node mode, as [`Self::halt`] is;
    /// rewrites the file as [`Self::halt`] does.
    pub async fn resume(&self, tokens: (Address, Address)) -> Result<String, String> {
        self.edit(tokens, "resume", resume_line).await
    }

    /// Both verbs: under the edit lock, find the pair by its tokens and apply `change`,
    /// which returns the line describing what it did; then write, reload, and revert if
    /// the reload refuses. The description is decided before the file is changed, so the
    /// pair is looked up twice: once for the line, once under the write.
    async fn edit(
        &self,
        tokens: (Address, Address),
        verb: &str,
        change: impl FnOnce(&mut RawPair) -> eyre::Result<String>,
    ) -> Result<String, String> {
        // Asked of the run rather than of `args`: the mode is only known once the file's
        // `[settings]` are applied, which the run does before it connects to anything, so
        // this waits a moment at most. A run that ended before getting there (its config
        // would not load) has nothing left to edit for.
        let node = {
            let mut mode = self.mode.clone();
            match mode.wait_for(Option::is_some).await {
                Ok(mode) => matches!(*mode, Some(crate::cli::Mode::Node)),
                Err(_) => return Err("the updater has stopped".to_owned()),
            }
        };
        if node {
            return Err(format!("node mode has no service to {verb}"));
        }
        let _serialized = self.edits.lock().await;
        let mut preview =
            crate::config::load_raw(&self.config_path).map_err(|err| format!("{err:#}"))?;
        let at = pair_index(&preview.pairs, tokens).map_err(|err| format!("{err:#}"))?;
        let what = change(&mut preview.pairs[at]).map_err(|err| format!("{err:#}"))?;
        let edited = preview.pairs.swap_remove(at);
        let changed = crate::service::change_config(
            &self.config_path,
            &self.reloads,
            "handle:",
            &what,
            |raw| {
                let at = pair_index(&raw.pairs, tokens)?;
                raw.pairs[at] = edited;
                Ok(())
            },
        )
        .await;
        // `Err` means the file is as it was, so a partial apply, which keeps the edit, is
        // not one: a caller retrying a halt it was told failed would be told it is already
        // halted.
        match changed {
            Ok(line) | Err(ReloadFailure::Partial(line)) => Ok(line),
            Err(ReloadFailure::Rejected(line)) => Err(line),
        }
    }

    /// Waits for the run to end and hands back how it ended: on shutdown, when `--once`
    /// has published, or when `--check` has reported. A panic inside the run (the
    /// supervisor catches a pair's; this is the process-level code's) comes back as an
    /// error here rather than as a panic in the caller.
    ///
    /// Cancel-safe: a wait dropped before the run ended (the losing arm of a `select!`)
    /// takes nothing with it, and the next one waits on. The outcome is handed back once;
    /// a wait after that answers at once with an error, since the run is not coming back.
    pub async fn wait(&self) -> eyre::Result<Outcome> {
        let mut slot = self.task.lock().await;
        let Some(task) = slot.as_mut() else {
            eyre::bail!("the updater's run has ended, and an earlier wait already handed back how");
        };
        // Through `&mut`, so a cancelled wait leaves the handle in the slot for the next.
        let joined = task.await;
        *slot = None;
        match joined {
            Ok(outcome) => outcome,
            Err(err) => Err(eyre::Report::new(err).wrap_err("the updater's task ended abnormally")),
        }
    }
}

/// The pair whose `tokens` are these two, in either order: the lane sorts them, so the
/// file may list them either way round too.
fn pair_index(pairs: &[RawPair], tokens: (Address, Address)) -> eyre::Result<usize> {
    let wanted = [tokens.0, tokens.1];
    pairs
        .iter()
        .position(|pair| {
            let parsed: Vec<Address> = pair
                .tokens
                .iter()
                .filter_map(|token| token.parse().ok())
                .collect();
            parsed.len() == 2
                && parsed.iter().all(|token| wanted.contains(token))
                && parsed[0] != parsed[1]
        })
        .ok_or_else(|| {
            eyre::eyre!(
                "no pair with tokens {:#x} and {:#x} in the config file",
                tokens.0,
                tokens.1
            )
        })
}

/// The pair as the handle's answer names it: its `symbol` and its key when it has a
/// symbol, its key alone otherwise. The on-chain label is not known here (it is read at
/// startup, not from the file), and the key is what the stanza is unique by.
fn pair_name(pair: &RawPair) -> String {
    match &pair.symbol {
        Some(symbol) => format!("{symbol} ({})", pair.key_env),
        None => pair.key_env.clone(),
    }
}

/// What [`UpdaterHandle::halt`] does to the stanza, and the line it answers with.
pub(crate) fn halt_line(pair: &mut RawPair, reason: &str) -> eyre::Result<String> {
    eyre::ensure!(!pair.halted, "pair {} is already halted", pair_name(pair));
    pair.halted = true;
    pair.halt_reason = Some(reason.to_owned());
    Ok(format!("halted pair {}: {reason}", pair_name(pair)))
}

/// What [`UpdaterHandle::resume`] does to the stanza, and the line it answers with.
fn resume_line(pair: &mut RawPair) -> eyre::Result<String> {
    eyre::ensure!(pair.halted, "pair {} is not halted", pair_name(pair));
    pair.halted = false;
    pair.halt_reason = None;
    Ok(format!("resumed pair {}", pair_name(pair)))
}

/// The error a run ended on, as `main` prints it: the message and its causes, without the
/// source location eyre's own `Debug` adds, which is this library's path on the machine
/// it was built on and no help to an operator.
fn render_error(err: &eyre::Report) -> String {
    use std::fmt::Write as _;
    let mut text = format!("Error: {err}");
    let causes: Vec<String> = err.chain().skip(1).map(ToString::to_string).collect();
    match causes.as_slice() {
        [] => {}
        [one] => {
            let _ = write!(text, "\n\nCaused by:\n    {one}");
        }
        many => {
            text.push_str("\n\nCaused by:");
            for (i, cause) in many.iter().enumerate() {
                let _ = write!(text, "\n    {i}: {cause}");
            }
        }
    }
    text
}

/// The process's last word on a failed run: what `fn main() -> Result` would print, minus
/// eyre's `Location:` (see [`render_error`]). Printed directly rather than through the
/// run's own output, so it reaches the terminal whatever that output is routed to.
#[allow(clippy::print_stderr)]
fn report_error(err: &eyre::Report) {
    eprintln!("{}", render_error(err));
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A binary names its crate once; `run_from_env` hands the names to the operator layer.
    #[test]
    fn log_targets_are_kept_in_order() {
        let builder = Updater::builder()
            .log_target("probe")
            .log_target("probe_tools");
        assert_eq!(builder.parts.log_targets, ["probe", "probe_tools"]);
    }

    fn raw_pair(key_env: &str, symbol: Option<&str>) -> RawPair {
        let symbol = symbol.map_or(String::new(), |s| format!("symbol = \"{s}\"\n"));
        toml::from_str(&format!(
            "tokens = [\"0x01\", \"0x02\"]\nkey_env = \"{key_env}\"\npricing = {{ kind = \"feed\" }}\n{symbol}"
        ))
        .unwrap()
    }

    /// The handle's answer names the pair as the file does: its `symbol` when it has one,
    /// else the key that identifies its stanza.
    #[test]
    fn a_halted_pair_is_named_by_its_symbol_when_it_has_one() {
        let mut with = raw_pair("K", Some("ETHUSDC"));
        assert_eq!(
            halt_line(&mut with, "vault audit").unwrap(),
            "halted pair ETHUSDC (K): vault audit"
        );
        let mut without = raw_pair("K", None);
        assert_eq!(
            halt_line(&mut without, "vault audit").unwrap(),
            "halted pair K: vault audit"
        );
        assert!(
            halt_line(&mut with, "again")
                .unwrap_err()
                .to_string()
                .contains("already halted")
        );
        assert_eq!(resume_line(&mut with).unwrap(), "resumed pair ETHUSDC (K)");
        assert!(
            resume_line(&mut with)
                .unwrap_err()
                .to_string()
                .contains("not halted")
        );
    }

    /// What `main` prints: the message and its causes, and never the source location
    /// eyre's own `Debug` adds, which is this library's path on the machine it was built
    /// on and no help to an operator.
    #[test]
    fn an_error_is_rendered_with_its_causes_and_no_location() {
        let one = eyre::eyre!("no such file").wrap_err("could not read config.toml");
        assert_eq!(
            render_error(&one),
            "Error: could not read config.toml\n\nCaused by:\n    no such file"
        );
        let plain = eyre::eyre!("refused");
        assert_eq!(render_error(&plain), "Error: refused");
        let three = eyre::eyre!("c").wrap_err("b").wrap_err("a");
        assert_eq!(
            render_error(&three),
            "Error: a\n\nCaused by:\n    0: b\n    1: c"
        );
        assert!(!render_error(&one).contains("Location"));
    }

    /// Two observers under one name would share both of its series: one panicking would
    /// show the other as not running, and their drops would merge. Refused like a kind
    /// registered twice, before any config is read (this one does not exist).
    #[tokio::test]
    async fn two_observers_under_one_name_are_refused_before_any_config_is_read() {
        struct Named(&'static str);
        impl crate::observe::Observer for Named {
            fn name(&self) -> &'static str {
                self.0
            }
            fn on_event<'a>(
                &'a mut self,
                _: &'a crate::observe::Event,
            ) -> crate::pricing::BoxFuture<'a, ()> {
                Box::pin(async {})
            }
        }
        let args = Args::try_parse_from([
            "quote-updater",
            "--config",
            "/nonexistent/quote-updater/config.toml",
        ])
        .unwrap();
        let err = Updater::builder()
            .observer(Named("slack"))
            .observer(Named("risk"))
            .observer(Named("slack"))
            .run(args)
            .await
            .expect_err("refused");
        let err = format!("{err:#}");
        assert!(err.contains("observer `slack` registered twice"), "{err}");
        assert!(!err.contains("risk"), "{err}");
    }

    /// The unit a binary names is what the operator hints print; none names the signal.
    #[test]
    fn the_unit_is_kept_for_the_hints() {
        assert_eq!(Updater::builder().parts.unit, None);
        assert_eq!(
            Updater::builder().systemd_unit("my-updater").parts.unit,
            Some("my-updater")
        );
    }

    #[test]
    fn the_two_methods_register_the_stock_kinds_under_their_canonical_names() {
        let builder = Updater::builder()
            .us_market_hours()
            .stock_issuer(crate::stocks::issuers::ondo::Ondo);
        let kinds = &builder.parts.kinds;
        assert_eq!(kinds.guard_name("us_market_hours"), Some("us_market_hours"));
        assert_eq!(kinds.guard_name("ondo"), Some("ondo"));
        assert_eq!(kinds.guard_name("ondo_token"), Some("ondo_token"));
        kinds.errors().unwrap();
    }

    #[test]
    fn a_second_issuer_registers_under_its_own_names() {
        use crate::{
            pricing::{BoxFuture, BuildCtx, QuoteGuardFactory},
            stocks::{
                Issuer,
                guard::RestrictionGuardFactory,
                issuers::ondo::{Ondo, OndoCfg, compliance::OndoCompliance},
                status::StatusGuard,
            },
        };
        /// Ondo's status guard and rules under another issuer's names.
        struct Other;
        static ONDO: Ondo = Ondo;
        impl QuoteGuardFactory for Other {
            type Config = OndoCfg;
            type Guard = StatusGuard;
            fn build<'a>(
                &'a self,
                cfg: &'a OndoCfg,
                ctx: &'a mut BuildCtx,
            ) -> BoxFuture<'a, eyre::Result<StatusGuard>> {
                ONDO.build(cfg, ctx)
            }
        }
        impl Issuer for Other {
            const STATUS_GUARD_KIND: &'static str = "other";
            const TOKEN_GUARD_KIND: &'static str = "other_token";
            type TokenGuard = RestrictionGuardFactory;
            fn token_guard(&self) -> RestrictionGuardFactory {
                RestrictionGuardFactory::new("Other", |chain, token| {
                    Box::new(OndoCompliance { chain, token })
                })
            }
        }
        let builder = Updater::builder()
            .us_market_hours()
            .stock_issuer(Ondo)
            .stock_issuer(Other);
        let kinds = &builder.parts.kinds;
        assert_eq!(kinds.guard_name("other"), Some("other"));
        assert_eq!(kinds.guard_name("other_token"), Some("other_token"));
        kinds.errors().unwrap();
    }
}
