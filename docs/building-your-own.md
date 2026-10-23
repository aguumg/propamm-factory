## Building your own quote updater

The updater is a library that a binary of yours is one
assembly of. Another binary can assemble its own, with pricing models of its own, and keep
everything else: the feeds and the composite, the freshness gate, the band and the core's
bounds on any price, reload, the builders, metrics, and this manual.

```toml
[dependencies]
quote-updater = { git = "https://github.com/lambdaclass/propamm-factory", rev = "<commit>" }
serde = { version = "1", features = ["derive"] }   # a stanza's `Deserialize`
# For `run(args)`/`start(args)` in a runtime of your own, and for an observer that logs:
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
tracing = "0.1"
```

A pricing model is a `Pricer`: built once per lane configuration, asked for a price every
tick, and handed the pair's market in the orientation a person says it (USDC per WETH,
however the lane carries it). `examples/custom_pricer.rs` is a complete binary
in 30 lines of code, compiled by CI: a `skewed` kind that publishes the market mid moved by a
fixed fraction at a fixed half-spread. Its `main` is the whole assembly:

```rust
fn main() -> std::process::ExitCode {
    Updater::builder()
        .pricer_fn("skewed", |cfg: Skewed, ctx: &mut BuildCtx| {
            let book = ctx.diagnostic("book_half_spread")?;
            Ok(SkewedPricer { cfg, book })
        })
        .run_from_env()
}
```

A pair names the kind in its `[pairs.pricing]` stanza, the same way it would name `feed`,
`fixed` or `volatile`; `symbol`/`sources` stay its reference market:

```toml
[[pairs]]
tokens  = ["0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2", "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"]
symbol  = "ETHUSDC"
key_env = "UPDATER_KEY_WETH_USDC"

[pairs.pricing]
kind        = "skewed"
half_spread = 0.0005
skew        = 0.001
```

What to know:

- **Checked like the rest of the file.** An unknown kind (named with the registered list), a
  stanza its kind cannot read, a market guard on a pair that streams no price, or a stanza
  its `validate` refuses rejects the file at startup, on
  reload and under `--check`; under `--check` every guard is also built, as the run builds
  it before the lane quotes, so a guard that cannot be built (a quote guard reading a
  diagnostic the pair's pricer never declares) fails the check under its row instead of
  passing it and refusing the start; a failed build keeps the old lane quoting and reports
  the reload partial. A panic in any of them counts as that failure, not as the process's.
- **The core still decides.** A pricer can refuse (`Refusal::new`, counted under the reason
  it declared), and the core withdraws anything unpublishable whatever a pricer returns: a
  spread of a whole unit or more, a zero mid, or a mid the contract cannot store (over 216
  bits), each counted under its own `reason`. How far a mid may sit from the market is the
  pair's `min_mid`/`max_mid` band, which applies to every kind alike; a kind with a bound
  of its own checks it in `validate` or refuses the tick.
- **Builds do not block.** A `pricer_fn` closure runs synchronously inside the lane's build,
  so a fetch there stalls startup or the reload, and the 30s build limit cannot interrupt
  it. A pricer that must fetch before it can price, or whose stanza needs checking against
  the pair, implements `Factory`, whose `build` is async and whose `validate` runs before
  anything is touched; its doc comment is a complete, compiling example.
- **What a pricer can reach.** Beyond its stanza and the reference market, its build context
  offers the chain (`ctx.chain()`: `eth_call` bounded by the updater's RPC timeout;
  `call_sig(to, "balanceOf(address)", &[Value::Address(a)])` encodes it with the ethrex
  types re-exported as `quote_updater::calldata`, so no ethrex pin of your own, and
  `calldata::decode_return_data("(uint256)", &answer)` decodes the answer; not
  `decode_calldata`, which expects a selector and would read every word four bytes late;
  `call_sig_as(from, to, …)` makes the call as `from` and answers a revert as
  `CallOutcome::Reverted` rather than an error, for a guard that asks a contract whether it
  would let an account do something by trying it),
  the
  market's σ history (`ctx.history()`: the process-level samples the volatile model reads,
  kept across reloads), the pair's vault inventory (`ctx.inventory().await`: read once,
  so a wrong target fails the build, then refreshed every 12s) and the lane's own landings
  (`ctx.landings()`: after each block the run reads the lane back, and the feed's
  `latest()` says whether the update quoted for that block landed, was missed, could not
  be verified, or was never quoted; a task started with `ctx.spawn` can await each with
  `next()`). Reading the vault is async,
  so it is the `Factory` path: `examples/tilted.rs` is a fixed spread tilted
  by the vault's base share, and `make e2e-custom` runs it against the mocks. A test seeds
  these with `testing::build_ctx_with` and needs no chain. An HTTP client comes from
  `ctx.http()`: the run's one `reqwest::Client`, shared with the polled venues, so every
  lane draws on one connection pool (`quote_updater::reqwest` is the crate it belongs to,
  re-exported because a `Client` from another reqwest version is a different type). Under
  `--check` the history is empty (the check does not wait the minute σ needs) and a refusal
  in `preview` fails the check, so a pricer that reads σ overrides `preview` to price at
  σ = 0 and `note` it, as the volatile model does; `Pricer::preview`'s doc shows the two
  lines. Not there yet: state shared between lanes, and block information on the tick.
- **Diagnostics** a build declares reach Prometheus as `quote_updater_diagnostic{pair,kind,name}`.
  Each is NaN from the moment the lane is adopted until a tick sets it, and again once the
  lane is removed or halted, so a rebuilt lane never shows the last build's numbers.
- **Guards** are the other half of the API, registered like pricers and configured per pair
  as `[[pairs.guards]]` stanzas in the order they judge:

  ```rust
  Updater::builder()
      .pricer_fn("skewed", ..)
      .market_guard_fn("dispersion", |cfg: Dispersion, ctx: &mut BuildCtx| Ok(DispersionGuard { .. }))
      .quote_guard_fn("cap", |cfg: Cap, ctx: &mut BuildCtx| Ok(CapGuard { share: ctx.read_diagnostic("base_share")?, .. }))
      .run_from_env()
  ```

  A `MarketGuard` judges every composite sample in the composite task, after the built-in
  deviation guard (`max_deviation`, which keeps its own keys and cannot be named in a
  stanza), and answers `Pass` or `Trip`, whose reason the alarm line prefixes with
  ``guard `<kind>` tripped: `` (so a reason says only what was seen). A pair that streams
  no price (a fixed `mid`, or a pricing kind with no `symbol` or `sources`) has no
  composite, so a market guard stanza on it is refused at startup, on reload and under
  `--check`, as `max_deviation` on a fixed mid is. A `QuoteGuard` judges our quote in the
  loop, after the pricer, the core's bounds and the band, on every tick: on the pricer's
  output *or a refusal*, the pricer's or the core's. That includes a feed with no sample yet
  or a stale one, which the core refuses before the pricer runs: on that tick the candidate
  has no market and no diagnostics, and `Refusal::reason` reads `no_sample` or `stale`, so
  "halt after ten minutes withdrawn" counts every withdrawn tick. A quote guard answers
  `Allow`, `Withdraw` (under a reason its build declared) or `Halt`; every quote guard runs
  and the most severe answer wins, and on a refusal `Allow` changes nothing, so a guard can
  escalate but never reverse. A quote guard reads the pricer's diagnostics by handle
  (`ctx.read_diagnostic`), bound at build. Guards are builder mode only, like
  `max_deviation`.
- **A guard with a form.** A factory struct implementing `MarketGuardFactory` or
  `QuoteGuardFactory` can declare `fields()` and `form_help()`, as a pricer's `Factory`
  does, and the backoffice then edits the pair's stanza of that kind from the guards section
  of its page: one block per registered kind, with an "enabled" box. A field is a string, a
  number, a list (one item per line) or the name of an environment variable
  (`FormField::env`, written as `{ env = "NAME" }`). The closure helpers above declare no
  fields, so a kind registered through them shows as edited in the file. `example/`'s two
  guards are factories for this reason.
- **Tokenized stocks** are two more calls: `.us_market_hours()` for the NYSE calendar guard
  and `.stock_issuer(quote_updater::stocks::issuers::ondo::Ondo)` for an issuer's pricer and
  token guard, under the names the issuer declares. See [stocks.md](stocks.md), including
  how an issuer of your own implements `stocks::Issuer` over the public ports;
  `quote-updater/examples/issuer.rs` is a whole one.
- **A trip is not a halt.** Every lane has a latch: a guard's `Trip`/`Halt`, a panic
  anywhere in a component's code (its `price`, `check`, `assess`, or a task it spawned), or
  a component's own `ctx.latch().trip(..)` trips it, the lane withdraws and stops, and only
  a reload re-arms it, which is the circuit breaker's contract extended to everything. The
  alarm and the re-arm line name what tripped (`quote_updater_trip_source{pair,source,cause}`
  beside `breaker_tripped`, which stays the page: 1 while the lane is halted on that
  source, 0 once a reload rebuilds the lane, re-armed or not, and gone with the lane), and
  the watchdog's notice
  names it too.
  A kill switch is therefore never a trip: "stop until I say so" is `halted = true` in the
  file, which a reload does not undo, and `UpdaterHandle::halt` is how a binary writes it
  (with a `halt_reason` the backoffice shows and the reload logs). A panic on the price path
  now trips the lane instead of restarting the loop: buggy code gets a human, not a retry.
- **What the run watches.** `quote_updater_guards{pair,kind}` is 1 per registered guard a
  pair runs, and NaN once it no longer does (its stanza removed, or the lane);
  `quote_updater_extension_duration_seconds{pair,kind}` times every call into a
  registered kind's code, since nothing can stop slow CPU work on the requote path but a
  histogram makes it visible; `quote_updater_extension_task_exits_total{pair,kind}` counts
  background tasks that returned on their own; `price_unusable_total{reason="panic"}` counts
  the tick a pricer panicked on. `--check` shows a `guards` column (each kind and its
  factory's `summary`) only when some pair has one.
- **Secrets** go in as `{ env = "NAME" }` (`EnvSecret`), never as values: the config is
  recorded to Postgres. A stanza key named like a credential (`key`, `secret`, `token`,
  `password`, `passphrase`, `auth`, `credential`, `bearer`, `private`, `mnemonic`,
  `seed`, `cookie`, `session`, `jwt`, ...) is redacted there, and so is a `Bearer`/`Basic`
  value, and any URL in a stanza is cut to scheme and host. A value typed where
  `{ env = "NAME" }` belongs is refused without the refusal repeating it.
- **The core's bounds in a test:** `testing::backstop(out, &tick)` puts what a pricer priced
  through them the way the run does, so a test cannot pass where the run would withdraw.
- **Testing a pricer** needs no runtime and no network: `quote_updater::testing` builds one
  and hands it a tick over a market you choose (`tests/pricer_without_runtime.rs`).
  A guard the same way: a quote guard from a candidate over the pricer's diagnostics, a
  market guard from a composite sample with named venues (`testing::composite_sample_with`,
  `source_sample`), and a `validate` against the pair it will really see
  (`testing::pair_for`); `tests/guard_without_runtime.rs` shows each.
- **Observers** watch and never decide: `Updater::builder().observer(o)` registers code
  (not a config kind: it lives for the process, takes no part in a reload, and its secrets
  belong in the environment) that is handed every `observe::Event` in order: a lane
  started, stopped, tripped (who and how) or re-armed, a block passed with what stood for it
  and the pricer's diagnostics, its landing, and a reload's outcome. Once per block, matching
  the summary line, never per tick, and none for the block a halt broke out of, which has
  not passed. A trip is always told between its lane's `LaneStarted` and `LaneStopped`: one
  while the lane was being built comes right after it started, and one in a rebuild that
  then failed is never told. Every enum and every event variant with fields is
  `#[non_exhaustive]`: match with a `_` arm and write a variant's fields with `..`
  (`Event::Tripped { pair, .. }`). Each observer's `name()` labels its series, so two under
  one name are refused before any config is read, as a kind registered twice is. Best
  effort: each observer runs in a task of its own behind a channel of 1024 events; a full
  channel drops the event and counts it in `quote_updater_observer_dropped_total{observer}`,
  and a panic logs the observer, removes it and sets `quote_updater_observer_running{observer}`
  to 0. A run that ends normally (a shutdown, `--once`) lets each observer deliver what it
  already holds, the lanes' last `LaneStopped` included, for up to two seconds before it
  returns; an error return aborts them. Builder mode only: `--mode node` runs no lane an
  observer is told of (it pushes a transaction per interval and has no reload), so it starts
  no observer and says so once at startup. An audit trail belongs in the recorder, not an
  observer. An observer that logs names the binary's crate on the builder
  (`.log_target(env!("CARGO_CRATE_NAME"))`) or its lines go nowhere: `run_from_env()`'s
  layer renders this crate's events and the targets it was told, never every crate's.
- **Your own process.** `run_from_env()` is a whole `main`. To run inside your own
  (multi-thread) tokio runtime, parse with `Args::from_env()` and pass the result to
  `Updater::builder().run(args).await`, which returns an `Outcome` instead of exiting, and use
  `shutdown_on(future)` to stop it your way. A binary with other work to do meanwhile calls
  `start(args)` instead: it spawns the run on the current runtime and hands back an
  `UpdaterHandle` whose `reload()` re-reads the file and answers with the reload's summary
  or its rejection (exactly what SIGHUP and the backoffice get), whose `shutdown()` stops
  it the graceful way, and whose `wait()` returns the `Outcome`. Ctrl-c is the caller's to
  wire to `shutdown()` on that path, as it is with `shutdown_on`. The handle's
  `halt(tokens, reason)` and `resume(tokens)` are a kill switch: a halt is written to the
  config file as `halted = true` with `halt_reason` beside it and applied by a reload, so
  no later reload undoes it and it survives a restart (a `Latch` trip is re-armed by the
  next reload, so a kill switch is never built on one); the file is rewritten in full in
  the backoffice's canonical form, so comments in it are not kept. Both are serialized with the
  backoffice's edits of the same file, and both are refused in `--mode node`. `--help` and a usage error come
  back from `from_env` as a `clap::Error` to `print()` and exit with its code (0 for help),
  not to propagate with `?`: the doc example on `UpdaterBuilder::run` is the whole pattern.
  A binary with flags of its own puts `#[command(flatten)] updater: Args` in its clap
  `Parser` (with `use quote_updater::clap;`, the clap the flags were declared with) and
  hands `cli.updater` to `run`: precedence and the blank-variable rule hold on that path
  too, because `Args` records what was given inside clap's own parse. `Args::config_path()`
  is the one setting read back, for a file of the binary's own beside the updater's.
  `systemd_unit("name")` puts your unit into the operator hints (how to reload after a
  halt, how to restart after a settings change); without it they say `SIGHUP`.
  Logs are `tracing` events: `run_from_env()` installs `quote_updater::output::layer_for(..)`
  for this crate's lines and the targets named with `log_target`; a binary with a
  subscriber of its own adds `output::layer()` (or `layer_for`) to a registry-based
  subscriber (`registry()` or `fmt()`) for today's lines, or formats them itself, and
  installs it before `run_from_env`, which then leaves it alone.
- **Build settings.** Copy the `[profile.release]` from this repository's root `Cargo.toml`
  (Cargo only reads a profile from the binary's own workspace). The library refuses to start
  when built with `panic = "abort"`, which would turn one pair's panic into every pair's.

