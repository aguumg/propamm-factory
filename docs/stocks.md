# Tokenized stocks

Stock pairs, a stock token against USDC on Ethereum mainnet, are quoted by the same binary as
any other pair: the library carries the stock kinds in its `stocks` module, and
[`example/`](../example), the binary the deployment builds, registers them in two lines. It
supports Ondo Global Markets tokens (AAPLon, TSLAon, SPYon, ...) today, and is laid out so
another token design is a module of its own (see [Adding another issuer](#adding-another-issuer)).
The PropAMM contract needs no change for these tokens: they are plain ERC-20s (see
`contracts/test/PropAMMStockToken.t.sol` and `PropAMMOndoToken.t.sol`). What differs is
everything around the price, which is what the stock kinds add:

- **`ondo`**, a pricing model. The mid is Ondo's own price for the token, polled from Ondo's
  GM API: the price Ondo mints and redeems around, already including the token's share
  multiplier. It withdraws the quote when the price is older than its limit, when it has not
  moved for `max_unchanged_secs` (a halted stock's price stops while Ondo may keep stamping
  it current), when Ondo reports a session the stanza's `sessions` does not list (`regular`
  alone by default; Ondo also trades in `premarket`, `postmarket`, `overnight` and, for some
  assets, `offhours`), during an active asset event
  (under the token's symbol or the stock's ticker), from `pause_lead_secs` before the start
  of an upcoming event that will move the share multiplier (from the moment Ondo lists it if
  it gives no start), from the start of any other upcoming event, from `pause_lead_secs`
  before a corporate action Ondo has scheduled on chain, and when Ondo's multiplier and the
  chain's disagree. The last two read the `SyntheticSharesOracle`, which lists some GM
  tokens and not others: a pair whose token it does not list is quoted on Ondo's price
  alone, without those two checks, which the build says at startup and under `--check`,
  and reads again on the next reload. The spread is a fixed `delta`. Ondo's market-wide status is read once
  per API, key and poll period, however many stock pairs share them, by the pairs' own
  tasks. The stock's ticker comes from Ondo's price answer, or from the stanza's `ticker`
  when the answer names none; with neither, the build fails, since events Ondo files under the
  ticker would go unseen.
- **`us_market_hours`**, a quote guard. It withdraws outside 09:30–16:00 New York time on
  trading days (13:00 on early closes), by the holiday calendar the config lists, and past the
  calendar's `listed_through` date. It publishes the days the calendar has left
  (`calendar_days_left`, which `PusherStockCalendarEnding` alerts on a month out and
  `PusherStockCalendarEnded` once it has passed) and warns in its log once a day from then.
- **`ondo_token`**, a quote guard. It withdraws while Ondo would refuse the fill: the token
  paused, or the vault or the PropAMM on Ondo's blocklist or the sanctions list. It also
  tries the transfer a fill makes, `transferFrom(vault, vault, 0)` as the PropAMM, so a
  refusal none of those lists explains (a check Ondo adds to the token) withdraws too, under
  the revert data it answers. PropAMM can't see any of it, so its `isActive` stays true until
  the quote is withdrawn.

By default it quotes the US regular session only, about a fifth of the week, while the
tokens trade on chain around the clock. A stanza that lists more `sessions`, with the
`us_market_hours` guard left off, quotes whenever Ondo reports one of them.

## Registering the kinds

```rust
Updater::builder()
    .us_market_hours()                                        // the NYSE calendar guard
    .stock_issuer(quote_updater::stocks::issuers::ondo::Ondo)  // "ondo" and "ondo_token"
```

`us_market_hours()` registers the calendar guard under the name the two calendar alerts
match on; it is its own call because the calendar is the exchange's, not an issuer's.
`stock_issuer(..)` takes an issuer and registers its pricer and its token guard under the
names the issuer declares. A binary names the issuers it integrates, one call each, and
nothing registers one implicitly. `example/` does both; `quote-updater/examples/stocks.rs`
is the same from outside the crate, and `quote-updater/examples/issuer.rs` registers a
second issuer of its own beside Ondo. The explicit `pricer`/`quote_guard` calls remain for
other names, at the cost of the alerts, which match on `us_market_hours`.

## Run a stock pair

You need:

- an Ondo GM API key (partner-only, through Ondo's onboarding) in `ONDO_API_KEY`;
- the pair's updater key in the variable its `key_env` names, registered on your PropAMM;
- a vault holding the stock token and USDC, with approvals for the PropAMM on both;
- the pair on the PropAMM (`addPair`), as for any pair ([production.md](production.md)).

Then add the pair from the backoffice: its tokens, key variable and band, the `ondo` pricing
kind with its fields (the API key as the name of the variable holding it), and in the guards
section `us_market_hours` with its calendar and `ondo_token`, both checked. Saving reloads
the running updater. In the file, the stanza is the one in
[`example/config.stocks.toml`](../example/config.stocks.toml): copy it into your config and
reload with a SIGHUP. Either way, `--check` first:

```bash
cd example && cargo run --release -- --config config.toml --check
```

`min_mid`/`max_mid` are in the lane's orientation, the number `--check`'s mid column prints:
USDC per token when the stock token's address sorts before USDC's, as AAPLon's does, and
token per USDC when it sorts after. The build refuses a band written the other way up.

Keep the `holidays`/`early_closes` lists current from
[NYSE's calendar](https://www.nyse.com/markets/hours-calendars), and `listed_through` with
them. The example lists them through 2027; `PusherStockCalendarEnding` warns a month out,
and `PusherStockCalendarEnded` says when the pair has stopped quoting for it.

## Layout

The checks a stock pair needs are the same whoever issued the token; what differs between
designs is where the answers come from. `quote-updater/src/stocks/` is split along that line:

- `ports.rs`: what the core asks of a design, and the shape of each answer. A
  `PriceSource` (the token's price), a `Valuation` (how many shares a token is worth, and the
  corporate action that will change it), an optional `MarketStatus` (the issuer's session
  and the stoppages it reports on the asset), and `Restrictions` (the token's transfer
  rules). Each answer is a struct built with its `new` (`Price::new`, `Shares::new`,
  `Stoppage::new`, ...) and read or set by field: they are `#[non_exhaustive]`, so a field
  the core gains later is not a change an issuer outside this crate has to make.
- `pricer.rs`, `guard.rs`, `hours.rs`: the core. The pricer and all its gates, the guard on
  the token's transfer rules, and `us_market_hours`. It reads every design through the ports,
  and is tested against fakes of them. `Stanza::new` carries the core's defaults for a
  kind's stanza, `Adapters::new` takes a design's adapters, and
  `RestrictionGuardFactory::new` a closure building its rules, which carries any setting
  they need.
- `issuers/ondo/`: Ondo's adapters (its API, the `SyntheticSharesOracle`, its compliance
  contracts, its status) and `Ondo`, the issuer built from them.
- `chain.rs`: the reads every design shares. Every on-chain read goes through its `Calls`,
  which tests answer with `quote_updater::testing::FakeChain`, a chain that knows only the
  exact calls it was given, so an adapter's wiring (which contract, which argument) is tested
  without a node. An adapter that reads an HTTP API is tested against
  `quote_updater::testing::MockApi` the same way.
- `quote_updater::poll`, at the crate root: what the core's own readers are built from, for
  an adapter's refresher to read and log like them: `every` (the schedule), `wall_clock_ms`
  (the clock a price is stamped on), `Failures` (one log line a minute while a source keeps
  failing, and one when it recovers).
- `mod.rs`: the `Issuer` trait.

## Adding another issuer

The safety checks are the same for every issuer; only where each answer comes from changes.
[`quote-updater/examples/issuer.rs`](../quote-updater/examples/issuer.rs) is a whole one,
`acme`, written against the public API alone the way a crate of your own is, and compiled
with the workspace, so that path cannot close unnoticed. An issuer is:

1. its adapters, one per port it answers, under `quote-updater/src/stocks/issuers/<issuer>/`
   or in a crate of your own: a `PriceSource` answering `Fetched::new(Price::new(..))`, a
   `Valuation` answering `Some(Shares::new(k))` (its `action` set for a scheduled corporate
   action), or `None` for a token it does not list, a `MarketStatus` if the issuer reports one, whose reader a task of yours keeps
   current (built from `quote_updater::poll`'s `every` and `Failures`, so it reads and logs
   like the core's own), and `Restrictions` answering the token's rules. Reads of the chain
   go through `stocks::chain::Calls` and its helpers; a test answers them with
   `testing::FakeChain`, and an HTTP API with `testing::MockApi`;
2. a config struct for its `[pairs.pricing]` stanza, `#[serde(deny_unknown_fields)]`, that
   fills the core's `Stanza` (`Stanza::new(kind, source, symbol, delta)` carries the core's
   defaults; set the keys your kind lets the operator change) and is checked with
   `Settings::new` plus its own rules;
3. a unit struct implementing `Factory` over that config (its `build` hands
   `Adapters::new(prices, valuation)`, with `status` set when the issuer reports one, to
   `pricer::build`) and `Issuer`, which names its two kinds and gives the token guard:
   `RestrictionGuardFactory::new(issuer, closure)` for a design whose rules the core's guard
   reads through `Restrictions`, the closure carrying any setting the rules need;
4. `fields()` and `form_help()` on the factory, so the backoffice edits the pair. The
   round-trip test beside `Ondo` is the template: every declared field is a key the config
   reads, and the example stanza survives the form unchanged. A key the config accepts but
   the form does not declare survives a save too, since the page rebuilds a stanza from the
   one the pair has;
5. for a token design of its own, a Solidity test of PropAMM against it. In this repository
   that is a mock under `contracts/test/mocks/` and a test inheriting
   `PropAMMStockTokenTest` (`PropAMMOndoToken.t.sol` is the template); from another
   repository, copy that suite. It takes a token on either side of USDC's address and of any
   decimals, and assumes a plain ERC-20 with no transfer fee and no rebasing, like Ondo's; a
   token that isn't one needs a look at PropAMM itself first.

Then one line in the binary, `.stock_issuer(Yours)` after `.us_market_hours()`. Nothing in
the core or the builder changes.

A design whose answers the ports cannot carry yet extends the port first, with a fake of the
new answer in the core's tests. Two cases we already expect: a token whose corporate actions
take effect at an instant rather than under a pause the issuer lifts, and a source that prices
only the stock.

## Smoke test

`e2e/stocks/run.sh` runs the example binary against anvil, mock Ondo contracts
(`contracts/test/mocks/MockOndoStack.sol`, one contract per role, each answering only for what
it holds) and a mock of Ondo's API (`e2e/stocks/ondo_api.py`). It checks that:

1. `--check` passes on the example stanza, and with the pair halted;
2. it fails on a wrong key, and passes, saying so, on a token the oracle does not list;
3. it names each refusal Ondo's stack holds (the token paused, the vault blocklisted, the
   PropAMM sanctioned), found where Ondo keeps them, and withdraws on a refusal none of
   them holds, which only the transfer the guard tries finds;
4. one node-mode push lands and PropAMM quotes Ondo's price less the delta;
5. an event Ondo files under the stock's ticker withdraws the quote.

It needs Foundry and Python 3, and no network. CI runs it in the `stocks-smoke` job.

```bash
bash e2e/stocks/run.sh
```
