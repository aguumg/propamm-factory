# propamm-stocks

The quote updater for tokenized stocks against USDC on Ethereum mainnet, built on the
[`quote-updater`](../quote-updater) library like [`example/`](../example). It supports Ondo
Global Markets tokens (AAPLon, TSLAon, SPYon, ...) today, and is laid out so another token
design is a module of its own (see [Layout](#layout)). The PropAMM contract needs no change for
these tokens: they are plain ERC-20s (see `contracts/test/PropAMMStockToken.t.sol` and
`PropAMMOndoToken.t.sol`). What differs is everything around the price, which is what this
crate adds:

- **`ondo`**, a pricing model. The mid is Ondo's own price for the token, polled from Ondo's
  GM API: the price Ondo mints and redeems around, already including the token's share
  multiplier. It withdraws the quote when the price is older than its limit, when it has not
  moved for `max_unchanged_secs` (a halted stock's price stops while Ondo may keep stamping
  it current), when Ondo reports any session but `regular`, during an active asset event
  (under the token's symbol or the stock's ticker), from `pause_lead_secs` before the start
  of an upcoming event that will move the share multiplier (from the moment Ondo lists it if
  it gives no start), from the start of any other upcoming event, from `pause_lead_secs`
  before a corporate action Ondo has scheduled on chain, and when Ondo's multiplier and the
  chain's disagree. The spread is a fixed `delta`. Ondo's market-wide status is read once
  per API, key and poll period, however many stock pairs share them, by the pairs' own
  tasks. The stock's ticker comes from Ondo's price answer, or from the stanza's `ticker`
  when the answer names none; with neither, the build fails, since events Ondo files under the
  ticker would go unseen.
- **`us_market_hours`**, a quote guard. It withdraws outside 09:30–16:00 New York time on
  trading days (13:00 on early closes), by the holiday calendar the config lists, and past the
  calendar's `listed_through` date. It publishes the days the calendar has left
  (`calendar_days_left`, which `PusherStockCalendarEnding` alerts on a month out) and warns
  in its log once a day from then.
- **`ondo_token`**, a quote guard. It withdraws while Ondo would refuse the fill: the token
  paused, or the vault or the PropAMM on Ondo's blocklist or the sanctions list. It also
  tries the transfer a fill makes, `transferFrom(vault, vault, 0)` as the PropAMM, so a
  refusal none of those lists explains (a check Ondo adds to the token) withdraws too, under
  the revert data it answers. PropAMM can't see any of it, so its `isActive` stays true until
  the quote is withdrawn.

It quotes the US regular session only, about a fifth of the week; the tokens trade on chain
around the clock.

## Run it

You need:

- an Ondo GM API key (partner-only, through Ondo's onboarding) in `ONDO_API_KEY`
- the pair's updater key in the variable its `key_env` names, registered on your PropAMM
- a vault holding the stock token and USDC, with approvals for the PropAMM on both

Copy `config.example.toml` to `config.toml` (git ignores it: it holds the builders' API
keys), fill in `target`, the RPC and the builders, then:

```bash
cargo run --release -- --config config.toml --check
cargo run --release -- --config config.toml
```

`min_mid`/`max_mid` are in the lane's orientation, the number `--check`'s mid column prints:
USDC per token when the stock token's address sorts before USDC's, as AAPLon's does, and
token per USDC when it sorts after. The build refuses a band written the other way up.

Keep the `holidays`/`early_closes` lists current from
[NYSE's calendar](https://www.nyse.com/markets/hours-calendars), and `listed_through` with
them. The example lists them through 2027.

## Layout

The checks a stock pair needs are the same whoever issued the token; what differs between
designs is where the answers come from. The code is split along that line:

- `src/ports.rs`: what the core asks of a design, and the shape of each answer. A
  `PriceSource` (the token's price), a `Valuation` (how many shares a token is worth, and the
  corporate action that will change it), an optional `MarketStatus` (the issuer's session
  and asset events), and `Restrictions` (the token's transfer rules).
- `src/stock/`: the core. The pricer and all its gates (`pricer.rs`), the guard on the token's
  transfer rules (`guard.rs`), and `us_market_hours` (`hours.rs`). It reads every design
  through the ports, and is tested against fakes of them.
- `src/issuers/ondo/`: Ondo's adapters (its API, the `SyntheticSharesOracle`, its compliance
  contracts, its status) and the `ondo` and `ondo_token` kinds built from them.
- `src/chain.rs`: the reads every design shares. Every on-chain read goes through its `Calls`,
  which tests answer with a fake that knows only the exact calls it was given, so an adapter's
  wiring (which contract, which argument) is tested without a node.

Adding a design means:

1. writing its adapters under `src/issuers/<design>/`;
2. writing a stanza for its pricer that fills the core's `Stanza`, with defaults of its own;
3. registering its kinds in `src/main.rs`;
4. adding a mock of its token to the contracts' tests, inheriting `PropAMMStockTokenTest`. The
   suite takes a token on either side of USDC's address and of any decimals.

A design whose answers the ports cannot carry yet extends the port first, with a fake
of the new answer in the core's tests. Two examples: a token whose corporate actions take
effect at an instant rather than under a pause the issuer lifts, and a source that prices
only the stock.

## Smoke test

`smoke/run.sh` runs the real binary against anvil, mock Ondo contracts
(`contracts/test/mocks/MockOndoStack.sol`, one contract per role, each answering only for what
it holds) and a mock of Ondo's API (`smoke/ondo_api.py`). It checks that:

1. `--check` passes on the example config, and with the pair halted;
2. it fails on a wrong key and on a token with no multiplier;
3. it names each refusal Ondo's stack holds (the token paused, the vault blocklisted, the
   PropAMM sanctioned), found where Ondo keeps them, and withdraws on a refusal none of
   them holds, which only the transfer the guard tries finds;
4. one node-mode push lands and PropAMM quotes Ondo's price less the delta;
5. an event Ondo files under the stock's ticker withdraws the quote.

It needs Foundry and Python 3, and no network.

```bash
bash stocks/smoke/run.sh
```
