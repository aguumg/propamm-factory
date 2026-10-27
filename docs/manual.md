## Builder mode (default)

Instead of submitting `updateState` transactions to an RPC node, the updater can act as a
maker to one or more block builders: it signs each block's update locally and streams it
to every configured builder as a Protocol Buffers quote update over its own authenticated
WebSocket. [Titan defined this quote-update protocol](https://docs.titanbuilder.xyz/propamms/makers)
and other builders implement it, so nothing here is specific to any one of them. Each
builder keeps only the latest quote per maker and places it ahead of taker trades in the
block it builds; an update no taker trades against never lands on-chain, and quote updates
through this endpoint pay no priority fees. Access is gated per builder: each one's
`Authorization` API key comes from its own onboarding — a business step, not a code one.
The RPC node is still used for reads (the latest block, for the base fee and timestamp the
update is stamped with, and the updater's nonce). An update is stamped one block time after
the latest block, and the registry accepts it only in a block carrying exactly that
timestamp; when that slot passes with no block in it (a missed slot), the updater re-stamps
and re-signs the same update for the following slot, so the builders are not left holding a
quote no block can carry.

How the updater learns of a new block is up to you. By default it polls the head over
`--rpc-url` every `--requote-ms` (at least 250ms) and fetches each new block once, which works
with any node. Pass `--rpc-ws-url wss://…` (`RPC_WS_URL`) and it subscribes to the node's
`newHeads` instead: every new block's header is pushed the moment the node has it, so the
pairs roll to the next block sooner and fetch nothing per block, and the head is only polled
as a slow liveness check. Polling covers any spell the subscription is down, so a WebSocket
endpoint that drops costs nothing but the push.

Every configured builder is quoted with the *same* signed transaction — signed once per
block and sent byte-identical to each connection. That is safe by construction: the
transaction's nonce and hash are the same everywhere it is held, so however many builders
hold the quote, at most one of them can ever get it included. Fan-out only multiplies the
chance of inclusion, never the chance of a double-send.

```bash
UPDATER_KEY_USDC_USDT=0x... my-quote-updater \
  --config config.toml \
  --rpc-url https://<your-mainnet-rpc>
# builders come from config.toml's [[builder]] entries; --mode builder is the default
```

### `[[builder]]`

The builders to quote are `[[builder]]` tables in the config file, each with a `name`
(unique, used in logs), an `endpoint` (`ws://` or `wss://`), and an `api_key` sent verbatim
as the `Authorization` header. They are why a filled-in config file holds secrets: keep it
0600 and out of git. Regional endpoints are just separate entries: add `titan-ap` and
`titan-us` tables alongside a `titan-eu` one to fan out across regions, each with its own
optional `disable_cross_region` override of the `[settings]` value. Unknown fields are an
error rather than a silently-ignored default, so a misspelled key fails at startup instead
of leaving an API key empty or a region unpinned.

`--builders <path>` still reads the same `[[builder]]` entries from a separate file, and
when given it replaces the config file's list rather than adding to it, so exactly one
list is ever in force.

At least one configured builder must connect for a pair to start quoting: a config where
nothing connects is almost always a typo or a revoked key, and refusing loudly beats
silently quoting to no one. Each pair's startup table shows which builders connected and
prints `quoting with N/M builders live`; if none did, the pair exits with `no builder
connected (N configured); check the endpoints and API keys in the config file, or pass
--mode node to send updateState transactions to --rpc-url instead` (and its supervisor
retries on a backoff). Once running, nothing is fatal: a builder that drops is redialed
every 2 seconds while the others keep quoting, and the per-block summary line shows each
builder's ack (or `down: <reason>`) in its own column.

### The config file

One TOML file, passed with `--config` (required), holds the whole configuration: the
target, the `[settings]`, the `[[pairs]]` and the `[[builder]]` entries. The only thing
outside it is the updater keys, which stay in the environment and are named, never valued,
by each pair's `key_env`. Copy `quote-updater/config.example.toml`, which documents every
key:

```toml
target = "0xYourPropAMM"      # one PropAMM; every lane below belongs to it

[settings]                    # every key optional; a flag or env var beats the file
mode = "builder"
rpc_url = "https://<your-mainnet-rpc>"
requote_ms = 50
metrics_addr = "127.0.0.1:9464"

[[pairs]]
tokens  = ["0xC02aaA39...", "0xA0b86991..."]   # [base, quote], market orientation
symbol  = "ETHUSDC"                            # one Binance market; or `sources`
key_env = "UPDATER_KEY_WETH_USDC"              # names this pair's key variable
pricing = { kind = "feed", delta = "0.0005" }  # the pricing kind and its keys; delta optional
```

### Where the mid comes from: one venue or several

`symbol = "ETHUSDC"` is the short form of one Binance market. A pair can instead read
several markets, at one venue or many, and publish their weighted average:

```toml
[[pairs]]
tokens  = ["0xC02aaA39...", "0xA0b86991..."]
key_env = "UPDATER_KEY_WETH_USDC"
pricing = { kind = "feed", delta = "0.0005" }
min_sources = "2"
sources = [
  { venue = "binance",  symbol = "ETHUSDC", weight = "2" },
  { venue = "coinbase", symbol = "ETH-USD", weight = "1" },
  { venue = "kraken",   symbol = "ETH/USD" },                 # weight blank is 1
]
```

Every source is its own feed: its own socket (or poll), its own reconnects, its own
`quote_updater_venue_feed_*` series labelled `(pair, venue)`. A task per pair, the
composite, recomputes `Σ weight·mid / Σ weight` whenever any source delivers, and writes
the result where the quote loop reads its price. Nothing after that point knows there
was more than one venue: the band, the breaker, the volatile pricing and the builder path
see one mid. Weights are relative (2 and 1 mean the same as 0.667 and 0.333) and are
applied over the sources whose latest price is fresh, under 30 seconds old. A source
that goes quiet drops out and its weight is spread over the rest, so one venue's outage
costs a little accuracy rather than the quote. `min_sources` (default 1) is the floor:
below it the pair publishes nothing, the last composite ages out like a stalled feed
always did, and the lane is withdrawn.

The breaker and the σ history judge the composite, because that is the number the lane
publishes. A pair with more than one source needs a `delta` or the volatile knobs: there
is no single book whose spread it could copy.

Three consequences of averaging worth choosing deliberately:

- **Every source is averaged in the pair's quote currency unless it says otherwise.** A
  source without `quote` is averaged as read: mixing Coinbase's `ETH-USD` with Binance's
  `ETHUSDC` that way averages dollars with USDC, which is fine only while the two trade at
  par. The startup cross-check accepts a USDC or USDT pair read from the base's USDC, USDT
  or USD market, with a warning that it is averaged at par; any other difference (a
  different asset, or another quote) fails it unless the pair sets
  `allow_symbol_mismatch`. A USD market can instead be converted; see below.
- **A venue dropping out steps the mid by the disagreement between venues.** The fresh set
  changes, so the average moves by however far the departing venue sat from the rest. The
  breaker judges that step like any other, so `max_deviation` has to be larger than the
  dispersion you expect between your venues, or one venue going quiet halts the pair.
  Halting is the right outcome for a dispersion nobody expected; the number just has to be
  chosen with it in mind.

The venues, and how each spells a market (wrapped tokens are spelled as the underlying,
`WETH` as `ETH`, so the startup cross-check knows what to expect at each):

| venue | spelling | how it is read |
|---|---|---|
| `binance` | `ETHUSDC` | bookTicker stream |
| `coinbase` | `ETH-USD` | ticker stream |
| `kraken` | `ETH/USD` | ticker stream (v2) |
| `okx` | `ETH-USDC` | tickers stream |
| `bybit` | `ETHUSDC` | level-1 order book stream |
| `kucoin` | `ETH-USDC` | level-1 order book stream, socket URL fetched over HTTP first |
| `bitget` | `ETHUSDC` | ticker stream |
| `gate` | `ETH_USDC` | book_ticker stream |
| `mexc` | `ETHUSDC` | REST, polled every second (its stream is protobuf-only) |

CoinGecko is not a price source. The updater uses it for one thing: the USD price of every
vault token, once a minute, for the dashboard's vault totals (`quote_updater_token_price_usd`
and `quote_updater_vault_value_usd`). A paid-plan API key in `COINGECKO_API_KEY` sends those
requests to its paid API (`pro-api.coingecko.com`); once a minute is about 43,000 calls a
month. `[settings.endpoints] coingecko` points them somewhere else, for a mock.

You do not have to know how each venue spells a market. On the backoffice's pair form,
once both token addresses are in, the venue table fills itself: the updater reads each
token's `symbol()` on chain (a wrapped token is read as its underlying, `WETH` as `ETH`),
asks every exchange for its own list of markets, and takes the tradable market whose base
and quote are exactly those two names, with the name the exchange gives it. The one
fallback: a pair quoted in USDC or USDT that an exchange does not list exactly takes
whichever of that exchange's USDC, USDT and USD markets trades the most, with a note. Any
other quote matches exactly or the row stays empty.
Weights are suggested from each market's 24h volume (largest 10), and the three busiest are
ticked. The ticked venues are then dialled live, with the
same feed code the updater runs, and each one's mid plus the weighted composite stream into
the table, so what would be published is on screen before Save. Nothing is saved until
Save. A row the lookup could not fill is filled by hand, and its live price shows whether
the symbol typed works.

`--check` dials every source and prints one line per venue under a multi-source row, so a
venue with a wrong symbol, or one disagreeing with the others, shows up before anything is
published. Each venue can be repointed (a mock, a regional endpoint) from
`[settings.endpoints]`; `binance_ws` is the older spelling of its Binance entry.

### Sources quoted in USD

A source marked `quote = "USD"` is converted into the pair's quote token before it is
averaged, by the rate the pair's `[pairs.fx]` stanza reads:

```toml
[[pairs]]
# ... a WETH/USDT pair
min_sources = "2"
sources = [
  { venue = "coinbase", symbol = "ETH-USD",  weight = "2", quote = "USD" },
  { venue = "okx",      symbol = "ETH-USDT", weight = "1" },
  { venue = "bybit",    symbol = "ETHUSDT",  weight = "1" },
]

[pairs.fx]
sources = [
  { venue = "kraken",   symbol = "USDT/USD" },
  { venue = "coinbase", symbol = "USDT-USD" },
]
min_sources = "2"            # default: all of them
max_depeg = "0.005"          # default
max_disagreement = "0.0005"  # default
```

The FX legs are the quote token's own USD markets. Each is a feed like a source, dialled
with the pair's sources and judged fresh by the same rules (30 seconds, and
`max_source_lag_ms` when set), with its own `quote_updater_venue_feed_*` series under the
venue label `fx:<venue>`. A leg carries no weight and does not count toward the pair's
`min_sources`. The rate is the plain mean of the fresh legs: the price of one quote token
in USD, about 0.998 for USDT. On every composite tick each fresh USD source's mid is
divided by it (USD per base over USD per quote token is quote token per base); on an
inverted lane, which carries the reciprocal, it is multiplied instead. The half-spread is
a fraction of the mid and is not touched. A converted sample is as old as the older of
its own price and the oldest leg.

The rate is refused, and every USD source is left out of that tick's average, while:

- fewer legs are fresh than the stanza's `min_sources` (`stale`);
- the rate is further from 1 than `max_depeg` (`depeg`);
- the legs are further apart than `max_disagreement`, highest less lowest over their mean
  (`disagreement`).

`quote_updater_fx_guard_tripped{reason}` says which, `quote_updater_fx_rate` and
`quote_updater_fx_sources_fresh` what the legs read, and `quote_updater_source_converted`
which sources were converted into the last average. `PusherFxGuardTripped` raises a ticket
after five minutes of it, per pair, so a guard that flaps or changes reason still does. A
leg that has not delivered within 15 seconds of the pair starting is named: a warning on a
run, which starts on the sources that need no rate, and a failure under `--check`. One leg
(`min_sources = "1"`) is allowed, with a startup warning: nothing checks a single leg's
print, and a bad one moves the mid by up to `max_depeg` times the USD sources' weight.

The pair's `min_sources` is judged after the USD sources are left out. Set it no higher
than the number of sources without `quote`, or a refused rate withdraws the whole pair
rather than leaving it quoting on the rest; startup warns when it is higher. A pair with
no `quote` and no `[pairs.fx]` behaves exactly as before. The startup cross-check reads
each leg against the quote token's USD market (`USDT/USD` at kraken on a USDT pair), which
`allow_symbol_mismatch` does not waive, and does not warn about a USD source that declares
`quote = "USD"`.

### Late venues: prices that arrive behind the exchange

One optional key, `max_source_lag_ms`, leaves a venue out of a pair's average while its
prices arrive later behind the exchange's own timestamps than the pair allows, and brings
it back on the first prompt frame. Off unless set; a pair that leaves it unset quotes
exactly as before.

```toml
[[pairs]]
# ...
sources = [
  { venue = "kucoin", symbol = "ETH-USDT", weight = "2" },
  { venue = "okx",    symbol = "ETH-USDT" },
]
min_sources = "1"
max_source_lag_ms = "1500"
```

Why the freshness rule is not enough. A venue is fresh while frames keep arriving: its
sample is re-stamped by every frame, and it is dropped only after 30 seconds with no frame
at all. A stream that is alive but running behind its exchange never trips that: frames
keep arriving on time, each one carrying a price from seconds or minutes ago. It happens on
a lossy route to an exchange: the socket's queue falls tens of seconds behind for minutes
at a time, then drains in seconds the way a queue does, while the exchange's other venues
stay current. Frames keep arriving, so nothing drops out, and the pair quotes prices tens
of seconds old.

What the key measures is the stream, not the market. Most venues write the exchange's own
clock reading into every price frame: Kraken (`timestamp`), Coinbase (`time`), OKX (`ts`),
Bybit (`ts`), KuCoin (`timestamp`), Bitget (`ts`) and Gate (`t`). The feed subtracts that from
the frame's arrival, which gives how long the frame took to reach us. A backlog makes every
frame late by seconds; a quiet book whose last frame arrived promptly reads as prompt
however long ago that was, so a thin market is not read as a late one. That difference is
the reason this is lateness and not the age of the price. "Usual" lag is subtracted first:
the least lag the venue's frames have had over the last hour, taken only from minutes in
which at least two frames arrived (so one odd frame cannot set it, and a venue sending
under two frames a minute gets no baseline and has its whole offset counted). That absorbs
the network's ordinary latency and this host's clock offset from the exchange's, within
bounds that differ by direction: a host clock up to 1 second ahead of the exchange's is
absorbed, since the baseline is capped there, and beyond it every frame reads late by the
excess, which fails closed (the venue drops out); a host clock up to 2 seconds behind is
absorbed, since a stamp more than 2 seconds ahead of its arrival is left out of the
baseline and counted in `quote_updater_venue_future_stamps_total`, and beyond it every
frame's lateness reads as zero, which fails open (a backlog goes unseen). Both are far
outside what a host kept on NTP drifts, which is why the host must be. Because of the 1
second cap a backlog always shows, even one that started before the feed was dialled or
lasts longer than the hour. A step of the host clock (an NTP correction, a resume from
sleep) restarts the baseline.

A venue whose latest frame was late is out of the average, its weight spread over the
rest, until a prompt frame arrives. The pair's `min_sources` then decides, as it already
does, whether the pair keeps publishing on the venues left. When it cannot, the composite
withdraws its price at once, so the next tick is refused and the quote is cancelled at
every builder (an empty-tx update, kept in force until enough venues are fresh again); it
does not leave the last price standing to age out, since with a late venue that price was
already behind. With a single venue, a late frame is therefore an immediate withdrawal.
Nothing latches. `quote_updater_venue_late{pair, venue}` reads 1
while a venue is out for this, the updater logs a line when it drops out and when it comes
back, and `PusherVenueLate` raises a ticket after five minutes of it.

Binance's spot `bookTicker` carries no event time (its frame is `u s b B a A`) and MEXC is
polled over REST, whose body carries none, so those two are never left out for lateness; a
pair whose venues are only those is refused the key, since it would do nothing. Binance
itself falling behind is therefore still not seen by anything in the updater.

1500 ms is a sensible value on any pair: it is a statement about the network and the
exchange's queue, not about the token. The backoffice's pair form has the field; blank
clears it, and a form loaded before the field existed keeps whatever the file sets.
Rolling back to a version without this key: the config parser refuses keys it does not
know, so remove `max_source_lag_ms` from every pair before downgrading.

### What the updater charges a taker

Every pair names how it is priced in its `pricing` table: a kind, and the keys that kind
takes. The binary registers the kinds that exist; the library ships `feed`, `fixed` and
`volatile`, and docs/building-your-own.md is how a binary adds its own. The rest of this
section is about `feed` and `fixed`.

A `feed` pair with no `delta` publishes the book's own half-spread,
`(ask - bid) / (ask + bid)`. On a liquid market that is around 0.02bp, which is what a
venue charges a taker it can quote away from in milliseconds. A published quote cannot be
pulled that way: it stands for the whole block, and the maker wears every move that happens
inside it. Quoting a book's spread on-chain therefore fills at close to mid and loses to
adverse selection on average.

Set `delta` in the pair's `pricing` table and that number is published instead, with only
the mid coming from the stream. It is the same key, in the same units, as on a `fixed` pair:
a fraction of the mid, so `"0.0005"` is 5bp and means the same thing whatever the pair
trades at. It is not
reoriented on an inverted lane, because `(ask - bid) / (ask + bid)` is unchanged by
inversion and a fraction standing in for it is too.

So `feed` alone tracks the book's spread, `feed` with a `delta` takes the mid live and
charges your own spread, and `fixed` with `mid` and `delta` fixes both. A `delta` at or above one whole
unit is refused at parse time, and a *book* that gapes that wide is refused per sample:
PropAMM subtracts the spread from the mid fill, so a whole unit leaves the taker nothing and
beyond it `_quote` reverts `SpreadTooWide`. Refusing withdraws the quote rather than
reverting every fill. `--check`'s delta column prints whichever number applies, because it
runs through the same function the service publishes with.

### Volatile pairs: a computed spread

A fixed `delta` is right for USDC/USDT and wrong for WETH/USDC: the pool is stuck holding
whatever it bought until the next block, 12 seconds later, and WETH moves in 12 seconds. For
such a pair, use the `volatile` kind: a `[pairs.pricing]` table with `kind = "volatile"`
and `gamma`, `k` and `kappa` (all three). Every block the
updater then computes two numbers and sends them to the chain, the spread and the mid, and
the pool sells at `mid + spread/2` and buys at `mid − spread/2`:

```
spread = γ·σ²·τ  +  (2/γ)·ln(1 + γ/k)  +  κ·σ·√Δ
         term 1      term 2                term 3

mid    = market mid · (1 − skew),   skew = q·γ·σ·√τ
```

The skew scales with σ·√τ, the standard deviation of the move over the hold, not with
the variance σ²·τ that term 1 uses: at the σ this feed measures (fractions of a bp per
√second) the variance form is ~1e-8 and the tilt is numerically dead, so the tilt uses
the √ form and a sane γ (order 1) moves the mid a few bps. The adverse-selection charge
(term 3) is separate and stays in the spread.

The market mid is the pair's feed: one venue's book, or the weighted average of several
(see above). σ is the only input we measure: take the last `volatility_window_secs` of
that mid (one per second, 600 by default), compute each second's percentage change, take
the standard deviation. For USDC/USDT that is basically 0, so term 1 and term 3 are 0 and
the spread is term 2, a constant. That is the same thing as a fixed spread, so this is one
formula for both kinds of pair. It needs 60 seconds of prices before it publishes anything,
and the prices are kept per market (the set of venues, weights aside) for the life of the
process, so changing a knob (which restarts the pair) does not restart that wait.

Term 1: someone sells us WETH at 2999 while the real price is 3000. Fine so far, we are up
1. But we cannot sell it until the next block, 12 seconds later, and by then the price might
be 2980. This term charges extra to cover that. σ²·τ is how big that move typically is over
τ = `hold_secs` seconds (12, one block). γ = `gamma` is a number we pick for how much to
charge against it. Nothing measures γ; we set it and change it until the pool makes money.

Term 2: terms 1 and 3 only cover what we expect to lose. This one is what we earn. Charge
more per trade and fewer people trade with us, because routers send the order to whoever is
cheapest; charge less and more do. k = `k` is a guess for how fast people leave when we
charge more, and the formula is the amount that makes the most money in total if that guess
is right. Nothing measures k, so pick it backwards from the spread you want: with gamma 0.1,
k = 2000 makes term 2 a 0.05% spread per side, k = 700 makes it 0.14%.

Term 3: we post "buy at 2999" based on Binance at 3000. Four seconds later Binance drops to
2990. Our price still says 2999, so a bot sells us WETH at 2999 that is worth 2990. We lost 9
the instant the trade happened. σ√Δ is how much the price typically moves in the Δ =
`fill_delay_secs` seconds (6, half a block) between us posting and someone trading. κ =
`kappa` is how much of that to charge for; set it by checking, after the fact, how much the
price moved right after each of our fills.

Skew: this is not about running out of inventory. It is about not holding a lot of a token
that can crash. It looks at two things: how much of the vault's value is in WETH compared to
`target_share` (q: if we want half and it is 75% WETH, q = 0.5), and how jumpy the price is
right now (σ). When both are high, we lower both our prices: people buy WETH from us because
it is cheap, and stop selling us more because we pay little, so the WETH share shrinks. When
we hold less than the target, we raise both prices. When the price is calm we barely move
them, and on a stablecoin pair (price never moves) we do not move them at all, even if the
vault is lopsided. We never move them by more than spread/2, so we never sell WETH below the
Binance price. To know the WETH share, the updater reads both tokens' `decimals()` once when
the pair starts, then every 12 seconds asks the contract which vault the pair fills from
(`vaultFor(token0, token1)`, re-read each time because the owner can move a pair to another
vault) and reads both balances there, valuing the WETH at the Binance mid. `target_share` is a share of value, not an amount, so adding
liquidity to the vault does not change it.

#### The inventory charge: one side gets worse far off target

The skew tilts the mid to lean the pool back toward `target_share`, but it is capped at
half the spread, so against a strong one-directional flow it gets pinned at that cap and
still loses ground. `inventory_aversion` (λ) adds a second, separate control. While the
base share stays inside the band [`inventory_band_lower`, `inventory_band_upper`] it does
nothing. Past an edge it makes the one trade that would push the vault further off target
more expensive, by this fraction of its price:

```
inventory_penalty = min( λ · σ·√τ · q_excess , 5% )
```

`q_excess` is how far past the edge the vault is, in the same units as q. The trade that
would bring the vault back is priced exactly as it would be with the charge off. Holding too
much WETH, the harmful trade is someone selling us more, so only our buy price drops; holding
too little, it is someone buying WETH from us, so only our sell price rises. Say the market
is at 2600, we would quote buy 2598 / sell 2600 without the charge, and the charge is 0.1%:
we quote buy 2595.40 / sell 2600.

The contract still takes one mid and one delta, and it buys at `mid'·(1 − delta)` and sells
at `mid'/(1 − delta)`. So the updater works out the two prices it wants and solves for the
pair that gives them: `mid' = market mid · √(buy·sell)` and `1 − delta = √(buy/sell)`, both
as fractions of the market mid. To raise only the sell price by 0.10%, that moves the mid up
about 0.05% and widens delta by about 0.05%. Neither price crosses the market mid: the
untouched one is where the skew left it, and the charge only moves the other one further
away.

It is off by default (`inventory_aversion = 0`), so a pair that does not set it prices
exactly as before. It has its own coefficient because one `gamma` cannot drive both the
tilt and this charge: the tilt is right near γ ~ 0.1–1, while a charge that bites needs a
much larger λ. The band edges are base shares (defaults `½·target_share` and
`2·target_share`); with `target_share ≥ 0.5` the default upper edge is `≥ 1` and never
fires, so set it to a reachable value (e.g. 0.55). The charge is capped at 5%, below the
refuse-to-publish guard, so an extreme inventory quotes very wide rather than stranding by
refusing.

A second, stronger tier can sit outside the first. Past a wider band
[`inventory_band_hard_lower`, `inventory_band_hard_upper`] the charge grows at
`inventory_aversion_hard` (λ_hard, which must be at least λ) instead of λ:

```
inventory_penalty = min( λ · σ·√τ · q_excess + (λ_hard − λ) · σ·√τ · q_excess_hard , 5% )
```

`q_excess_hard` is the distance past the hard edge, in the same units. The charge is
continuous at both edges and the inner band's behaviour is unchanged. A blank hard edge turns
the hard tier off on that side, and a blank `inventory_aversion_hard` equals λ, which also
turns it off: both must be set for the tier to do anything. Leave all three blank for the
single-band charge above.

#### Why the contract does not change

With two deltas we would send `[δ_ask, δ_bid, mid]`, three numbers, and the contract would
need new code to pick δ_ask or δ_bid depending on trade direction. With the shift we send
`[delta, mid']` where `delta = spread/2` and `mid' = mid − skew`: the same two numbers, in the
same slots, the updater sends today, so the contract code that reads `slots[0]` as the spread
and `slots[1]` as the mid and charges `mid ± delta` works unchanged. Stable pairs send
`[delta, mid]` exactly as now (σ = 0 means skew = 0, so `mid' = mid`), volatile pairs send
`[spread/2, mid − skew]`, and the contract cannot tell the difference and does not need to.

The algebra, for the record:

```
δ_ask = spread/2 − skew          δ_bid = spread/2 + skew
sell  = mid + δ_ask = mid + spread/2 − skew = (mid − skew) + spread/2
buy   = mid − δ_bid = mid − spread/2 − skew = (mid − skew) − spread/2
```

Both prices are `(mid − skew) ± spread/2`. The one side effect is that the mid stored on
chain is the Binance mid minus the skew, a few bps at most, which is what `min_mid`/`max_mid`
and the dashboards see.

What can stop a volatile pair from quoting, beyond what stops a plain feed: under 60 seconds
of price history (`warming_up`), or a vault balance older than 60 seconds (`no_inventory`).
Both withdraw the quote the same way a stale feed does. `--check` does not wait a minute for
history, so it prints the spread at σ = 0 with a note saying so. Every term is exported as
a diagnostic of the kind, `quote_updater_diagnostic{kind="volatile",name=...}`: `sigma`,
`hold`, `edge`, `stale`, `inventory_penalty`, `q`, `skew`, `target_share`, `inventory_base`,
`inventory_quote` and `inventory_base_share`, so a spread that moved can be traced to the
term that moved it.

Three things are derived rather than configured, so they cannot be set wrong:

- **The lane** is `keccak256(token0 ++ token1)` over the address-sorted tokens, matching
  `PropAMM._pairKey`. There is no `--lane`.
- **Whether the feed is inverted** follows from whether sorting flipped the declared order.
  Above, USDC sorts below WETH, so the lane carries USDC priced in WETH and the ETHUSDC
  stream is inverted automatically. There is deliberately no `invert` key. A fixed `mid` is
  read in the same market orientation and inverted the same way, so `symbol` and `mid` never
  mean different things behind one `tokens` array.
- **The label in every log line** comes from on-chain ERC-20 `symbol()` calls, so a log line
  cannot disagree with what is being quoted.

Each pair needs **its own** updater key, named by `key_env`. That is the point of the
multi-pair design: two pairs sharing an address share a nonce, so only one of their lanes
could land per block and the other would be starved. The updater refuses to start on a config
whose keys resolve to the same address.

### Checking a config before it sends anything

`--check` validates the config, resolves each key, proves the chain agrees, prints what
every pair would publish right now, and exits without sending:

```bash
cd quote-updater && set -a && . ./.env && set +a
my-quote-updater --config config.toml --check
```

It reports, per pair: the lane, the orientation, the stream, the circuit-breaker threshold
(`breaker`, or `-` for a pair with none), the updater address derived from that pair's key,
whether the target has authorized it (`addUpdater`), and how many more updates its balance
covers at the current base fee. Then a second table gives each pair's
live mid and delta — with an `≈ 4001.98 USDC per WETH` line under an inverted lane, because
`0.000249876` is not checkable by eye. It exits non-zero if anything failed, and the same
report prints on every normal run too, before the first update is sent.

### Reloading the config

The config file is not only the updater's startup input, it is the desired state of a
running one. Send a SIGHUP and the updater re-reads it and converges the pairs it is running
onto it:

```bash
$EDITOR config.toml
my-quote-updater --config config.toml --check   # validate first; sends nothing
kill -HUP $(pidof my-quote-updater)                         # or: kill -HUP $(pidof quote-updater)
```

`target` and the `[settings]` table are read once at startup, and a reload that finds
either changed is refused with the key named, rather than applied to the file and not the
process.

A changed `[[builder]]` list *is* applied, but it restarts every lane rather than only the
ones whose stanza moved: each builder connection is opened inside a pair's own quoting
loop, so a lane cannot pick up a new list without being rebuilt. The reload dials the new
list first and refuses the whole thing, touching nothing, unless at least one of them
connects. A lane opens its builders after it starts, unlike its feed, so without that check
a typo would take every lane down at once.

It diffs the file against what is running, per lane, and touches only what differs:

| In the file | Running | What happens |
|---|---|---|
| unchanged | quoting | **nothing** — no withdraw, no reconnect, no gap |
| changed | quoting | restarted from the new stanza |
| changed or not | halted on its breaker | restarted, re-arming it |
| absent | quoting | quote withdrawn at every builder, then stopped |
| new | — | started |

Two properties make it safe to run casually. A reload that fails to validate applies
**nothing** — a bad parse, a `target` that moved, a duplicate signer, a failed preflight all
leave every pair exactly as it was. And a lane that is being started or restarted has its new
feed dialled *before* the old one is stopped, so a stanza that turns out to be wrong costs
that lane nothing: it keeps quoting on the config it already had, and only it is reported as
failed. Everything else is all-or-nothing; the feed dial is the one thing that cannot be
known without doing it.

This is also how a pair halted by its circuit breaker comes back, with no edit needed — a
halted lane is precisely a lane the file says should be quoting and which is not. The line it
prints names what it cleared:

```
reload: [WETH/USDC] re-armed, clearing the halt it took 14m ago on a 4.10% move from 0.000251 to 0.000261 (limit 2.00%)
```

A reload re-arms **every** halted lane, including one you were not thinking about when you
ran it. That is the cost of having a single verb rather than a separate "resume this pair",
and why each one is named individually rather than folded into a count.

Some things still need a restart, and the updater refuses the reload and says which:

- **`target`** — every lane's identity is derived from it.
- **Command-line and environment settings** — `--mode`, `--rpc-url`, `--registry`,
  `--price-decimals`, `--requote-ms`.
- **A newly added `UPDATER_KEY_*`** — systemd reads `EnvironmentFile` once, at exec, so a key
  written into `.env` after the process started is not in its environment. Adding a pair
  whose key was already there works.
- **The `[settings]` table.** Every key in it is consumed once, at startup: the RPC client
  is built from `rpc_url`, the exporter bound to `metrics_addr`, the requote timer owned by
  each pair's loop.

### The backoffice

Set `[settings] backoffice_addr` and the updater serves a small web page for the changes an
operator actually makes: add or remove a pair, choose which venues its mid is averaged over
and with what weights, edit its band and every knob its pricing kind declares, add or remove
its guards and edit theirs (one block per registered guard kind, with an "enabled" box), add
or remove a builder, repoint a venue's endpoint, halt a pair (or all of them), and resume a
halted pair. A kind that declares no form, one registered through the closure helpers, shows
as edited in the file and is kept as written. A field that holds a key, Ondo's API key say,
takes the name of the environment variable holding it, never the value. It is server-rendered HTML with
plain form posts, and every change does exactly what an operator would have done over SSH:
rewrite `config.toml` and reload. The one piece of JavaScript is on the pair form: it looks up
which venues list a pair (`GET /pairs/lookup`, which asks each exchange) and shows each ticked
venue's live price (a websocket to `/pairs/preview`). The form still works without it.

The live prices run over one websocket per open page. Ticking a venue tells the server to
connect to it, unticking tells it to disconnect that venue, and a new weight reconnects
nothing, so a change never touches the other venues. The preview connects to the venues from
this host, the same address the live feeds use, and a venue can block an address that opens
too many connections, so it is kept small: at most 4 pages open at once, and each venue
connected at most once every 2 seconds per page however fast the page changes it. The socket
only opens from the backoffice's own page (it checks the browser's `Origin`), so another site
cannot use a logged-in operator's session to open it.

```toml
[settings]
backoffice_addr = "100.64.1.2:8088"   # this host's tailnet address
```

**Halting keeps the pair.** Halt sets `halted = true` on the pair in `config.toml`, and the
updater treats a halted pair as absent: its quote is withdrawn at every builder, as a removal
would, but the stanza and every setting stay in the file. It stays halted across reloads and
restarts until someone resumes it, from its row or with Restart halted pairs, which clears
the `halted` flag of every pair halted from the page along with re-arming pairs stopped on
their circuit breaker. A halt with a `halt_reason` beside it is left alone by that button: a
binary's `UpdaterHandle::halt` always writes one (it is that binary's kill switch), and the
page's own Halt writes none. The banner names each pair it left halted and its reason; only
that pair's own Resume (or the handle's `resume`) clears it. A halted pair's vault balances
are still exported.

**Every change is write, reload, and revert if refused.** The file is rewritten atomically,
the service is asked to reload it, and if the reload is rejected the previous file is put
back before the page answers. So what is on disk always describes what is running, and a
bad edit costs a rejected reload and nothing else. Without the revert a rejected edit would
sit in the file until some later, unrelated reload silently applied it. A reload that applies
only in part (some pair's feed would not come up, so that pair keeps its previous config or
is not started) is not reverted: it already stopped, started and restarted every other lane
it reached, a halt among them, and putting the file back would have the next reload undo
that. The page shows it as an error all the same, saying the file kept the change, and the
next reload retries the pair that failed.

**Builder mode only.** The reload it drives belongs to the per-lane service that builder
mode runs; node mode pushes `updateState` on a timer and has no convergence to trigger. A
`backoffice_addr` under `--mode node` is refused at startup rather than bound and left
inert, so trying it locally means the mock-builder recipe below, not the anvil `make`
targets.

**Access control is the tailnet.** There is no login. Binding an unspecified address
(`0.0.0.0`) is refused at startup rather than warned about: unlike `/metrics`, which only
leaks, this page writes, and anything that can reach it can repoint a pair's price source
at a market of its choosing. Every change is logged with the peer that made it.

A server with a backoffice may start with no pairs and no builders at all: `pairs = []`
plus a `backoffice_addr` is a complete config, and everything else is added through the
page. That is what lets a deploy seed a new server from two values (a target and an RPC
endpoint) instead of carrying its whole configuration as a secret.
That is refused without one, because a updater quoting nothing is normally a config that
failed to say what it meant, but on a server that is about to be configured through the
browser it is just the first state, and refusing it would leave nowhere to do the
configuring.

One thing it deliberately cannot do: a **new** `UPDATER_KEY_*` has to be in the process
environment already, since systemd reads `EnvironmentFile` once at exec, so adding a pair
with a brand new key is a redeploy and a restart. Builder changes do apply immediately,
at the cost of restarting every lane onto the new list.

A reload that does not fully take is worth noticing, because nothing else in the process
says so — the pairs stay healthy and every other series reads exactly as before. Both ways it
can happen are counted by `quote_updater_config_reloads_total` and alerted on:

- `result="rejected"` — the file did not validate, so nothing was touched. Every lane is on
  the previous config, together. `PusherConfigReloadFailed`.
- `result="partial"` — the file was applied, but a lane's feed would not come up, so that
  lane is still on its old stanza (or was never started) while the rest moved. A mixture,
  which is the harder of the two to reason about. `PusherConfigReloadPartial`.

`quote_updater_config_generation` advances only when a reload put **every** configured pair
on the new file, so reading it answers "is the file I am looking at live?" without having to
also check that nothing failed.

Removing a pair takes its metrics with it: every per-pair gauge goes to `NaN` on the way out,
so a lane deleted while halted does not leave `PusherBreakerTripped` firing for a pair that no
longer exists. Its counters stop where they are, which `rate()` already reads as zero.

Ctrl-c reaches every lane directly, including while a reload is in flight, so a shutdown
during one still withdraws at every builder rather than being cut off at `TimeoutStopSec`.

SIGHUP is handled in every mode. In `--mode node` and under `--once` there is no supervised
pair set to converge, so the signal is acknowledged on stderr and otherwise ignored — it
never terminates the process, which is what an unhandled SIGHUP would do.

### Configuring it with a `.env`

`quote-updater/.env` holds the updater keys and nothing else; see
`quote-updater/.env.example`. Everything that used to live there (the RPC endpoint, the
requote cadence, the metrics address) is now the config file's `[settings]` table, so a
sourced `.env` plus a config supplies the whole configuration and the run command carries
no flags at all. Keeping the keys in the environment rather than the argument list also
keeps them away from `ps`, which would show them to any local user.

**Every** option still reads from an environment variable too (`--help` prints each one's
name next to it), and one that is actually set beats the file. That is the precedence:
flag, then environment variable, then `[settings]`, then the built-in default.

The binary does not read the file itself (there is no dotenv dependency), so source it
first. `set -a` matters twice over: it exports the options clap reads, and each pair's key
is looked up **by name at runtime**, so it has to be exported rather than merely assigned.

```bash
cd quote-updater && set -a && . ./.env && set +a
my-quote-updater --check      # inspect, send nothing
my-quote-updater
```

A flag on the command line always wins over its variable, so a sourced `.env` is still
overridable per run: `my-quote-updater --requote-ms 25`.

Two conventions make a file that documents every knob safe to keep around:

- A blank value (`MINE=`) reads as *not set*, and the option falls back to its default.
- The on/off flags (`ONCE`, `NO_PIN`, `MINE`, `DISABLE_CROSS_REGION`) take `false`, `f`,
  `no`, `n`, `off`, `0` and blank as off, anything else as on — so a variable left at
  `false` is genuinely off rather than merely present. This matters because the updater
  checks the *values* of mutually exclusive options rather than leaning on clap's
  `conflicts_with`, which counts anything that came from the environment as set.

What goes in it:

- `CONFIG` — the pair config (`--config`). Required.
- `UPDATER_KEY_*` — one private key per pair, under whatever name that pair's `key_env`
  gives. Each key's address must be authorized by the target via `addUpdater`, and must hold
  ETH for base fee; `--check` reports both and prints the exact `addUpdater` calls that are
  missing. Keys pay no priority fee in builder mode, but still need base-fee ETH for the
  updates that land.
Everything below is a `[settings]` key first and an environment variable second: the
variable exists so a flag-driven run stays possible, and setting one overrides the file.

- `MODE` — where updates go: `builder` (the default) signs each pair's per-block update
  and streams it to every `[[builder]]` in the config file; `node` submits `updateState`
  transactions to `RPC_URL` instead — an anvil for local testing, a fork, or a real
  network. Builder mode rules out `MINE` and `NO_PIN`, which are anvil-only.
- `RPC_URL` — reads only, but on the hot loop (latest block, pending nonce): each requote
  has a ~50ms budget, so endpoint latency shows up directly.
- `RPC_WS_URL` — a WebSocket endpoint of the same node, for the `newHeads` subscription
  described under builder mode above; builder mode only. Unset, the head is polled over
  `RPC_URL`; set, each new block is pushed and the poll drops to a slow liveness check.
- `BUILDERS` — a separate TOML file to read the `[[builder]]` entries from instead of the
  config file. An override, not an addition: given it, the config file's own list is
  ignored entirely.
- `REQUOTE_MS` — the requote cadence; keep it well under the builder's ~400ms eviction
  (Titan's protocol, and its own guidance is a ~50ms cadence).

`REGISTRY`, `PRICE_DECIMALS` and `BINANCE_WS` already default to the mainnet values.
`INTERVAL` paces node mode only and has no effect in builder mode, and `MINE`/`NO_PIN` are
anvil-only (node mode).

What each pair *publishes* is not here — it is in the config, including `min_mid`/`max_mid`,
the band a published mid must fall inside. That band is the only thing standing between a
corrupt book and an arbitrary price on chain that the next fill is priced against: a mid
outside it is refused, so the RPC service skips the push and builder mode withdraws the live
quote at every builder. Startup warns for any feed-priced pair that sets neither. It is per
pair rather than per process because the magnitudes are not comparable — one band covering
both ETHUSDC (~4000) and USDCUSDT (~1) would have to be so wide it bounds nothing.

`max_deviation` lives there too, and per pair for exactly the same reason: a 2% move is a
depeg on USDCUSDT and an ordinary hour on ETHUSDC, so a single process-wide threshold would
have to be loose enough for the loosest pair — which is the pair the guard exists for. See
"Circuit breaker" below.

One caveat, now flipped from what it used to be: because the `make` targets below run the
updater from `quote-updater/`, a `.env` sourced into the same shell is inherited by them
too, and the explicit flags they pass still win. That used to matter: a sourced mainnet
`.env` could leak `TITAN_WS` into a target that never passed it, silently switching a local
test onto a real builder. It can't happen anymore — the targets below now pass
`--mode node` explicitly, so builder-only variables they don't reference are simply unused.
The equivalent slip today is forgetting `--mode node` on a hand-run command while a mainnet
`.env` is sourced: that fails at startup (no `[[builder]]` entries) instead of silently
doing the wrong thing.

`.env` is gitignored, and should stay that way: it holds live private keys.

A `.env` (or shell) still exporting `TITAN_WS` or `TITAN_API_KEY` — the variables that used
to configure the single-builder mode — is refused rather than silently ignored: startup
names whichever one is set and points at the config file's `[[builder]]` entries as where
it now belongs, so nothing runs on an authentication that is not actually being used.

What builder mode does on the wire:

- Per target block (head + 1) each pair signs one update stamped `parent.timestamp + 12`
  and requotes it every `--requote-ms` (default 50ms, Titan's suggested cadence — builders
  evict quotes idle for longer than ~400ms) under one stable 16-byte `replacement_uuid`
  per builder connection with a strictly increasing sequence number, re-signing whenever
  the feed price moves.
- Each pair signs with its own key, so their nonces are independent and no pair's update can
  be blocked behind another's. A per-block read-back through `getState` reports whether the
  update actually landed, and a lane that has landed nothing for 50 consecutive blocks says
  so loudly — a builder ack is not inclusion.
- A stale or unusable price cancels the live quote (an empty-tx update) at every builder
  instead of leaving it standing, and ctrl-c cancels at every builder before exiting — a
  dead feed halts quoting there too, not just on-chain.
- Success is each builder's own ack (matching uuid and sequence, empty error), not a
  receipt: without a taker the update never lands, by design.

### Circuit breaker

Set a pair's `max_deviation` in the config to guard it against a bad print or a flash move
on its feed: if the mid deviates from the mid *observed before it* by more than this
fraction, the updater stops trusting the feed rather than publishing whatever it says next.

Three settings guard the price, and each bounds something different:

| Setting | Bounds | Asks |
|---|---|---|
| `max_deviation` | a single step | did the price *jump*? |
| `max_deviation_window` | the speed | is the price *travelling* too fast? |
| `min_mid` / `max_mid` | the destination | is the price *absurd*? |

`max_deviation_window` trips when the mid is further than the given fraction from where it
was `max_deviation_window_blocks` ago. The anchor slides, so `0.20` over `1000` blocks means
"20% within any 1000-block span", not "20% from wherever we started" — a move slow enough
that no single span sees 20% never trips it. That is what a speed limit is; `min_mid`/
`max_mid` is the setting that bounds where the price may end up.

Two things it deliberately does not catch. A crash that fully recovers inside the window
leaves the net move small, so it does not trip — the tick-to-tick check is what catches that,
whenever the crash arrives as a jump rather than a walk. And for one window's length after a
start or a config reload, the buffer is still filling, so the guard measures over less than
its full span — and for the first second it measures nothing at all, because the only bucket
it holds is the price being judged. `min_mid`/`max_mid` is what covers a cold start.

A reload re-anchors the window for a pair whose stanza changed or that was halted. That is
intended: resuming a halted pair means an operator has looked at the feed and accepted the
price it resumes at. A `builders.toml` edit re-anchors every pair's window at once, though,
since a changed builder list restarts every lane regardless of its own stanza.

The comparison is **tick to tick** — each print against the one before it — because what
this catches is a *discontinuity*, which is what a bad print or a flash move looks like. It
is judged in the pair's composite task, on every averaged sample and before the quote loop
sees it, so the reference is always the previous sample: not the last mid delivered to a builder
(that reference would freeze whenever nothing was reaching the wire, and ordinary drift on a
healthy feed would then read as a jump), and not the last mid the quote loop happened to
look at (that loop stops ticking during an RPC outage or a restart backoff, with the same
effect). An outage on the delivery side cannot manufacture a halt on the price side.

The trade is explicit: a move that arrives in sub-threshold steps never trips, however far it
eventually travels. That is a real market move, and `min_mid`/`max_mid` is what bounds where
it may end up. On trip, an empty-tx cancel goes out to every builder holding that pair's quote and
the lane stops quoting.

One consequence worth knowing before it happens: the reference is the last print the feed
*delivered*, however long ago. While the feed itself is away — a Binance reconnect, a
stream that went silent — the pair is withdrawn in the ordinary way, and the first print
after the gap is judged against the last one before it. So a **feed gap** that spans a real
move larger than the threshold does halt the lane, even though no single print jumped. That
is deliberate: a feed returning at a strange price is exactly the glitch this guards
against, and taking it on faith would re-open the hole the breaker closes at startup.
Expect it after a long feed gap on a volatile pair, and reload once the price is confirmed.

Because every print is judged, the band and the breaker see the same prices: a print far
outside `min_mid`/`max_mid` is withdrawn by the band *and* trips the breaker, rather than
slipping past a guard that only ever saw in-band prices and quietly resuming on the next
good one.

**It does not come back on its own.** A tripped pair stays down until an operator has
looked at the feed and reloaded the config (`kill -HUP $(pidof my-quote-updater)`; see
[Reloading the config](#reloading-the-config)). That is deliberate: a breaker that re-arms
itself decides, with no more information than it had when it tripped, that whatever caused a
violent move has passed — and the cost of being wrong is quoting a bad price to every
builder. Latching moves that judgement to a human, which is the only place it can actually be
made. Set the threshold to the move you would want to be woken up for, because that is
exactly what it means.

What a reload changed is only *how* the human acts, not whether they have to. Recovery used
to mean restarting the process, which brought the lane back by throwing away every other
lane's breaker reference and stopping their quoting for the restart. A reload converges the
one lane instead.

The breaker is per pair in both settings and state: each lane carries its own threshold, and
a trip halts only that lane while every other pair keeps quoting at cadence.  A pair that
sets no `max_deviation` has no breaker at all.

What the operator sees (every line is prefixed with the pair it belongs to; a real run
prints one ack column per configured builder):

```
[USDC/USDT] circuit breaker tripped: mid moved 4.12% > 2.00% (previous 0.9998, new 1.041)
[USDC/USDT] quoting block 19999999: withdrawn (price deviation 4.12% exceeds the 2.00% limit)
[USDC/USDT]   mock-a withdrawn
[USDC/USDT] halted; this pair will not quote again until you reload (kill -HUP $(pidof my-quote-updater)) once the feed is trusted
```

The trip line and the halt line go to **stderr**, like every other alarm here; the block
summary between them is an ordinary stdout line. The halt waits briefly for each builder to
report its cancel before printing that summary, so the withdraw is on the record rather than
a stale ack from earlier in the block. No landing check runs: the loop left the block early,
so it was never mined and there is nothing to verify.

The threshold is measured on the mid the lane publishes, so on an **inverted** lane it is
slightly asymmetric in market terms — with `0.02`, a market move of −2.00% trips while
+2.00% needs +2.04%. Second-order, but see `config.example.toml` for why.

Other pairs are unaffected and keep quoting, and the halt is repeated on stderr once a
minute for as long as the process is up — one line at the trip and then silence is how a
halted lane goes unnoticed for a day:

```
1/3 pair(s) halted on their circuit breaker, 2 still quoting. Check that feed and reload once you trust it (kill -HUP $(pidof my-quote-updater))
```

If nothing is left quoting, the line says so instead, and the updater **stays running**:

```
nothing is quoting: 1/3 pair(s) halted on their circuit breaker, 2 waiting to restart. NOT
exiting, because a restart would clear the breakers; check those feeds and reload once you
trust them (kill -HUP $(pidof my-quote-updater))
```

Staying up is the point, and doubly so now: exiting would be read by systemd, k8s or a shell
loop as "restart me", which clears the breaker and puts the updater straight back to quoting
the price that tripped it — automatic recovery through the back door — and a process that
exited would not be there to reload, which is how the lane is meant to come back. That holds even when most of the
pairs that are down are merely failing rather than halted: they lose nothing by it, because
`supervise` retries them forever on their own and does not need the process to die to bring
them back.

The same now goes for an outage with no halt in it, which used to exit non-zero so a
supervisor would restart the process. It does not any more. The cause is usually the
config, a restart reads that same config and fails the same way, and on a server run from
the backoffice the exit takes down the page that could have fixed it. `PusherNothingQuoting`
already pages on exactly this condition, so the alerting keeps doing the useful half and the
process stays up and says so.

`--once` is the exception: it is a batch invocation with a script waiting on it, so a trip
fails the run with a non-zero status instead of parking.

A handful of settings are refused rather than quietly ignored, because for a safety feature
the worst failure mode is an operator who believes a pair is guarded when it is not:

- `max_deviation` on a pair with a fixed `mid` — a static price cannot deviate. Give the
  pair a `symbol`, or drop the setting.
- any armed pair under `--mode node` — node mode sends `updateState` straight to the RPC and
  has no live quote to withdraw, so an update that landed cannot be recalled. The startup
  error names the pairs that arm one.
- a threshold of `1` or more — it is a fraction, not a percentage, so `"2"` means 200% and
  leaves the pair effectively unguarded. The error reports how the value was read
  (`this is 200.00%`) rather than only that it was rejected.
- a threshold that rounds to zero at `--price-decimals` — every price would read as a
  deviation, so the pair would halt on its second sample.
- a threshold with more decimal digits than `--price-decimals` can hold — it would be
  silently truncated to a tighter guard than the one configured. The error says how it
  would have been read.

`--check` prints a `breaker` column showing each pair's threshold, or `-` for a pair with
none, so a `max_deviation` that was mistyped or left off one row of a multi-pair config is
visible before anything is published rather than after.

### Metrics

Set `--metrics-addr` (or `METRICS_ADDR` in `.env`) to an address like `127.0.0.1:9464` and
the updater serves the Prometheus registry as plaintext at `/metrics` on that address.
Unset, which is the default, no listener is created at all. The metrics are recorded into
an in-process registry regardless of whether anything is bound to scrape it, so turning the
exporter on later costs nothing but the flag.

`--check` and `--once` never bind, whatever the address says, even though both still source
`.env` the same as a normal run. That is deliberate: `.env` is sourced for `--check` too, so
an unconditional bind would make `--check` fail with "address in use" whenever the service
was already running — breaking the one command an operator reaches for to diagnose it.
`--once` is a batch invocation nothing lives long enough to scrape, so binding there would
only cost the run a port conflict for nothing.

A sample of what a scrape returns, for one pair (illustrative values, not a live capture,
but every metric name, help string and label matches the registry as built in
`quote-updater/src/metrics.rs`):

```
# HELP quote_updater_feed_mid Latest mid observed on the feed, lane orientation.
# TYPE quote_updater_feed_mid gauge
quote_updater_feed_mid{pair="WETH/USDC"} 3542.11
# HELP quote_updater_published_mid Mid most recently signed and published.
# TYPE quote_updater_published_mid gauge
quote_updater_published_mid{pair="WETH/USDC"} 3541.98
# HELP quote_updater_breaker_deviation_ratio Last tick-to-tick move as a fraction of the configured limit.
# TYPE quote_updater_breaker_deviation_ratio gauge
quote_updater_breaker_deviation_ratio{pair="WETH/USDC"} 0.0421
# HELP quote_updater_breaker_tripped 1 once the circuit breaker has latched.
# TYPE quote_updater_breaker_tripped gauge
quote_updater_breaker_tripped{pair="WETH/USDC"} 0
# HELP quote_updater_consecutive_landing_misses Consecutive quoted blocks whose update could not be read back (RPC failure). A block with no swap, and so no update, does not count.
# TYPE quote_updater_consecutive_landing_misses gauge
quote_updater_consecutive_landing_misses{pair="WETH/USDC"} 2
# HELP quote_updater_head_number Latest block the head watcher has published.
# TYPE quote_updater_head_number gauge
quote_updater_head_number 21504312
```

Note the last one carries no `pair` label. One watcher polls the chain head for every pair,
so there is no pair a sample of it could belong to; the same is true of its poll duration
and error count. In `--mode node` those three are absent rather than zero — nothing there
starts a watcher.

Seven series are worth watching ahead of everything else the registry exposes:

- `quote_updater_breaker_tripped` — the circuit breaker latches (see "Circuit breaker"
  above): once this reads 1 for a pair it stays there until a human has looked at the feed
  and reloaded (`quote_updater_breaker_rearms_total` counts those). It is the one series that
  says a lane needs a person, not just time.
- `quote_updater_consecutive_landing_misses` — after every block the updater reads the lane
  back from the registry to see whether its update got in. Most blocks it did not, and that
  is normal: a builder includes the update only when a swap hits the lane in that block.
  What this counts is blocks where the read-back itself failed, so the updater cannot tell;
  the `PusherLandingUnverified` alert in `deploy/alerts/` pages on it.
- the gap between `quote_updater_feed_mid` and `quote_updater_published_mid` — that gap *is*
  withdrawn time. A stale price, an out-of-band print or a tripped breaker withdraws the
  published quote while the feed keeps moving underneath it, and watching the two series
  pull apart answers "why is this lane not trading" faster than any log line does.
- `quote_updater_breaker_deviation_ratio` — the last tick-to-tick move as a fraction of the
  pair's configured `max_deviation`, where `1.0` is that limit itself (the breaker trips
  just past it, not on it — `beyond_threshold` is a strict `>`). It is the only *leading*
  indicator anywhere in this layer; everything else above reports on something that has
  already happened.
- `quote_updater_head_number` — the one series that is not per pair, and the only one whose
  failure is everyone's. No pair can quote for a new block until the head watcher publishes
  it, so a value that stops moving stops every lane at once; the four series above would all
  report it, one per lane, without any of them saying why. Read it as a rate — five blocks a
  minute on mainnet — since the number itself climbs too slowly to look like anything but a
  flat line.
- `quote_updater_venue_source_lag_ms{pair, venue}` — how late the venue's latest price
  arrived behind the exchange's own timestamp, over the venue's usual lag (see "Late
  venues" above); NaN for a venue whose frames carry no timestamp.
- `quote_updater_venue_late{pair, venue}` — 1 while the venue is left out of the pair's
  average for arriving later than `max_source_lag_ms` (`PusherVenueLate` raises a ticket
  after five minutes).
- `quote_updater_venue_future_stamps_total{pair, venue}` — frames whose exchange timestamp
  was more than 2 seconds ahead of their arrival, left out of the venue's usual lag; a
  steady count means this host's clock is behind.

`--mode node` records a smaller registry than builder mode — no builders, no head watcher, no
landing tracker — but it now records the two series that matter for safety rather than for
performance: `price_unusable_total`, which is what `PusherMidOutOfBand` fires on, and the
signer's balance and runway, on the same cadence builder mode prices them (every
`RUNWAY_CHECK_BLOCKS` pushes, off the parent block each push already fetches). Both were
silent there before, which meant the mode that spends gas on every push was the one whose
key could drain unwatched. What node mode records and builder mode does not is
`node_pushes_total`, classifying each receipt — a page-severity rule reads it, since
`PusherLandingUnverified` cannot see a mode with no quote loop.

Note what `up` does and does not tell you. The updater stays running when every pair has
halted on its circuit breaker, and the systemd unit is `Restart=on-failure`, so a fully
halted updater is a live process that nothing will restart and no liveness probe will
flag. `quote_updater_pairs_halted` and `quote_updater_breaker_tripped` are what say so.

`quote_updater_config_reloads_total{result="rejected"}` is the same shape of problem one
level up: a reload that failed to validate changed nothing, so every pair is quoting happily
on the previous config while the config file says something else. No other series moves, because
nothing is unhealthy — the gap is only between the file and what is running.
`quote_updater_config_generation` counts the reloads that did apply.

The alert rules for all of this live in `deploy/alerts/` (their thresholds are pinned to the same
Rust constants the updater itself enforces at startup, and `make`'s promtool test checks they
fire), and `deploy/` has the Prometheus scrape config, the Alertmanager routing and a Grafana
dashboard, provisioned by its compose file.

To look at any of it with invented numbers and no chain, keys or venue, `metrics_fixture_server`
is an `#[ignore]`d test in `quote-updater/src/exporter.rs` that drives the real registry
through the real exporter on port 9464:

```bash
cd quote-updater && cargo test metrics_fixture_server -- --ignored --nocapture
```

It covers what a local chain cannot: `--mode node` has no builders, so `make price-service`
leaves every builder series empty, and neither path easily produces the never-recorded gauge
case four of the alert rules depend on. Point a Prometheus at it, or run the stack in
`deploy/` against it with `updater:9464` swapped for the host in its scrape config.

### Recording the feed

With `RECORD_DB_URL` set (a Postgres URL; `.env` on the server, see `.env.example`), the
updater writes into a database what nothing on chain has: the working behind every update
it publishes (`feed.quotes`: the feed mid, the published mid and spread, the pricing kind's
declared diagnostics as one `terms` jsonb map by name, which for a volatile pair is its σ,
terms, skew and inventory, and the vault holdings and Binance mid at that moment), every
configuration it has run (`feed.config_changes`, secrets redacted: builder keys, a pricing
stanza's credentials and any URL in it cut to its host; a row when it changes, even only
inside a redacted value, and `feed.pairs`, one row per pair those configs ran), and the two
series a P&L dashboard needs: the mid of every market it prices from, and each pair's vault
balances. A Binance market is keyed by its bare symbol (`ETHUSDC`); any other venue is
`VENUE:SYMBOL` (`KRAKEN:ETH/USD`), and each pair's composite is recorded under the pair's
label. All of it goes into a `feed` schema the updater creates on first connect
(`feed.mids`, `feed.vault_balances`, `feed.quotes`, `feed.config_changes`, `feed.pairs`).
The schema's P&L functions join these against the tables of the PropAMM indexer, when the
same database holds one.

How: the feed task hands every tick to a recorder handle (a mutex around the latest book
per symbol) and the vault exporter every reading; one writer task looks once a second and
keeps a row when the value changed or a heartbeat is due (a minute for mids, five for
balances), then writes the batch in one statement per table. Mids are stamped with the
second they were seen; balances are read at one block, pinned, and stamped with that block
and its timestamp, which is what the P&L join uses ("the inventory during a fill" is the
newest row for that pair's vault at or before the fill's block). The tables, and the two SQL
functions the panels are built on, are defined once in `quote-updater/sql/feed.sql`, which
the updater compiles in and runs on every connect; nothing else writes that schema, so
redeploying the updater is how a change to it reaches the database. Expect about a million
mid rows a month per pair; a few hundred balance rows.

What it can never do is slow quoting: the database is not on the quoting path, a refused
connection or a failed write is a log line and a retry with backoff, and rows are kept in
a bounded backlog meanwhile (an hour's worth; older ones are dropped and counted). Off
under `--check` and `--once`, and without the variable, in which case the run says so once.

### End-to-end test against mocks

`make e2e` runs the binary against Python mocks of everything it talks to: a JSON-RPC chain,
two builders speaking the maker protocol, and a Binance feed. A fixed script quotes two pairs
(one at a fixed price, one from the Binance mock) through seven blocks, moves the price,
kills one builder and ends with Ctrl-C. It then decodes every update the builders received
and checks it field by field (signer, registry, lane, timestamp, `[delta, mid]`), checks
that each block included both pairs at the right nonce and that the updater logged each one
as landed, that the surviving builder kept quoting, that shutdown sent the cancels, and that
`/metrics` answered. About 30 seconds, and nothing leaves 127.0.0.1: the binary gets none
of your shell's environment but `PATH`, `HOME` and `TMPDIR`, so an `RPC_WS_URL` or
`RECORD_DB_URL` exported there (env vars beat `[settings]`) does not reach it.

It needs Python 3.9+, `pip install -r e2e/requirements.txt` (aiohttp, websockets) and
foundry's `cast`. Output lands in `target/e2e/`.

```bash
make e2e                                             # a debug build of this checkout
make e2e E2E_BINARY=target/release/quote-updater     # an already-built binary instead
make e2e E2E_BASELINE=/path/to/binary/from/main      # both, compared
```

With `E2E_BASELINE`, the second binary runs the same script on the same ports and the two
are compared: the `--check` report, every signed byte (signatures are deterministic, and so
is the mock chain) and every log line must match. That is how a refactor proves it changed
nothing. Build the baseline from a `git worktree` of the other commit.

### Running a binary of your own against the mocks

`make mocks` starts the same mocks on fixed ports and keeps them up, mining a block every
3s: the chain at `127.0.0.1:8545` (which also answers CoinGecko's token prices), Binance at
`:8546`, two builders at `:8547` and `:8548`. Point a config at them:

```toml
target = "0x70997970C51812dc3A010C7d01b50e0d17dc79C8"

[settings]
rpc_url      = "http://127.0.0.1:8545"
metrics_addr = "127.0.0.1:9464"
binance_ws   = "ws://127.0.0.1:8546/ws"

[settings.endpoints]
coingecko = "http://127.0.0.1:8545"

[[builder]]
name     = "mock-a"
endpoint = "ws://127.0.0.1:8547/ws/sendquoteupdate"
api_key  = "key-a"

[[builder]]
name     = "mock-b"
endpoint = "ws://127.0.0.1:8548/ws/sendquoteupdate"
api_key  = "key-b"

[[pairs]]
tokens  = ["0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2", "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"]
symbol  = "ETHUSDC"
key_env = "UPDATER_KEY_WETH_USDC"
pricing = { kind = "feed" }
```

The mock registry accepts any updater, so any key works; the e2e uses anvil's public dev
keys (`e2e/common.py`, `KEYS`), for example
`UPDATER_KEY_WETH_USDC=0x5de4111afa1a4b94908f83103eb1f1706367c2e68ca870fc3fb9a804cdab365a`.
Unlike `make e2e`, this binary runs with your shell's environment, and its env vars beat
the config's `[settings]`: run it from a shell that has not sourced a real `.env`, or an
`RPC_WS_URL` or `RECORD_DB_URL` there points it at a real node or a real Postgres.
The chain's control endpoint drives a scenario by hand:
`/control/price?symbol=ETHUSDC&bid=4100.00&ask=4100.20` moves the market, `/control/mine`
mines a block now, `/control/kill-builder?name=mock-a` drops a builder, and `/control/dump`
returns everything the mocks saw (every update decoded, every RPC call, any `eth_call` they
could not answer).

### Smoke-testing without an API key

The bundled mock endpoint (`builder_mock_server`, an `#[ignore]`d test) acks every update
and prints what it receives, so the wiring can be exercised without a builder account and
— run against a plain anvil with a stubbed registry — without any network access either:

```bash
anvil --order fifo &
# Stub the registry: 10 bytes of EVM that return 32 bytes of 0x..01 for any call, so the
# get_code check and preflight's isUpdater both pass without deploying the real registry.
cast rpc anvil_setCode 0xDa7AfEeD021EAFC1c1Af9C362dE477DaD0396B81 \
  0x600160005260206000F3 --rpc-url http://localhost:8545

(cd quote-updater && BUILDER_MOCK_PORT=8560 cargo test builder_mock_server -- --ignored --nocapture) &
(cd quote-updater && BUILDER_MOCK_PORT=8561 cargo test builder_mock_server -- --ignored --nocapture) &

cd quote-updater && cat > mock-config.toml <<'TOML'
target = "0x70997970C51812dc3A010C7d01b50e0d17dc79C8"

[[pairs]]
tokens  = ["0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48", "0xdAC17F958D2ee523a2206206994597C13D831ec7"]
key_env = "UPDATER_KEY_USDC_USDT"
pricing = { kind = "fixed", mid = "5", delta = "0" }

[[builder]]
name = "mock-a"
endpoint = "ws://127.0.0.1:8560/ws/sendquoteupdate"
api_key = "test"

[[builder]]
name = "mock-b"
endpoint = "ws://127.0.0.1:8561/ws/sendquoteupdate"
api_key = "test"
TOML
UPDATER_KEY_USDC_USDT=0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80 \
  my-quote-updater --config mock-config.toml --rpc-url http://localhost:8545
```

The startup table lists `mock-a` and `mock-b` as connected and `quoting with 2/2 builders
live`; both mock terminals print the identical update they were sent (same block, same
byte length); and the per-block line shows one ack column per builder. This anvil is never
sent a transaction (builder mode only reads from it), so the quoted block only rolls
forward when a block is mined by hand: `cast rpc anvil_mine --rpc-url http://localhost:8545`.
Ctrl-C ends the run and cancels the live quote at both mocks, each printing a `CANCEL`
line. Killing one mock leaves the other quoting at cadence with its column still filling
in, and the dead one's column reads `down: <reason>` — reported once per redial, not once
per requote.

The same mock setup exercises the circuit breaker end to end, live prices included. Point a
pair at a real Binance `symbol` and give it an absurdly tight `max_deviation` — `0.00001`,
one thousandth of a percent — so it trips within seconds on any liquid pair instead of
waiting for a real dislocation. Because the trip is permanent, this run ends with the updater
sitting there quoting nothing, which is the behaviour being demonstrated:

```bash
(cd quote-updater && BUILDER_MOCK_PORT=8560 cargo test builder_mock_server -- --ignored --nocapture) &

cd quote-updater && cat > mock-breaker.toml <<'TOML'
target = "0x70997970C51812dc3A010C7d01b50e0d17dc79C8"

[[pairs]]
tokens  = ["0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2", "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"]
symbol  = "ETHUSDC"
key_env = "UPDATER_KEY_WETH_USDC"
pricing = { kind = "feed" }
max_deviation = "0.00001"

[[builder]]
name = "mock-a"
endpoint = "ws://127.0.0.1:8560/ws/sendquoteupdate"
api_key = "test"
TOML
UPDATER_KEY_WETH_USDC=0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80 \
  my-quote-updater --config mock-breaker.toml --rpc-url http://localhost:8545
```

The pair quotes for a few seconds, then the log reports the trip, the mock prints a
`CANCEL` line, and the run keeps going with nothing to quote until ctrl-c. Resuming it is a
reload (`SIGHUP`, or a save from the backoffice) once the feed is trusted again.

