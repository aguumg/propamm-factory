# Capitán Monasterio — a maker agent that quotes well, then widens its spread over time

(named after the villain of season 1 of Disney's classic Zorro series: the one who moves the price against the Zorro.)

**What it is:** a real on-chain contract, not a test mock. It writes against
`MockPURAuthenticated`'s own write shape (`setState(target, delta, mid)`) — this repo only has the
real registry's read/authorization interface (`contracts/src/interfaces/IPrioUpdateRegistry.sol`:
`getState`/`addUpdater`/`removeUpdater`, no write function), so a real deployment would need
writing against whatever that registry's actual write function is. Two moves, nothing else:

- `quoteCompetitive(delta, mid)` — publish an attractive price and reset the widening clock.
  `mid` never changes afterward.
- `tick()` — recompute and republish the currently-widened `delta`, purely from elapsed time
  since the last `quoteCompetitive`.

Both are only callable by `controller`, the account actually running this agent (set once, at
construction).

**Why decay instead of a hand-timed pull:** the agent has no way to see a taker's pending swap —
`PropAMM.quote()` is a `view` call, an off-chain simulation that leaves no trace on chain, so
there's no event or mempool entry to react to. The widening shape (monotonic, capped, clock-driven)
is modeled on UniswapX's Dutch-auction pricing, but only the SHAPE: UniswapX's decay is set by the
*swapper* on their *own* order (favoring fillers early, the swapper late) — the opposite direction
and the opposite party from a maker worsening a quote it already published against an unwilling
counterparty.

**What widens, and why `delta` and not `mid`.** `PropAMM` prices a pair off one shared `mid`, read
in whichever direction a swap goes (`PropAMM.sol:306-310`): moving `mid` worsens one direction's
output and *improves* the other's by construction (both directions invert around the same number).
A quote worse "for the taker" would open an arbitrage every block the shifted price stood, since
anyone could trade the other way at the now-favorable rate. Widening `delta` (the spread)
discounts the output by the same fraction regardless of direction (`PropAMM.sol`'s `c =
SPREAD_SCALE - spread` factor), so it worsens BOTH directions and creates no such arbitrage.
Verified directly against the live contract in both directions
(`test_reverseDirectionTaker_alsoGetsWorsePrice_notBetter`), not assumed from the formula — an
earlier version of this agent moved `mid` instead, which a review caught failing exactly this way.

**Practical read**, given how this system actually orders things (price updates land before that
block's taker trades): if `controller` calls `tick()` every block, a taker reading `quote()` in
block N and landing in N+1 sees the fully widened spread almost every time, not "whichever happens
to land." In practice this is a maker displaying a price it is not actually willing to fill at.

**What this is NOT:** a flaw in `PropAMM.sol`. `swap()` does exactly what it documents — re-quotes
fresh, and the taker's `minAmountOut` bounds the outcome. This is a strategy a maker already
holding a valid updater key can run, not a bypass of anything `src/` enforces. The same shape is
detectable on chain: a `tick()` landing right after a `quoteCompetitive()`, and fills clustering at
the bottom of whatever tolerance takers declare.

## Files

- `contracts/test/CapitanMonasterio.sol` — the agent.
- `contracts/test/CapitanMonasterio.t.sol` — runs it against the real `PropAMM.sol`.
- `contracts/test/mocks/MockPURAuthenticated.sol` — the oracle stand-in, scoped per `target` the
  way the real registry is (`addUpdater` is self-service per caller; `setState` requires
  authorization for the specific `target` being written; `getState` reads the caller's own lane).

## How to use it

1. Deploy (or `vm.etch`, for a local test) the oracle `PropAMM.sol` will read, at
   `0xDa7AfEeD021EAFC1c1Af9C362dE477DaD0396B81` — see `setUp()` in `CapitanMonasterio.t.sol`.
2. Deploy a `PropAMM` instance with an EMPTY `updaters` list (same order as
   `docs/production.md`'s own production deploy flow).
3. Deploy `CapitanMonasterio`, pointing it at that oracle address, the `PropAMM` instance's own
   address as `target`, and a `controller` address.
4. The `PropAMM` instance's owner authorizes it: `propAmm.addUpdater(address(agent))`.
5. `controller` calls `quoteCompetitive(delta, mid)` with an attractive price. A taker reading
   `quote()` now sees that price.
6. `controller` (or a keeper acting for it) calls `tick()` periodically. Each call republishes
   the currently-widened `delta`, computed purely from elapsed time.
7. A taker's `swap()` re-quotes fresh at execution (`PropAMM.sol:168`): if it lands before much
   widening, it settles near the competitive price; if it lands after a `tick()` has published
   the widened spread, it settles worse, in either trading direction.

## Run it

No RPC, no mainnet fork, no network calls — fully local and deterministic:

```
cd contracts
forge test --match-path 'test/CapitanMonasterio.t.sol' -vv
```

7 scenarios, all passing:

- **Access control** (`test_quoteCompetitive_refusesNonController`,
  `test_tick_refusesNonController`): a non-`controller` address cannot drive the agent.
- **Immediate taker** (`test_immediateTaker_getsTheCompetitivePrice`): no time passes, nobody calls
  `tick()` — the swap settles at (within rounding) the competitive price.
- **Tight floor after full widening** (`test_takerLandsAfterFullWidening_tightFloor_reverts`): the
  taker read the competitive quote, but its swap lands `DECAY_DURATION_SECS` later, after
  `controller`'s `tick()` has published the fully widened spread. Its floor (pinned to what it
  saw) fails the fresh quote, so `swap()` reverts instead of settling worse.
- **Loose floor after full widening** (`test_takerLandsAfterFullWidening_looseFloor_settlesAtTheFloor`):
  same timing, but the taker left slippage room — the swap settles at the widened-spread floor,
  logged in the test output.
- **Widening shape** (`test_widening_worsensTheTakersOutputMonotonicallyAndIsCapped`): verifies the
  taker's output (read from the live contract, not the formula in isolation) is unchanged at t=0,
  strictly worse at the halfway point, strictly worse again at full widening, and never worsens
  further past the cap.
- **Both directions** (`test_reverseDirectionTaker_alsoGetsWorsePrice_notBetter`): a taker trading
  the opposite direction after the spread has fully widened also gets a worse price, not a better
  one — the check a `mid`-shifting version of this agent would have failed.
