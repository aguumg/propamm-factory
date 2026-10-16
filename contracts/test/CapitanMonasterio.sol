// SPDX-License-Identifier: MIT
pragma solidity ^0.8.35;

import {MockPURAuthenticated} from "./mocks/MockPURAuthenticated.sol";

/// @title Capitán Monasterio — a maker agent that quotes well, then widens its spread over time
/// @notice This writes against `MockPURAuthenticated`'s own write shape
/// (`setState(target, delta, mid)`), not a confirmed real registry write path — this repo only has
/// the real registry's read/authorization interface (`contracts/src/interfaces/
/// IPrioUpdateRegistry.sol`: `getState`/`addUpdater`/`removeUpdater`, no write function). A real
/// deployment would need writing against whatever that registry's actual write function is.
///
/// Only `controller` (set once, at construction — the account/key actually running this agent)
/// may drive it.
///
/// Why decay instead of a hand-timed "pull": the agent has no way to see a taker's pending swap —
/// `PropAMM.quote()` is a `view` call, an off-chain simulation with no on-chain trace, so there is
/// no event for it to react to. The decay shape here (monotonic, capped, clock-driven) is modeled
/// on UniswapX's Dutch-auction pricing, but only the SHAPE: UniswapX's decay is set by the
/// swapper, on their own order, favoring fillers early and the swapper late — the opposite
/// direction and the opposite party from a maker decaying a quote it already published against an
/// unwilling counterparty.
///
/// What decays, and why: `delta` (the spread), not `mid`. `PropAMM._pricing` prices a pair off one
/// shared `mid`, read in whichever direction a swap goes (`PropAMM.sol:306-310`): moving `mid`
/// worsens one direction's output and IMPROVES the other's by construction, since both directions
/// invert around the same number — that would make the opposite direction arbitrageable against
/// the vault every block the shifted price stood, not just worse for one taker. Widening `delta`
/// discounts `_amountOut` by the same fraction regardless of direction (`PropAMM.sol`'s `c =
/// SPREAD_SCALE - spread` factor), so it worsens BOTH directions and creates no such arbitrage.
/// Verified directly against the real contract in both directions, not assumed from the formula.
///
/// Practical read, given how this system actually orders things (`docs/manual.md`: each block's
/// price updates land before that block's taker trades): if `controller` calls `tick()` every
/// block, a taker reading `quote()` in block N and landing in N+1 sees the fully widened spread
/// almost every time, not "whichever happens to land". In practice this is a maker displaying a
/// price it is not actually willing to fill at.
///
/// What this is NOT: a flaw in `PropAMM.sol`. `swap()` does exactly what it documents — re-quotes
/// fresh, and the taker's `minAmountOut` bounds the outcome. This is a strategy a maker already
/// holding a valid updater key can run, not a bypass of anything `src/` enforces. The same shape
/// is detectable on chain: a `tick()` landing right after a `quoteCompetitive()`, and fills
/// clustering at the bottom of whatever tolerance takers declare.
///
/// Two moves:
///   1. `quoteCompetitive` — publish an attractive `(delta, mid)` and reset the decay clock. A
///      taker reading `quote()` right after this sees (close to) this price; `mid` never changes
///      afterward.
///   2. `tick` — recompute and republish the currently-widened `delta`, purely from elapsed time
///      since the last `quoteCompetitive`. `controller`-only.
contract CapitanMonasterio {
    error NotController(address caller);

    MockPURAuthenticated public immutable oracle;
    address public immutable target;
    address public immutable controller;

    uint256 public competitiveDelta;
    uint256 public competitiveMid;
    uint256 public lastQuoteTimestamp;

    /// @dev How long the widening takes to reach its cap. One block, matching the gap between a
    /// taker reading a quote off-chain and its swap landing on the next one.
    uint256 public constant DECAY_DURATION_SECS = 12;
    /// @dev How much `delta` widens by, at most, in bps added on top of `competitiveDelta`.
    uint256 public constant MAX_WIDENING_BPS = 200;
    uint256 private constant BPS_SCALE = 10_000;

    modifier onlyController() {
        require(msg.sender == controller, NotController(msg.sender));
        _;
    }

    constructor(address oracle_, address target_, address controller_) {
        oracle = MockPURAuthenticated(oracle_);
        target = target_;
        controller = controller_;
    }

    /// @notice Publishes a fresh competitive price and resets the decay clock.
    function quoteCompetitive(uint256 delta, uint256 mid) external onlyController {
        competitiveDelta = delta;
        competitiveMid = mid;
        lastQuoteTimestamp = block.timestamp;
        oracle.setState(target, delta, mid);
    }

    /// @notice Republishes `(currentDelta(), competitiveMid)`. No information about any taker goes
    /// into this call — it is purely a function of elapsed time.
    function tick() external onlyController {
        oracle.setState(target, currentDelta(), competitiveMid);
    }

    /// @notice What `tick()` would publish right now: `competitiveDelta` widened linearly by up to
    /// `MAX_WIDENING_BPS` over `DECAY_DURATION_SECS`, then flat at that cap. `mid` is never
    /// adjusted by this agent.
    function currentDelta() public view returns (uint256) {
        uint256 elapsed = block.timestamp - lastQuoteTimestamp;
        uint256 wideningBps =
            elapsed >= DECAY_DURATION_SECS ? MAX_WIDENING_BPS : MAX_WIDENING_BPS * elapsed / DECAY_DURATION_SECS;
        return competitiveDelta + (wideningBps * 1e18 / BPS_SCALE);
    }
}
