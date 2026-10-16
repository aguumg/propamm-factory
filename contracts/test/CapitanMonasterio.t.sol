// SPDX-License-Identifier: MIT
pragma solidity ^0.8.35;

import {Test} from "forge-std/Test.sol";
import {ERC1967Proxy} from "@openzeppelin/contracts/proxy/ERC1967/ERC1967Proxy.sol";
import {PropAMM, InsufficientOutput} from "../src/PropAMM.sol";
import {MockERC20} from "./mocks/MockERC20.sol";
import {MockPURAuthenticated} from "./mocks/MockPURAuthenticated.sol";
import {CapitanMonasterio} from "./CapitanMonasterio.sol";

/// @title Running Capitán Monasterio against the real PropAMM contract
/// @notice Fully local: no RPC, no mainnet fork, no network calls. Demonstrates the agent's
/// pattern end to end: it quotes a competitive price, then its published spread widens on a fixed
/// clock (`CapitanMonasterio.tick()`), with no information about any taker. A taker who reads
/// `quote()` early and whose swap lands early gets (close to) the competitive price; one whose
/// swap lands later, after `controller` has called `tick()`, gets a worse price instead — purely
/// because of when its transaction happened to land, not because the agent reacted to it. `swap()`
/// always re-quotes fresh (`PropAMM.sol:168`).
///
/// Deploy order mirrors `docs/production.md`: the `PropAMM` instance is created with an EMPTY
/// updaters list, then its owner authorizes the agent afterward via `addUpdater` -- not baked into
/// `initialize`. This is also what lets the oracle's self-scoped `addUpdater`
/// (`MockPURAuthenticated.sol`) register the agent correctly: the authorizing call's `msg.sender`
/// is the `PropAMM` proxy itself, the same address `CapitanMonasterio` later writes to as `target`.
contract CapitanMonasterioTest is Test {
    address constant ORACLE = 0xDa7AfEeD021EAFC1c1Af9C362dE477DaD0396B81;
    uint256 constant AMOUNT_IN = 1 ether;

    // Representative (delta, mid): not fetched live, not tied to any specific deployment. No
    // assertion in this file depends on the absolute price level, only on how it changes.
    uint256 constant COMPETITIVE_DELTA = 5e14; // 5 bps
    uint256 constant COMPETITIVE_MID = 2_500 * 1e18;

    MockERC20 internal tokenIn;
    MockERC20 internal tokenOut;
    PropAMM internal propAmm;
    CapitanMonasterio internal agent;

    address internal owner = makeAddr("owner");
    address internal vault = makeAddr("vault");
    address internal taker = makeAddr("taker");
    address internal controller = makeAddr("controller"); // the key/account actually running the agent
    address internal keeper = makeAddr("keeper"); // anyone NOT the controller -- should be refused

    function setUp() public {
        vm.etch(ORACLE, address(new MockPURAuthenticated()).code);

        tokenIn = new MockERC20();
        tokenOut = new MockERC20();

        PropAMM.PairConfig[] memory pairs = new PropAMM.PairConfig[](1);
        pairs[0] = PropAMM.PairConfig({token0: address(tokenIn), token1: address(tokenOut), vault: vault});

        propAmm = PropAMM(
            address(
                new ERC1967Proxy(
                    address(new PropAMM()),
                    abi.encodeCall(PropAMM.initialize, (owner, new address[](0), ORACLE, pairs))
                )
            )
        );

        agent = new CapitanMonasterio(ORACLE, address(propAmm), controller);

        vm.prank(owner);
        propAmm.addUpdater(address(agent));

        tokenOut.mint(vault, 10_000 ether);
        vm.prank(vault);
        tokenOut.approve(address(propAmm), type(uint256).max);

        // Fund the vault with tokenIn too, and have it approve, so a REVERSE-direction swap
        // (tokenOut -> tokenIn) can also be filled -- needed for the arbitrage check below.
        tokenIn.mint(vault, 10_000 ether);
        vm.prank(vault);
        tokenIn.approve(address(propAmm), type(uint256).max);
    }

    function _fundTaker(MockERC20 token, uint256 amount) internal {
        token.mint(taker, amount);
        vm.prank(taker);
        token.transfer(address(propAmm), amount);
    }

    /// @dev `vm.expectRevert`/`vm.expectPartialRevert` only guard the very next call, so a test
    /// that needs one must fund the taker FIRST and call `propAmm.swap` itself right after the
    /// cheatcode -- not through this helper, which the success-path tests use.
    function _swapAsTaker(uint256 minOut) internal returns (uint256 amountOut) {
        _fundTaker(tokenIn, AMOUNT_IN);
        vm.prank(taker);
        return propAmm.swap(address(tokenIn), address(tokenOut), AMOUNT_IN, minOut, taker, block.timestamp);
    }

    //------------------------------
    // Access control: can only `controller` drive the agent?
    //------------------------------

    function test_quoteCompetitive_refusesNonController() public {
        vm.prank(keeper);
        vm.expectRevert(abi.encodeWithSelector(CapitanMonasterio.NotController.selector, keeper));
        agent.quoteCompetitive(COMPETITIVE_DELTA, COMPETITIVE_MID);
    }

    function test_tick_refusesNonController() public {
        vm.prank(controller);
        agent.quoteCompetitive(COMPETITIVE_DELTA, COMPETITIVE_MID);

        vm.prank(keeper);
        vm.expectRevert(abi.encodeWithSelector(CapitanMonasterio.NotController.selector, keeper));
        agent.tick();
    }

    //------------------------------
    // The widening pattern
    //------------------------------

    /// @notice No time passes, nobody calls `tick()`: the taker's swap lands right after the
    /// competitive quote and gets (within rounding) the competitive price. The baseline the
    /// widened cases below are measured against.
    function test_immediateTaker_getsTheCompetitivePrice() public {
        vm.prank(controller);
        agent.quoteCompetitive(COMPETITIVE_DELTA, COMPETITIVE_MID);
        uint256 quotedAtCompetitivePrice = propAmm.quote(address(tokenIn), address(tokenOut), AMOUNT_IN);

        uint256 amountOut = _swapAsTaker(quotedAtCompetitivePrice);

        assertEq(amountOut, quotedAtCompetitivePrice, "no widening has elapsed, so the price has not moved");
    }

    /// @notice The taker read `quote()` right after the competitive publish, but its swap lands
    /// `DECAY_DURATION_SECS` later — enough for `controller`'s `tick()` to have published the
    /// fully widened spread in between. A tight floor (pinned to what the taker saw) makes the
    /// swap revert instead of settling worse.
    function test_takerLandsAfterFullWidening_tightFloor_reverts() public {
        vm.prank(controller);
        agent.quoteCompetitive(COMPETITIVE_DELTA, COMPETITIVE_MID);
        uint256 quotedAtCompetitivePrice = propAmm.quote(address(tokenIn), address(tokenOut), AMOUNT_IN);

        vm.warp(block.timestamp + agent.DECAY_DURATION_SECS());
        vm.prank(controller);
        agent.tick();

        _fundTaker(tokenIn, AMOUNT_IN);
        vm.prank(taker);
        vm.expectPartialRevert(InsufficientOutput.selector);
        propAmm.swap(address(tokenIn), address(tokenOut), AMOUNT_IN, quotedAtCompetitivePrice, taker, block.timestamp);
    }

    /// @notice Same timing, but the taker left slippage room: the swap settles, at (approximately)
    /// the fully widened spread's floor -- computed here from `mid` staying fixed and `delta`
    /// widening to its cap, the way the agent actually prices it, not assumed.
    function test_takerLandsAfterFullWidening_looseFloor_settlesAtTheFloor() public {
        vm.prank(controller);
        agent.quoteCompetitive(COMPETITIVE_DELTA, COMPETITIVE_MID);
        uint256 quotedAtCompetitivePrice = propAmm.quote(address(tokenIn), address(tokenOut), AMOUNT_IN);

        vm.warp(block.timestamp + agent.DECAY_DURATION_SECS());
        vm.prank(controller);
        agent.tick();

        uint256 looseFloor = quotedAtCompetitivePrice * 95 / 100; // 5% slippage tolerance
        uint256 amountOut = _swapAsTaker(looseFloor);

        assertLt(amountOut, quotedAtCompetitivePrice, "taker settled below the price it quoted against");

        // mid never moves; only delta widens. Expected output = amountIn*mid/SCALE discounted by
        // (SCALE - widenedDelta)/SCALE, ignoring the (tiny, at this size) price-impact term.
        uint256 widenedDelta = COMPETITIVE_DELTA + (agent.MAX_WIDENING_BPS() * 1e18 / 10_000);
        uint256 midAmountOut = AMOUNT_IN * COMPETITIVE_MID / 1e18;
        uint256 expectedFloorOut = midAmountOut * (1e18 - widenedDelta) / 1e18;
        assertApproxEqRel(amountOut, expectedFloorOut, 0.001e18, "should settle at the widened-delta floor");
        emit log_named_uint("quoted at the competitive price", quotedAtCompetitivePrice);
        emit log_named_uint("settled at (after full widening)", amountOut);
    }

    /// @notice Widening is monotonic and bounded, verified directly against the live contract's
    /// own quote -- not against the formula in isolation.
    function test_widening_worsensTheTakersOutputMonotonicallyAndIsCapped() public {
        vm.prank(controller);
        agent.quoteCompetitive(COMPETITIVE_DELTA, COMPETITIVE_MID);

        uint256 outAtStart = propAmm.quote(address(tokenIn), address(tokenOut), AMOUNT_IN);

        vm.warp(block.timestamp + agent.DECAY_DURATION_SECS() / 2);
        vm.prank(controller);
        agent.tick();
        uint256 outAtHalfway = propAmm.quote(address(tokenIn), address(tokenOut), AMOUNT_IN);

        vm.warp(block.timestamp + agent.DECAY_DURATION_SECS());
        vm.prank(controller);
        agent.tick();
        uint256 outAtFloor = propAmm.quote(address(tokenIn), address(tokenOut), AMOUNT_IN);

        vm.warp(block.timestamp + 365 days);
        vm.prank(controller);
        agent.tick();
        uint256 outAtFloorMuchLater = propAmm.quote(address(tokenIn), address(tokenOut), AMOUNT_IN);

        assertLt(outAtHalfway, outAtStart, "widening should have started worsening the taker's output");
        assertLt(outAtFloor, outAtHalfway, "widening should keep worsening up to the cap");
        assertEq(outAtFloorMuchLater, outAtFloor, "widening must not worsen further past the cap");
    }

    /// @notice The check a shifted-mid version of this agent would have failed: a taker trading
    /// the OPPOSITE direction (selling `tokenOut`, buying `tokenIn`) after the spread has fully
    /// widened must ALSO get a worse price than it would have at the competitive quote -- not a
    /// better one. A version that moved `mid` instead of widening `delta` would open an arbitrage
    /// here (the reverse direction prices off the same `mid`, inverted); widening `delta` cannot,
    /// because it discounts `_amountOut` by the same fraction regardless of direction
    /// (`PropAMM.sol`'s `c = SPREAD_SCALE - spread` factor).
    function test_reverseDirectionTaker_alsoGetsWorsePrice_notBetter() public {
        vm.prank(controller);
        agent.quoteCompetitive(COMPETITIVE_DELTA, COMPETITIVE_MID);
        uint256 reverseQuoteAtCompetitivePrice = propAmm.quote(address(tokenOut), address(tokenIn), AMOUNT_IN);

        vm.warp(block.timestamp + agent.DECAY_DURATION_SECS());
        vm.prank(controller);
        agent.tick();
        uint256 reverseQuoteAfterWidening = propAmm.quote(address(tokenOut), address(tokenIn), AMOUNT_IN);

        assertLt(
            reverseQuoteAfterWidening,
            reverseQuoteAtCompetitivePrice,
            "the reverse direction must also be worse after widening, or the vault is arbitrageable"
        );
    }
}
