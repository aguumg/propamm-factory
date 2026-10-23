// SPDX-License-Identifier: MIT
pragma solidity ^0.8.35;

import {Test} from "forge-std/Test.sol";
import {IERC20} from "@openzeppelin/contracts/token/ERC20/IERC20.sol";
import {IERC20Metadata} from "@openzeppelin/contracts/token/ERC20/extensions/IERC20Metadata.sol";
import {ERC1967Proxy} from "@openzeppelin/contracts/proxy/ERC1967/ERC1967Proxy.sol";
import {PropAMM} from "../src/PropAMM.sol";
import {MockPUR} from "./mocks/MockPUR.sol";
import {MockToken} from "./mocks/MockToken.sol";

/// What a PropAMM listing a tokenized stock against USDC must do, whoever issues the token:
/// fill at the quote both ways, and revert whole when the token refuses the transfer (paused,
/// or a party refused), while `isActive` and `quote` answer as usual. Each design's test
/// inherits this with a mock of its token behind the hooks below: `PropAMMOndoToken.t.sol` for
/// Ondo's.
///
/// What the contract cannot do is see a refusal coming, which the tests named `CannotSee` pin:
/// the quote updater's restriction guard for the design closes that gap by withdrawing the
/// pair's quote, which turns `isActive` false from the next block. If one of them ever fails,
/// the contract learned to see that refusal and the guard's check for it is redundant.
///
/// The stock token may sort either side of USDC's address (0xA0b8…): the mid is set in the
/// pair's own orientation, USDC per token when the stock is token0 and token per USDC when
/// USDC is. Amounts are in whole tokens of whatever decimals the design's token has.
/// `PropAMMStockTokenShapes.t.sol` runs the suite on both sides and at 4 decimals.
abstract contract PropAMMStockTokenTest is Test {
    address constant USDC = 0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48;
    address constant ORACLE = 0xDa7AfEeD021EAFC1c1Af9C362dE477DaD0396B81;

    uint256 constant SPREAD_SCALE = 1e18;
    /// $250 a token, at PRICE_SCALE.
    uint256 constant MID = 250e18;
    /// 10 bp.
    uint256 constant DELTA = 1e15;

    address owner = makeAddr("owner");
    address vault = makeAddr("vault");
    address trader = makeAddr("trader");

    PropAMM propAmm;
    address stock;
    /// One whole stock token, in its smallest units.
    uint256 one;

    //------------------------------
    // The design's hooks
    //------------------------------

    /// Puts the design's token in place and answers its address.
    function _deployStock() internal virtual returns (address);

    function _mintStock(address to, uint256 amount) internal virtual;

    /// Makes the token refuse every transfer.
    function _pauseStock() internal virtual;

    /// Makes the token refuse every transfer `party` takes part in.
    function _refuse(address party) internal virtual;

    /// The revert data of a transfer the pause refuses.
    function _pausedRevert() internal view virtual returns (bytes memory);

    /// The revert data of a transfer refused for `party`.
    function _refusedRevert(address party) internal view virtual returns (bytes memory);

    function setUp() public virtual {
        stock = _deployStock();
        one = 10 ** IERC20Metadata(stock).decimals();
        vm.etch(ORACLE, address(new MockPUR()).code);
        vm.etch(USDC, address(new MockToken()).code);
        MockToken(USDC).setDecimals(6);

        PropAMM.PairConfig[] memory pairs = new PropAMM.PairConfig[](1);
        (address token0, address token1) = stock < USDC ? (stock, USDC) : (USDC, stock);
        pairs[0] = PropAMM.PairConfig({token0: token0, token1: token1, vault: vault});
        propAmm = PropAMM(
            address(
                new ERC1967Proxy(
                    address(new PropAMM()), abi.encodeCall(PropAMM.initialize, (owner, new address[](0), ORACLE, pairs))
                )
            )
        );

        // The mid prices one token0 in token1: $250 a token, or 1/250 of a token per USDC.
        MockPUR(ORACLE).setState(DELTA, stock < USDC ? MID : 1e36 / MID);
        _mintStock(vault, 1_000 * one);
        MockToken(USDC).mint(vault, 250_000e6);
        vm.startPrank(vault);
        IERC20(stock).approve(address(propAmm), type(uint256).max);
        MockToken(USDC).approve(address(propAmm), type(uint256).max);
        vm.stopPrank();
    }

    //------------------------------
    // Fills, as with any ERC-20
    //------------------------------

    /// Whole tokens in, 6-decimal USDC out, at a stock's price: 2 tokens at $250 is $500 less
    /// the spread.
    function test_swap_sellsTheStockTokenForUSDCAtTheQuote() public {
        uint256 expected = propAmm.quote(stock, USDC, 2 * one);
        _assertSpread(expected, 500e6);

        _mintStock(trader, 2 * one);
        vm.startPrank(trader);
        IERC20(stock).transfer(address(propAmm), 2 * one); // push-payment, as IPropAMM specifies
        uint256 amountOut = propAmm.swap(stock, USDC, 2 * one, expected, trader, block.timestamp);
        vm.stopPrank();

        assertEq(amountOut, expected);
        assertEq(MockToken(USDC).balanceOf(trader), expected);
        assertEq(IERC20(stock).balanceOf(vault), 1_002 * one);
        assertEq(IERC20(stock).balanceOf(address(propAmm)), 0);
    }

    /// The other direction: $500 buys 2 tokens less the spread.
    function test_swap_buysTheStockTokenWithUSDCAtTheQuote() public {
        uint256 expected = propAmm.quote(USDC, stock, 500e6);
        _assertSpread(expected, 2 * one);

        MockToken(USDC).mint(trader, 500e6);
        vm.startPrank(trader);
        MockToken(USDC).transfer(address(propAmm), 500e6);
        uint256 amountOut = propAmm.swap(USDC, stock, 500e6, expected, trader, block.timestamp);
        vm.stopPrank();

        assertEq(amountOut, expected);
        assertEq(IERC20(stock).balanceOf(trader), expected);
        assertEq(MockToken(USDC).balanceOf(vault), 250_500e6);
    }

    //------------------------------
    // The issuer's pause
    //------------------------------

    /// A pause set between the router's push and the fill reverts the fill, with the token's
    /// own error: the stock is the input leg here, forwarded to the vault first.
    function test_swap_revertsWholeWhileTheTokenIsPaused() public {
        _mintStock(trader, 2 * one);
        vm.prank(trader);
        IERC20(stock).transfer(address(propAmm), 2 * one);
        _pauseStock();

        vm.prank(trader);
        vm.expectRevert(_pausedRevert());
        propAmm.swap(stock, USDC, 2 * one, 0, trader, block.timestamp);
    }

    /// The stock is the output leg here: the USDC input reaches the vault before the stock's
    /// transfer is refused, and the swap still reverts, so no fill is ever half made. (The
    /// revert takes the input leg with it; the EVM sees to that, nothing in PropAMM.)
    function test_swap_revertsWholeWhenThePausedTokenIsTheOutput() public {
        MockToken(USDC).mint(trader, 500e6);
        vm.prank(trader);
        MockToken(USDC).transfer(address(propAmm), 500e6);
        _pauseStock();

        vm.prank(trader);
        vm.expectRevert(_pausedRevert());
        propAmm.swap(USDC, stock, 500e6, 0, trader, block.timestamp);
    }

    /// The gap the updater closes: nothing in PropAMM reads the pause, so `isActive` and
    /// `quote` keep answering while every swap would revert.
    function test_isActiveAndQuoteCannotSeeThePause() public {
        _pauseStock();

        assertTrue(propAmm.isActive(stock, USDC));
        assertGt(propAmm.quote(stock, USDC, 2 * one), 0);
    }

    //------------------------------
    // Refused parties
    //------------------------------

    /// A refused recipient is refused by the token, so the fill reverts rather than delivering.
    function test_swap_revertsWhenTheRecipientIsRefused() public {
        address refusedRecipient = makeAddr("refusedRecipient");
        _refuse(refusedRecipient);
        MockToken(USDC).mint(trader, 500e6);
        vm.prank(trader);
        MockToken(USDC).transfer(address(propAmm), 500e6);

        vm.prank(trader);
        vm.expectRevert(_refusedRevert(refusedRecipient));
        propAmm.swap(USDC, stock, 500e6, 0, refusedRecipient, block.timestamp);
    }

    /// A refused vault can neither receive the stock nor send it: the pair is dead both ways.
    function test_swap_revertsBothWaysWhenTheVaultIsRefused() public {
        _refuse(vault);
        _mintStock(trader, 2 * one);
        MockToken(USDC).mint(trader, 500e6);
        vm.startPrank(trader);
        IERC20(stock).transfer(address(propAmm), 2 * one);
        MockToken(USDC).transfer(address(propAmm), 500e6);

        vm.expectRevert(_refusedRevert(vault));
        propAmm.swap(stock, USDC, 2 * one, 0, trader, block.timestamp);
        vm.expectRevert(_refusedRevert(vault));
        propAmm.swap(USDC, stock, 500e6, 0, trader, block.timestamp);
        vm.stopPrank();
    }

    /// The PropAMM is a party to both legs: the sender of the input it forwards to the vault, and
    /// the spender of the vault's output. Refusing the instance itself stops it both ways.
    function test_swap_revertsBothWaysWhenThePropAMMIsRefused() public {
        _mintStock(trader, 2 * one);
        MockToken(USDC).mint(trader, 500e6);
        vm.startPrank(trader);
        IERC20(stock).transfer(address(propAmm), 2 * one);
        MockToken(USDC).transfer(address(propAmm), 500e6);
        vm.stopPrank();
        _refuse(address(propAmm));

        vm.startPrank(trader);
        vm.expectRevert(_refusedRevert(address(propAmm)));
        propAmm.swap(stock, USDC, 2 * one, 0, trader, block.timestamp);
        vm.expectRevert(_refusedRevert(address(propAmm)));
        propAmm.swap(USDC, stock, 500e6, 0, trader, block.timestamp);
        vm.stopPrank();
    }

    /// As with the pause, a refused vault is invisible to `isActive` and `quote`.
    function test_isActiveAndQuoteCannotSeeARefusedVault() public {
        _refuse(vault);

        assertTrue(propAmm.isActive(stock, USDC));
        assertGt(propAmm.quote(USDC, stock, 500e6), 0);
    }

    //------------------------------
    // The updater's trial transfer
    //------------------------------

    // The restriction guard asks the token itself, every poll, by an `eth_call` of
    // `transferFrom(vault, vault, 0)` made as the PropAMM: the checks a fill makes that do not
    // depend on the taker. That holds only if the token runs its checks on a zero amount and
    // needs no allowance for one, which these pin for each design's token.

    function test_theGuardsTrialTransferGoesThroughOnAHealthyTokenWithNoAllowance() public {
        vm.prank(vault);
        IERC20(stock).approve(address(propAmm), 0);

        vm.prank(address(propAmm));
        assertTrue(IERC20(stock).transferFrom(vault, vault, 0));
    }

    function test_theGuardsTrialTransferRevertsWhileTheTokenIsPaused() public {
        _pauseStock();

        vm.prank(address(propAmm));
        vm.expectRevert(_pausedRevert());
        IERC20(stock).transferFrom(vault, vault, 0);
    }

    function test_theGuardsTrialTransferRevertsWhenTheVaultIsRefused() public {
        _refuse(vault);

        vm.prank(address(propAmm));
        vm.expectRevert(_refusedRevert(vault));
        IERC20(stock).transferFrom(vault, vault, 0);
    }

    function test_theGuardsTrialTransferRevertsWhenThePropAMMIsRefused() public {
        _refuse(address(propAmm));

        vm.prank(address(propAmm));
        vm.expectRevert(_refusedRevert(address(propAmm)));
        IERC20(stock).transferFrom(vault, vault, 0);
    }

    //------------------------------
    // Helpers
    //------------------------------

    /// Asserts `amountOut` is `midAmountOut` less DELTA, allowing for the impact term on top and
    /// for rounding down to a whole unit of a token with few decimals.
    function _assertSpread(uint256 amountOut, uint256 midAmountOut) internal pure {
        uint256 atDelta = midAmountOut - (midAmountOut * DELTA / SPREAD_SCALE);
        assertLe(amountOut, atDelta, "taker got more than mid less delta");
        assertApproxEqAbs(amountOut, atDelta, atDelta / 1e5 + 1, "spread is not delta on this pair");
    }
}
