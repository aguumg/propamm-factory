// SPDX-License-Identifier: MIT
pragma solidity ^0.8.35;

import "@openzeppelin/contracts/token/ERC20/IERC20.sol";
import "@openzeppelin/contracts/token/ERC20/extensions/IERC20Metadata.sol";
import "@openzeppelin/contracts/token/ERC20/utils/SafeERC20.sol";
import "@openzeppelin/contracts/utils/math/Math.sol";
import "@openzeppelin/contracts-upgradeable/access/Ownable2StepUpgradeable.sol";
import "@openzeppelin/contracts-upgradeable/proxy/utils/UUPSUpgradeable.sol";
import "@openzeppelin/contracts-upgradeable/utils/PausableUpgradeable.sol";
import "@openzeppelin/contracts-upgradeable/utils/introspection/ERC165Upgradeable.sol";
import {IPrioUpdateRegistry} from "./interfaces/IPrioUpdateRegistry.sol";
import {IPropAMM} from "./interfaces/IPropAMM.sol";
import {IPropAMMFillable} from "./interfaces/IPropAMMFillable.sol";

error Expired();
error InsufficientOutput(uint256 expectedOutput, uint256 receivedAmount);
error UnsupportedPair(address tokenIn, address tokenOut);
error PairAlreadyExists(address tokenA, address tokenB);
error InsufficientOracleData(uint256 pairKey, uint256 slots);
error InvalidMid(uint256 pairKey);
error EmptyVault(address token);
error SpreadTooWide(uint256 spread);
error InsufficientVaultBalance(address token, uint256 needed, uint256 available);
/// A zero vault passed to `addPair` or `setPairVault`: a pair always names the account it fills
/// from, because that account is also what registers it (see `pairVaults`).
error NoVault(address token0, address token1);

/// @dev `ERC165Upgradeable` holds no state, so adding it moved no storage slot.
contract PropAMM is
    IPropAMM,
    IPropAMMFillable,
    ERC165Upgradeable,
    PausableUpgradeable,
    Ownable2StepUpgradeable,
    UUPSUpgradeable
{
    using SafeERC20 for IERC20;

    /// @notice A pair and the vault it fills from, as given to `initialize` and to the factory.
    /// @dev Input only. `getPairs` keeps returning `IPropAMM.TokenPair`, the router-facing
    /// standard, which knows nothing about vaults. `token0`/`token1` may come in either order;
    /// both are canonicalized on entry.
    struct PairConfig {
        address token0;
        address token1;
        address vault;
    }

    /// @notice What `_pricing` reads for one pair in one direction, for `_amountOut` to price with.
    /// @dev A fill's worth at mid, in tokenOut units, is `amountIn * midNum / midDen`.
    struct Pricing {
        address vault;
        uint256 vaultBalance;
        uint256 delta;
        uint256 midNum;
        uint256 midDen;
    }

    /// @dev Every pair mutation announces itself, in canonical token order, so pair and vault
    /// state has one on-chain source: this instance's own log. The factory's creation event
    /// carries neither pairs nor vaults for that reason.
    event PairAdded(address indexed token0, address indexed token1, address indexed vault);
    event PairRemoved(address indexed token0, address indexed token1);
    event PairVaultChanged(address indexed token0, address indexed token1, address indexed vault);
    event OracleChanged(address indexed newOracle);
    event UpdaterAdded(address indexed updater);
    event UpdaterRemoved(address indexed updater);
    event ERC20Withdrawn(address indexed token, address indexed to, uint256 amount);

    /// @dev Fixed-point scale the oracle publishes `mid` in. A `mid` of 1e18 means one whole
    /// token0 is worth one whole token1, whatever decimals the two tokens use, so `_quote`
    /// converts through this scale and the tokens' decimals on the way in and out.
    uint256 constant PRICE_SCALE = 1e18;
    /// @dev Fixed-point scale of the fractions the mid price is discounted by: the oracle's
    /// `delta` and the price impact. Being fractions rather than absolute prices, they mean the
    /// same thing on every pair: a `delta` of 5e14 is 5 basis points whatever the pair trades at.
    uint256 constant SPREAD_SCALE = 1e18;
    /// @dev Price impact charged on a fill that would consume the vault's whole `tokenOut`
    /// balance, as a fraction at SPREAD_SCALE. Impact is linear in the share the fill consumes.
    uint256 constant IMPACT_FACTOR = 1e14; // 1bps

    address private __DEPRECATED_vault;
    TokenPair[] public tokenPairs;

    mapping(uint256 => bool) private __DEPRECATED_tokenPairKeys;
    /// @dev The vault each pair fills from, by pair key, and the registry of pairs in one: a pair
    /// is registered exactly when its vault is nonzero. `_addPair` sets it, `removePair` clears
    /// it, and `setPairVault` never writes zero, so there is no separate set of keys to keep in
    /// step with it. Instances created before vaults were per pair are not upgraded onto this
    /// layout; they are replaced.
    mapping(uint256 pairKey => address) public pairVaults;
    /// @dev The registry this instance reads prices from and registers its updaters with. Set at
    /// creation rather than hardcoded, so an instance can be pointed at another registry (see
    /// `setOracle`) without a new implementation.
    IPrioUpdateRegistry public oracle;

    constructor() {
        _disableInitializers();
    }

    /// @notice Sets up a proxy's instance of PropAMM.
    /// @dev `pairs` is here rather than left to `addPair` because the deployer is not the owner:
    /// `PropAMMFactory` creates an instance owned by its caller, so this is its only chance to
    /// list pairs. The registry scopes updater authorization to `msg.sender`, which under a
    /// proxy is the proxy itself, so the updaters registered here are the instance's own.
    /// @param owner The account that will own the instance.
    /// @param updaters The addresses allowed to push price updates for the instance's lanes.
    /// @param oracle_ The oracle registry the instance will use for price updates.
    /// @param pairs The token pairs the instance starts out supporting, each with its vault.
    function initialize(address owner, address[] calldata updaters, address oracle_, PairConfig[] calldata pairs)
        external
        initializer
    {
        __Ownable2Step_init();
        __Ownable_init(owner);
        __Pausable_init();
        oracle = IPrioUpdateRegistry(oracle_);
        for (uint256 i = 0; i < updaters.length; i++) {
            oracle.addUpdater(updaters[i]);
        }
        for (uint256 i = 0; i < pairs.length; i++) {
            _addPair(pairs[i].token0, pairs[i].token1, pairs[i].vault);
        }
    }

    function reinitializeV2(address oracle_) external reinitializer(2) {
        __DEPRECATED_vault = address(0);
        oracle = IPrioUpdateRegistry(oracle_);
    }

    /// @inheritdoc IPropAMM
    function isActive(address tokenIn, address tokenOut) external view override returns (bool active) {
        if (paused()) return false;

        try oracle.getState(_pairKey(tokenIn, tokenOut), uint32(block.timestamp), uint32(block.timestamp)) {
            return true;
        } catch {
            return false;
        }
    }

    /// @inheritdoc IPropAMM
    function getPairs() external view override returns (TokenPair[] memory pairs) {
        return tokenPairs;
    }

    /// @inheritdoc IPropAMM
    /// @dev Revert if the contract is paused following the IPropAMM interface's
    /// requirement "MUST revert if the propAMM is inactive for the pair."
    function quote(address tokenIn, address tokenOut, uint256 amountIn)
        external
        view
        override
        whenNotPaused
        returns (uint256 amountOut)
    {
        return _quote(tokenIn, tokenOut, amountIn);
    }

    /// @inheritdoc IPropAMM
    function swap(
        address tokenIn,
        address tokenOut,
        uint256 amountIn,
        uint256 minAmountOut,
        address recipient,
        uint256 deadline
    ) external override whenNotPaused returns (uint256 amountOut) {
        require(deadline >= block.timestamp, Expired());

        uint256 realAmountOut = _quote(tokenIn, tokenOut, amountIn);
        require(realAmountOut >= minAmountOut, InsufficientOutput(minAmountOut, realAmountOut));

        // One vault per pair, so the account the quote was just priced against is the one both
        // legs move through. `_quote` already proved it is set; this re-read of the same slot is
        // warm, and cheaper than threading the address out of the pricing function.
        address vault = pairVaults[_pairKey(tokenIn, tokenOut)];

        IERC20(tokenIn).safeTransfer(vault, amountIn);
        IERC20(tokenOut).safeTransferFrom(vault, recipient, realAmountOut);

        emit Swapped(msg.sender, tokenIn, tokenOut, amountIn, realAmountOut, recipient);

        return realAmountOut;
    }

    /// @inheritdoc IPropAMMFillable
    /// @dev Lets `PropAMMRouter.swapSplitV1` take the part of an order the vault can fill. Without
    /// it the router probes `quote` at the order and at half of it, and `quote` reverts once a fill
    /// outgrows the vault, so an order over about twice the vault's depth would skip this instance.
    ///
    /// Capacity is what `swap` can move out of the pair's vault: the lesser of its tokenOut
    /// balance and its allowance to this instance. `quote` checks only the balance. Here the
    /// allowance counts too, because the router builds a leg around the answer, and a leg that
    /// reverts sends its input to the Uniswap remainder, which has no minimum output of its own.
    ///
    /// Reverts wherever `quote` does for any reason but size (paused, unlisted pair, stale or
    /// malformed oracle data, empty vault), and for a `delta` so close to one that even the
    /// best-paying fill spreads past it. The router reads a revert as no candidate.
    function quoteFillable(address tokenIn, address tokenOut, uint256 amountIn)
        external
        view
        override
        whenNotPaused
        returns (uint256 fillableAmountIn, uint256 amountOut)
    {
        Pricing memory p = _pricing(tokenIn, tokenOut);
        uint256 capacity = Math.min(p.vaultBalance, IERC20(tokenOut).allowance(p.vault, address(this)));
        uint256 midCap = _midCap(p, capacity);

        // Compared at mid before converting the cap back to tokenIn. The conversion overflows for
        // a vault worth more than 2^256 wei of tokenIn, which any order fits inside; past this
        // comparison the cap is below `amountIn`, so it fits a word. Rounded up, so an order
        // taken whole is worth at most `midCap` exactly, not just after flooring.
        fillableAmountIn = Math.mulDiv(amountIn, p.midNum, p.midDen, Math.Rounding.Ceil) <= midCap
            ? amountIn
            : Math.mulDiv(midCap, p.midDen, p.midNum);
        amountOut = _amountOut(p, tokenOut, fillableAmountIn);
    }

    /// @notice ERC-165. `PropAMMRouter` checks for `IPropAMMFillable` here before pricing this
    /// instance through `quoteFillable`. `IPropAMM` is reported too, for off-chain discovery.
    function supportsInterface(bytes4 interfaceId) public view override returns (bool) {
        return interfaceId == type(IPropAMMFillable).interfaceId || interfaceId == type(IPropAMM).interfaceId
            || super.supportsInterface(interfaceId);
    }

    /// @notice The vault a pair fills from.
    /// @dev Reverts `UnsupportedPair` for a pair the instance does not list. Off-chain readers
    /// use this, in either token order, instead of a per-instance vault getter; `pairVaults`
    /// answers the same by key without reverting.
    function vaultFor(address tokenX, address tokenY) external view returns (address vault) {
        (vault,) = _vaultFor(tokenX, tokenY);
    }

    /// @notice Puts two token addresses in canonical order, the order every pair is stored,
    /// keyed and reported in.
    function _sortTokens(address tokenX, address tokenY) internal pure returns (address token0, address token1) {
        return uint160(tokenX) < uint160(tokenY) ? (tokenX, tokenY) : (tokenY, tokenX);
    }

    /// @notice The key of an already-sorted pair: the registry lane, and the key of `pairVaults`.
    function _keyOf(address token0, address token1) internal pure returns (uint256) {
        return uint256(keccak256(abi.encodePacked(token0, token1)));
    }

    /// @notice Computes a unique key for a token pair, independent of the order of the tokens.
    /// @param tokenX The first token in the pair.
    /// @param tokenY The second token in the pair.
    /// @return A unique uint256 key representing the token pair.
    function _pairKey(address tokenX, address tokenY) internal pure returns (uint256) {
        (address token0, address token1) = _sortTokens(tokenX, tokenY);
        return _keyOf(token0, token1);
    }

    /// @notice Sorts, keys and checks that a pair is registered: the preamble every owner
    /// function that acts on an existing pair shares, so the `UnsupportedPair` convention (the
    /// caller's token order) is written once.
    function _registeredPair(address tokenX, address tokenY)
        internal
        view
        returns (address token0, address token1, uint256 pairKey)
    {
        (token0, token1) = _sortTokens(tokenX, tokenY);
        pairKey = _keyOf(token0, token1);
        require(pairVaults[pairKey] != address(0), UnsupportedPair(tokenX, tokenY));
    }

    /// @notice Resolves the vault a pair fills from, and the pair's key while at it.
    /// @dev Both quote and swap go through here. One storage read answers both questions,
    /// because a nonzero vault is what registration means. `UnsupportedPair` keeps the caller's
    /// token order, like every other revert that names the tokens a caller passed.
    function _vaultFor(address tokenX, address tokenY) internal view returns (address vault, uint256 pairKey) {
        pairKey = _pairKey(tokenX, tokenY);
        vault = pairVaults[pairKey];
        require(vault != address(0), UnsupportedPair(tokenX, tokenY));
    }

    /// @notice Quotes the amount of tokens out for a given amount of tokens in.
    /// @param tokenIn The token being sent.
    /// @param tokenOut The token being received.
    /// @param amountIn The amount of tokens being sent.
    /// @return amountOut The amount of tokens that will be received.
    function _quote(address tokenIn, address tokenOut, uint256 amountIn) internal view returns (uint256 amountOut) {
        return _amountOut(_pricing(tokenIn, tokenOut), tokenOut, amountIn);
    }

    /// @notice Reads what pricing a fill on a pair, in one direction, needs from outside this
    /// contract: the pair's vault and its tokenOut balance, the oracle's `delta`, and `mid`
    /// folded together with both tokens' decimals into one tokenIn-to-tokenOut conversion.
    /// @dev Kept apart from `_amountOut` so `quoteFillable` can price two sizes off one read.
    function _pricing(address tokenIn, address tokenOut) internal view returns (Pricing memory p) {
        uint256 pairKey;
        (p.vault, pairKey) = _vaultFor(tokenIn, tokenOut);

        (, uint256[] memory slots) = oracle.getState(pairKey, uint32(block.timestamp), uint32(block.timestamp));
        require(slots.length >= 2, InsufficientOracleData(pairKey, slots.length));

        p.delta = slots[0];
        uint256 mid = slots[1];
        require(mid != 0, InvalidMid(pairKey));

        uint256 decimalsMultiplierIn = 10 ** IERC20Metadata(tokenIn).decimals();
        uint256 decimalsMultiplierOut = 10 ** IERC20Metadata(tokenOut).decimals();

        p.vaultBalance = IERC20(tokenOut).balanceOf(p.vault);
        require(p.vaultBalance != 0, EmptyVault(tokenOut));

        // `mid` prices one whole token0 in whole token1, so selling token0 multiplies by it and
        // buying token0 divides by it.
        (p.midNum, p.midDen) = tokenIn < tokenOut
            ? (mid * decimalsMultiplierOut, PRICE_SCALE * decimalsMultiplierIn)
            : (PRICE_SCALE * decimalsMultiplierOut, mid * decimalsMultiplierIn);
    }

    /// @notice Prices `amountIn` against what `_pricing` read.
    function _amountOut(Pricing memory p, address tokenOut, uint256 amountIn)
        internal
        pure
        returns (uint256 amountOut)
    {
        // What the fill is worth at mid, before any spread.
        uint256 midAmountOut = Math.mulDiv(amountIn, p.midNum, p.midDen);

        // The taker pays the spread either way, as a fraction taken off the mid fill, so both
        // directions are discounted identically and a round trip costs exactly (1 - spread)^2.
        uint256 spread = p.delta + _priceImpact(midAmountOut, p.vaultBalance);
        require(spread < SPREAD_SCALE, SpreadTooWide(spread));

        // Rounded once: amountIn * midNum * (S - spread) / (midDen * S), floored, rather than
        // flooring `midAmountOut` and then the spread off it. Flooring twice loses up to a wei
        // whatever the fill's size, and a router that floors a partial leg at its pro-rata share
        // of this quote (`PropAMMRouter.swapSplitV1`) can then find the smaller leg a wei short.
        // Floored once, the output is a non-increasing rate times the size, and no smaller fill
        // falls under its share.
        //
        // Taken in two steps so no product past `amountIn * midNum` (which mulDiv holds in 512
        // bits) has to fit a word: multiplying `midNum` by the spread's scale first would overflow
        // for prices 1e18 times smaller than `_pricing` accepts. With amountIn * midNum =
        // midAmountOut * midDen + rem, the exact value is midAmountOut * c / S plus
        // rem * c / (midDen * S), and the second term, under c / S <= 1, adds one unit at most.
        uint256 c = SPREAD_SCALE - spread;
        amountOut = Math.mulDiv(midAmountOut, c, SPREAD_SCALE);
        uint256 rem = mulmod(amountIn, p.midNum, p.midDen);
        if (mulmod(midAmountOut, c, SPREAD_SCALE) + Math.mulDiv(rem, c, p.midDen) >= SPREAD_SCALE) amountOut += 1;

        require(amountOut <= p.vaultBalance, InsufficientVaultBalance(tokenOut, amountOut, p.vaultBalance));
    }

    /// @notice Prices the impact a fill has, on top of the oracle's spread.
    /// @dev Linear in the share of the vault's `tokenOut` balance the fill consumes: a fill that
    /// would take the whole balance is charged `IMPACT_FACTOR`, half the balance half of it.
    /// Both arguments are in `tokenOut` units, so the share is what carries the mid price and
    /// the impact itself comes out as a plain fraction.
    /// @param midAmountOut What the fill is worth at `mid`, in `tokenOut` units.
    /// @param vaultBalance The vault's `tokenOut` balance.
    /// @return impact The fraction to discount the fill by, at `SPREAD_SCALE`.
    function _priceImpact(uint256 midAmountOut, uint256 vaultBalance) internal pure returns (uint256 impact) {
        return Math.mulDiv(IMPACT_FACTOR, midAmountOut, vaultBalance);
    }

    /// @notice The most a fill may be worth at mid, in tokenOut wei, for it and every smaller fill
    /// to price at no more than `capacity`: the largest such worth, short of rounding.
    /// @dev Inverts `_amountOut`. With S = SPREAD_SCALE, B the vault balance and u a fill's exact,
    /// unrounded worth at mid (`amountIn * midNum / midDen`), `_amountOut` never exceeds the
    /// concave quadratic
    ///     f(u) = (a*u - IMPACT_FACTOR*u^2/B) / S,    a = S - delta + 1 + ceil(IMPACT_FACTOR/B)
    /// The extra terms in `a` cover `_priceImpact` flooring a floored `midAmountOut`, which can
    /// charge up to 1 + IMPACT_FACTOR/B units of spread less than the exact curve would. f rises
    /// to a peak at u = a*B / (2*IMPACT_FACTOR), and past it a larger fill pays less.
    ///
    /// When the peak is above `capacity`, the cap is the smaller root of f(u) = capacity, written as
    ///     u = 2*capacity*S / (a + sqrt(a^2 - 4*IMPACT_FACTOR*S*capacity/B))
    /// rather than the textbook (a - sqrt(...))*B / (2*IMPACT_FACTOR), which subtracts two nearly
    /// equal numbers and loses most of its precision doing it. When it is not (only for a `delta`
    /// within about 2% of S), no fill can overdraw, and the cap is the peak itself: the most the
    /// pair can pay. A cap past it would pay less than a smaller fill, and far enough past, take
    /// the spread over one.
    ///
    /// Either way the cap is on f's rising side, so every smaller fill is under `capacity` too:
    /// the router can trim the leg to whatever its better-ranked venues left over and it still
    /// fits. Every step rounds toward a smaller cap.
    function _midCap(Pricing memory p, uint256 capacity) internal pure returns (uint256) {
        require(p.delta < SPREAD_SCALE, SpreadTooWide(p.delta));
        uint256 a = SPREAD_SCALE - p.delta + 1 + Math.ceilDiv(IMPACT_FACTOR, p.vaultBalance);
        // 4*IMPACT_FACTOR*S*capacity/B, at most 4e32 since capacity <= B. Floored, which keeps the
        // peak test exact (a^2 is an integer) and only grows the root below.
        uint256 k = Math.mulDiv(4 * IMPACT_FACTOR * SPREAD_SCALE, capacity, p.vaultBalance);
        if (a * a <= k) return Math.mulDiv(a, p.vaultBalance, 2 * IMPACT_FACTOR);
        uint256 root = Math.sqrt(a * a - k, Math.Rounding.Ceil);
        return Math.mulDiv(capacity, 2 * SPREAD_SCALE, a + root);
    }

    //------------------------------
    // Management functions
    //------------------------------

    /// @notice Pauses the contract, preventing swaps from being executed.
    function pause() external onlyOwner {
        _pause();
    }

    /// @notice Unpauses the contract, allowing swaps to be executed.
    function unpause() external onlyOwner {
        _unpause();
    }

    /// @notice Adds a token pair, filled from `vault`.
    /// @param tokenX The first token in the pair.
    /// @param tokenY The second token in the pair.
    /// @param vault The account that holds this pair's inventory and has approved this instance.
    function addPair(address tokenX, address tokenY, address vault) external onlyOwner {
        _addPair(tokenX, tokenY, vault);
    }

    /// @notice Registers a pair, canonically ordered. Shared by `addPair` and `initialize`, so
    /// a pair listed at creation is stored exactly like one added later.
    function _addPair(address tokenX, address tokenY, address vault) internal {
        (address token0, address token1) = _sortTokens(tokenX, tokenY);
        require(vault != address(0), NoVault(token0, token1));

        uint256 pairKey = _keyOf(token0, token1);
        require(pairVaults[pairKey] == address(0), PairAlreadyExists(token0, token1));

        tokenPairs.push(TokenPair({token0: token0, token1: token1}));
        pairVaults[pairKey] = vault;
        emit PairAdded(token0, token1, vault);
    }

    /// @notice Moves a registered pair to another vault.
    /// @dev Fund the new vault and have it approve this instance for both tokens *before* calling
    /// this, or every swap on the pair reverts on the output leg until you do.
    function setPairVault(address tokenX, address tokenY, address vault) external onlyOwner {
        (address token0, address token1, uint256 pairKey) = _registeredPair(tokenX, tokenY);
        require(vault != address(0), NoVault(token0, token1));

        pairVaults[pairKey] = vault;
        emit PairVaultChanged(token0, token1, vault);
    }

    /// @notice Removes a token pair from the list of supported pairs, and forgets its vault.
    /// @dev Reverts for an unregistered pair rather than no-oping: it emits an event indexers act
    /// on, and an event for a removal that did not happen would be a lie.
    function removePair(address tokenX, address tokenY) external onlyOwner {
        (address token0, address token1, uint256 pairKey) = _registeredPair(tokenX, tokenY);

        delete pairVaults[pairKey];
        for (uint256 i = 0; i < tokenPairs.length; i++) {
            if (tokenPairs[i].token0 == token0 && tokenPairs[i].token1 == token1) {
                tokenPairs[i] = tokenPairs[tokenPairs.length - 1];
                tokenPairs.pop();
                break;
            }
        }
        emit PairRemoved(token0, token1);
    }

    /// @notice Updates the oracle address used for price updates.
    /// @dev Note that this does not transfer any updaters from the old oracle to the new one; the new oracle must be set up separately.
    /// @param newOracle The new oracle address.
    function setOracle(address newOracle) external onlyOwner {
        oracle = IPrioUpdateRegistry(newOracle);
        emit OracleChanged(newOracle);
    }

    /// @notice Adds a new updater to the oracle registry.
    /// @param updater The address of the new updater.
    function addUpdater(address updater) external onlyOwner {
        oracle.addUpdater(updater);
        emit UpdaterAdded(updater);
    }

    /// @notice Removes an updater from the oracle registry.
    /// @param updater The address of the updater to remove.
    function removeUpdater(address updater) external onlyOwner {
        oracle.removeUpdater(updater);
        emit UpdaterRemoved(updater);
    }

    /// @notice Withdraws ERC20 tokens from the contract to a specified address.
    /// @dev This function allows the owner to withdraw any ERC20 tokens that may have been sent to the contract by mistake or for other reasons.
    /// @param token The address of the ERC20 token to withdraw.
    /// @param to The address to send the withdrawn tokens to.
    /// @param amount The amount of tokens to withdraw.
    function withdrawERC20(address token, address to, uint256 amount) external onlyOwner {
        IERC20(token).safeTransfer(to, amount);
        emit ERC20Withdrawn(token, to, amount);
    }

    function _authorizeUpgrade(address newImplementation) internal override onlyOwner {}
}
