// SPDX-License-Identifier: MIT
pragma solidity ^0.8.35;

import {Test} from "forge-std/Test.sol";
import {ERC1967Proxy} from "@openzeppelin/contracts/proxy/ERC1967/ERC1967Proxy.sol";
import {PropAMM, NoVault, PairAlreadyExists, UnsupportedPair} from "../src/PropAMM.sol";
import {IPropAMM} from "../src/interfaces/IPropAMM.sol";
import {MockPUR} from "./mocks/MockPUR.sol";
import {MockERC20} from "./mocks/MockERC20.sol";

contract PropAMMTest is Test {
    address constant USDC = 0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48;
    address constant USDT = 0xdAC17F958D2ee523a2206206994597C13D831ec7;
    // The PUR oracle address these tests initialize against. `initialize` calls it straight
    // away, so tests need code there.
    address constant ORACLE = 0xDa7AfEeD021EAFC1c1Af9C362dE477DaD0396B81;
    /// keccak256(token0 ++ token1) over the sorted pair: the registry lane, and the key of
    /// `pairVaults`.
    uint256 constant USDC_USDT_KEY = uint256(keccak256(abi.encodePacked(USDC, USDT)));

    address owner = makeAddr("owner");
    address vault = makeAddr("vault");
    address otherVault = makeAddr("other-vault");
    address updater = makeAddr("updater");
    PropAMM propAmm;

    function setUp() public {
        // Same trick as `make local`'s anvil_setCode: a bare test chain has no code at the
        // PUR address under test, so initialize's addUpdater calls would go nowhere.
        vm.etch(ORACLE, address(new MockPUR()).code);

        address[] memory updaters = new address[](1);
        updaters[0] = updater;
        propAmm = _deploy(updaters, new PropAMM.PairConfig[](0));
    }

    //------------------------------
    // initialize
    //------------------------------

    function test_initialize_registersUpdatersWithTheOracle() public view {
        assertTrue(MockPUR(ORACLE).isUpdater(updater));
        // Only the addresses passed in, nothing else.
        assertFalse(MockPUR(ORACLE).isUpdater(owner));
    }

    function test_initialize_listsThePairsItIsGivenWithTheirVaults() public {
        // The implementation is deployed before the expectation is set: a CREATE between
        // expectEmit and the call it guards would consume the expectation itself.
        address implementation = address(new PropAMM());
        // Reversed on purpose: initialize canonicalizes exactly like addPair does, and the event
        // carries the canonical order. The emitter is the proxy, whose address is not known until
        // it exists, so the expectation is not pinned to an address.
        vm.expectEmit(true, true, true, true);
        emit PropAMM.PairAdded(USDC, USDT, vault);
        PropAMM instance = _proxy(implementation, new address[](0), _one(USDT, USDC, vault));

        IPropAMM.TokenPair[] memory pairs = instance.getPairs();
        assertEq(pairs.length, 1);
        assertEq(pairs[0].token0, USDC);
        assertEq(pairs[0].token1, USDT);
        assertEq(instance.vaultFor(USDT, USDC), vault);
        assertEq(instance.vaultFor(USDC, USDT), vault);
        assertEq(instance.pairVaults(USDC_USDT_KEY), vault);
    }

    function test_initialize_refusesAPairWithNoVault() public {
        // Same as above: the implementation's CREATE must not be the call expectRevert guards.
        address implementation = address(new PropAMM());
        vm.expectRevert(abi.encodeWithSelector(NoVault.selector, USDC, USDT));
        _proxy(implementation, new address[](0), _one(USDC, USDT, address(0)));
    }

    /// The registry is no longer baked into the implementation, so `initialize` has to record
    /// the one it was handed: everything else on the contract reads prices through it.
    function test_initialize_storesTheOracleItIsGiven() public view {
        assertEq(address(propAmm.oracle()), ORACLE);
    }

    /// `initialize` runs `__Pausable_init`, and an instance upgraded onto this implementation
    /// without re-running it lands on the same default: open for business.
    function test_initialize_startsUnpaused() public view {
        assertFalse(propAmm.paused());
    }

    //------------------------------
    // pairs
    //------------------------------

    function test_getPairs_isEmptyUntilAPairIsAdded() public view {
        assertEq(propAmm.getPairs().length, 0);
    }

    function test_addPair_storesTheCanonicalPairAndItsVault() public {
        vm.expectEmit(true, true, true, true, address(propAmm));
        emit PropAMM.PairAdded(USDC, USDT, vault);
        vm.prank(owner);
        propAmm.addPair(USDT, USDC, vault);

        IPropAMM.TokenPair[] memory pairs = propAmm.getPairs();
        assertEq(pairs.length, 1);
        // Stored canonically ordered, whatever order they were passed in.
        assertEq(pairs[0].token0, USDC);
        assertEq(pairs[0].token1, USDT);
        assertEq(propAmm.pairVaults(USDC_USDT_KEY), vault);
        assertEq(propAmm.vaultFor(USDC, USDT), vault);
    }

    function test_addPair_refusesAZeroVault() public {
        vm.prank(owner);
        vm.expectRevert(abi.encodeWithSelector(NoVault.selector, USDC, USDT));
        propAmm.addPair(USDC, USDT, address(0));
    }

    function test_addPair_refusesADuplicateInEitherOrder() public {
        vm.startPrank(owner);
        propAmm.addPair(USDC, USDT, vault);
        vm.expectRevert(abi.encodeWithSelector(PairAlreadyExists.selector, USDC, USDT));
        propAmm.addPair(USDT, USDC, otherVault);
        vm.stopPrank();
    }

    //------------------------------
    // setPairVault
    //------------------------------

    function test_setPairVault_movesThePairToAnotherVault() public {
        vm.startPrank(owner);
        propAmm.addPair(USDC, USDT, vault);

        vm.expectEmit(true, true, true, true, address(propAmm));
        emit PropAMM.PairVaultChanged(USDC, USDT, otherVault);
        propAmm.setPairVault(USDT, USDC, otherVault);
        vm.stopPrank();

        assertEq(propAmm.vaultFor(USDC, USDT), otherVault);
        assertEq(propAmm.pairVaults(USDC_USDT_KEY), otherVault);
        // The listing itself is untouched: same pair, new account behind it.
        assertEq(propAmm.getPairs().length, 1);
    }

    function test_setPairVault_refusesAnUnregisteredPair() public {
        vm.prank(owner);
        vm.expectRevert(abi.encodeWithSelector(UnsupportedPair.selector, USDC, USDT));
        propAmm.setPairVault(USDC, USDT, vault);
    }

    function test_setPairVault_refusesAZeroVault() public {
        vm.startPrank(owner);
        propAmm.addPair(USDC, USDT, vault);
        vm.expectRevert(abi.encodeWithSelector(NoVault.selector, USDC, USDT));
        propAmm.setPairVault(USDC, USDT, address(0));
        vm.stopPrank();
    }

    //------------------------------
    // removePair
    //------------------------------

    function test_removePair_clearsTheVaultAndEmits() public {
        vm.startPrank(owner);
        propAmm.addPair(USDC, USDT, vault);

        vm.expectEmit(true, true, false, true, address(propAmm));
        emit PropAMM.PairRemoved(USDC, USDT);
        propAmm.removePair(USDT, USDC);
        vm.stopPrank();

        assertEq(propAmm.getPairs().length, 0);
        // Cleared, not left behind: the zero vault is what unregisters the pair, and one re-added
        // later must name its vault again rather than silently inherit this one.
        assertEq(propAmm.pairVaults(USDC_USDT_KEY), address(0));
        vm.expectRevert(abi.encodeWithSelector(UnsupportedPair.selector, USDC, USDT));
        propAmm.vaultFor(USDC, USDT);
    }

    function test_removePair_refusesAnUnregisteredPair() public {
        vm.prank(owner);
        vm.expectRevert(abi.encodeWithSelector(UnsupportedPair.selector, USDC, USDT));
        propAmm.removePair(USDC, USDT);
    }

    //------------------------------
    // access
    //------------------------------

    function test_nonOwnerCannotManageVaults() public {
        vm.prank(owner);
        propAmm.addPair(USDC, USDT, vault);

        address rando = makeAddr("rando");
        vm.startPrank(rando);
        vm.expectRevert();
        propAmm.addPair(makeAddr("token-a"), makeAddr("token-b"), vault);
        vm.expectRevert();
        propAmm.setPairVault(USDC, USDT, rando);
        vm.expectRevert();
        propAmm.removePair(USDC, USDT);
        vm.stopPrank();
    }

    //------------------------------
    // setOracle, addUpdater, removeUpdater, withdrawERC20
    //------------------------------

    function test_setOracle_storesTheNewOracleAndEmits() public {
        address newOracle = makeAddr("new-oracle");
        vm.expectEmit(true, false, false, true, address(propAmm));
        emit PropAMM.OracleChanged(newOracle);
        vm.prank(owner);
        propAmm.setOracle(newOracle);
        assertEq(address(propAmm.oracle()), newOracle);
    }

    function test_addUpdater_registersWithTheOracleAndEmits() public {
        address another = makeAddr("another-updater");
        vm.expectEmit(true, false, false, true, address(propAmm));
        emit PropAMM.UpdaterAdded(another);
        vm.prank(owner);
        propAmm.addUpdater(another);
        assertTrue(MockPUR(ORACLE).isUpdater(another));
    }

    function test_removeUpdater_unregistersFromTheOracleAndEmits() public {
        vm.expectEmit(true, false, false, true, address(propAmm));
        emit PropAMM.UpdaterRemoved(updater);
        vm.prank(owner);
        propAmm.removeUpdater(updater);
        assertFalse(MockPUR(ORACLE).isUpdater(updater));
    }

    function test_withdrawERC20_movesTheTokensAndEmits() public {
        MockERC20 token = new MockERC20();
        token.mint(address(propAmm), 1_000);
        address to = makeAddr("recipient");

        vm.expectEmit(true, true, false, true, address(propAmm));
        emit PropAMM.ERC20Withdrawn(address(token), to, 400);
        vm.prank(owner);
        propAmm.withdrawERC20(address(token), to, 400);
        assertEq(token.balanceOf(to), 400);
        assertEq(token.balanceOf(address(propAmm)), 600);
    }

    function test_nonOwnerCannotManageOracleUpdatersOrFunds() public {
        address rando = makeAddr("rando");
        vm.startPrank(rando);
        vm.expectRevert();
        propAmm.setOracle(rando);
        vm.expectRevert();
        propAmm.addUpdater(rando);
        vm.expectRevert();
        propAmm.removeUpdater(updater);
        vm.expectRevert();
        propAmm.withdrawERC20(USDC, rando, 1);
        vm.stopPrank();
    }

    //------------------------------
    // Helpers
    //------------------------------

    function _deploy(address[] memory updaters, PropAMM.PairConfig[] memory pairs) internal returns (PropAMM) {
        return _proxy(address(new PropAMM()), updaters, pairs);
    }

    /// PropAMM is UUPS: the implementation holds no state, all calls go through the proxy. Split
    /// from `_deploy` so a test can put the implementation's CREATE before a cheatcode expectation
    /// and leave the proxy's CREATE, which runs initialize, as the call the expectation guards.
    function _proxy(address implementation, address[] memory updaters, PropAMM.PairConfig[] memory pairs)
        internal
        returns (PropAMM)
    {
        return PropAMM(
            address(
                new ERC1967Proxy(implementation, abi.encodeCall(PropAMM.initialize, (owner, updaters, ORACLE, pairs)))
            )
        );
    }

    function _one(address tokenX, address tokenY, address vault_)
        internal
        pure
        returns (PropAMM.PairConfig[] memory pairs)
    {
        pairs = new PropAMM.PairConfig[](1);
        pairs[0] = PropAMM.PairConfig({token0: tokenX, token1: tokenY, vault: vault_});
    }
}
