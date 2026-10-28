// SPDX-License-Identifier: MIT
pragma solidity ^0.8.35;

import {Test} from "forge-std/Test.sol";
import {Ownable} from "@openzeppelin/contracts/access/Ownable.sol";
import {PropAMM} from "../src/PropAMM.sol";
import {CreationIsDisabled, PropAMMFactory} from "../src/PropAMMFactory.sol";
import {IPropAMM} from "../src/interfaces/IPropAMM.sol";

/// Recording stand-in for the registry: creating an instance calls addUpdater on the registry
/// address it is created against, which holds no code in the test environment, so setUp etches this
/// contract's code there. It records authorizations per target, exactly like the real registry,
/// which is what makes it possible to check each instance authorized its own updaters.
contract RegistryStub {
    mapping(address target => mapping(address updater => bool)) public isUpdater;

    function addUpdater(address updater) external {
        isUpdater[msg.sender][updater] = true;
    }

    function removeUpdater(address updater) external {
        isUpdater[msg.sender][updater] = false;
    }
}

contract PropAMMFactoryTest is Test {
    address constant PUR_ADDR = 0xDa7AfEeD021EAFC1c1Af9C362dE477DaD0396B81;
    /// A second registry, so tests can tell a per-instance oracle from a shared one. Any address
    /// with registry code behind it will do; this one is deliberately not the mainnet PUR.
    address constant OTHER_PUR_ADDR = 0x00000000000000000000000000000000000000E2;
    address constant USDC = 0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48;
    address constant USDT = 0xdAC17F958D2ee523a2206206994597C13D831ec7;

    event PropAMMCreated(address indexed propAmm, address indexed creator, address oracle, address[] updaters);

    /// The owner every test creates with: the test contract itself, so it can call the instance's
    /// owner-only functions without pranking. Creating one for someone else, which is what the
    /// parameter is for, is covered by test_createPropAMM_ownerIsTheParamNotTheCreator.
    address owner = address(this);
    /// The factory's own owner, the one account that can disable creation. Deliberately not
    /// `owner`, so tests can tell the two apart.
    address factoryOwner = makeAddr("factory-owner");
    address vault = makeAddr("vault");
    address updater = makeAddr("updater");
    address secondUpdater = makeAddr("second-updater");
    PropAMMFactory factory;
    /// The updater set every test creates with, more than one so the plural path is the default.
    address[] updaters;
    PropAMM.PairConfig[] pairs;

    function setUp() public {
        vm.etch(PUR_ADDR, address(new RegistryStub()).code);
        vm.etch(OTHER_PUR_ADDR, address(new RegistryStub()).code);
        factory = new PropAMMFactory(factoryOwner);
        updaters.push(updater);
        updaters.push(secondUpdater);
        pairs.push(PropAMM.PairConfig({token0: USDC, token1: USDT, vault: vault}));
    }

    function test_createPropAMM_deploysAtComputedAddress() public {
        address predicted = factory.computeAddress(address(this), owner, PUR_ADDR, updaters, pairs);
        address propAmm = factory.createPropAMM(owner, PUR_ADDR, updaters, pairs);
        assertEq(propAmm, predicted);
        assertGt(propAmm.code.length, 0);
    }

    function test_createPropAMM_registersInstance() public {
        address propAmm = factory.createPropAMM(owner, PUR_ADDR, updaters, pairs);
        assertEq(factory.allPropAMMsLength(), 1);
        assertEq(factory.allPropAMMs(0), propAmm);
        address[] memory mine = factory.propAMMsOf(address(this));
        assertEq(mine.length, 1);
        assertEq(mine[0], propAmm);
    }

    function test_createPropAMM_setsPairsAndVault() public {
        PropAMM propAmm = PropAMM(factory.createPropAMM(owner, PUR_ADDR, updaters, pairs));
        IPropAMM.TokenPair[] memory got = propAmm.getPairs();
        assertEq(got.length, 1);
        assertEq(got[0].token0, USDC);
        assertEq(got[0].token1, USDT);
        assertEq(propAmm.vaultFor(USDC, USDT), vault);
    }

    function test_createPropAMM_authorizesEveryUpdaterOnRegistry() public {
        address propAmm = factory.createPropAMM(owner, PUR_ADDR, updaters, pairs);
        // Authorized for the instance itself, not the factory: under a proxy, initialize runs
        // with the proxy as msg.sender, which is the target whose lanes the updaters may write.
        assertTrue(RegistryStub(PUR_ADDR).isUpdater(propAmm, updater));
        assertTrue(RegistryStub(PUR_ADDR).isUpdater(propAmm, secondUpdater));
        assertFalse(RegistryStub(PUR_ADDR).isUpdater(address(factory), updater));
        assertFalse(RegistryStub(PUR_ADDR).isUpdater(address(factory), secondUpdater));
        // Only the addresses passed in, nothing else.
        assertFalse(RegistryStub(PUR_ADDR).isUpdater(propAmm, address(this)));
    }

    /// The oracle is per-instance, not per-factory: each instance stores the registry it was
    /// created against and authorizes its updaters there, leaving the other registry untouched.
    function test_createPropAMM_authorizesUpdatersOnItsOwnOracle() public {
        address propAmm = factory.createPropAMM(owner, PUR_ADDR, updaters, pairs);
        address other = factory.createPropAMM(owner, OTHER_PUR_ADDR, updaters, pairs);

        assertEq(address(PropAMM(propAmm).oracle()), PUR_ADDR);
        assertEq(address(PropAMM(other).oracle()), OTHER_PUR_ADDR);

        assertTrue(RegistryStub(PUR_ADDR).isUpdater(propAmm, updater));
        assertTrue(RegistryStub(OTHER_PUR_ADDR).isUpdater(other, updater));
        // Neither instance registered itself on the registry it was not created against.
        assertFalse(RegistryStub(OTHER_PUR_ADDR).isUpdater(propAmm, updater));
        assertFalse(RegistryStub(PUR_ADDR).isUpdater(other, updater));
    }

    /// The same creator can run one instance per registry: the oracle reaches the CREATE2 address
    /// through both the salt and the initialize calldata, so instances that differ only by it land
    /// apart and computeAddress still predicts where. (Either route alone would separate them, so
    /// no test from outside can tell which one is doing the work.)
    function test_createPropAMM_differentOraclesGetDifferentAddresses() public {
        address propAmm = factory.createPropAMM(owner, PUR_ADDR, updaters, pairs);
        address other = factory.createPropAMM(owner, OTHER_PUR_ADDR, updaters, pairs);
        assertTrue(propAmm != other);
        assertEq(factory.computeAddress(address(this), owner, OTHER_PUR_ADDR, updaters, pairs), other);
    }

    /// An instance may start out with nobody allowed to push updates; the owner can add updaters
    /// later, so an empty set is a valid way to create one.
    function test_createPropAMM_acceptsNoUpdaters() public {
        address[] memory none = new address[](0);
        address propAmm = factory.createPropAMM(owner, PUR_ADDR, none, pairs);
        assertGt(propAmm.code.length, 0);
        assertFalse(RegistryStub(PUR_ADDR).isUpdater(propAmm, updater));
    }

    /// Two events per creation, in this order: the instance's own PairAdded (emitted inside
    /// initialize, so from the predicted address, before the proxy constructor returns), then the
    /// factory's PropAMMCreated. The creation event names neither pairs nor vaults; PairAdded is
    /// where indexers read them.
    function test_createPropAMM_emitsPairAddedThenCreated() public {
        address predicted = factory.computeAddress(address(this), owner, PUR_ADDR, updaters, pairs);
        vm.expectEmit(true, true, true, true, predicted);
        emit PropAMM.PairAdded(USDC, USDT, vault);
        vm.expectEmit(true, true, false, true, address(factory));
        emit PropAMMCreated(predicted, address(this), PUR_ADDR, updaters);
        factory.createPropAMM(owner, PUR_ADDR, updaters, pairs);
    }

    function test_createPropAMM_revertsOnDuplicate() public {
        factory.createPropAMM(owner, PUR_ADDR, updaters, pairs);
        vm.expectRevert();
        factory.createPropAMM(owner, PUR_ADDR, updaters, pairs);
    }

    function test_createPropAMM_differentCreatorsGetDifferentAddresses() public {
        address propAmm = factory.createPropAMM(owner, PUR_ADDR, updaters, pairs);
        vm.prank(makeAddr("someone-else"));
        address other = factory.createPropAMM(owner, PUR_ADDR, updaters, pairs);
        assertTrue(propAmm != other);
    }

    /// The salt commits to the updater set, so the same creator can deploy one instance per set,
    /// and the order they list them in is part of that set.
    function test_createPropAMM_differentUpdaterSetsGetDifferentAddresses() public {
        address propAmm = factory.createPropAMM(owner, PUR_ADDR, updaters, pairs);

        address[] memory justOne = new address[](1);
        justOne[0] = updater;
        address fewer = factory.createPropAMM(owner, PUR_ADDR, justOne, pairs);

        address[] memory reversed = new address[](2);
        reversed[0] = secondUpdater;
        reversed[1] = updater;
        address swapped = factory.createPropAMM(owner, PUR_ADDR, reversed, pairs);

        assertTrue(propAmm != fewer);
        assertTrue(propAmm != swapped);
        assertTrue(fewer != swapped);
    }

    /// Every instance is a proxy over the factory's single implementation, but keeps its own
    /// state, so one instance's pairs are invisible to another.
    function test_instancesShareTheImplementationButNotState() public {
        PropAMM first = PropAMM(factory.createPropAMM(owner, PUR_ADDR, updaters, pairs));
        vm.prank(makeAddr("someone-else"));
        PropAMM second = PropAMM(factory.createPropAMM(owner, PUR_ADDR, updaters, pairs));

        assertGt(address(factory.implementation()).code.length, 0);
        assertEq(first.getPairs().length, 1);

        vm.prank(address(this));
        first.addPair(makeAddr("token-a"), makeAddr("token-b"), vault);
        assertEq(first.getPairs().length, 2);
        assertEq(second.getPairs().length, 1);
    }

    /// Ownership comes from the parameter, not the caller: one account can create an instance for
    /// another, and keeps no rights over what it deployed.
    function test_createPropAMM_ownerIsTheParamNotTheCreator() public {
        address theirOwner = makeAddr("their-owner");
        address tokenA = makeAddr("token-a");
        address tokenB = makeAddr("token-b");
        PropAMM propAmm = PropAMM(factory.createPropAMM(theirOwner, PUR_ADDR, updaters, pairs));

        assertEq(propAmm.owner(), theirOwner);

        // The creator is not the owner, so the owner-only functions are closed to it.
        vm.expectRevert();
        propAmm.addPair(tokenA, tokenB, vault);

        // The factory's index is keyed on whoever created the instance, not on its owner.
        address[] memory created = factory.propAMMsOf(address(this));
        assertEq(created.length, 1);
        assertEq(created[0], address(propAmm));
        assertEq(factory.propAMMsOf(theirOwner).length, 0);
    }

    /// The salt commits to the owner too, so one creator can deploy an instance per owner with
    /// otherwise identical parameters.
    function test_createPropAMM_differentOwnersGetDifferentAddresses() public {
        address propAmm = factory.createPropAMM(owner, PUR_ADDR, updaters, pairs);
        address other = factory.createPropAMM(makeAddr("another-owner"), PUR_ADDR, updaters, pairs);
        assertTrue(propAmm != other);
    }

    /// The salt commits to each pair's vault, so the same pairs behind a different account are a
    /// different instance, and computeAddress predicts each correctly.
    function test_createPropAMM_differentVaultsGetDifferentAddresses() public {
        address otherVault = makeAddr("other-vault");
        PropAMM.PairConfig[] memory elsewhere = new PropAMM.PairConfig[](1);
        elsewhere[0] = PropAMM.PairConfig({token0: USDC, token1: USDT, vault: otherVault});

        address predictedHere = factory.computeAddress(address(this), owner, PUR_ADDR, updaters, pairs);
        address predictedThere = factory.computeAddress(address(this), owner, PUR_ADDR, updaters, elsewhere);
        assertTrue(predictedHere != predictedThere);

        address here = factory.createPropAMM(owner, PUR_ADDR, updaters, pairs);
        address there = factory.createPropAMM(owner, PUR_ADDR, updaters, elsewhere);
        assertEq(here, predictedHere);
        assertEq(there, predictedThere);
        assertEq(PropAMM(here).vaultFor(USDC, USDT), vault);
        assertEq(PropAMM(there).vaultFor(USDC, USDT), otherVault);
    }

    /// Several pairs at creation, each on its own vault, and one pair may reuse another's.
    function test_createPropAMM_listsEachPairWithItsOwnVault() public {
        address wbtc = 0x2260FAC5E5542a773Aa44fBCfeDf7C193bc2C599;
        address weth = 0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2;
        address vaultB = makeAddr("vault-b");
        PropAMM.PairConfig[] memory three = new PropAMM.PairConfig[](3);
        three[0] = PropAMM.PairConfig({token0: USDC, token1: USDT, vault: vault});
        three[1] = PropAMM.PairConfig({token0: weth, token1: USDC, vault: vaultB});
        three[2] = PropAMM.PairConfig({token0: wbtc, token1: USDC, vault: vaultB});

        PropAMM propAmm = PropAMM(factory.createPropAMM(owner, PUR_ADDR, updaters, three));
        assertEq(propAmm.getPairs().length, 3);
        assertEq(propAmm.vaultFor(USDC, USDT), vault);
        assertEq(propAmm.vaultFor(weth, USDC), vaultB);
        assertEq(propAmm.vaultFor(wbtc, USDC), vaultB);
    }

    function test_ownerCanManageUpdaters() public {
        PropAMM propAmm = PropAMM(factory.createPropAMM(owner, PUR_ADDR, updaters, pairs));
        address newUpdater = makeAddr("new-updater");
        propAmm.addUpdater(newUpdater);
        assertTrue(RegistryStub(PUR_ADDR).isUpdater(address(propAmm), newUpdater));
        propAmm.removeUpdater(newUpdater);
        assertFalse(RegistryStub(PUR_ADDR).isUpdater(address(propAmm), newUpdater));
    }

    function test_ownerCanManagePairs() public {
        PropAMM propAmm = PropAMM(factory.createPropAMM(owner, PUR_ADDR, updaters, pairs));
        address tokenA = makeAddr("token-a");
        address tokenB = makeAddr("token-b");
        propAmm.addPair(tokenA, tokenB, vault);
        assertEq(propAmm.getPairs().length, 2);
        propAmm.removePair(USDC, USDT);
        IPropAMM.TokenPair[] memory got = propAmm.getPairs();
        assertEq(got.length, 1);
        (address token0, address token1) = uint160(tokenA) < uint160(tokenB) ? (tokenA, tokenB) : (tokenB, tokenA);
        assertEq(got[0].token0, token0);
        assertEq(got[0].token1, token1);
    }

    function test_ownerCanUpgradeTheirInstance() public {
        PropAMM propAmm = PropAMM(factory.createPropAMM(owner, PUR_ADDR, updaters, pairs));
        address newImplementation = address(new PropAMM());

        propAmm.upgradeToAndCall(newImplementation, "");

        // State survives, and the instance is no longer on the factory's implementation.
        assertEq(propAmm.getPairs().length, 1);
        assertEq(propAmm.owner(), address(this));
        assertTrue(newImplementation != address(factory.implementation()));
    }

    function test_nonOwnerCannotManage() public {
        PropAMM propAmm = PropAMM(factory.createPropAMM(owner, PUR_ADDR, updaters, pairs));
        address rando = makeAddr("rando");
        // Deployed up front: a CREATE between expectRevert and the call it guards would consume
        // the expectation itself.
        address newImplementation = address(new PropAMM());

        vm.startPrank(rando);
        vm.expectRevert();
        propAmm.addUpdater(rando);
        vm.expectRevert();
        propAmm.removeUpdater(updater);
        vm.expectRevert();
        propAmm.addPair(makeAddr("token-a"), makeAddr("token-b"), vault);
        vm.expectRevert();
        propAmm.removePair(USDC, USDT);
        vm.expectRevert();
        propAmm.setPairVault(USDC, USDT, rando);
        vm.expectRevert();
        propAmm.upgradeToAndCall(newImplementation, "");
        vm.expectRevert();
        propAmm.setOracle(OTHER_PUR_ADDR);
        vm.expectRevert();
        propAmm.pause();
        vm.expectRevert();
        propAmm.unpause();
        vm.expectRevert();
        propAmm.withdrawERC20(USDC, rando, 1);
        vm.expectRevert();
        propAmm.transferOwnership(rando);
        vm.stopPrank();
    }

    /// An instance is set up once, by the factory, and nobody can set it up again: not with
    /// `initialize`, and not with a migration step. An instance pointed at a price source of a
    /// caller's choosing fills at whatever price that source names, out of vaults that have
    /// approved it, so the oracle must stay the owner's to change, through `setOracle`.
    function test_nobodyCanReinitializeAFactoryInstance() public {
        PropAMM propAmm = PropAMM(factory.createPropAMM(owner, PUR_ADDR, updaters, pairs));
        address rando = makeAddr("rando");
        address evilOracle = makeAddr("evil-oracle");

        vm.startPrank(rando);
        vm.expectRevert();
        propAmm.initialize(rando, new address[](0), evilOracle, new PropAMM.PairConfig[](0));
        // The migration step `reinitializeV2(address)` is gone: no function answers its selector.
        (bool ok,) = address(propAmm).call(abi.encodeWithSignature("reinitializeV2(address)", evilOracle));
        assertFalse(ok);
        vm.stopPrank();

        assertEq(address(propAmm.oracle()), PUR_ADDR);
        assertEq(propAmm.owner(), owner);
    }

    function test_constructor_setsFactoryOwnerWithCreationOpen() public view {
        assertEq(factory.owner(), factoryOwner);
        assertFalse(factory.creationDisabled());
    }

    function test_constructor_revertsOnZeroOwner() public {
        vm.expectRevert(abi.encodeWithSelector(Ownable.OwnableInvalidOwner.selector, address(0)));
        new PropAMMFactory(address(0));
    }

    /// The factory's owner can stop creation, and has no say over the instances themselves.
    function test_factoryOwnerCannotManageInstances() public {
        PropAMM propAmm = PropAMM(factory.createPropAMM(owner, PUR_ADDR, updaters, pairs));
        vm.prank(factoryOwner);
        vm.expectRevert(abi.encodeWithSelector(Ownable.OwnableUnauthorizedAccount.selector, factoryOwner));
        propAmm.addPair(makeAddr("token-a"), makeAddr("token-b"), vault);
    }

    function test_disableCreation_emitsAndStopsCreation() public {
        vm.expectEmit(true, true, true, true, address(factory));
        emit PropAMMFactory.CreationDisabled();
        vm.prank(factoryOwner);
        factory.disableCreation();

        assertTrue(factory.creationDisabled());
        vm.expectRevert(CreationIsDisabled.selector);
        factory.createPropAMM(owner, PUR_ADDR, updaters, pairs);
        assertEq(factory.allPropAMMsLength(), 0);
    }

    /// Only the factory's owner: not an outsider, and not the owner of an instance it created.
    function test_disableCreation_onlyFactoryOwner() public {
        factory.createPropAMM(owner, PUR_ADDR, updaters, pairs);
        address rando = makeAddr("rando");

        vm.prank(rando);
        vm.expectRevert(abi.encodeWithSelector(Ownable.OwnableUnauthorizedAccount.selector, rando));
        factory.disableCreation();
        vm.expectRevert(abi.encodeWithSelector(Ownable.OwnableUnauthorizedAccount.selector, owner));
        factory.disableCreation();

        assertFalse(factory.creationDisabled());
        factory.createPropAMM(makeAddr("another-owner"), PUR_ADDR, updaters, pairs);
        assertEq(factory.allPropAMMsLength(), 2);
    }

    /// Disabling is one-way: calling it again reverts rather than emitting a second event, and a
    /// new owner inherits a factory that stays disabled, with no way to turn creation back on.
    function test_disableCreation_isIrrevocable() public {
        vm.prank(factoryOwner);
        factory.disableCreation();

        vm.prank(factoryOwner);
        vm.expectRevert(CreationIsDisabled.selector);
        factory.disableCreation();

        address newOwner = makeAddr("new-factory-owner");
        vm.prank(factoryOwner);
        factory.transferOwnership(newOwner);
        vm.prank(newOwner);
        factory.acceptOwnership();

        assertEq(factory.owner(), newOwner);
        assertTrue(factory.creationDisabled());
        vm.prank(newOwner);
        vm.expectRevert(CreationIsDisabled.selector);
        factory.createPropAMM(owner, PUR_ADDR, updaters, pairs);
    }

    /// Disabling stops new instances only. Those already created stay listed and stay their
    /// owners' to manage and upgrade, and computeAddress still names where they live.
    function test_disableCreation_leavesExistingInstancesAlone() public {
        PropAMM propAmm = PropAMM(factory.createPropAMM(owner, PUR_ADDR, updaters, pairs));
        address newImplementation = address(new PropAMM());

        vm.prank(factoryOwner);
        factory.disableCreation();

        assertEq(factory.allPropAMMsLength(), 1);
        assertEq(factory.allPropAMMs(0), address(propAmm));
        assertEq(factory.propAMMsOf(address(this))[0], address(propAmm));
        assertEq(factory.computeAddress(address(this), owner, PUR_ADDR, updaters, pairs), address(propAmm));

        propAmm.addPair(makeAddr("token-a"), makeAddr("token-b"), vault);
        assertEq(propAmm.getPairs().length, 2);
        propAmm.upgradeToAndCall(newImplementation, "");
        assertEq(propAmm.owner(), owner);
    }

    /// Ownership moves in two steps: the account named holds no power until it accepts.
    function test_transferOwnership_isTwoStep() public {
        address newOwner = makeAddr("new-factory-owner");
        vm.prank(factoryOwner);
        factory.transferOwnership(newOwner);

        assertEq(factory.owner(), factoryOwner);
        assertEq(factory.pendingOwner(), newOwner);
        vm.prank(newOwner);
        vm.expectRevert(abi.encodeWithSelector(Ownable.OwnableUnauthorizedAccount.selector, newOwner));
        factory.disableCreation();

        vm.prank(newOwner);
        factory.acceptOwnership();
        assertEq(factory.owner(), newOwner);

        vm.prank(factoryOwner);
        vm.expectRevert(abi.encodeWithSelector(Ownable.OwnableUnauthorizedAccount.selector, factoryOwner));
        factory.disableCreation();
        vm.prank(newOwner);
        factory.disableCreation();
        assertTrue(factory.creationDisabled());
    }

    /// Renouncing ownership is the opposite commitment: nobody can disable creation afterwards,
    /// so the factory stays open for good.
    function test_renounceOwnership_leavesCreationOpenForGood() public {
        vm.prank(factoryOwner);
        factory.renounceOwnership();

        assertEq(factory.owner(), address(0));
        vm.prank(factoryOwner);
        vm.expectRevert(abi.encodeWithSelector(Ownable.OwnableUnauthorizedAccount.selector, factoryOwner));
        factory.disableCreation();

        assertFalse(factory.creationDisabled());
        factory.createPropAMM(owner, PUR_ADDR, updaters, pairs);
        assertEq(factory.allPropAMMsLength(), 1);
    }
}
