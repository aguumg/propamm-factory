// SPDX-License-Identifier: MIT
pragma solidity ^0.8.35;

import {Script, console} from "forge-std/Script.sol";
import {PropAMM} from "../src/PropAMM.sol";
import {PropAMMFactory} from "../src/PropAMMFactory.sol";

contract Deploy is Script {
    address constant USDC = 0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48;
    address constant USDT = 0xdAC17F958D2ee523a2206206994597C13D831ec7;
    address constant ORACLE = 0xDa7AfEeD021EAFC1c1Af9C362dE477DaD0396B81;

    function run() external {
        address vault = vm.envAddress("VAULT_ADDRESS");
        // Comma-separated, so a deployment can start out with several updaters.
        address[] memory updaters = vm.envAddress("UPDATER_ADDRESS", ",");
        // Ownership is the factory's caller's to give away, so it defaults to the deployer but can
        // be handed to anyone: `OWNER_ADDRESS=0x... forge script ...`.
        address owner = vm.envOr("OWNER_ADDRESS", msg.sender);

        // One pair, filled from VAULT_ADDRESS. On a real network each pair would name its own.
        PropAMM.PairConfig[] memory pairs = new PropAMM.PairConfig[](1);
        pairs[0] = PropAMM.PairConfig({token0: USDC, token1: USDT, vault: vault});

        vm.startBroadcast();
        // The deployer also owns the factory, and so is who can disable creation on it.
        PropAMMFactory factory = new PropAMMFactory(msg.sender);
        address propAmm = factory.createPropAMM(owner, ORACLE, updaters, pairs);
        vm.stopBroadcast();

        console.log("PropAMMFactory deployed at:", address(factory));
        console.log("factory owner:", factory.owner());
        console.log("PropAMM implementation at:", address(factory.implementation()));
        console.log("PropAMM created at:", propAmm);
        console.log("owner:", owner);
        console.log("vault:", vault);
        for (uint256 i = 0; i < updaters.length; i++) {
            console.log("updater:", updaters[i]);
        }
    }
}
