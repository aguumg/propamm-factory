// SPDX-License-Identifier: MIT
pragma solidity ^0.8.35;

import {Script, console} from "forge-std/Script.sol";
import {PropAMMFactory} from "../src/PropAMMFactory.sol";

/// Factory-only deployment for real networks, once per chain. Deploy.s.sol is the local
/// flavor (it also creates a first PropAMM from env vars); on a real network instances are
/// created by their owners through the factory afterwards. See DEPLOYMENT.md.
contract DeployFactory is Script {
    function run() external {
        // The account that can disable creation on this factory, for good. Defaults to the
        // deployer: `FACTORY_OWNER=0x... make deploy-factory ...`.
        address factoryOwner = vm.envOr("FACTORY_OWNER", msg.sender);

        vm.startBroadcast();
        PropAMMFactory factory = new PropAMMFactory(factoryOwner);
        vm.stopBroadcast();
        console.log("PropAMMFactory deployed at:", address(factory));
        console.log("factory owner:", factoryOwner);
    }
}
