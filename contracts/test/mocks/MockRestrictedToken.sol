// SPDX-License-Identifier: MIT
pragma solidity ^0.8.35;

import {MockGMToken} from "./MockGMToken.sol";

/// MockGMToken's pause and refusals, at any decimals: the stand-in for a token design the stock
/// suite has no mock of yet, so the suite is run on token shapes AAPLon is not. Decimals live in
/// storage, set after the etch.
contract MockRestrictedToken is MockGMToken {
    uint8 private _decimals;

    function setDecimals(uint8 decimals_) external {
        _decimals = decimals_;
    }

    function decimals() public view override returns (uint8) {
        return _decimals;
    }
}
