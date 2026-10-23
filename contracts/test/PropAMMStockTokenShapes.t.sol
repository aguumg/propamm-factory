// SPDX-License-Identifier: MIT
pragma solidity ^0.8.35;

import {PropAMMStockTokenTest} from "./PropAMMStockToken.t.sol";
import {MockGMToken} from "./mocks/MockGMToken.sol";
import {MockRestrictedToken} from "./mocks/MockRestrictedToken.sol";

/// The stock suite on token shapes Ondo's AAPLon is not, so it holds for whatever design is
/// listed next: a token whose address sorts after USDC's, which makes USDC token0 and the mid
/// token per USDC, and a token with few decimals, where a quote rounds to whole units.
abstract contract RestrictedTokenShape is PropAMMStockTokenTest {
    function _stockAddress() internal pure virtual returns (address);

    function _stockDecimals() internal pure virtual returns (uint8);

    function _deployStock() internal override returns (address) {
        vm.etch(_stockAddress(), address(new MockRestrictedToken()).code);
        MockRestrictedToken(_stockAddress()).setDecimals(_stockDecimals());
        return _stockAddress();
    }

    function _mintStock(address to, uint256 amount) internal override {
        MockRestrictedToken(_stockAddress()).mint(to, amount);
    }

    function _pauseStock() internal override {
        MockRestrictedToken(_stockAddress()).setPaused(true);
    }

    function _refuse(address party) internal override {
        MockRestrictedToken(_stockAddress()).setBlocked(party, true);
    }

    function _pausedRevert() internal pure override returns (bytes memory) {
        return abi.encodeWithSelector(MockGMToken.TokenPaused.selector);
    }

    function _refusedRevert(address party) internal pure override returns (bytes memory) {
        return abi.encodeWithSelector(MockGMToken.UserBlocked.selector, party);
    }
}

/// 0xB0… sorts after USDC's 0xA0b8…: USDC is token0.
contract PropAMMStockTokenAfterUSDCTest is RestrictedTokenShape {
    function _stockAddress() internal pure override returns (address) {
        return 0xB0B0b0B0B0B0B0b0B0B0B0b0b0b0b0B0b0b0B0B0;
    }

    function _stockDecimals() internal pure override returns (uint8) {
        return 18;
    }
}

/// Four decimals: a quote of a few tokens is a few tens of thousands of units.
contract PropAMMStockTokenFewDecimalsTest is RestrictedTokenShape {
    function _stockAddress() internal pure override returns (address) {
        return 0x1400000000000000000000000000000000000014;
    }

    function _stockDecimals() internal pure override returns (uint8) {
        return 4;
    }
}
