// SPDX-License-Identifier: MIT
pragma solidity ^0.8.35;

import {PropAMMStockTokenTest} from "./PropAMMStockToken.t.sol";
import {MockGMToken} from "./mocks/MockGMToken.sol";

/// A PropAMM listing an Ondo Global Markets stock against USDC, with the stock token behaving
/// as `GMToken` does: transfers revert while Ondo pauses the token, and whenever a party to
/// them (sender, receiver, or a third-party spender) is blocklisted or sanctioned.
///
/// Nothing in PropAMM changes for such a token: it is a plain 18-decimal ERC-20 with no fee and
/// no rebasing, so a fill prices and settles exactly as any other, and a fill the token refuses
/// reverts whole. The suite it inherits pins that, and that `isActive` and `quote` cannot see
/// the pause or a blocked vault: the quote updater's `ondo_token` guard closes that gap.
///
/// AAPLon sits at its mainnet address so the ordering is the real one: AAPLon (0x14) < USDC
/// (0xA0), so AAPLon is token0 and the mid is USDC per AAPLon.
contract PropAMMOndoTokenTest is PropAMMStockTokenTest {
    address constant AAPLON = 0x14c3abF95Cb9C93a8b82C1CdCB76D72Cb87b2d4c;

    function _deployStock() internal override returns (address) {
        vm.etch(AAPLON, address(new MockGMToken()).code);
        return AAPLON;
    }

    function _mintStock(address to, uint256 amount) internal override {
        MockGMToken(AAPLON).mint(to, amount);
    }

    function _pauseStock() internal override {
        MockGMToken(AAPLON).setPaused(true);
    }

    /// Ondo's blocklist and the sanctions list refuse a party alike; one set stands in for both.
    function _refuse(address party) internal override {
        MockGMToken(AAPLON).setBlocked(party, true);
    }

    function _pausedRevert() internal pure override returns (bytes memory) {
        return abi.encodeWithSelector(MockGMToken.TokenPaused.selector);
    }

    function _refusedRevert(address party) internal pure override returns (bytes memory) {
        return abi.encodeWithSelector(MockGMToken.UserBlocked.selector, party);
    }
}
