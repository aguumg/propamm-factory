// SPDX-License-Identifier: MIT
pragma solidity ^0.8.35;

import {ERC20} from "@openzeppelin/contracts/token/ERC20/ERC20.sol";

/// Stand-in for Ondo's `GMToken`, the contract behind AAPLon and every other Global Markets
/// stock: an 18-decimal ERC-20 whose every balance change first checks a pause and then the
/// compliance of each party, in the order `GMToken._beforeTokenTransfer` does on mainnet:
///     _checkTokenIsPaused();
///     if (from != msg.sender && to != msg.sender) _checkIsCompliant(msg.sender);
///     if (from != address(0)) _checkIsCompliant(from);
///     if (to != address(0)) _checkIsCompliant(to);
/// On mainnet the pause lives in Ondo's TokenPauseManager and the compliance in its blocklist
/// and the Chainalysis sanctions list; a PropAMM sees either only as a transfer that reverts,
/// so one flag and one set stand in for both. Storage rather than constructor state, so the
/// mock still works after vm.etch onto the mainnet address.
contract MockGMToken is ERC20 {
    error TokenPaused();
    error UserBlocked(address user);

    bool public paused;
    mapping(address => bool) public blocked;

    constructor() ERC20("Mock GM", "MOCKon") {}

    function mint(address to, uint256 amount) external {
        _mint(to, amount);
    }

    function setPaused(bool paused_) external {
        paused = paused_;
    }

    function setBlocked(address user, bool blocked_) external {
        blocked[user] = blocked_;
    }

    function _update(address from, address to, uint256 value) internal override {
        require(!_isPaused(), TokenPaused());
        // The spender of a `transferFrom` between two other parties is checked too, which is
        // what makes a PropAMM's own address a party to every fill out of its vault.
        if (from != msg.sender && to != msg.sender) _checkIsCompliant(msg.sender);
        if (from != address(0)) _checkIsCompliant(from);
        if (to != address(0)) _checkIsCompliant(to);
        super._update(from, to, value);
    }

    /// Whether the token refuses every transfer: this flag here; the pause manager's in the
    /// smoke test's MockOndoGM.
    function _isPaused() internal view virtual returns (bool) {
        return paused;
    }

    function _checkIsCompliant(address user) internal view virtual {
        require(!blocked[user], UserBlocked(user));
    }
}
