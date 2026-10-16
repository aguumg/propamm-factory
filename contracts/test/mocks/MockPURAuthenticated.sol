// SPDX-License-Identifier: MIT
pragma solidity ^0.8.35;

/// Stand-in for the PUR registry, scoped per `target` the way the real one is: `addUpdater`
/// registers an updater for `msg.sender`'s OWN lane (self-service, matching `PropAMM.initialize`'s
/// own comment that "the registry scopes updater authorization to msg.sender"), `setState` writes
/// a given `target`'s lane and requires the caller be authorized for THAT target, and `getState`
/// reads the CALLER's own lane (no target argument), matching the real `getState(laneIndex,
/// minTs, maxTs)` signature observed in production traces. Unlike the earlier, unscoped version
/// of this mock (a single global `isUpdater` set), this prevents any address from registering
/// itself as an updater for a target it doesn't own, and prevents the shared `PropAMM.sol`'s own
/// oracle writes from colliding across instances that happen to share this mock.
contract MockPURAuthenticated {
    error NotAnUpdater(address caller, address target);

    mapping(address target => mapping(address updater => bool)) public isUpdater;
    mapping(address target => uint256) public delta;
    mapping(address target => uint256) public mid;

    /// @notice Writes `target`'s lane. `msg.sender` must be an updater `target` itself registered
    /// via `addUpdater` -- not necessarily `target` itself, same self-service model `addUpdater`
    /// grants.
    function setState(address target, uint256 delta_, uint256 mid_) external {
        require(isUpdater[target][msg.sender], NotAnUpdater(msg.sender, target));
        delta[target] = delta_;
        mid[target] = mid_;
    }

    /// @notice Reads the CALLER's own lane -- no `target` argument, matching the real registry:
    /// a `PropAMM` instance reads its own price by calling this on its own behalf.
    function getState(uint256, uint32, uint32) external view returns (uint32 updateTimestamp, uint256[] memory slots) {
        slots = new uint256[](2);
        slots[0] = delta[msg.sender];
        slots[1] = mid[msg.sender];
        return (uint32(block.timestamp), slots);
    }

    /// @notice Self-service: authorizes `updater` to write `msg.sender`'s OWN lane. A `PropAMM`
    /// instance calls this on itself (directly from `initialize`, or via its own `onlyOwner`
    /// `addUpdater` wrapper) -- nothing here lets an address grant write access to a DIFFERENT
    /// target's lane.
    function addUpdater(address updater) external {
        isUpdater[msg.sender][updater] = true;
    }

    function removeUpdater(address updater) external {
        isUpdater[msg.sender][updater] = false;
    }
}
