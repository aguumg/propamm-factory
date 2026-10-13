// SPDX-License-Identifier: MIT
pragma solidity ^0.8.35;

import {MockGMToken} from "./MockGMToken.sol";

/// Ondo's stack around a GM token, for the propamm-stocks smoke test (`stocks/smoke/run.sh`):
/// one contract per role, as on mainnet, each answering only for what it holds. A lookup under
/// another token or another id answers zero, as Ondo's do, and the blocklist cannot answer for
/// the sanctions list, so the smoke test checks the updater's walk, not only its decoding:
///     token.tokenPauseManager().isTokenPaused(token)
///     token.compliance() -> view.compliance(), view.gmIdentifier()
///       -> rwaTokenToBlocklist(id).isBlocked(party), rwaTokenToSanctionsList(id).isSanctioned(party)
///     oracle.assetData(token)
/// This deploys every contract but the token and wires them together; run.sh etches the token
/// (MockOndoGM) at AAPLon's address and points it at them.
contract MockOndoStack {
    /// The id Ondo keeps a GM token's lists under: not any token's address.
    address public constant GM_IDENTIFIER = address(uint160(uint256(keccak256("global_markets"))));

    MockTokenPauseManager public immutable pauseManager;
    MockOndoBlocklist public immutable blocklist;
    MockOndoSanctionsList public immutable sanctionsList;
    MockOndoCompliance public immutable ondoCompliance;
    MockGMComplianceView public immutable complianceView;
    MockSyntheticSharesOracle public immutable oracle;

    constructor() {
        pauseManager = new MockTokenPauseManager();
        blocklist = new MockOndoBlocklist();
        sanctionsList = new MockOndoSanctionsList();
        ondoCompliance = new MockOndoCompliance();
        ondoCompliance.setLists(GM_IDENTIFIER, address(blocklist), address(sanctionsList));
        complianceView = new MockGMComplianceView(address(ondoCompliance), GM_IDENTIFIER);
        oracle = new MockSyntheticSharesOracle();
    }
}

/// TokenPauseManager: which tokens Ondo has paused.
contract MockTokenPauseManager {
    mapping(address token => bool) public isTokenPaused;

    function setPaused(address token, bool paused) external {
        isTokenPaused[token] = paused;
    }
}

/// Ondo's blocklist.
contract MockOndoBlocklist {
    mapping(address user => bool) public isBlocked;

    function setBlocked(address user, bool blocked) external {
        isBlocked[user] = blocked;
    }
}

/// The sanctions list Ondo's compliance points at.
contract MockOndoSanctionsList {
    mapping(address user => bool) public isSanctioned;

    function setSanctioned(address user, bool sanctioned) external {
        isSanctioned[user] = sanctioned;
    }
}

/// OndoCompliance: the lists it keeps, by the id they are kept under.
contract MockOndoCompliance {
    mapping(address id => address) public rwaTokenToBlocklist;
    mapping(address id => address) public rwaTokenToSanctionsList;

    function setLists(address id, address blocklist, address sanctionsList) external {
        rwaTokenToBlocklist[id] = blocklist;
        rwaTokenToSanctionsList[id] = sanctionsList;
    }
}

/// The GM compliance view a GM token names: OndoCompliance, and the id to ask it under.
contract MockGMComplianceView {
    address public immutable compliance;
    address public immutable gmIdentifier;

    constructor(address compliance_, address gmIdentifier_) {
        compliance = compliance_;
        gmIdentifier = gmIdentifier_;
    }
}

/// SyntheticSharesOracle: each token's share multiplier and scheduled pause; zeros for a token it
/// does not know, as `assetData` answers on mainnet.
contract MockSyntheticSharesOracle {
    struct Asset {
        uint128 sValue;
        uint256 pauseStartTime;
    }

    mapping(address token => Asset) private _assets;

    function setMultiplier(address token, uint128 sValue, uint256 pauseStartTime) external {
        _assets[token] = Asset(sValue, pauseStartTime);
    }

    function assetData(address token) external view returns (uint128, uint128, uint256, uint256, uint16, uint48) {
        Asset memory asset = _assets[token];
        return (asset.sValue, 0, 0, asset.pauseStartTime, 0, 0);
    }
}

/// The GM token, with `GMToken`'s transfer checks (from MockGMToken) asked of the stack it is
/// pointed at, the way the real token asks Ondo's. Name, symbol and the stack's addresses live
/// in storage, so the mock still has them after anvil_setCode.
contract MockOndoGM is MockGMToken {
    string private _metaName;
    string private _metaSymbol;
    address public tokenPauseManager;
    /// The GM compliance view.
    address public compliance;
    /// Parties the token refuses for a reason no list in the stack holds: a check Ondo might
    /// add to the token that no read of the updater's knows, which only trying the transfer
    /// finds.
    mapping(address user => bool) public refusedElsewhere;

    function setMeta(string calldata name_, string calldata symbol_) external {
        _metaName = name_;
        _metaSymbol = symbol_;
    }

    function setStack(address pauseManager_, address complianceView_) external {
        tokenPauseManager = pauseManager_;
        compliance = complianceView_;
    }

    function setRefusedElsewhere(address user, bool refused) external {
        refusedElsewhere[user] = refused;
    }

    function name() public view override returns (string memory) {
        return _metaName;
    }

    function symbol() public view override returns (string memory) {
        return _metaSymbol;
    }

    function _isPaused() internal view override returns (bool) {
        return MockTokenPauseManager(tokenPauseManager).isTokenPaused(address(this));
    }

    function _checkIsCompliant(address user) internal view override {
        MockGMComplianceView complianceView = MockGMComplianceView(compliance);
        MockOndoCompliance lists = MockOndoCompliance(complianceView.compliance());
        address id = complianceView.gmIdentifier();
        address blocklist = lists.rwaTokenToBlocklist(id);
        address sanctionsList = lists.rwaTokenToSanctionsList(id);
        require(blocklist == address(0) || !MockOndoBlocklist(blocklist).isBlocked(user), UserBlocked(user));
        require(
            sanctionsList == address(0) || !MockOndoSanctionsList(sanctionsList).isSanctioned(user), UserBlocked(user)
        );
        require(!refusedElsewhere[user], UserBlocked(user));
    }
}

/// The registry the smoke test's PropAMM reads, answering what the updater and PropAMM call:
/// `addUpdater` (scoped to the calling PropAMM, as the real one is), `isUpdater(target, updater)`
/// for the startup check, and `updateState`/`getState` without the real registry's timestamp
/// rules, so a run on anvil needs no builder to land its updates.
contract MockSmokeRegistry {
    mapping(address target => mapping(address updater => bool)) public isUpdater;
    mapping(uint256 lane => uint256[]) private _slots;
    mapping(uint256 lane => uint32) private _stamped;

    function addUpdater(address updater) external {
        isUpdater[msg.sender][updater] = true;
    }

    function removeUpdater(address updater) external {
        isUpdater[msg.sender][updater] = false;
    }

    function updateState(address target, uint256 lane, uint32 timestamp, uint256[] calldata slots) external {
        require(isUpdater[target][msg.sender], "not an updater of target");
        _slots[lane] = slots;
        _stamped[lane] = timestamp;
    }

    function getState(uint256 lane, uint32, uint32) external view returns (uint32, uint256[] memory) {
        require(_slots[lane].length != 0, "no state");
        return (_stamped[lane], _slots[lane]);
    }
}
