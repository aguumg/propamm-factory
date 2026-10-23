// SPDX-License-Identifier: MIT
pragma solidity ^0.8.35;

import "@openzeppelin/contracts/token/ERC20/IERC20.sol";
import "@openzeppelin/contracts/token/ERC20/utils/SafeERC20.sol";
import {IPropAMM} from "../../src/interfaces/IPropAMM.sol";

/// A pull-style router used only for testing: it takes `amountIn` from the caller, forwards it
/// to the PropAMM and swaps. Not part of the production deployment.
contract Swapper {
    using SafeERC20 for IERC20;

    IPropAMM immutable propamm;

    constructor(address propamm_) {
        propamm = IPropAMM(propamm_);
    }

    function swap(
        address tokenIn,
        address tokenOut,
        uint256 amountIn,
        uint256 minAmountOut,
        address recipient,
        uint256 deadline
    ) external returns (uint256 amountOut) {
        IERC20(tokenIn).safeTransferFrom(msg.sender, address(propamm), amountIn);

        return propamm.swap(tokenIn, tokenOut, amountIn, minAmountOut, recipient, deadline);
    }
}
