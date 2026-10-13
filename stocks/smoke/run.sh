#!/usr/bin/env bash
# Smoke test for propamm-stocks: the real binary against a local chain and a local Ondo API,
# so the parts unit tests cannot reach (HTTP to Ondo, every eth_call and its decoding, the
# build's first reads, a published update and the quote PropAMM prices from it) run for real.
# Nothing here touches the network: anvil, the contracts' own mocks, and ondo_api.py.
#
#   bash stocks/smoke/run.sh   from anywhere; builds what it needs, exits 0 when every check holds
#
# 1. `--check` on a full builder-mode config (pricer and both guards) passes, and passes with
#    the pair halted, which the library labels by its lane.
# 2. `--check` fails, naming why, on a wrong API key, and on a token the oracle has no
#    multiplier for.
# 3. `--check` names each refusal Ondo's stack holds for the pair, found where Ondo keeps it:
#    the token paused, the vault blocklisted, the PropAMM sanctioned. Each mock contract
#    answers only for what it holds, so a lookup under the wrong id or of the wrong list
#    finds nothing. And it withdraws on a refusal no list holds, which only the transfer the
#    guard tries on the token as the PropAMM can find.
# 4. One node-mode push lands, and PropAMM quotes 1 AAPLon at Ondo's price less the delta.
# 5. An event Ondo files under the stock's ticker (AAPL) withdraws the AAPLon quote.
set -euo pipefail

ROOT=$(cd "$(dirname "$0")/../.." && pwd)
RPC_PORT=8555
API_PORT=8556
RPC=http://127.0.0.1:$RPC_PORT
WORK=$(mktemp -d)

# anvil's well-known accounts: 0 deploys and owns, 1 is the vault, 2 signs updates.
DEPLOYER_KEY=0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80
DEPLOYER=0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266
VAULT_KEY=0x59c6995e998f97a5a0044966f0945389dc9e86dae88c7a8412f4603b6b78690d
VAULT=0x70997970C51812dc3A010C7d01b50e0d17dc79C8
UPDATER_KEY=0x5de4111afa1a4b94908f83103eb1f1706367c2e68ca870fc3fb9a804cdab365a
UPDATER=0x3C44CdDdB6a900fa2b585dd299e03d12FA4293BC

AAPLON=0x14c3abF95Cb9C93a8b82C1CdCB76D72Cb87b2d4c
USDC=0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48
API_KEY=smoke-key
# $256.79 a token at a multiplier of 1.002124: the stock at 256.79 / 1.002124.
TOKEN_PRICE=256.79
STOCK_PRICE=256.24573
SVALUE=1002124000000000000

PIDS=()
cleanup() {
  # Not "${PIDS[@]}": macOS's bash 3.2 calls an empty array unbound under `set -u`, and the
  # trap would die before removing $WORK whenever the build failed before anything started.
  for pid in ${PIDS[@]+"${PIDS[@]}"}; do kill "$pid" 2>/dev/null || true; done
  rm -rf "$WORK"
}
trap cleanup EXIT

fail() { echo "FAIL: $*" >&2; exit 1; }
# Until `$2` (a command) succeeds, for up to 10s; then fails naming `$1`.
wait_for() { for _ in $(seq 1 50); do "$2" && return 0; sleep 0.2; done; fail "$1 did not start"; }
chain_up() { cast block-number --rpc-url "$RPC" >/dev/null 2>&1; }
api_up() { grep -q READY "$WORK/api.log"; }
send() { cast send --rpc-url "$RPC" "$@" >/dev/null; }
deploy() { forge create --broadcast --rpc-url "$RPC" --private-key "$DEPLOYER_KEY" "$@" | awk '/Deployed to:/ {print $3}'; }

echo "== building"
# forge's lint notes go to stderr on every build: kept in a log, shown only if the build fails.
(cd "$ROOT/contracts" && forge build --offline >"$WORK/forge.log" 2>&1) \
  || { cat "$WORK/forge.log"; fail "the contracts do not build"; }
(cd "$ROOT/stocks" && cargo build --offline --locked -q)
BIN=$ROOT/stocks/target/debug/propamm-stocks

echo "== local chain and Ondo API"
# The asset events the mock lists, re-read on every request: none until step 4.
EVENTS=$WORK/events.json
echo '[]' >"$EVENTS"
# Disowned, so bash does not announce their termination when cleanup kills them.
anvil --port "$RPC_PORT" --silent & PIDS+=($!); disown
python3 "$ROOT/stocks/smoke/ondo_api.py" "$API_PORT" "$API_KEY" AAPLon "$TOKEN_PRICE" "$STOCK_PRICE" \
  "$EVENTS" >"$WORK/api.log" 2>&1 & PIDS+=($!); disown
wait_for anvil chain_up
wait_for "the mock Ondo API" api_up

echo "== contracts"
cd "$ROOT/contracts"
REGISTRY=$(deploy test/mocks/MockOndoStack.sol:MockSmokeRegistry)
# Ondo's stack, one contract per role: the pause manager, the lists and the compliance that
# keeps them under Ondo's id, the view the token names, and the multiplier oracle.
STACK=$(deploy test/mocks/MockOndoStack.sol:MockOndoStack)
member() { cast call "$STACK" "$1()(address)" --rpc-url "$RPC"; }
PAUSE_MANAGER=$(member pauseManager)
BLOCKLIST=$(member blocklist)
SANCTIONS=$(member sanctionsList)
COMPLIANCE_VIEW=$(member complianceView)
ORACLE=$(member oracle)
cast rpc anvil_setCode "$AAPLON" "$(forge inspect MockOndoGM deployedBytecode)" --rpc-url "$RPC" >/dev/null
cast rpc anvil_setCode "$USDC" "$(forge inspect MockERC20 deployedBytecode)" --rpc-url "$RPC" >/dev/null
send "$AAPLON" 'setMeta(string,string)' "Apple (Ondo Tokenized Stock)" AAPLon --private-key "$DEPLOYER_KEY"
send "$AAPLON" 'setStack(address,address)' "$PAUSE_MANAGER" "$COMPLIANCE_VIEW" --private-key "$DEPLOYER_KEY"
send "$USDC" 'setMeta(string,string,uint8)' "USD Coin" USDC 6 --private-key "$DEPLOYER_KEY"
send "$ORACLE" 'setMultiplier(address,uint128,uint256)' "$AAPLON" "$SVALUE" 0 --private-key "$DEPLOYER_KEY"
IMPL=$(deploy src/PropAMM.sol:PropAMM)
INIT=$(cast calldata 'initialize(address,address[],address,(address,address,address)[])' \
  "$DEPLOYER" "[$UPDATER]" "$REGISTRY" "[($AAPLON,$USDC,$VAULT)]")
PROPAMM=$(deploy lib/openzeppelin-contracts/contracts/proxy/ERC1967/ERC1967Proxy.sol:ERC1967Proxy \
  --constructor-args "$IMPL" "$INIT")
send "$AAPLON" 'mint(address,uint256)' "$VAULT" 1000000000000000000000 --private-key "$DEPLOYER_KEY"
send "$USDC" 'mint(address,uint256)' "$VAULT" 250000000000 --private-key "$DEPLOYER_KEY"
send "$AAPLON" 'approve(address,uint256)' "$PROPAMM" "$(cast max-uint)" --private-key "$VAULT_KEY"
send "$USDC" 'approve(address,uint256)' "$PROPAMM" "$(cast max-uint)" --private-key "$VAULT_KEY"
echo "PropAMM $PROPAMM, registry $REGISTRY"

# The example config, pointed at this chain and the mock API. Builder mode keeps the guards;
# node mode refuses guards and builders, so everything from the first guard on is cut.
# `halted` is builder mode with the pair halted.
write_config() {
  python3 - "$1" "$PROPAMM" "$RPC" "$API_PORT" "$ORACLE" \
    "$ROOT/stocks/config.example.toml" "$WORK/$1.toml" <<'PY'
import re
import sys

name, propamm, rpc, api_port, oracle, src, dst = sys.argv[1:]
mode = "builder" if name == "halted" else name
text = open(src).read()
text = re.sub(r"(?m)^target = .*$", f'target = "{propamm}"', text)
text = re.sub(r"(?m)^mode = .*$", f'mode = "{mode}"', text)
text = re.sub(r"(?m)^rpc_url = .*$", f'rpc_url = "{rpc}"', text)
ondo = 'kind    = "ondo"\n'
assert ondo in text, "the example config's pricing stanza moved"
text = text.replace(
    ondo, f'{ondo}api = "http://127.0.0.1:{api_port}"\nmultiplier_oracle = "{oracle}"\n'
)
if mode == "node":
    # The first guard *table*: the header comment names [[pairs.guards]] too.
    text = text[: re.search(r"(?m)^\[\[pairs\.guards\]\]", text).start()]
if name == "halted":
    key_env = re.search(r"(?m)^key_env .*$", text).group(0)
    text = text.replace(key_env, f"{key_env}\nhalted  = true")
assert "[[pairs]]" in text and ondo in text, f"the {name} config lost its pair"
open(dst, "w").write(text)
PY
}
run() { UPDATER_KEY_AAPLON_USDC=$UPDATER_KEY "$BIN" --registry "$REGISTRY" "$@"; }

echo "== 1. --check passes on the full config"
write_config builder
ONDO_API_KEY=$API_KEY run --config "$WORK/builder.toml" --check >"$WORK/check.out" 2>&1 \
  || { cat "$WORK/check.out"; fail "--check did not pass"; }
grep -q "ondo" "$WORK/check.out" || { cat "$WORK/check.out"; fail "--check did not show the ondo pair"; }
# The transfer the guard tries goes through on a healthy stack.
if grep -q "would refuse" "$WORK/check.out"; then
  cat "$WORK/check.out"; fail "--check found the token refusing a fill on a healthy stack"
fi
cat "$WORK/check.out"
# Halting is the backoffice's kill switch: it writes `halted = true` and reloads, and the
# reload is refused if the stanza cannot validate a pair labelled by its lane.
write_config halted
ONDO_API_KEY=$API_KEY run --config "$WORK/halted.toml" --check >"$WORK/halted.out" 2>&1 \
  || { cat "$WORK/halted.out"; fail "--check refused the pair halted"; }
echo "  halted: --check passes"

echo "== 2. --check refuses a wrong key and an unknown token"
if ONDO_API_KEY=wrong run --config "$WORK/builder.toml" --check >"$WORK/badkey.out" 2>&1; then
  fail "--check passed with a wrong API key"
fi
# The error's own words: a bare 401 also matches whatever base fee the chain reports.
grep -q "HTTP 401" "$WORK/badkey.out" || { cat "$WORK/badkey.out"; fail "a wrong key was not reported as a 401"; }
grep -m1 "HTTP 401" "$WORK/badkey.out" | sed 's/^ */  wrong key: /'
send "$ORACLE" 'setMultiplier(address,uint128,uint256)' "$AAPLON" 0 0 --private-key "$DEPLOYER_KEY"
if ONDO_API_KEY=$API_KEY run --config "$WORK/builder.toml" --check >"$WORK/nosv.out" 2>&1; then
  fail "--check passed with no multiplier on chain"
fi
grep -q "no multiplier" "$WORK/nosv.out" || { cat "$WORK/nosv.out"; fail "a missing multiplier was not named"; }
grep -m1 "no multiplier" "$WORK/nosv.out" | sed 's/^ */  no multiplier: /'
send "$ORACLE" 'setMultiplier(address,uint128,uint256)' "$AAPLON" "$SVALUE" 0 --private-key "$DEPLOYER_KEY"

echo "== 3. --check names what Ondo's stack would refuse"
send "$PAUSE_MANAGER" 'setPaused(address,bool)' "$AAPLON" true --private-key "$DEPLOYER_KEY"
send "$BLOCKLIST" 'setBlocked(address,bool)' "$VAULT" true --private-key "$DEPLOYER_KEY"
send "$SANCTIONS" 'setSanctioned(address,bool)' "$PROPAMM" true --private-key "$DEPLOYER_KEY"
# A refusal withdraws the quote, it does not fail the build: the check passes and says why.
ONDO_API_KEY=$API_KEY run --config "$WORK/builder.toml" --check >"$WORK/refused.out" 2>&1 \
  || { cat "$WORK/refused.out"; fail "--check failed while the token refuses: the quote should start withdrawn"; }
for refusal in "Ondo paused the token" "vault $VAULT is on Ondo's blocklist" \
  "PropAMM $PROPAMM is on the sanctions list"; do
  # The updater prints addresses in lowercase.
  grep -qiF "$refusal" "$WORK/refused.out" || { cat "$WORK/refused.out"; fail "not named: $refusal"; }
done
grep -m1 "would refuse" "$WORK/refused.out" | sed 's/^ */  refused: /'
send "$PAUSE_MANAGER" 'setPaused(address,bool)' "$AAPLON" false --private-key "$DEPLOYER_KEY"
send "$BLOCKLIST" 'setBlocked(address,bool)' "$VAULT" false --private-key "$DEPLOYER_KEY"
send "$SANCTIONS" 'setSanctioned(address,bool)' "$PROPAMM" false --private-key "$DEPLOYER_KEY"
# A refusal none of Ondo's lists holds: only trying the transfer finds it.
send "$AAPLON" 'setRefusedElsewhere(address,bool)' "$VAULT" true --private-key "$DEPLOYER_KEY"
ONDO_API_KEY=$API_KEY run --config "$WORK/builder.toml" --check >"$WORK/unlisted.out" 2>&1 \
  || { cat "$WORK/unlisted.out"; fail "--check failed while the token refuses: the quote should start withdrawn"; }
grep -qF "rules do not name" "$WORK/unlisted.out" \
  || { cat "$WORK/unlisted.out"; fail "a refusal no list holds went unseen"; }
grep -m1 "rules do not name" "$WORK/unlisted.out" | sed 's/^ */  refused: /'
send "$AAPLON" 'setRefusedElsewhere(address,bool)' "$VAULT" false --private-key "$DEPLOYER_KEY"

echo "== 4. one node-mode push lands and PropAMM quotes Ondo's price less the delta"
write_config node
ONDO_API_KEY=$API_KEY run --config "$WORK/node.toml" --mode node --once >"$WORK/once.out" 2>&1 \
  || { cat "$WORK/once.out"; fail "the node-mode push failed"; }
OUT=$(cast call "$PROPAMM" 'quote(address,address,uint256)(uint256)' "$AAPLON" "$USDC" 1000000000000000000 \
  --rpc-url "$RPC" | awk '{print $1}')
# 256.79 USDC less 30 bp is 256.01963; the impact term on a 250,000 USDC vault takes about 26
# units (millionths of a USDC) more. Anything within a cent is the price arriving intact.
python3 -c "import sys; out = int('$OUT'); want = 256_019_630; sys.exit(0 if abs(out - want) <= 10_000 else 1)" \
  || fail "PropAMM quotes $OUT USDC units for 1 AAPLon, not ~256019630"
echo "quote(1 AAPLon) = $OUT USDC units"

echo "== 5. an event under the stock's ticker withdraws the quote"
# The mock names the stock AAPL in its price answer, as Ondo does; the event says only AAPL.
cat >"$EVENTS" <<'JSON'
[{"symbol": "AAPL", "status": "active", "type": "unscheduled", "eventId": "smoke",
  "updateSharesMultiplier": false, "reason": {"code": "ASSET_PAUSED", "message": "smoke halt"}}]
JSON
# Under --once node mode pushes each pair once, and a refused quote fails the run.
if ONDO_API_KEY=$API_KEY run --config "$WORK/node.toml" --mode node --once >"$WORK/event.out" 2>&1; then
  cat "$WORK/event.out"; fail "--once pushed through an active event filed under AAPL"
fi
grep -q "smoke halt" "$WORK/event.out" || { cat "$WORK/event.out"; fail "the event was not named"; }
grep -m1 "smoke halt" "$WORK/event.out" | sed 's/^ */  withdrawn: /'
echo "OK"
