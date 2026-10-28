# Deploying to production

Deploying to production consists, roughly, of three things:

- Deploying the PropAMM contract through the mainnet factory
- Creating, funding and setting up wallets for both the signers (the accounts that will sign quote updates) and the vault that will hold the liquidity/inventory.
- Deploying the quote updater, filling the config with all the addressed and keys obtained above.

## 1. Deploy the PropAMM contract

If there is no factory on your chain yet, deploy one:

```bash
make deploy-factory DEPLOY_RPC_URL=https://... PRIVATE_KEY=0x... FACTORY_OWNER=0x...
```

`FACTORY_OWNER` is the account that can call `disableCreation`, which stops the factory from
creating any more PropAMMs, for good, and emits `CreationDisabled`. It defaults to the deployer.

Note: on Ethereum mainnet the current factory's address is `0x7625E38581124da157586759466E87601416172c`.

Create your PropAMM contract by calling `createPropAMM` on the factory:

```bash
cast send $FACTORY "createPropAMM(address,address,address[],(address,address,address)[])" \
  $OWNER $REGISTRY $UPDATERS $PAIRS \
  --rpc-url $RPC_URL --private-key $DEPLOYER_KEY
```

- `OWNER`: the wallet that will manage the PropAMM afterwards (add pairs, add updaters,
  pause). Can be the same as the deployer.
- `REGISTRY`: the priority update registry the PropAMM reads prices from. On mainnet it is
  `0xDa7AfEeD021EAFC1c1Af9C362dE477DaD0396B81`.
- `UPDATERS`: the wallets allowed to publish prices. Leave it empty, `"[]"`, and add them in
  step 3 once you have created them.
- `PAIRS`: one entry per pair, `"[(TOKEN0,TOKEN1,VAULT)]"`, where `TOKEN0` and `TOKEN1` are
  the two token addresses and `VAULT` is the wallet holding the inventory for that pair.
  Two pairs look like `"[(TOKEN0,TOKEN1,VAULT),(TOKEN0,TOKEN1,VAULT)]"`.

The vault must hold the tokens and have approved the PropAMM to spend both of them. The
PropAMM address does not exist until the call, so either approve it right after creating,
or compute it beforehand with the factory's `computeAddress` (same arguments).

So for a WETH/USDC pair on mainnet:

```bash
cast send $FACTORY "createPropAMM(address,address,address[],(address,address,address)[])" \
  $OWNER 0xDa7AfEeD021EAFC1c1Af9C362dE477DaD0396B81 "[]" \
  "[(0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2,0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48,$VAULT)]" \
  --rpc-url $RPC_URL --private-key $DEPLOYER_KEY
```

You do not need to add all the pairs upfront on contract creation. You can both and or remove pairs by having the owner calling the appropriate contract functions.

## 2. Create one wallet per pair and fund it with ETH

Each pair publishes its price from its own wallet. Make a fresh one per pair and send it ETH: every price update is a transaction and pays gas.

## 3. Authorize those wallets on the PropAMM

From the owner account, once per wallet:

```bash
cast send $PROPAMM "addUpdater(address)" $UPDATER_ADDRESS --rpc-url $RPC_URL --private-key $OWNER_KEY
```

## 4. Get API keys from the builders

The api to send builders quote updates is permissioned, behind an API key. You will have to Ask each builder for one:

- Titan: `wss://eu.rpc.titanbuilder.xyz/ws/sendquoteupdate`
- Quasar: `wss://rpc.quasar.win/ws/sendquoteupdate`
- BuilderNet: `wss://direct-eu.buildernet.org/ws/sendquoteupdate`

## 5. Fill in the config on the server

Clone the repo, then in `deploy/`:

```bash
cp .env.example .env
cp config.example.toml config/config.toml
cp alertmanager/slack-webhook.example alertmanager/slack-webhook
```

Edit the three files:

- `.env`, one line per variable:
  - `RPC_URL`: the Ethereum RPC the updater reads the chain through.
  - `RPC_WS_URL`: the same node's WebSocket endpoint (`wss://…`), so it pushes each new
    block to the updater instead of being polled for it four times a second. Optional but
    worth setting: the run confirms it with `[head] subscribed to new heads at <host>`, and
    falls back to polling whenever the socket is down.
  - `UPDATER_KEY_<PAIR>`: the private key of each wallet from step 2, one variable per pair.
    The name is yours to pick; the pair's config refers to it by that name (`key_env`).
    For example `UPDATER_KEY_WETH_USDC=0x...` and `UPDATER_KEY_USDC_USDT=0x...`.
  - `POSTGRES_PASSWORD`: password for the Postgres the updater records into. Any string.
  - `GRAFANA_ADMIN_PASSWORD`: password for Grafana's `admin` user.
  - `BACKOFFICE_BIND` and `GRAFANA_BIND`: the IP of this server the backoffice (port 8088)
    and Grafana (port 3000) listen on. Neither has a login, so use a private IP such as the
    server's tailnet address. `127.0.0.1` keeps them reachable only from the server itself.
- `config/config.toml`: the PropAMM address, the builders with their API keys, and the
  pairs. Pairs can also be added later from the backoffice.
- `alertmanager/slack-webhook`: the Slack webhook URL alerts go to.

## 6. Start it

```bash
docker compose build
docker compose run --rm updater --check
docker compose up -d
```

`--check` connects to the chain and reports anything wrong (a wallet not authorized or
without ETH, a pair the contract doesn't have, a typo in the config).

## 7. Inspect Grafana and the backoffice to manage updater/pair settings 

Open `http://<IP from .env>:8088` to go into the backoffice and change pair settings, pair curves, etc.

If you add a new pair (and thus a new updater), the new updater key goes in `.env` first, then `docker compose up -d` so the updater picks it up.

Grafana is at `http://<IP from .env>:3000`. Day-to-day commands (reload, logs, updates) are in
[`deploy/README.md`](../deploy/README.md).

## Next steps

The pairs quote with the pricing models the library ships. To tune their settings, change
them from the backoffice. To change the pricing itself, write your own model or adjust the
example one: [pricing.md](pricing.md) covers what the shipped models do, how to write one,
and what the library gives you to do it.
