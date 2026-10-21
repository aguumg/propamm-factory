# Running it in production

One server with Docker, one `docker compose up`: the quote updater, the Postgres it records
into, Prometheus with the shipped alert rules, Alertmanager posting to Slack, and Grafana
with a dashboard of the updater's metrics. Two files to fill, both gitignored: `deploy/.env`
(keys, passwords, addresses) and `deploy/config/config.toml` (the pairs and the builders).
The contracts are not part of it: deploying a PropAMM is a one-off `forge script`, see
[`contracts/`](../contracts/).

## Setup

From a clean state (the first block stops and removes a previous run, including its data
volumes; skip it to keep Prometheus history and the recorder's tables):

```bash
cd deploy
docker compose down --volumes --remove-orphans
rm -f .env config/config.toml alertmanager/slack-webhook

cp .env.example .env
cp config.example.toml config/config.toml
cp alertmanager/slack-webhook.example alertmanager/slack-webhook
$EDITOR .env                        # RPC_URL, RPC_WS_URL, one UPDATER_KEY_* per pair, the passwords, the bind addresses
$EDITOR config/config.toml          # target, builders, pairs
$EDITOR alertmanager/slack-webhook  # the Slack incoming-webhook URL, one line
docker compose build
docker compose run --rm updater --check   # validates everything against the chain, sends nothing
docker compose up -d
docker compose logs -f updater
```

`--check` prints one row per pair: the lane, the stream, whether each key is authorized by
the target and how much ETH it has, and the price it would publish. It exits 1 on anything
the run would refuse, and prints the exact `addUpdater` calls for a key the target does not
know. Run it after every edit to `config.toml` too.

`RPC_WS_URL` is the same node's WebSocket endpoint (`wss://…`). Without it the updater
polls the chain head over `RPC_URL` four times a second and fetches each new block; with
it the node pushes every new block's header the moment it has it, the poll drops to one
liveness check every 3 seconds, and the pairs roll to the next block sooner. There is no
mode to switch: polling covers any spell the subscription is down. `--check` refuses a
URL that is not `ws://` or `wss://` but does not dial it; only a run does, and confirms
it with `[head] subscribed to new heads at <host>`. One that does not answer is logged
once per distinct error while polling carries the head.

## What is where

| Service | Where to reach it | What it holds |
| --- | --- | --- |
| updater | backoffice at `BACKOFFICE_BIND:8088` | quotes; rewrites `config/config.toml` from the backoffice |
| grafana | `GRAFANA_BIND:3000`, user `admin` | the "Quote updater" dashboard; Prometheus and the recorder as datasources |
| prometheus | `127.0.0.1:9090` | 90 days of `quote_updater_*` metrics; the rules from [`alerts/`](alerts/) |
| alertmanager | `127.0.0.1:9093` | routes pages and tickets to `#propamm-alerts` |
| postgres | compose network only | the `feed` schema the recorder writes (quotes, mids, vault balances, config history) |

Neither the backoffice nor Grafana has a login worth the name (Grafana's admin password is
in `.env`; the backoffice has none), so publish them on this host's tailnet address and
nothing else: `BACKOFFICE_BIND` and `GRAFANA_BIND` in `.env`. Left at `127.0.0.1` they are
reachable from the host only, or through an ssh tunnel.

## Day to day

```bash
docker compose logs -f updater                 # the run
docker compose run --rm updater --check        # after editing config/config.toml by hand
docker compose kill -s HUP updater             # reload the config without a restart
docker compose up -d updater                   # after editing .env (keys, RPC_WS_URL); restart would not
docker compose pull && docker compose up -d    # newer Prometheus, Alertmanager, Grafana, Postgres
docker compose build updater && docker compose up -d updater   # a new build of the updater
```

A reload (`SIGHUP`, or any save in the backoffice) re-reads `config.toml` and changes only
the pairs and builders that changed; `target` and `[settings]` are read once at startup,
and `.env` only when the container is created (`docker compose up -d`, not `restart`).
[docs/manual.md](../docs/manual.md) covers the config file, the reload, the backoffice, the
breaker, the metrics and the recorder.

## Your own pricing models

The image builds `example/`. To run a pricing model of your own, add it to
`example/src/main.rs` (docs/building-your-own.md) and `docker compose build`.

## Notes

- The updater runs as root inside its container so it can rewrite the mounted
  `config/config.toml`; nothing else in the image needs it.
- Postgres keeps its data in the `postgres-data` volume; the updater creates and migrates the
  `feed` schema itself on every connect, so there is nothing to run by hand.
- Alert rules are mounted straight from `deploy/alerts/quote-updater.rules.yml`; a change there is
  live after `docker compose restart prometheus`.
