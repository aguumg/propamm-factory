# propamm-factory

Deploy your own PropAMM in a single command.

## Overview

A PropAMM is an on-chain market maker that does not rely on a passive inventory curve; instead the maker streams signed quote updates to the builder, ensuring prices are updated in real-time. For a more thorough introduction, see [here](https://docs.titanbuilder.xyz/propamms#overview).

At a high level, a PropAMM is two things:

- A smart contract, which knows your pairs and fills trades at the published price.
- An off-chain service that continuosly streams quotes for those pairs to every builder. This service we call the `quote-updater`.

This repo provides the tools to build these two things, and a set of examples and the necessary infra surrounding it to deploy on ethereum mainnet.

## Deploying to production

See [docs/production.md](docs/production.md): deploying the PropAMM contract through the mainnet factory, setting up the signer and vault wallets, and running the quote updater on a server.

## Running it locally

```bash
make install-nix   # once, if you don't have nix
make quickstart
make local-down    # tear it all down when you're done
```

This starts a local chain with anvil, deploys the factory and a PropAMM with a `USDC/USDT` pair, builds `example/`, and starts quoting that pair using Binance's live price as the mid.

You can check out a backoffice to manage settings, pairs, etc at http://localhost:8088 and run

```
make swap
```

to do trades agains the PropAMM.

There's also a Grafana dashboard at http://localhost:3000 with the updater's dashboard (quoting, landings, feeds,
builders, signer balance).

## Documentation

- [Deep dive into the internals](docs/components.md)
- [Pricing models: the shipped ones, and writing your own](docs/pricing.md)
- [Tokenized stocks: Ondo's pairs, and adding an issuer](docs/stocks.md)
