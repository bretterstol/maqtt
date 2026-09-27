# måqtt

A desktop MQTT client for exploring and debugging live traffic on several Brokers at once. Built with Tauri (Rust) and React, for Linux and macOS.

- Domain language: [CONTEXT.md](./CONTEXT.md)
- Decisions: [docs/adr/](./docs/adr/)
- Roadmap: [docs/v1-plan.md](./docs/v1-plan.md)

## Prerequisites

- Rust (stable), Node 22+, pnpm
- Podman (or Docker with `CONTAINER=docker`) and OpenSSL for the dev Broker
- Tauri system dependencies: <https://tauri.app/start/prerequisites/>

## Development

```sh
pnpm install
pnpm dev:env      # dev Broker + fake traffic (leave running)
pnpm tauri dev    # the app, in another terminal
```

`pnpm dev:env` runs `dev/env.sh up`, which starts a Mosquitto container and runs `dev/trafficgen`. The certificates and password file are generated into `dev/broker/generated/` on first run.

| Host port | Listener                                                                  |
| --------- | ------------------------------------------------------------------------- |
| 21883     | TCP, anonymous                                                            |
| 21884     | TCP, `maqtt` / `maqtt`                                                    |
| 28883     | TLS (CA `dev/broker/generated/ca.crt`), `maqtt` / `maqtt`                 |
| 28884     | mTLS (`client.crt` + `client.key` from the same folder), user `device-01` |

The ports are offset from the MQTT defaults so they don't clash with a locally installed Mosquitto.

Examples of other traffic shapes (see `cargo run -p trafficgen -- --help` for all options):

```sh
dev/env.sh up --rate 2000             # heavy sensor traffic
dev/env.sh up --fleet 10000           # 10k extra topics for Topic Tree stress tests
dev/env.sh down                       # stop the Broker
```

## Broker Profiles

Profiles are stored in `~/.config/maqtt/profiles.toml` (Linux) or `~/Library/Application Support/maqtt/profiles.toml` (macOS); set `MAQTT_CONFIG_DIR` to use another directory. Passwords are kept in the OS keyring (Secret Service / Keychain), never in the file.

On first run, debug builds create Profiles for the dev Broker (plain, TLS and mTLS) so you can connect straight away.

## Checks

```sh
pnpm lint      # ESLint, Prettier, rustfmt, clippy
pnpm format    # Prettier + rustfmt
pnpm test      # Vitest + cargo test
pnpm test:broker  # integration tests against the dev Broker (needs `dev/env.sh broker`)
```
