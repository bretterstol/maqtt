# mÅqtt

A desktop MQTT client for exploring and debugging live traffic on several Brokers at once. Built with Tauri (Rust) and React, for Linux and macOS.

- Domain language: [CONTEXT.md](./CONTEXT.md)
- Decisions: [docs/adr/](./docs/adr/)
- Plans: [v1](./docs/v1-plan.md), [Pipelines](./docs/processing-plan.md), [Settings](./docs/settings-plan.md)

## Quick start

```sh
make dev       # dev Broker, fake traffic and the app in one terminal; Ctrl+C stops it all
make sandbox   # the same with a throwaway config, every Profile connected
make           # every shortcut
```

`make dev` uses your real config (`~/.config/maqtt`); the first run adds the dev Broker Profiles there. `make sandbox` keeps its config in `/tmp/maqtt-sandbox` and `make clean-sandbox` removes it, along with its keyring entry. `make stop` stops the dev Broker.

## Installing

Download a build from the [Releases](https://github.com/bretterstol/maqtt/releases) page, or build one yourself (below).

- **Fedora:** `sudo dnf install ./maqtt-<version>-1.x86_64.rpm`
- **Debian, Ubuntu:** `sudo apt install ./maqtt_<version>_amd64.deb`
- **Any Linux:** `chmod +x maqtt_<version>_amd64.AppImage` and run it
- **macOS:** open the `.dmg` and drag mÅqtt to Applications. The build isn't signed, so the first time, right-click the app and choose **Open**, or run `xattr -dr com.apple.quarantine /Applications/maqtt.app`. macOS asks once whether mÅqtt may use the Keychain; allow it so stored passwords work.

## Building a release

```sh
pnpm tauri build
```

This builds `target/release/bundle/` with a `.deb`, `.rpm` and AppImage on Linux, or a `.app` and `.dmg` on macOS. The AppImage is large (about 110 MB) because it carries its own WebKitGTK.

To publish one, bump the version in `package.json`, `src-tauri/Cargo.toml` and `src-tauri/tauri.conf.json`, then push a tag:

```sh
git tag v0.2.0 && git push origin v0.2.0
```

The `Release` workflow builds Linux packages and a universal macOS `.dmg` and attaches them to a draft release on GitHub; publish the draft when it looks right.

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

A Profile can have API Specs: AsyncAPI files (2.x or 3.x, YAML or JSON) describing the Broker's topics. Add them in the Profile editor; the Publish panel then suggests their topics, with parameters filled in from the spec and from topics already seen, and their payloads' keys and values, and underlines what a payload's schema doesn't allow. Specs are read where they are, and again when they change. With **Check received messages against these specs** ticked, each message received is compared with them: topics no channel describes, and payloads their schema doesn't allow, get a ≠ spec mark in the Topic Tree, and the topic's panel says how many differed and why.

Profiles are stored in `~/.config/maqtt/profiles.toml` (Linux) or `~/Library/Application Support/maqtt/profiles.toml` (macOS); set `MAQTT_CONFIG_DIR` to use another directory. Passwords are kept in the OS keyring (Secret Service / Keychain), never in the file.

On first run, debug builds create Profiles for the dev Broker (plain, TLS and mTLS) so you can connect straight away. A Profile with **Connect when mÅqtt starts** ticked connects at startup; `MAQTT_AUTOCONNECT=all` (or a comma-separated list of Profile names) connects others too, for a single run.

## Pipelines

The Pipelines tab runs short programs over live messages: `from` a Broker, a file or another Pipeline, through Stages such as `filter`, `map`, `window` and `join` (expressions are jq), `to` the app as Derived Topics, a Broker, a file, a map or a chart. The editor previews a draft on recent messages as you type, and the Reference panel beside it lists every Stage with examples.

Each Pipeline is a `.pipe` file in the `pipelines` folder beside `profiles.toml`, and `to file` and `from file` use the `files` folder next to it. Both can be edited outside mÅqtt. A Pipeline marked to start with mÅqtt does so on launch.

## Settings

The **Settings** button at the right of the title bar opens them: whether Pipelines and the Message Log are shown, history and scrollback limits, how the Topic Tree opens, and the theme. They're kept in `settings.toml` beside `profiles.toml`, readable and editable by hand. Whether a Broker connects on start is set in its Profile (**Connect when mÅqtt starts**).

## Checks

```sh
pnpm lint      # ESLint, Prettier, rustfmt, clippy
pnpm format    # Prettier + rustfmt
pnpm test      # Vitest + cargo test
pnpm types     # regenerate the frontend's copies of the Rust types (src/generated, src/processing/generated)
pnpm test:broker  # integration tests against the dev Broker, one at a time (needs `dev/env.sh broker`; restarts it)
```
