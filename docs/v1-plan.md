# måqtt v1 plan

v1 = explore, debug, and publish across several Brokers at once. Vocabulary follows [CONTEXT.md](../CONTEXT.md); architectural decisions live in [docs/adr/](./adr/).

Each milestone is a vertical slice that ends in something runnable and checked against the local dev Broker.

## Stack

- **Shell**: Tauri 2, targeting Linux and macOS ([ADR 0001](./adr/0001-rust-tauri-desktop-shell.md))
- **Backend**: Rust, `tokio`, `rumqttc` (both `rumqttc` and `rumqttc::v5`), `keyring`, `serde` + `toml`, `directories`
- **Frontend**: React + TypeScript + Vite, TanStack Virtual for large lists, Zustand for state, `jsondiffpatch` (or similar) for payload diffs
- **Identifiers**: `maqtt` (crate, binary, config dir); display name **måqtt**; bundle ID `no.<TBD>.maqtt`

## Architecture outline

```
 Connection A ─┐                         ┌─► History Store (per-topic + per-Connection caps)
 Connection B ─┼─► Message Bus (backend) ─┼─► UI Bridge ──batched──► React (Topic Tree, Message Log)
 Connection C ─┘                         └─► (later) processing, which may publish via any Connection
```

- **Connection actor**: one tokio task per live Connection. It owns the MQTT event loop, hides the 3.1.1/v5 split behind one internal message type, and handles reconnects and resubscribing.
- **Message Bus**: every received message, plus connection-state events, goes onto one bus ([ADR 0002](./adr/0002-central-message-bus.md)).
- **History Store**: lives in the backend and is the source of truth for message history. The frontend asks it for a topic's history or a page of the Message Log when needed. It never mirrors 100k messages.
- **UI Bridge**: groups bus traffic into batches at about 10–20 Hz and streams them over a Tauri `Channel`. The frontend keeps only the Topic Tree structure, latest values, counts and rates, plus the visible window of the Message Log.
- **Profile Store**: a TOML file in the platform config dir. Secrets are stored in the OS keyring, keyed by Profile ID.

## Milestones

### M0: Scaffold and dev environment

- `git init`, Tauri 2 + React + TS app, `cargo fmt`/`clippy`, ESLint/Prettier
- `dev/broker/`: Podman compose for Mosquitto with plain TCP, TLS (self-signed CA plus a client cert for mTLS), and username/password auth
- `dev/trafficgen`: a small Rust binary that publishes fake sensor traffic (steady JSON devices, bursty topics, retained topics, binary payloads) at a configurable rate

**Done when:** `pnpm tauri dev` opens an empty window, and the Broker plus trafficgen run with a single command (`pnpm dev:env`).

### M1: Tracer bullet (one Connection to the Message Log)

- A hardcoded Broker Profile connects over TCP with MQTT 5 and subscribes to `#`
- Messages go from the Connection actor through the Message Bus and UI Bridge to a basic virtualized Message Log

**Done when:** trafficgen traffic shows up live in the Message Log, and the UI stays responsive at 1k msg/s.

### M2: History Store and batching

- Per-topic cap (default 100) and per-Connection cap (default about 100k), dropping the oldest first
- Commands: fetch a topic's history, fetch a page of the Message Log with filters (Connection, payload substring)
- The UI Bridge throttles and batches, and reports dropped/overflow counts

**Done when:** memory plateaus under sustained 5k msg/s, the Message Log filter works, and the caps are covered by unit tests.

### M3: Broker Profiles and multiple Connections

- Profile Store (TOML plus keyring) and a Profile editor: address, port, MQTT version (3.1.1/5), client ID, credentials, TLS (custom CA, client cert/key, insecure toggle with a warning), clean vs persistent session, saved Subscriptions (default `#`), history cap
- Connection list: connect/disconnect per Profile, at most one Connection per Profile
- Add or remove Subscriptions on a live Connection
- MQTT 3.1.1 support behind the same internal message type

**Done when:** three Profiles (plain, TLS, mTLS) against the dev Broker are live at the same time, no secrets appear in the TOML, and settings survive a restart on both Linux and macOS.

### M4: Topic Tree

- One merged tree with a root per Connection and `/`-level grouping, showing latest value, message count and a retained badge
- Virtualized rendering, with lazy expansion for large trees
- Search by substring or MQTT wildcard (`+`, `#`) over topic names

**Done when:** trafficgen with 10k+ distinct topics stays smooth, wildcard search matches MQTT semantics (unit-tested), and retained topics show the badge after reconnecting.

### M5: Topic detail panel

- Payload view with auto-detection: JSON (pretty-printed), UTF-8 text, hex
- Metadata: QoS, retain, timestamp, and MQTT 5 properties when present
- The topic's history list (from the History Store) and message rate and count
- Diff of the selected message against the previous one (structural for JSON, by line for text)

**Done when:** selecting a trafficgen JSON device shows readable diffs between consecutive messages, and binary payloads render as hex.

### M6: Connection resilience

- Automatic reconnect with exponential backoff, resubscribing to all active Subscriptions
- The Topic Tree and history survive reconnects
- Disconnect and reconnect markers in the Message Log, and connection state shown on each Topic Tree root

**Done when:** stopping and restarting the dev Broker (and suspending the laptop) recovers without user action and shows the markers.

### M7: Publishing

- Publish panel: target Connection, topic, payload, QoS, retain, and an "advanced" section for MQTT 5 properties (user properties, content type, response topic, correlation data, message expiry)
- Publish History, re-sendable with one click
- "Clear retained" from the Topic Tree context menu, with confirmation

**Done when:** a message published from måqtt round-trips into the Topic Tree, and clearing a retained topic removes its retained value on the Broker.

### M8: Getting data out

- Copy topic or payload to the clipboard
- Export the filtered Message Log as JSON Lines
- Export and import Broker Profiles, without secrets

**Done when:** an exported file re-imports on the other OS and keeps all non-secret settings.

### M9: Packaging

- Local `cargo tauri build` produces a working app on Fedora and macOS
- Afterwards: a GitHub Actions workflow on tag builds an AppImage/.rpm and an unsigned .dmg

**Done when:** v1 is installed and used day-to-day on both machines.

## Risks and things to check early

- **3.1.1 vs 5 in `rumqttc`**: they are separate APIs. Spike the shared internal message type in M1/M3 before building on it.
- **IPC throughput**: prove in M1–M2 that batched `Channel` streaming keeps up. Fall back to sending binary batches if JSON serialization becomes the bottleneck.
- **Keyring on macOS with unsigned builds**: check that Keychain access prompts behave sensibly in M3.
- **Very large trees in React**: flattened, virtualized tree rows from day one; no recursive component per node.

## Out of scope for v1

In-flight processing, visualization/charts, saved publish templates, recording sessions to disk, WebSocket transport, split-pane layout, Sparkplug/Protobuf decoding.

## Open items

- Reverse domain for the bundle ID (`no.<TBD>.maqtt`)
