# Pipelines plan

Pipelines let you write short `|>` programs that take live messages, transform them, and send the results somewhere: first back into måqtt as Derived Topics, later to Brokers, files and a map. Vocabulary follows [CONTEXT.md](../CONTEXT.md); the big decisions are [ADR 0003](./adr/0003-pipeline-language.md) (language and runtime) and [ADR 0004](./adr/0004-processing-is-removable.md) (a removable module).

Work happens on the `processing` branch. Each milestone is merged into `main` after you have tried it and approved it.

## Principles

- **Opt-in.** Nothing runs until you start a Pipeline. Nothing starts with the app.
- **Isolated.** Each Pipeline is its own task with a bounded queue. A Pipeline that falls behind drops messages and says how many; it never slows the Connections, the tree or the log. A Pipeline that fails on a message skips it and reports the error in its own panel.
- **Removable.** All processing code lives in `src-tauri/src/processing/` and `src/processing/`. It reaches the rest of the app only through the Message Bus, its own commands, and one entry point each in the tab bar and the Topic Tree. Deleting it is a small, local change, and those entry points are where a later setting can hide it.

## The language

A Pipeline is a `from` line followed by Stages joined with `|>`. `#` starts a comment.

```
# Rooms that are too warm, at most one update per room every 10 s
from "Dev Broker"
|> filter topic "home/+/climate-1"
|> map { temp: .temperature, room: $levels[2] }
|> filter .temp > 22
|> changes
|> throttle 10s
|> to app "hot/{.room}"
```

### Stages

| Stage                | Example                                                                            | Does                                                                                                                            |
| -------------------- | ---------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------- |
| `from`               | `from "Dev Broker"`, `from "Dev Broker", "Office"`, `from all`                     | Picks the Connections to read. Always first.                                                                                    |
| `filter`             | `filter .temperature > 22`, `filter topic "home/#"`, `filter not topic "+/status"` | Keeps messages where the jq condition is true, or whose topic matches an MQTT filter.                                           |
| `map`                | `map { temp: .temperature / 10 }`                                                  | Replaces the current value with the jq result.                                                                                  |
| `changes`            | `changes`, `changes by .room`                                                      | Passes a message only if its value differs from the previous one with the same key (the incoming topic, unless `by` names one). |
| `throttle`           | `throttle 5s`, `throttle 5s by .room`                                              | At most one message per interval per key (the incoming topic, unless `by` names one); the latest wins.                          |
| `inspect`            | `inspect`, `inspect "after map"`                                                   | Shows the value in the Pipeline's panel and passes it on unchanged.                                                             |
| `to`                 | `to app "hot/{$levels[2]}"`                                                        | Sends the value to a Sink and passes it on, so several Sinks can sit along one Pipeline.                                        |
| `scan` _(later)_     | `scan { last: null } { last: ., delta: . - .acc.last }`                            | Carries state from message to message and emits it each time.                                                                   |
| `debounce` _(later)_ | `debounce 2s`                                                                      | Emits once a topic has been quiet for the interval.                                                                             |
| `take` _(later)_     | `take 10`                                                                          | Stops after N messages.                                                                                                         |
| windows _(later)_    | to be designed                                                                     | Aggregates over time, such as an average per minute.                                                                            |

Durations are written `500ms`, `5s`, `1m`, `1h`. State (`changes`, `throttle`, `scan`, `debounce`) is kept **per incoming topic**, so sensors don't mix. When several topics feed one result, such as two sensors in one room, `by` gives the key to keep state by instead: `throttle 5s by .room`.

### Values and variables

- `.` is the **current value**. It starts as the payload (parsed as JSON when it is JSON, otherwise a string) and changes as `map` reshapes it.
- `$payload` is always the **original** payload.
- `$topic`, `$levels` (the topic split at `/`), `$connection`, `$time`, `$qos`, `$retained` and `$properties` always describe the original message.

Expressions inside `filter`, `map` and the other Stages are **jq**, evaluated by `jaq`.

### Strings

In every string, including strings inside jq expressions, `{…}` fills in the value of a jq expression: `"hot/{$levels[2]}"`, `"{.temp}°C"`. `{{` and `}}` give literal braces, as in Python f-strings. The parser translates this into jq's own `\(…)` before jq sees it.

### Derived Topics

`to app "hot/kitchen"` publishes a Derived Topic that appears in the Topic Tree as **Pipelines ▸ _pipeline name_ ▸ hot ▸ kitchen**. Derived Topics have history, diffs and an activity meter like any topic. They stay when the Pipeline stops (its branch is marked as stopped), and are cleared when it starts again.

`from all` doesn't read Derived Topics, so a Pipeline can't feed itself. Reading another Pipeline's output (`from pipelines`) can come later if needed.

### Storage

One Pipeline per `.pipe` file in `~/.config/maqtt/pipelines/` (or `$MAQTT_CONFIG_DIR/pipelines/`), named after the file. They're plain text, so they work with git and your own editor.

## Architecture

```
 Connections ─► Message Bus ─┬─► History Store, UI Bridge (as today)
                             │
                             └─► Pipeline runner (one task per started Pipeline)
                                   bounded queue ─► Stages ─► Sinks
                                                               └─ app Sink ─► Message Bus
                                                                  (Derived Topics under the
                                                                   Pipelines pseudo-Connection)
```

- **Derived Topics go back onto the Message Bus** as ordinary messages from a reserved "Pipelines" pseudo-Connection. The History Store, Topic Index, Topic Tree, Message Log, detail panel and exports then work on them with no changes. That is what keeps processing removable.
- **Backend** (`src-tauri/src/processing/`):
  - `lang/`: the lexer and parser for the language, turning source into Stages with precise error positions, and the string rewrite from `{…}` to `\(…)`.
  - `jq.rs`: compiling and running jq expressions with `jaq-core`, `jaq-std` and `jaq-json`, with the message variables bound.
  - `stages.rs`: the runtime for each Stage, including per-topic state.
  - `runner.rs`: starting and stopping Pipelines, the bounded queue from the bus, and counters (in, out, dropped, errors).
  - `sinks/`: the app Sink first.
  - `store.rs`: `.pipe` files.
  - `mod.rs`: the Tauri commands.
- **Preview reads the History Store** through one closure that `lib.rs` hands to the module (`RecentMessages`), read-only. It's the only way in besides the bus.
- **Frontend** (`src/processing/`): the Pipelines tab (list, editor, Preview, output), a CodeMirror 6 language mode for highlighting, and calls through `bridge.ts`.

## Milestones

### P1: Engine and Derived Topics

- Lexer and parser for `from`, `filter` (jq and `topic`), `map`, `changes`, `throttle`, `inspect` and `to app`, with errors that name the line and column
- jq expressions through `jaq`, with `.`, `$payload`, `$topic`, `$levels` and the other variables
- `{…}` string filling
- A runner task per Pipeline, with a bounded queue, drop counting and errors per message
- The app Sink: Derived Topics on the bus under the Pipelines pseudo-Connection
- `.pipe` file storage; start and stop commands; a status stream (in, out, dropped, errors, last error)

Status: done on the `processing` branch. Checked in the app (two `.pipe` files started through the commands put Derived Topics under the Pipelines root) and by a broker integration test.

**Done when:** a `.pipe` file started through the backend puts Derived Topics into the Topic Tree from live dev Broker traffic. Parser, jq and Stage semantics are unit-tested, and a runaway Pipeline (a slow jq expression under heavy traffic) drops messages without slowing the Topic Tree.

### P2: The Pipelines tab

- A tab next to Topics and Message Log: the list of Pipelines (new, rename, delete, start, stop) with live status
- A CodeMirror 6 editor: highlighting for Stage keywords, jq and strings; inline errors from the parser
- Saving a running Pipeline restarts it; code that doesn't compile is refused and doesn't replace the running version
- Output: the latest `inspect` values and errors per Pipeline

**Done when:** you can write, save, start and watch a Pipeline entirely in the app, and a mistake shows where it is.

Status: done on the `processing` branch. Checked over WebDriver: a mistake is underlined and Save refuses it, Start saves and runs, `inspect` values appear, and new, rename and delete work. The tab loads as its own chunk. Still open: a restarted Pipeline's old Derived Topics stay in the tree, and a stopped Pipeline's branch isn't marked; both need a "forget topics" event.

### P3: Preview

- The draft runs, as you type, over the last ~100 matching messages from the History Store, without starting it
- Its output (or first error) shows next to the code, Stage by Stage

**Done when:** changing a `map` shows the new output within a moment, on real data, without starting the Pipeline.

Status: done on the `processing` branch. The draft runs over the newest 100 messages that pass `from` and the leading `filter topic` Stages (looking back up to 50,000 per Connection), replayed on their own timestamps so `throttle` behaves as it did live. Nothing is sent anywhere. After each Stage the editor shows how many values passed and the latest one, or the number of errors and the first. The output panel switches between Preview and the running Pipeline. It runs 300 ms after typing stops and every 3 s after that. Checked over WebDriver: a `map` change shows in about half a second.

### Help while writing

Added after P3, at your request. One file, `src/processing/reference.json`, describes every Stage, variable, piece of syntax and a set of everyday jq functions. It feeds three things: autocomplete, hover help and a searchable Reference panel. A Rust test parses every example in it and checks every function example's output, so the reference can't drift from the language.

- Autocomplete offers Stages after `|>` (with placeholders to fill in), `all` and Broker names after `from`, `$` variables, jq functions, and, after `.`, the field names found in recent real data at that point in the Pipeline.
- Hovering a Stage, variable or function explains it.

### P4: Stateful Stages

- `scan`, `debounce`, `take`
- Time windows (design them together first: the syntax for "average of `.temp` per minute")

### P5: More Sinks

1. **Republish**: `to broker "Office" "topic/{…}"`, with a guard against republishing into something the Pipeline itself reads
2. **File**: JSON Lines or CSV, rotating by size or day
3. **Map**: messages with latitude and longitude on a map (design together first: which map library, and how to pick the fields)

## Testing

- **Parser and language:** unit tests per Stage, error positions and string filling
- **Semantics:** tests with synthetic messages for each Stage (per-topic state, throttle timing, `changes`)
- **Integration:** a `pnpm test:broker` test that runs a Pipeline on dev Broker traffic and checks the Derived Topics
- **Visual:** a WebDriver scenario for the tab (P2) and Preview (P3), added to the report

## Risks and things to check early

- **`jaq` 3.x API and speed:** check in P1 that binding variables and compiling once per Pipeline (not per message) work as expected, and measure messages per second per Pipeline.
- **Language creep:** keep P1 to the Stages above, and add others only when a real Pipeline needs them.
- **String filling vs jq objects:** `{…}` means filling only inside strings; jq objects stay jq. Cover the edge cases with tests.
- **CodeMirror bundle size:** load the editor only when the Pipelines tab opens.

## Later

- A setting to hide Pipelines entirely
- Pipelines that start with the app
- Joining streams (for example matching temperature and humidity from different topics)
- `from pipelines`, reading other Pipelines' Derived Topics
- `delta` and `pairwise` as shortcuts built on `scan`
