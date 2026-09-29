# Pipelines plan

Pipelines let you write short `|>` programs that take live messages, transform them, and send the results somewhere: first back into måqtt as Derived Topics, later to Brokers, files and a map. Vocabulary follows [CONTEXT.md](../CONTEXT.md); the big decisions are [ADR 0003](./adr/0003-pipeline-language.md) (language and runtime) and [ADR 0004](./adr/0004-processing-is-removable.md) (a removable module).

Work happens on the `processing` branch. Each milestone is merged into `main` after you have tried it and approved it.

## Principles

- **Opt-in.** Nothing runs until you start a Pipeline. Nothing starts with the app unless you have marked it to.
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

| Stage      | Example                                                                                        | Does                                                                                                                                                                                                                                                      |
| ---------- | ---------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `from`     | `from "Dev Broker"`, `from "Dev Broker", "Office"`, `from all`                                 | Picks the Connections to read. Always first.                                                                                                                                                                                                              |
| `filter`   | `filter .temperature > 22`, `filter topic "home/#", "office/#"`, `filter not topic "+/status"` | Keeps messages where the jq condition is true, or whose topic matches any of the MQTT filters (with `not`: none of them).                                                                                                                                 |
| `map`      | `map { temp: .temperature / 10 }`                                                              | Replaces the current value with the jq result.                                                                                                                                                                                                            |
| `changes`  | `changes`, `changes by .room`                                                                  | Passes a message only if its value differs from the previous one with the same key (the incoming topic, unless `by` names one).                                                                                                                           |
| `throttle` | `throttle 5s`, `throttle 5s by .room`                                                          | At most one message per interval per key (the incoming topic, unless `by` names one); the latest wins.                                                                                                                                                    |
| `inspect`  | `inspect`, `inspect "after map"`                                                               | Shows the value in the Pipeline's panel and passes it on unchanged.                                                                                                                                                                                       |
| `to`       | `to app "hot/{$levels[2]}"`, `to broker "Office" "hot/{.room}"`                                | Sends the value to a Sink and passes it on, so several Sinks can sit along one Pipeline.                                                                                                                                                                  |
| `scan`     | `scan 0 ($acc + .)`, `scan 0 ($acc + 1) by .room`                                              | Carries a result from value to value: `$acc` is the previous result, the update gives the next, and each one is sent on. The start is one value; wrap anything longer in `( )`.                                                                           |
| `debounce` | `debounce 2s`, `debounce 2s by .room`                                                          | Sends the latest value once there has been no new one for the interval.                                                                                                                                                                                   |
| `take`     | `take 10`, `take 1 by .room`                                                                   | Passes the first N values, then the Pipeline finishes. With `by`: the first N per key, and it keeps running.                                                                                                                                              |
| `delta`    | `delta`, `delta by .meter`                                                                     | The change since the previous number with the same key (the incoming topic, unless `by` names one). The first per key sends nothing.                                                                                                                      |
| `pairwise` | `pairwise`, `pairwise by .room`                                                                | `[previous, current]` for values with the same key; the first per key sends nothing.                                                                                                                                                                      |
| `join`     | `join by $room as $sensor`, `join … needs "t", "h"`, `join … within 5m`                        | Keeps the latest value per field (named by `as`, else the topic) for each `by` key, and sends them as one object whenever one arrives. `needs` waits for listed fields; `within` drops fields gone quiet. Without `by`, everything joins into one object. |
| `window`   | `window 1m`, `window 10`, `window 1m by .room`                                                 | Collects values and sends them as a list when the window closes: every minute on the clock (12:00–12:01), or every N values. Empty windows send nothing.                                                                                                  |

A wildcard in `filter topic` can be named by writing a name right after it: `filter topic "maqtt/home/+room/#"` matches like `maqtt/home/+/#`, and from then on `$room` is the level the `+` matched, so `window 1m by $room` keeps a window per room. `#rest` gives the levels `#` matched as one string (`"climate/1"`), or null when it matched none. With several filters, a name is taken from the one that matched, and is null if that one doesn't have it. Names can't reuse a built-in variable, and `filter not topic` can't name anything, as nothing matched. The syntax follows the `mqtt-pattern` library; `{room}` would clash with filling in strings.

Durations are written `500ms`, `5s`, `1m`, `1h`. State (`changes`, `delta`, `pairwise`, `join`, `throttle`, `scan`, `debounce`, `window`, `take … by`) is kept **per incoming topic**, so sensors don't mix. When several topics feed one result, such as two sensors in one room, `by` gives the key to keep state by instead: `throttle 5s by .room`. After such a Stage, `$key` is the key a value was kept under: the `by` value, or the topic. Each Stage keeps state for at most 10,000 keys; past that, the least recently seen are forgotten.

A window's list is shaped with an ordinary `map`, helped by aggregate functions and `$key`. Objects may span lines and end with a trailing comma:

```
|> window 1m by .room
|> map {
     room: $key,
     avg:  avg(.temp),
     max:  max(.temp),
     readings: count,
   }
```

### Values and variables

- `.` is the **current value**. It starts as the payload (parsed as JSON when it is JSON, otherwise a string) and changes as `map` reshapes it.
- `$payload` is always the **original** payload.
- `$topic`, `$levels` (the topic split at `/`), `$connection`, `$time`, `$qos`, `$retained` and `$properties` always describe the original message.

Expressions inside `filter`, `map` and the other Stages are **jq**, evaluated by `jaq`.

### Strings

In every string, including strings inside jq expressions, `{…}` fills in the value of a jq expression: `"hot/{$levels[2]}"`, `"{.temp}°C"`. `{{` and `}}` give literal braces, as in Python f-strings. The parser translates this into jq's own `\(…)` before jq sees it.

### Derived Topics

`to app "hot/kitchen"` publishes a Derived Topic that appears in the Topic Tree as **Pipelines ▸ _pipeline name_ ▸ hot ▸ kitchen**. Derived Topics have history, diffs and an activity meter like any topic. They stay when the Pipeline stops (its branch is marked as stopped), and are cleared when it starts again.

`from all` doesn't read Derived Topics, so a Pipeline can't feed itself. `from pipeline` reads them by name; see Chaining Pipelines.

### Republishing

`to broker "Office" "hot/{.room}"` publishes the value on the Connection named `Office` (named as in `from`), QoS 0 and not retained. Strings go as text, anything else as JSON. A Broker that isn't connected, or a Connection too busy to take the message right away, counts as an error in the Pipeline's panel; sending never waits.

When the Pipeline reads that Broker, a topic that gets past its `from` and leading `filter topic` Stages would come straight back in, so it is refused as an error instead of sent. `filter not topic "hot/#"` right after `from` (or a `filter topic` that doesn't match the output) lets it through; `filter not topic "hot/#", "alerts/#"` keeps out several outputs at once. Preview shows what `to broker` would send, and the refusals, without sending anything.

That check goes by Connection name, but several Broker Profiles can reach one Broker (the dev profiles are three listeners on one Mosquitto). So a running Pipeline also remembers what it republished in the last 10 s (topic and payload, up to 10,000 messages) and leaves out any copy that comes back, on whichever Connection. Its panel counts them as "came back". Without this, `from "Dev Broker" |> to broker "Dev Broker (TLS)" "test/{$topic}"` republished its own output with one more level each round, until Mosquitto dropped the Connection for a topic over 200 levels deep.

### Files

`to file "temps.jsonl"` appends each value to a file, and passes it on like the other Sinks. The extension decides the format:

- `.jsonl` (or `.ndjson`): one JSON value per line, strings quoted.
- `.csv`: one row per value. A new file takes its columns from the first value's keys (`value` if it isn't an object) and writes them as a header; a file that already exists keeps its header. Keys without a column are left out of the row and reported as an error, so nothing changes shape silently. Text goes in as is, null as an empty cell, lists and objects as JSON.

The name is a string like a topic, so `to file "rooms/{$room}.csv"` writes a file per room; folders are created as needed. Names go into a `files` folder beside the `pipelines` folder (shown under the Pipeline list), unless they start with `/` or `~/`. `rotate daily` or `rotate hourly` puts the local date in the name (`temps.2026-09-29.jsonl`); `rotate 10MB` (or `KB`, `GB`) renames a full file with a timestamp (`temps.2026-09-29T14-03-12.jsonl`) and starts a new one. A Pipeline keeps at most 32 files open, writes on its own thread, and flushes after each batch. Preview shows what would be written, and writes nothing. The Pipelines tab lists the files in the folder under the Pipelines; picking one shows its size, when it changed, how to read it back (`from file "…"`), and its last 50 lines as they change. Files written with `/` or `~/` names are outside the folder and not listed.

### Reading files

`from file "recording.jsonl"` reads records instead of live messages: one message per JSON line or CSV row, read once, after which the Pipeline finishes. Several files are read one after the other (`from file "a.csv", "b.csv"`). Names resolve like `to file`'s, so a recording made with `to file` is read back by name.

- Each record is the payload. `$topic` is the file's name as written, so `filter topic` can tell files apart; `$connection` is `file`, `$time` when it was read.
- CSV rows become objects keyed by the header. Empty cells are null; numbers, `true`, `false` and JSON lists and objects come back typed; anything else is text.
- **Pace.** At most 1,000 records a second by default, so a Broker's request queue keeps up with `to broker`. `rate 200/s` changes that; `replay .at` uses a time in each record and keeps the gaps between records. The time may be seconds (as `$time` gives) or milliseconds since 1970 (numbers past 10^11 are taken as milliseconds), as a number or as text, or a timestamp such as `2026-09-29T12:00:00.25Z` or `2026-09-29 14:00:00+02:00`; one without an offset is local time. So `replay .recordTimestamp` works on an export with ISO timestamps, and `recordTimestamp: ($time | todate)` writes them, so `throttle` and `window` behave as they did when it was recorded. A record without a time goes at once and is reported.
- `follow` keeps reading lines added later, like `tail -f`, and waits for a line's end before taking it.
- A Pipeline can't `to file` into a file it reads.
- Preview runs over the first 100 records, timed as they would be read, and says if a file can't be read.

To record and replay traffic:

```
# record
from "Test" |> filter topic "plant/#" |> map { topic: $topic, at: $time, payload: . } |> to file "plant.jsonl"
# replay into dev, at the original pace
from file "plant.jsonl" replay .at |> map .payload |> to broker "Dev" "{$payload.topic}"
```

### Maps

`to geo` puts a point on the Pipeline's map for each value shaped `{lat, lon, label?}`, so fields are picked with an ordinary `map` first:

```
from "Fleet" |> filter topic "trucks/+id/gps" |> map { lat: .lat, lon: .lng, label: $id } |> to geo
```

- One marker per topic, or per `$key` after a `by`: it moves as new points come, with a trail of its last 20 positions. At most 1,000 markers are kept; past that, the one quiet longest goes.
- `lat` must be a number from -90 to 90 and `lon` from -180 to 180; other values are reported as errors.
- A list of points is a snapshot of the whole map: markers named in it move on (keeping their trails), new ones are placed, and the rest are removed. Each point is kept under its `label`, or its place in the list without one; a bad point is reported and the rest are still used. So `map { lat: .lat, lon: .lng, label: .id } |> window 3s |> to geo` shows exactly the trucks that reported in the last 3 s. Empty windows send nothing, so if every truck goes quiet the last snapshot stays.
- **Map** in the output panel shows the markers instead of the list: the draft's under Preview, the running Pipeline's under Live. It zooms to the markers when they first appear.
- The map is Leaflet (loaded only when first shown) with OpenStreetMap's raster tiles. That is the one server besides the Brokers that måqtt contacts, only while a map is on screen, and the app's content security policy allows images from `tile.openstreetmap.org` for it. OpenStreetMap's credit is shown as plain text, since a link would open inside the app.

### Charts

`to chart` draws a Pipeline's numbers as lines over time. The value is a number, or `{value, series?}` to name the line:

```
from "Dev Broker"
|> filter topic "maqtt/home/+room/#"
|> map { value: .temperature, series: $room }
|> to chart
```

- One line per series: the value's `series`, or `$key` after a `by`, or the topic. Each keeps its last 600 values. `window 1m by $room |> map avg(.temp) |> to chart` charts per-room averages.
- One value axis, never two: measures on different scales belong in separate Pipelines (or `series` indexed to a common base).
- At most 8 lines, the validated categorical palette's size; further series are counted and reported rather than given generated colours. A line's colour follows the order its series first appeared, so a new series never repaints the others.
- **Chart** in the output panel shows it, like Map: the draft's lines under Preview, the running Pipeline's under Live. A legend names each line with its latest value (the palette's lighter colours are under 3:1 on the light panel, so the text carries identity), hovering shows every line's value at the nearest time, and the newest value of each line has a dot.
- Drawn as SVG in the app itself: no chart library, and no network.

### Joining streams

`join` combines values from different topics into one object, a field each, from their latest values:

```
from "Dev Broker"
|> filter topic "home/+room/+sensor"
|> join by $room as $sensor needs "temperature", "humidity" within 5m
|> to app "rooms/{$key}"
```

- `by` says which values belong together (here one object per room, with `$key` the room); without it, everything joins into one object, as per topic each object would hold one part.
- `as` gives each value's field name in the object (here the sensor: `temperature`, `humidity`); without it, the topic does. Fields keep the order they first arrived in. It isn't called a key because `by` sets the key: which object a value goes into.
- It sends on every value that arrives, with the latest of the others, so the object follows the fastest sensor. `needs` holds everything back until the listed fields are all there.
- `within` leaves out fields older than the duration, so a dead sensor drops out instead of repeating its last value for ever.
- The clauses may come in any order; a `by` or `as` expression that uses jq's own `as` goes in `( )`.

### Chaining Pipelines

`from pipeline "clean"` reads another Pipeline's Derived Topics, so one Pipeline can tidy data and others build on it:

```
# clean: raw readings into one shape
from "Dev Broker" |> filter topic "raw/+room" |> map { t: .temp_c } |> to app "rooms/{$room}"

# hot: builds on clean
from pipeline "clean" |> filter topic "rooms/+room" |> filter .t > 25 |> to app "{$room}"
```

- `$topic` is the Derived Topic's path within that Pipeline (`rooms/kitchen`, what its `to app` wrote), so `filter topic` reads as it was written; `$connection` is the source Pipeline's name. `from pipeline "a", "b"` reads several.
- Loops are refused at start: a Pipeline can't read itself, and one whose start would close a cycle with running Pipelines (A reads B while B reads A, or through others) doesn't start and says why. As every start is checked, a loop can never be running.
- Preview samples the named Pipelines' recent Derived Topics from history.
- A source that isn't running sends nothing; renaming one means updating the `from pipeline` lines that name it.

### Starting with måqtt

**Start with måqtt** in a Pipeline's header marks it to start whenever the app starts, so a recording with `to file` or a mirror with `to broker` keeps going without anyone pressing Start. Ticking it doesn't start or stop the Pipeline now.

- Marked Pipelines start right after the Brokers set to autoconnect begin connecting. A Pipeline reading a Broker that isn't connected yet simply waits for its messages, as a started one always does.
- One that can't start (its file was edited into something that doesn't parse) shows "Didn't start with måqtt" and why in its panel. A mark for a file that's gone is dropped.
- The marks are kept in `settings.json` beside the `.pipe` files, not in the Pipeline's code, so the language stays about what a Pipeline does. Renaming a Pipeline carries its mark; deleting it removes it.

### Storage

One Pipeline per `.pipe` file in `~/.config/maqtt/pipelines/` (or `$MAQTT_CONFIG_DIR/pipelines/`), named after the file. They're plain text, so they work with git and your own editor.

## Architecture

```
 Connections ─► Message Bus ─┬─► History Store, UI Bridge (as today)
                             │
                             └─► Pipeline runner (one task per started Pipeline)
                                   bounded queue ─► Stages ─► Sinks
                                                               ├─ app Sink ─► Message Bus
                                                               │  (Derived Topics under the
                                                               │   Pipelines pseudo-Connection)
                                                               ├─ broker Sink ─► a Connection
                                                               ├─ file Sink ─► files on disk
                                                               ├─ map Sink ─► markers in the status
                                                               └─ chart Sink ─► lines in the status
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
- **Preview reads the History Store** through one closure that `lib.rs` hands to the module (`RecentMessages`), read-only. **`to broker` sends through another** (`Republish`), which queues a message on a Connection by name and says at once if it couldn't. These two and the bus are the only ways in.
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

Status: done on the `processing` branch. Checked over WebDriver: a mistake is underlined and Save refuses it, Start saves and runs, `inspect` values appear, and new, rename and delete work. The tab loads as its own chunk. Clearing a restarted Pipeline's old Derived Topics and marking a stopped one's branch followed on `pipelines-p4`, through a `TopicsCleared` bus event.

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

Decided together after P3:

- `scan <start> <update>` with `$acc`, per topic or `by` key
- `window <duration>` (on the clock) and `window <count>`, per topic or `by` key, sending a list; shaped with `map`
- Aggregate functions for lists: `avg(f)`, `sum(f)`, `min(f)`, `max(f)`, `count`
- `$key` after any Stage that keeps state
- `debounce <duration> [by …]`
- `take N` finishes the Pipeline after N values; `take N by …` passes the first N per key and keeps running
- Objects in expressions may span lines and end with a trailing comma
- Sliding windows (`window 5m every 1m`) later, if missed

**Done when:** the average temperature per room per minute can be written as in the example above, previewed on real data, and run.

Status: done on the `pipelines-p4` branch. Checked over WebDriver on live traffic: the example previews each room's average right away, and once started, the first window closed on the minute with one Derived Topic per room.

Hardening that came with it:

- Payloads nested deeper than 256 levels are treated as text: parsing one would overflow the stack and abort the app.
- The parser refuses `def`, `recurse(f)`, `repeat`, `while` and `until`, which can recurse without end, for the same reason.
- An expression may give at most 10,000 results per value.
- Stopping a Pipeline waits at most 1 s and sends nothing after; one stuck in an endless expression says so in its status.
- Preview runs on its own thread, one at a time, and gives up after 3 s.

### P5: More Sinks

1. **Republish**: `to broker "Office" "topic/{…}"`, with a guard against republishing into something the Pipeline itself reads. Done on the `pipelines-p5` branch; see Republishing above. Checked over WebDriver against the dev Broker: Preview lists what would be sent, a Pipeline reading its own output is refused in Preview, and once started the Broker receives the republished message while its echo isn't taken in again. A broker integration test covers the same round trip.
2. **File**: JSON Lines or CSV, rotating by size or day. Done on the `pipelines-p5-file` branch; see Files above.
3. **Map**: messages with latitude and longitude on a map. Decided together: Leaflet with OpenStreetMap tiles, values shaped with `map` first, and the map in the output panel. Named `to geo` rather than `to map`, as `map` is already the Stage that reshapes values; `to map` says so. Done on the `pipelines-p5-map` branch; see Maps above.

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
