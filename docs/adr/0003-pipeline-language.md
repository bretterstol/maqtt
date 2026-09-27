# Pipelines use måqtt's own pipe language, with jq expressions, run in the backend

Pipelines are written in a small language of our own: one keyword Stage per step, joined with `|>` (for example `from "home/+/temp" |> where .temperature > 22 |> to app "hot"`). The expressions inside a Stage (conditions and new shapes of data) are jq, evaluated with the `jaq` crate, so we design the Stages but not an expression language. Pipelines run as Rust tasks reading the Message Bus (ADR 0002), which keeps heavy processing off the UI and makes republishing and file Sinks straightforward.

## Considered options

- **jq alone**: pipe-shaped and proven, but it knows nothing of Sources, Sinks, time windows or MQTT, and its `|` would mean something different from our Stages.
- **TypeScript or JavaScript with a pipe library**: fully general, but it would run in the webview (competing with the UI and stopping on reload) or need a JavaScript engine embedded in the backend, and it is less pipe-shaped than wanted.
