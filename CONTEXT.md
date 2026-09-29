# måqtt

A personal desktop MQTT client for exploring and debugging live traffic on one or more Brokers at the same time.

## Language

### Brokers and connections

**Broker**:
An external MQTT server that måqtt connects to. måqtt never acts as a Broker itself.
_Avoid_: Server, host

**Broker Profile**:
The saved settings needed to reach one Broker as one identity: address, transport, credentials and client ID.
_Avoid_: Connection settings, config, account

**Connection**:
A live session to a Broker, opened from a Broker Profile. A Broker Profile has at most one Connection at a time.
_Avoid_: Session, client, link

**Subscription**:
A topic filter that a Connection is subscribed to. Subscriptions saved on a Broker Profile are applied automatically when its Connection opens.
_Avoid_: Filter, watch

### Exploring

**Topic Tree**:
The single hierarchical view of every topic seen across all Connections, with one root per Connection and topics grouped by `/` levels beneath it, each showing its latest message.
_Avoid_: Explorer, browser

**Retained Message**:
A message the Broker stores for a topic and replays to every new subscriber. Clearing it means publishing an empty retained payload to that topic.
_Avoid_: Sticky message, last value

**Publish History**:
The list of messages recently published from måqtt, which can be sent again.
_Avoid_: Sent log, outbox

**Message Log**:
The chronological stream of messages received across all Connections, filterable by Connection, including markers for disconnects and reconnects.
_Avoid_: Feed, history, console

### Processing

**Pipeline**:
A piece of code, written in måqtt's pipeline language, that takes messages from a Source, passes them through Stages and hands the results to Sinks. It runs only while you have started it.
_Avoid_: Script, rule, flow, job

**Stage**:
One step of a Pipeline, named by a keyword (`from`, `filter`, `map`, `changes`, `throttle`, `debounce`, `scan`, `window`, `take`, `inspect`, `to`) and joined to the next with `|>`.
_Avoid_: Operator, step, node

**Source**:
Where a Pipeline's messages come from, named in its `from` Stage: the live messages of Connections, or records read from files (`from file`). Topics are narrowed afterwards with `filter topic`.
_Avoid_: Input, feed

**Sink**:
Where a Pipeline's results go: the app, a Broker (republishing), a file or a map.
_Avoid_: Output, target, destination

**Geo Sink**:
`to geo`: a Pipeline's values as points on a map, shown with Map in its output panel. One marker per key, or a list as a snapshot of the whole map.
_Avoid_: map Sink, `to map` (`map` is the Stage that reshapes values)

**Derived Topic**:
A topic created by a Pipeline's app Sink. It appears in the Topic Tree under the Pipelines root, in a branch per Pipeline, and can be explored like any received topic.
_Avoid_: Virtual topic, computed topic

**Preview**:
A dry run of the Pipeline being edited over recent history, showing what it would emit or where it fails, without running it for real.
_Avoid_: Test run, simulation
