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
