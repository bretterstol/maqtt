//! Connections: live sessions to Brokers, each opened from a Broker Profile and run as its
//! own task that feeds the Message Bus.

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use serde::Serialize;
use tokio::sync::Notify;

use crate::bus::{Bus, BusEvent};
use crate::message::{ConnectionId, ConnectionMarker, MarkerEvent, ReceivedMessage, now_ms};
use crate::mqtt::{self, Incoming};
use crate::profile::{Profile, ProfileId, Subscription};

const BACKOFF_FIRST: Duration = Duration::from_secs(1);
const BACKOFF_MAX: Duration = Duration::from_secs(30);
/// How long a closing Connection waits for its DISCONNECT to go out.
const DISCONNECT_GRACE: Duration = Duration::from_secs(2);

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ConnectionStatus {
    pub connection_id: ConnectionId,
    pub profile_id: ProfileId,
    pub name: String,
    pub state: ConnectionState,
    pub subscriptions: Vec<SubscriptionStatus>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
// `rename_all_fields` too: `rename_all` alone renames variants, not their fields.
#[serde(
    tag = "kind",
    rename_all = "camelCase",
    rename_all_fields = "camelCase"
)]
pub enum ConnectionState {
    Connecting,
    Connected,
    /// Lost or refused; retrying at `retry_at_ms`.
    Disconnected {
        reason: String,
        retry_at_ms: u64,
    },
    /// Closed on request; no longer retrying.
    Closed,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SubscriptionStatus {
    pub filter: String,
    pub qos: u8,
    pub state: SubscriptionState,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
// `rename_all_fields` too: `rename_all` alone renames variants, not their fields.
#[serde(
    tag = "kind",
    rename_all = "camelCase",
    rename_all_fields = "camelCase"
)]
pub enum SubscriptionState {
    Pending,
    Active { granted_qos: u8 },
    Rejected { reason: String },
}

/// The latest known state of every Connection, so late subscribers to the Message Bus
/// (a freshly loaded UI) can start from a snapshot instead of waiting for the next change.
#[derive(Clone, Default)]
pub struct ConnectionStatuses {
    statuses: Arc<Mutex<HashMap<ConnectionId, ConnectionStatus>>>,
}

impl ConnectionStatuses {
    pub fn snapshot(&self) -> Vec<ConnectionStatus> {
        let mut statuses: Vec<_> = self.statuses.lock().unwrap().values().cloned().collect();
        statuses.sort_by_key(|s| s.connection_id.0);
        statuses
    }

    pub fn publish(&self, bus: &Bus, status: ConnectionStatus) {
        self.statuses
            .lock()
            .unwrap()
            .insert(status.connection_id, status.clone());
        bus.publish(BusEvent::ConnectionState(status));
    }
}

/// Everything a Connection shares between its task and the commands sent to it.
struct Session {
    status: ConnectionStatus,
    connected: bool,
    /// Filters sent in Subscribe requests, waiting to learn their packet id.
    awaiting_pkid: VecDeque<String>,
    /// Filters waiting for the Broker's SubAck, by packet id.
    awaiting_suback: HashMap<u16, String>,
}

impl Session {
    fn request_subscription(&mut self, client: &mqtt::Client, filter: &str) {
        let Some(sub) = self
            .status
            .subscriptions
            .iter_mut()
            .find(|s| s.filter == filter)
        else {
            return;
        };
        sub.state = SubscriptionState::Pending;
        // `try_` because awaiting here would block the task that drains the requests.
        match client.try_subscribe(filter, sub.qos) {
            Ok(()) => self.awaiting_pkid.push_back(filter.to_string()),
            Err(err) => sub.state = SubscriptionState::Rejected { reason: err },
        }
    }

    fn on_suback(&mut self, pkid: u16, results: Vec<Result<u8, String>>) {
        let Some(filter) = self.awaiting_suback.remove(&pkid) else {
            return;
        };
        let Some(sub) = self
            .status
            .subscriptions
            .iter_mut()
            .find(|s| s.filter == filter)
        else {
            return;
        };
        sub.state = match results.into_iter().next() {
            Some(Ok(granted_qos)) => SubscriptionState::Active { granted_qos },
            Some(Err(reason)) => SubscriptionState::Rejected { reason },
            None => SubscriptionState::Rejected {
                reason: "Empty SubAck".into(),
            },
        };
    }

    fn mark_offline(&mut self, state: ConnectionState) {
        self.connected = false;
        self.status.state = state;
        for sub in &mut self.status.subscriptions {
            sub.state = SubscriptionState::Pending;
        }
    }
}

#[derive(Clone)]
struct Context {
    id: ConnectionId,
    bus: Bus,
    statuses: ConnectionStatuses,
    session: Arc<Mutex<Session>>,
    /// Signalled by `Connection::close`.
    closing: Arc<Notify>,
    /// Signalled by `Connection::reset`: drop the network connection and reconnect now.
    reset: Arc<Notify>,
    /// Signalled by `Connection::retry_now`: skip the rest of the backoff wait.
    retry: Arc<Notify>,
}

impl Context {
    fn update(&self, change: impl FnOnce(&mut Session)) {
        let status = {
            let mut session = self.session.lock().unwrap();
            change(&mut session);
            session.status.clone()
        };
        self.statuses.publish(&self.bus, status);
    }

    fn marker(&self, event: MarkerEvent) {
        self.bus
            .publish(BusEvent::Marker(Arc::new(ConnectionMarker {
                seq: self.bus.next_seq(),
                connection_id: self.id,
                at_ms: now_ms(),
                event,
            })));
    }
}

pub struct Connection {
    client: mqtt::Client,
    context: Context,
    task: tauri::async_runtime::JoinHandle<()>,
}

impl Connection {
    pub fn open(
        id: ConnectionId,
        profile: &Profile,
        password: Option<&str>,
        bus: Bus,
        statuses: ConnectionStatuses,
    ) -> Result<Self, String> {
        let (client, eventloop) = mqtt::connect(profile, password)?;
        let session = Session {
            status: ConnectionStatus {
                connection_id: id,
                profile_id: profile.id.clone(),
                name: profile.name.clone(),
                state: ConnectionState::Connecting,
                subscriptions: profile
                    .subscriptions
                    .iter()
                    .map(|s| SubscriptionStatus {
                        filter: s.filter.clone(),
                        qos: s.qos,
                        state: SubscriptionState::Pending,
                    })
                    .collect(),
            },
            connected: false,
            awaiting_pkid: VecDeque::new(),
            awaiting_suback: HashMap::new(),
        };
        let context = Context {
            id,
            bus,
            statuses,
            session: Arc::new(Mutex::new(session)),
            closing: Arc::new(Notify::new()),
            reset: Arc::new(Notify::new()),
            retry: Arc::new(Notify::new()),
        };
        context.update(|_| {});
        let task = tauri::async_runtime::spawn(run(eventloop, client.clone(), context.clone()));
        Ok(Self {
            client,
            context,
            task,
        })
    }

    pub fn is_finished(&self) -> bool {
        self.task.inner().is_finished()
    }

    /// The client to publish with, if the Connection is online right now.
    pub fn publisher(&self) -> Result<mqtt::Client, String> {
        if self.context.session.lock().unwrap().connected {
            Ok(self.client.clone())
        } else {
            Err("Not connected to the Broker right now".into())
        }
    }

    /// Adds a Subscription for this session only, or changes its QoS.
    pub fn subscribe(&self, subscription: Subscription) -> Result<(), String> {
        subscription.validate()?;
        self.context.update(|session| {
            let subs = &mut session.status.subscriptions;
            match subs.iter_mut().find(|s| s.filter == subscription.filter) {
                Some(existing) => existing.qos = subscription.qos,
                None => subs.push(SubscriptionStatus {
                    filter: subscription.filter.clone(),
                    qos: subscription.qos,
                    state: SubscriptionState::Pending,
                }),
            }
            if session.connected {
                session.request_subscription(&self.client, &subscription.filter);
            }
        });
        Ok(())
    }

    /// Removes a Subscription for this session only.
    pub fn unsubscribe(&self, filter: &str) {
        self.context.update(|session| {
            session.status.subscriptions.retain(|s| s.filter != filter);
            if session.connected
                && let Err(err) = self.client.try_unsubscribe(filter)
            {
                eprintln!("maqtt: unsubscribe from {filter} failed: {err}");
            }
        });
    }

    /// Reconnects straight away, e.g. after the computer woke from sleep.
    pub fn reset(&self) {
        self.context.reset.notify_waiters();
    }

    /// Ends the backoff wait and tries again now.
    pub fn retry_now(&self) {
        self.context.retry.notify_waiters();
    }

    /// Disconnects gracefully and stops retrying; the task ends shortly after.
    pub fn close(&self) {
        if self.context.session.lock().unwrap().connected {
            let _ = self.client.try_disconnect();
        }
        self.context.closing.notify_one();
    }
}

/// Exponential backoff between reconnect attempts, with jitter so many Connections to one
/// Broker don't retry in lockstep.
#[derive(Default)]
struct Backoff {
    attempt: u32,
}

impl Backoff {
    /// The next wait; `jitter` in 0..1 scales it by 0.8 to 1.2.
    fn next(&mut self, jitter: f64) -> Duration {
        let base = BACKOFF_FIRST
            .saturating_mul(1 << self.attempt.min(16))
            .min(BACKOFF_MAX);
        self.attempt += 1;
        base.mul_f64(0.8 + 0.4 * jitter.clamp(0.0, 1.0))
    }

    fn reset(&mut self) {
        self.attempt = 0;
    }
}

fn jitter() -> f64 {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.subsec_nanos())
        .unwrap_or_default();
    f64::from(nanos % 1000) / 1000.0
}

/// Decides which link changes become Message Log markers, so a Broker that stays down for an
/// hour leaves one marker, not one per retry.
#[derive(Default)]
struct Link {
    online: bool,
    offline_since_ms: Option<u64>,
    failure_reported: bool,
}

impl Link {
    fn connected(&mut self, now_ms: u64) -> MarkerEvent {
        self.online = true;
        self.failure_reported = false;
        match self.offline_since_ms.take() {
            Some(since) => MarkerEvent::Reconnected {
                offline_ms: now_ms.saturating_sub(since),
            },
            None => MarkerEvent::Connected,
        }
    }

    fn failed(&mut self, reason: &str, now_ms: u64) -> Option<MarkerEvent> {
        if self.online {
            self.online = false;
            self.offline_since_ms = Some(now_ms);
            self.failure_reported = true;
            Some(MarkerEvent::Lost {
                reason: reason.into(),
            })
        } else if !self.failure_reported {
            self.failure_reported = true;
            Some(MarkerEvent::Failed {
                reason: reason.into(),
            })
        } else {
            None
        }
    }
}

/// What woke the Connection's task.
enum Wake {
    Event(Result<Incoming, String>),
    Reset,
    Closing,
}

async fn run(mut eventloop: mqtt::EventLoop, client: mqtt::Client, cx: Context) {
    let mut backoff = Backoff::default();
    let mut link = Link::default();

    loop {
        let wake = tokio::select! {
            biased;
            // Abandoning `poll` mid-way is fine for these two: the network connection is
            // dropped straight after.
            _ = cx.closing.notified() => Wake::Closing,
            _ = cx.reset.notified() => Wake::Reset,
            event = eventloop.poll() => Wake::Event(event),
        };
        match wake {
            Wake::Closing => break,
            Wake::Reset => {
                eventloop.reset();
                backoff.reset();
                if let Some(event) =
                    link.failed("Reconnecting after the computer woke up", now_ms())
                {
                    cx.marker(event);
                }
                cx.update(|session| session.mark_offline(ConnectionState::Connecting));
            }
            Wake::Event(Ok(Incoming::ConnAck)) => {
                backoff.reset();
                cx.marker(link.connected(now_ms()));
                cx.update(|session| {
                    session.connected = true;
                    session.status.state = ConnectionState::Connected;
                    session.awaiting_pkid.clear();
                    session.awaiting_suback.clear();
                    // A clean start forgets our Subscriptions, and a resumed session may have
                    // been changed while we were away, so (re)apply them all.
                    let filters: Vec<String> = session
                        .status
                        .subscriptions
                        .iter()
                        .map(|s| s.filter.clone())
                        .collect();
                    for filter in filters {
                        session.request_subscription(&client, &filter);
                    }
                });
            }
            Wake::Event(Ok(Incoming::Publish {
                topic,
                payload,
                qos,
                retain,
                properties,
            })) => {
                let message = ReceivedMessage {
                    seq: cx.bus.next_seq(),
                    connection_id: cx.id,
                    topic,
                    // `rumqttc` payloads are slices of its network read buffer; holding them in
                    // the History Store would pin those whole buffers, so copy them out.
                    payload: Bytes::copy_from_slice(&payload),
                    qos,
                    retain,
                    received_at_ms: now_ms(),
                    properties,
                };
                cx.bus.publish(BusEvent::Message(Arc::new(message)));
            }
            Wake::Event(Ok(Incoming::SubscribeSent { pkid })) => {
                let mut session = cx.session.lock().unwrap();
                if let Some(filter) = session.awaiting_pkid.pop_front() {
                    session.awaiting_suback.insert(pkid, filter);
                }
            }
            Wake::Event(Ok(Incoming::SubAck { pkid, results })) => {
                cx.update(|session| session.on_suback(pkid, results));
            }
            Wake::Event(Ok(Incoming::DisconnectSent | Incoming::Other)) => {}
            Wake::Event(Err(reason)) => {
                if let Some(event) = link.failed(&reason, now_ms()) {
                    cx.marker(event);
                }
                let delay = backoff.next(jitter());
                let retry_at_ms = now_ms() + delay.as_millis() as u64;
                cx.update(|session| {
                    session.mark_offline(ConnectionState::Disconnected {
                        reason,
                        retry_at_ms,
                    })
                });
                // Polling again reconnects.
                tokio::select! {
                    _ = cx.closing.notified() => break,
                    _ = cx.retry.notified() => {}
                    _ = cx.reset.notified() => backoff.reset(),
                    _ = tokio::time::sleep(delay) => {}
                }
                cx.update(|session| session.status.state = ConnectionState::Connecting);
            }
        }
    }

    let was_connected = cx.session.lock().unwrap().connected;
    if was_connected {
        // Let the DISCONNECT that `close` queued reach the Broker.
        let _ = tokio::time::timeout(DISCONNECT_GRACE, async {
            while let Ok(event) = eventloop.poll().await {
                if matches!(event, Incoming::DisconnectSent) {
                    break;
                }
            }
        })
        .await;
    }
    cx.marker(MarkerEvent::Closed);
    cx.update(|session| session.mark_offline(ConnectionState::Closed));
}

/// Whether the computer slept between two readings: the monotonic clock stands still during
/// sleep on Linux and macOS, while wall-clock time keeps going.
pub fn slept(monotonic: Duration, wall: Duration) -> bool {
    wall > monotonic + Duration::from_secs(10)
}

/// The live Connections, at most one per Broker Profile. A Profile keeps its ConnectionId for
/// the whole run, so reconnecting continues the same history.
#[derive(Default)]
pub struct ConnectionManager {
    live: HashMap<ProfileId, Connection>,
    ids: HashMap<ProfileId, ConnectionId>,
    next_id: u32,
}

impl ConnectionManager {
    pub fn connect(
        &mut self,
        profile: &Profile,
        password: Option<&str>,
        bus: &Bus,
        statuses: &ConnectionStatuses,
    ) -> Result<ConnectionId, String> {
        if self.get(&profile.id).is_some() {
            return Err(format!("{} is already connected", profile.name));
        }
        let id = *self.ids.entry(profile.id.clone()).or_insert_with(|| {
            self.next_id += 1;
            ConnectionId(self.next_id)
        });
        let connection = Connection::open(id, profile, password, bus.clone(), statuses.clone())?;
        self.live.insert(profile.id.clone(), connection);
        Ok(id)
    }

    /// The Profile's live Connection, unless it has finished closing.
    pub fn get(&self, profile_id: &ProfileId) -> Option<&Connection> {
        self.live.get(profile_id).filter(|c| !c.is_finished())
    }

    /// Queues a plain message on the Connection with this name, without waiting: for a
    /// Pipeline's `to broker`, which names Connections as `from` does.
    pub fn try_publish_by_name(
        &self,
        statuses: &ConnectionStatuses,
        name: &str,
        topic: &str,
        payload: Vec<u8>,
    ) -> Result<(), String> {
        let profile_id = statuses
            .snapshot()
            .into_iter()
            .find(|s| s.name == name && self.ids.get(&s.profile_id) == Some(&s.connection_id))
            .map(|s| s.profile_id)
            .ok_or_else(|| format!("there is no Connection called {name}"))?;
        self.get(&profile_id)
            .ok_or_else(|| format!("{name} isn't connected"))?
            .publisher()?
            .try_publish(topic, payload)
            .map_err(|_| format!("{name} is too busy to send right now"))
    }

    pub fn reset_all(&self) {
        for connection in self.live.values().filter(|c| !c.is_finished()) {
            connection.reset();
        }
    }

    pub fn disconnect(&mut self, profile_id: &ProfileId) {
        if let Some(connection) = self.live.remove(profile_id) {
            connection.close();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn states_serialize_with_camel_case_fields_for_the_frontend() {
        use serde_json::{json, to_value};
        let disconnected = ConnectionState::Disconnected {
            reason: "x".into(),
            retry_at_ms: 5,
        };
        assert_eq!(
            to_value(disconnected).unwrap(),
            json!({"kind": "disconnected", "reason": "x", "retryAtMs": 5})
        );
        assert_eq!(
            to_value(SubscriptionState::Active { granted_qos: 1 }).unwrap(),
            json!({"kind": "active", "grantedQos": 1})
        );
        assert_eq!(
            to_value(MarkerEvent::Reconnected { offline_ms: 7 }).unwrap(),
            json!({"kind": "reconnected", "offlineMs": 7})
        );
    }

    #[test]
    fn backoff_doubles_from_one_second_up_to_thirty() {
        let mut backoff = Backoff::default();
        let waits: Vec<u64> = (0..8).map(|_| backoff.next(0.5).as_secs()).collect();
        assert_eq!(waits, [1, 2, 4, 8, 16, 30, 30, 30]);
        backoff.reset();
        assert_eq!(backoff.next(0.5), Duration::from_secs(1));
    }

    #[test]
    fn backoff_jitter_stays_within_twenty_percent() {
        let mut low = Backoff { attempt: 3 };
        let mut high = Backoff { attempt: 3 };
        assert_eq!(low.next(0.0), Duration::from_millis(6_400));
        assert_eq!(high.next(1.0), Duration::from_millis(9_600));
    }

    #[test]
    fn a_broker_that_stays_down_leaves_one_marker() {
        let mut link = Link::default();
        assert_eq!(
            link.failed("refused", 0),
            Some(MarkerEvent::Failed {
                reason: "refused".into()
            })
        );
        assert_eq!(link.failed("refused", 1_000), None);
        assert_eq!(link.connected(2_000), MarkerEvent::Connected);
    }

    #[test]
    fn a_lost_connection_reports_how_long_it_was_offline() {
        let mut link = Link::default();
        link.connected(0);
        assert_eq!(
            link.failed("timeout", 10_000),
            Some(MarkerEvent::Lost {
                reason: "timeout".into()
            })
        );
        assert_eq!(link.failed("refused", 11_000), None);
        assert_eq!(
            link.connected(25_000),
            MarkerEvent::Reconnected { offline_ms: 15_000 }
        );
        assert_eq!(
            link.failed("again", 30_000),
            Some(MarkerEvent::Lost {
                reason: "again".into()
            })
        );
    }

    #[test]
    fn sleep_is_detected_when_wall_time_outruns_monotonic_time() {
        let five = Duration::from_secs(5);
        assert!(!slept(five, five + Duration::from_millis(300)));
        assert!(slept(five, Duration::from_secs(600)));
    }
}

/// Against the dev Broker from `dev/env.sh`; run with `cargo test -p maqtt -- --ignored`.
#[cfg(test)]
mod dev_broker_tests {
    use super::*;
    use crate::dev_profiles;
    use crate::profile_store::{MemorySecrets, ProfileStore};
    use std::collections::HashSet;

    async fn wait_until(what: &str, mut done: impl FnMut() -> bool) {
        for _ in 0..100 {
            if done() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        panic!("timed out waiting until {what}");
    }

    #[tokio::test]
    #[ignore = "needs the dev Broker"]
    async fn the_dev_profiles_connect_side_by_side() {
        let path =
            std::env::temp_dir().join(format!("maqtt-it-{}/profiles.toml", std::process::id()));
        let mut profiles = ProfileStore::load(path, Box::new(MemorySecrets::default())).unwrap();
        dev_profiles::seed(&mut profiles).unwrap();

        let bus = Bus::new();
        let mut rx = bus.subscribe();
        let statuses = ConnectionStatuses::default();
        let mut manager = ConnectionManager::default();
        for view in profiles.list() {
            let password = profiles.password(&view.profile.id).unwrap();
            manager
                .connect(&view.profile, password.as_deref(), &bus, &statuses)
                .unwrap();
        }
        assert_eq!(statuses.snapshot().len(), 3);

        wait_until("all three are connected and subscribed", || {
            statuses.snapshot().iter().all(|s| {
                s.state == ConnectionState::Connected
                    && s.subscriptions
                        .iter()
                        .all(|sub| matches!(sub.state, SubscriptionState::Active { .. }))
            })
        })
        .await;

        let topic = format!("maqtt/it/{}", std::process::id());
        let status = std::process::Command::new("mosquitto_pub")
            .args([
                "-h",
                "localhost",
                "-p",
                "21883",
                "-t",
                &topic,
                "-m",
                "hello",
            ])
            .status()
            .expect("mosquitto_pub");
        assert!(status.success());

        let mut from = HashSet::new();
        tokio::time::timeout(Duration::from_secs(5), async {
            while from.len() < 3 {
                if let Ok(BusEvent::Message(m)) = rx.recv().await
                    && m.topic == topic
                {
                    assert_eq!(&m.payload[..], b"hello");
                    from.insert(m.connection_id);
                }
            }
        })
        .await
        .expect("the message arrives on every Connection");

        for view in profiles.list() {
            assert!(manager.get(&view.profile.id).is_some());
            manager.disconnect(&view.profile.id);
        }
        wait_until("all three are closed", || {
            statuses
                .snapshot()
                .iter()
                .all(|s| s.state == ConnectionState::Closed)
        })
        .await;
    }

    /// Publishes with `mosquitto_pub` to the dev Broker's plain listener; an empty payload is
    /// sent as a null message (`-n`), which is how a Retained Message is cleared.
    fn publish(topic: &str, payload: &str, retain: bool) {
        let mut command = std::process::Command::new("mosquitto_pub");
        command.args(["-h", "localhost", "-p", "21883", "-t", topic]);
        if payload.is_empty() {
            command.arg("-n");
        } else {
            command.args(["-m", payload]);
        }
        if retain {
            command.arg("-r");
        }
        assert!(command.status().expect("mosquitto_pub").success());
    }

    /// The retain flags seen for `topic`, in order, until `count` messages arrived.
    async fn retain_flags(
        rx: &mut tokio::sync::broadcast::Receiver<BusEvent>,
        topic: &str,
        count: usize,
    ) -> Vec<(bool, bool)> {
        let mut seen = Vec::new();
        tokio::time::timeout(Duration::from_secs(5), async {
            while seen.len() < count {
                if let Ok(BusEvent::Message(m)) = rx.recv().await
                    && m.topic == topic
                {
                    seen.push((m.retain, m.payload.is_empty()));
                }
            }
        })
        .await
        .expect("messages arrive");
        seen
    }

    #[tokio::test]
    #[ignore = "needs the dev Broker"]
    async fn retain_as_published_keeps_live_retain_flags_on_mqtt5_only() {
        for (protocol, live_flag) in [
            (crate::profile::Protocol::Mqtt5, true),
            (crate::profile::Protocol::Mqtt311, false),
        ] {
            let topic = format!("maqtt/it/retained/{}/{protocol:?}", std::process::id());
            publish(&topic, "stored", true);

            let mut profile = Profile::new("retain test", "localhost", 21883);
            profile.protocol = protocol;
            profile.subscriptions = vec![Subscription {
                filter: topic.clone(),
                qos: 0,
            }];
            let bus = Bus::new();
            let mut rx = bus.subscribe();
            let statuses = ConnectionStatuses::default();
            let mut manager = ConnectionManager::default();
            manager.connect(&profile, None, &bus, &statuses).unwrap();

            // The replay on subscribe is always flagged retained.
            assert_eq!(retain_flags(&mut rx, &topic, 1).await, [(true, false)]);

            publish(&topic, "updated", true);
            publish(&topic, "", true);
            assert_eq!(
                retain_flags(&mut rx, &topic, 2).await,
                [(live_flag, false), (live_flag, true)],
                "{protocol:?}"
            );
            manager.disconnect(&profile.id);
        }
    }

    #[tokio::test]
    #[ignore = "needs the dev Broker"]
    async fn mqtt5_publish_properties_reach_the_message_detail() {
        let topic = format!("maqtt/it/props/{}", std::process::id());
        let mut profile = Profile::new("props test", "localhost", 21883);
        profile.subscriptions = vec![Subscription {
            filter: topic.clone(),
            qos: 0,
        }];
        let bus = Bus::new();
        let mut rx = bus.subscribe();
        let statuses = ConnectionStatuses::default();
        let mut manager = ConnectionManager::default();
        manager.connect(&profile, None, &bus, &statuses).unwrap();
        wait_until("subscribed", || {
            statuses.snapshot().iter().all(|s| {
                s.subscriptions
                    .iter()
                    .all(|sub| matches!(sub.state, SubscriptionState::Active { .. }))
            })
        })
        .await;

        let status = std::process::Command::new("mosquitto_pub")
            .args([
                "-V",
                "5",
                "-h",
                "localhost",
                "-p",
                "21883",
                "-t",
                &topic,
                "-m",
                "{}",
            ])
            .args(["-D", "publish", "content-type", "application/json"])
            .args(["-D", "publish", "user-property", "unit", "celsius"])
            .args(["-D", "publish", "response-topic", "reply/here"])
            .args(["-D", "publish", "correlation-data", "req-1"])
            .args(["-D", "publish", "message-expiry-interval", "60"])
            .status()
            .expect("mosquitto_pub");
        assert!(status.success());

        let message = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if let Ok(BusEvent::Message(m)) = rx.recv().await
                    && m.topic == topic
                {
                    return m;
                }
            }
        })
        .await
        .expect("the message arrives");

        let detail =
            serde_json::to_value(crate::ui_bridge::MessageDetail::from(message.as_ref())).unwrap();
        let properties = &detail["properties"];
        assert_eq!(properties["contentType"], "application/json");
        assert_eq!(properties["responseTopic"], "reply/here");
        assert_eq!(properties["correlationData"]["text"], "req-1");
        assert_eq!(properties["userProperties"][0][1], "celsius");
        assert!(properties["messageExpiryInterval"].as_u64().unwrap() <= 60);
        assert_eq!(detail["payload"]["text"], "{}");
        manager.disconnect(&profile.id);
    }

    /// The markers seen on the bus, until `until` matches one.
    async fn markers_until(
        rx: &mut tokio::sync::broadcast::Receiver<BusEvent>,
        timeout: Duration,
        until: impl Fn(&MarkerEvent) -> bool,
    ) -> Vec<MarkerEvent> {
        let mut seen = Vec::new();
        tokio::time::timeout(timeout, async {
            loop {
                if let Ok(BusEvent::Marker(m)) = rx.recv().await {
                    let done = until(&m.event);
                    seen.push(m.event.clone());
                    if done {
                        return;
                    }
                }
            }
        })
        .await
        .unwrap_or_else(|_| panic!("timed out; markers so far: {seen:?}"));
        seen
    }

    fn podman(action: &str) {
        let status = std::process::Command::new("podman")
            .args([action, "maqtt-dev-broker"])
            .stdout(std::process::Stdio::null())
            .status()
            .expect("podman");
        assert!(status.success(), "podman {action}");
    }

    fn all_subscribed(statuses: &ConnectionStatuses) -> bool {
        statuses.snapshot().iter().all(|s| {
            s.state == ConnectionState::Connected
                && s.subscriptions
                    .iter()
                    .all(|sub| matches!(sub.state, SubscriptionState::Active { .. }))
        })
    }

    #[tokio::test]
    #[ignore = "needs the dev Broker; restarts it"]
    async fn a_broker_restart_is_survived_with_markers() {
        let profile = Profile::new("restart test", "localhost", 21883);
        let bus = Bus::new();
        let mut rx = bus.subscribe();
        let statuses = ConnectionStatuses::default();
        let mut manager = ConnectionManager::default();
        manager.connect(&profile, None, &bus, &statuses).unwrap();
        let first = markers_until(&mut rx, Duration::from_secs(5), |e| {
            *e == MarkerEvent::Connected
        })
        .await;
        assert_eq!(first, [MarkerEvent::Connected]);

        podman("stop");
        let lost = markers_until(&mut rx, Duration::from_secs(10), |e| {
            matches!(e, MarkerEvent::Lost { .. })
        })
        .await;
        assert_eq!(lost.len(), 1);
        wait_until("disconnected with a retry time", || {
            statuses.snapshot().iter().any(|s| {
                matches!(&s.state, ConnectionState::Disconnected { retry_at_ms, .. } if *retry_at_ms > now_ms())
            })
        })
        .await;

        podman("start");
        let back = markers_until(&mut rx, Duration::from_secs(40), |e| {
            matches!(e, MarkerEvent::Reconnected { .. })
        })
        .await;
        // Retries while the Broker was down add no markers.
        assert_eq!(back.len(), 1, "{back:?}");
        wait_until("resubscribed", || all_subscribed(&statuses)).await;

        publish("maqtt/it/after-restart", "hello", false);
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if let Ok(BusEvent::Message(m)) = rx.recv().await
                    && m.topic == "maqtt/it/after-restart"
                {
                    return;
                }
            }
        })
        .await
        .expect("messages flow again");

        manager.disconnect(&profile.id);
        let closed = markers_until(&mut rx, Duration::from_secs(5), |e| {
            *e == MarkerEvent::Closed
        })
        .await;
        assert_eq!(closed, [MarkerEvent::Closed]);
    }

    #[tokio::test]
    #[ignore = "needs the dev Broker"]
    async fn a_reset_reconnects_straight_away() {
        let profile = Profile::new("reset test", "localhost", 21883);
        let bus = Bus::new();
        let mut rx = bus.subscribe();
        let statuses = ConnectionStatuses::default();
        let mut manager = ConnectionManager::default();
        manager.connect(&profile, None, &bus, &statuses).unwrap();
        wait_until("subscribed", || all_subscribed(&statuses)).await;

        manager.reset_all();
        let events = markers_until(&mut rx, Duration::from_secs(5), |e| {
            matches!(e, MarkerEvent::Reconnected { .. })
        })
        .await;
        assert!(
            matches!(&events[..], [MarkerEvent::Connected, MarkerEvent::Lost { reason }, MarkerEvent::Reconnected { .. }] if reason.contains("woke up")),
            "{events:?}"
        );
        wait_until("resubscribed", || all_subscribed(&statuses)).await;
        manager.disconnect(&profile.id);
    }

    async fn next_on(
        rx: &mut tokio::sync::broadcast::Receiver<BusEvent>,
        topic: &str,
    ) -> Arc<ReceivedMessage> {
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if let Ok(BusEvent::Message(m)) = rx.recv().await
                    && m.topic == topic
                {
                    return m;
                }
            }
        })
        .await
        .unwrap_or_else(|_| panic!("nothing arrived on {topic}"))
    }

    #[tokio::test]
    #[ignore = "needs the dev Broker"]
    async fn published_messages_round_trip_with_properties() {
        use crate::publish::{OutgoingMessage, OutgoingProperties};
        let topic = format!("maqtt/it/publish/{}", std::process::id());
        let mut profile = Profile::new("publish test", "localhost", 21883);
        profile.subscriptions = vec![Subscription {
            filter: format!("{topic}/#"),
            qos: 1,
        }];
        let bus = Bus::new();
        let mut rx = bus.subscribe();
        let statuses = ConnectionStatuses::default();
        let mut manager = ConnectionManager::default();
        manager.connect(&profile, None, &bus, &statuses).unwrap();
        wait_until("subscribed", || all_subscribed(&statuses)).await;
        let client = manager.get(&profile.id).unwrap().publisher().unwrap();

        let message = OutgoingMessage {
            topic: format!("{topic}/json"),
            payload: r#"{"on":true}"#.into(),
            qos: 1,
            retain: false,
            properties: Some(OutgoingProperties {
                content_type: Some("application/json".into()),
                response_topic: Some(format!("{topic}/reply")),
                correlation_data: Some("req-7".into()),
                message_expiry_interval: Some(30),
                payload_is_utf8: true,
                user_properties: vec![("source".into(), "maqtt".into())],
            }),
        };
        client.publish(&message).await.unwrap();
        let received = next_on(&mut rx, &message.topic).await;
        assert_eq!(&received.payload[..], br#"{"on":true}"#);
        assert_eq!(received.qos, 1);
        let props = received.properties.as_ref().unwrap();
        assert_eq!(props.content_type.as_deref(), Some("application/json"));
        assert_eq!(props.correlation_data.as_deref(), Some(&b"req-7"[..]));
        assert_eq!(props.payload_format_indicator, Some(1));
        assert_eq!(props.user_properties, [("source".into(), "maqtt".into())]);

        // Retained, then cleared: with Retain As Published both arrive flagged retained.
        let retained_topic = format!("{topic}/retained");
        let retained = OutgoingMessage {
            topic: retained_topic.clone(),
            payload: "kept".into(),
            qos: 1,
            retain: true,
            properties: None,
        };
        client.publish(&retained).await.unwrap();
        assert!(next_on(&mut rx, &retained_topic).await.retain);
        let clear = OutgoingMessage {
            payload: String::new(),
            ..retained
        };
        client.publish(&clear).await.unwrap();
        let cleared = next_on(&mut rx, &retained_topic).await;
        assert!(cleared.retain && cleared.payload.is_empty());

        // A fresh subscriber gets nothing retained any more.
        let output = std::process::Command::new("mosquitto_sub")
            .args([
                "-h",
                "localhost",
                "-p",
                "21883",
                "-t",
                &retained_topic,
                "--retained-only",
                "-W",
                "1",
            ])
            .output()
            .expect("mosquitto_sub");
        assert!(
            output.stdout.is_empty(),
            "{:?}",
            String::from_utf8_lossy(&output.stdout)
        );
        manager.disconnect(&profile.id);
    }

    #[tokio::test]
    #[ignore = "needs the dev Broker"]
    async fn mqtt311_refuses_properties() {
        use crate::publish::{OutgoingMessage, OutgoingProperties};
        let mut profile = Profile::new("3.1.1 publish test", "localhost", 21883);
        profile.protocol = crate::profile::Protocol::Mqtt311;
        let bus = Bus::new();
        let statuses = ConnectionStatuses::default();
        let mut manager = ConnectionManager::default();
        manager.connect(&profile, None, &bus, &statuses).unwrap();
        wait_until("subscribed", || all_subscribed(&statuses)).await;
        let client = manager.get(&profile.id).unwrap().publisher().unwrap();
        let message = OutgoingMessage {
            topic: "maqtt/it/v311".into(),
            payload: "x".into(),
            qos: 0,
            retain: false,
            properties: Some(OutgoingProperties {
                content_type: Some("text/plain".into()),
                ..OutgoingProperties::default()
            }),
        };
        assert!(client.publish(&message).await.is_err());
        let plain = OutgoingMessage {
            properties: None,
            ..message
        };
        assert!(client.publish(&plain).await.is_ok());
        manager.disconnect(&profile.id);
    }

    #[tokio::test]
    #[ignore = "needs the dev Broker"]
    async fn a_pipeline_turns_live_traffic_into_derived_topics() {
        use crate::processing::{
            lang,
            runner::{PIPELINES, Runner},
        };

        let base = format!("maqtt/it/pipe/{}", std::process::id());
        let profile = Profile::new("pipeline test", "localhost", 21883);
        let bus = Bus::new();
        let mut rx = bus.subscribe();
        let statuses = ConnectionStatuses::default();
        let mut manager = ConnectionManager::default();
        manager.connect(&profile, None, &bus, &statuses).unwrap();
        wait_until("subscribed", || all_subscribed(&statuses)).await;

        let runner = Runner::new(
            bus.clone(),
            statuses.clone(),
            Arc::new(|_, _, _| Err("not in this test".into())),
        );
        let source = format!(
            r#"from "pipeline test"
            |> filter topic "{base}/+/climate"
            |> map {{ celsius: .temperature, room: $levels[-2] }}
            |> filter .celsius > 22
            |> to app "hot/{{.room}}""#
        );
        runner
            .start("warm rooms", lang::parse(&source).unwrap())
            .unwrap();
        tokio::time::sleep(Duration::from_millis(200)).await;

        publish(
            &format!("{base}/office/climate"),
            r#"{"temperature": 19}"#,
            false,
        );
        publish(
            &format!("{base}/kitchen/climate"),
            r#"{"temperature": 24}"#,
            false,
        );

        let derived = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if let Ok(BusEvent::Message(m)) = rx.recv().await
                    && m.connection_id == PIPELINES
                {
                    return m;
                }
            }
        })
        .await
        .expect("a Derived Topic arrives");
        assert_eq!(derived.topic, "warm rooms/hot/kitchen");
        assert_eq!(&derived.payload[..], br#"{"celsius":24,"room":"kitchen"}"#);

        let status = runner.status("warm rooms").unwrap();
        assert_eq!((status.received, status.emitted, status.errors), (2, 1, 0));
        runner.stop("warm rooms");
        manager.disconnect(&profile.id);
    }

    #[tokio::test]
    #[ignore = "needs the dev Broker"]
    async fn a_pipeline_republishes_to_a_broker_without_reading_it_back() {
        use crate::processing::{lang, runner::Runner};

        let base = format!("maqtt/it/republish/{}", std::process::id());
        let profile = Profile::new("republish test", "localhost", 21883);
        let bus = Bus::new();
        let mut rx = bus.subscribe();
        let statuses = ConnectionStatuses::default();
        let manager = Arc::new(Mutex::new(ConnectionManager::default()));
        manager
            .lock()
            .unwrap()
            .connect(&profile, None, &bus, &statuses)
            .unwrap();
        wait_until("subscribed", || all_subscribed(&statuses)).await;

        let republish = {
            let (manager, statuses) = (manager.clone(), statuses.clone());
            Arc::new(move |name: &str, topic: &str, payload| {
                manager
                    .lock()
                    .unwrap()
                    .try_publish_by_name(&statuses, name, topic, payload)
            })
        };
        let runner = Runner::new(bus.clone(), statuses.clone(), republish);
        // Reads `in/`, writes `out/` on the same Broker; the Broker sends `out/` back to måqtt,
        // which the Pipeline must not take in again.
        let source = format!(
            r#"from "republish test"
            |> filter topic "{base}/in/+"
            |> map {{ doubled: .n * 2 }}
            |> to broker "republish test" "{base}/out/{{$levels[-1]}}""#
        );
        runner.start("copy", lang::parse(&source).unwrap()).unwrap();
        tokio::time::sleep(Duration::from_millis(200)).await;

        publish(&format!("{base}/in/a"), r#"{"n": 21}"#, false);

        let republished = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if let Ok(BusEvent::Message(m)) = rx.recv().await
                    && m.topic.starts_with(&format!("{base}/out/"))
                {
                    return m;
                }
            }
        })
        .await
        .expect("the republished message comes back from the Broker");
        assert_eq!(republished.topic, format!("{base}/out/a"));
        assert_eq!(&republished.payload[..], br#"{"doubled":42}"#);

        tokio::time::sleep(Duration::from_millis(300)).await;
        // The echo of `out/a` arrived too, and was left alone: still one sent.
        let status = runner.status("copy").unwrap();
        assert_eq!((status.emitted, status.errors), (1, 0), "{status:?}");
        runner.stop("copy");
        manager.lock().unwrap().disconnect(&profile.id);
    }
}
