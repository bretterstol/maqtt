//! Connections: live sessions to Brokers, each opened from a Broker Profile and run as its
//! own task that feeds the Message Bus.

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use serde::Serialize;
use tokio::sync::Notify;

use crate::bus::{Bus, BusEvent};
use crate::message::{ConnectionId, ReceivedMessage, now_ms};
use crate::mqtt::{self, Incoming};
use crate::profile::{Profile, ProfileId, Subscription};

const RETRY_DELAY: Duration = Duration::from_secs(2);
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
#[serde(tag = "kind", rename_all = "camelCase")]
pub enum ConnectionState {
    Connecting,
    Connected,
    /// Lost or refused; retrying.
    Disconnected {
        reason: String,
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
#[serde(tag = "kind", rename_all = "camelCase")]
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

    fn publish(&self, bus: &Bus, status: ConnectionStatus) {
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

    /// Disconnects gracefully and stops retrying; the task ends shortly after.
    pub fn close(&self) {
        if self.context.session.lock().unwrap().connected {
            let _ = self.client.try_disconnect();
        }
        self.context.closing.notify_one();
    }
}

async fn run(mut eventloop: mqtt::EventLoop, client: mqtt::Client, cx: Context) {
    loop {
        let event = tokio::select! {
            biased;
            // Abandoning `poll` mid-way is fine here: the event loop is about to be dropped.
            _ = cx.closing.notified() => break,
            event = eventloop.poll() => event,
        };
        match event {
            Ok(Incoming::ConnAck) => cx.update(|session| {
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
            }),
            Ok(Incoming::Publish {
                topic,
                payload,
                qos,
                retain,
            }) => {
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
                };
                cx.bus.publish(BusEvent::Message(Arc::new(message)));
            }
            Ok(Incoming::SubscribeSent { pkid }) => {
                let mut session = cx.session.lock().unwrap();
                if let Some(filter) = session.awaiting_pkid.pop_front() {
                    session.awaiting_suback.insert(pkid, filter);
                }
            }
            Ok(Incoming::SubAck { pkid, results }) => {
                cx.update(|session| session.on_suback(pkid, results));
            }
            Ok(Incoming::DisconnectSent | Incoming::Other) => {}
            Err(reason) => {
                cx.update(|session| session.mark_offline(ConnectionState::Disconnected { reason }));
                // Polling again reconnects; proper backoff arrives in M6.
                tokio::select! {
                    _ = cx.closing.notified() => break,
                    _ = tokio::time::sleep(RETRY_DELAY) => {}
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
    cx.update(|session| session.mark_offline(ConnectionState::Closed));
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

    pub fn disconnect(&mut self, profile_id: &ProfileId) {
        if let Some(connection) = self.live.remove(profile_id) {
            connection.close();
        }
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
}
