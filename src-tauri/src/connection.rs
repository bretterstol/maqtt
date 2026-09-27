//! A Connection: one live session to a Broker, run as its own task that feeds the Message Bus.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use rumqttc::v5::mqttbytes::QoS;
use rumqttc::v5::mqttbytes::v5::Packet;
use rumqttc::v5::{AsyncClient, Event, MqttOptions};

use crate::bus::{Bus, BusEvent, ConnectionState, ConnectionStatus};
use crate::message::{ConnectionId, ReceivedMessage, now_ms};

/// The largest packet MQTT allows. `rumqttc` defaults to 10 KiB and drops the Connection on
/// anything bigger, which is wrong for a tool meant to show whatever the Broker sends.
const MAX_PACKET_SIZE: u32 = 268_435_455;
const RETRY_DELAY: Duration = Duration::from_secs(2);

/// What a Connection needs to reach its Broker. Stands in for the Broker Profile until M3.
#[derive(Debug, Clone)]
pub struct ConnectionSettings {
    pub name: String,
    pub host: String,
    pub port: u16,
    pub client_id: String,
    pub subscriptions: Vec<String>,
}

/// The latest known state of every Connection, so late subscribers to the Message Bus
/// (a freshly loaded UI) can start from a snapshot instead of waiting for the next change.
#[derive(Clone, Default)]
pub struct Connections {
    statuses: Arc<Mutex<HashMap<ConnectionId, ConnectionStatus>>>,
}

impl Connections {
    pub fn snapshot(&self) -> Vec<ConnectionStatus> {
        let mut statuses: Vec<_> = self.statuses.lock().unwrap().values().cloned().collect();
        statuses.sort_by_key(|s| s.connection_id.0);
        statuses
    }

    fn set(&self, bus: &Bus, status: ConnectionStatus) {
        self.statuses
            .lock()
            .unwrap()
            .insert(status.connection_id, status.clone());
        bus.publish(BusEvent::ConnectionState(status));
    }
}

pub fn spawn(id: ConnectionId, settings: ConnectionSettings, bus: Bus, connections: Connections) {
    tauri::async_runtime::spawn(run(id, settings, bus, connections));
}

async fn run(id: ConnectionId, settings: ConnectionSettings, bus: Bus, connections: Connections) {
    let status = |state| ConnectionStatus {
        connection_id: id,
        name: settings.name.clone(),
        state,
    };
    connections.set(&bus, status(ConnectionState::Connecting));

    let mut options = MqttOptions::new(&settings.client_id, &settings.host, settings.port);
    options.set_keep_alive(Duration::from_secs(30));
    options.set_max_packet_size(Some(MAX_PACKET_SIZE));
    let (client, mut eventloop) = AsyncClient::new(options, 64);

    loop {
        match eventloop.poll().await {
            Ok(Event::Incoming(Packet::ConnAck(_))) => {
                connections.set(&bus, status(ConnectionState::Connected));
                // Clean start means the Broker forgot our Subscriptions, so (re)apply them.
                // `try_subscribe` because awaiting here would block the loop that drains requests.
                for filter in &settings.subscriptions {
                    if let Err(err) = client.try_subscribe(filter.as_str(), QoS::AtMostOnce) {
                        eprintln!("maqtt: subscribe to {filter} failed: {err}");
                    }
                }
            }
            Ok(Event::Incoming(Packet::Publish(publish))) => {
                let message = ReceivedMessage {
                    seq: bus.next_seq(),
                    connection_id: id,
                    topic: String::from_utf8_lossy(&publish.topic).into_owned(),
                    // `rumqttc` payloads are slices of its network read buffer; holding them in
                    // the History Store would pin those whole buffers, so copy them out.
                    payload: Bytes::copy_from_slice(&publish.payload),
                    qos: publish.qos as u8,
                    retain: publish.retain,
                    received_at_ms: now_ms(),
                };
                bus.publish(BusEvent::Message(Arc::new(message)));
            }
            Ok(_) => {}
            Err(err) => {
                connections.set(
                    &bus,
                    status(ConnectionState::Disconnected {
                        reason: err.to_string(),
                    }),
                );
                // Polling again reconnects; proper backoff arrives in M6.
                tokio::time::sleep(RETRY_DELAY).await;
                connections.set(&bus, status(ConnectionState::Connecting));
            }
        }
    }
}
