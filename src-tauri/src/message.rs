//! The MQTT-version-neutral shape of a message received on a Connection.

use std::time::{SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, ts_rs::TS)]
#[ts(export, export_to = "generated/")]
pub struct ConnectionId(pub u32);

#[derive(Debug, Clone)]
pub struct ReceivedMessage {
    /// Unique and increasing across all Connections, in the order messages entered the bus.
    pub seq: u64,
    pub connection_id: ConnectionId,
    pub topic: String,
    pub payload: Bytes,
    pub qos: u8,
    pub retain: bool,
    pub received_at_ms: u64,
    /// Present only for messages received over MQTT 5.
    pub properties: Option<MessageProperties>,
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct MessageProperties {
    pub payload_format_indicator: Option<u8>,
    pub message_expiry_interval: Option<u32>,
    pub content_type: Option<String>,
    pub response_topic: Option<String>,
    pub correlation_data: Option<Bytes>,
    pub user_properties: Vec<(String, String)>,
}

impl From<rumqttc::v5::mqttbytes::v5::PublishProperties> for MessageProperties {
    fn from(p: rumqttc::v5::mqttbytes::v5::PublishProperties) -> Self {
        Self {
            payload_format_indicator: p.payload_format_indicator,
            message_expiry_interval: p.message_expiry_interval,
            content_type: p.content_type,
            response_topic: p.response_topic,
            correlation_data: p.correlation_data,
            user_properties: p.user_properties,
        }
    }
}

/// A change in a Connection's link to its Broker, shown as a row in the Message Log.
#[derive(Debug, Clone)]
pub struct ConnectionMarker {
    /// From the same sequence as messages, so markers sort among them.
    pub seq: u64,
    pub connection_id: ConnectionId,
    pub at_ms: u64,
    pub event: MarkerEvent,
}

#[derive(Debug, Clone, PartialEq, Serialize, ts_rs::TS)]
// `rename_all_fields` too: `rename_all` alone renames variants, not their fields.
#[serde(
    tag = "kind",
    rename_all = "camelCase",
    rename_all_fields = "camelCase"
)]
#[ts(export, export_to = "generated/")]
pub enum MarkerEvent {
    Connected,
    Reconnected {
        offline_ms: u64,
    },
    /// An established connection dropped.
    Lost {
        reason: String,
    },
    /// The first failed attempt while never having been connected; later failures are quiet.
    Failed {
        reason: String,
    },
    /// Disconnected on request.
    Closed,
}

pub fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or_default()
}
