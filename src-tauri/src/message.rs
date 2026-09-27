//! The MQTT-version-neutral shape of a message received on a Connection.

use std::time::{SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
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
}

pub fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or_default()
}
