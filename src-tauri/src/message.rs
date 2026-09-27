//! The MQTT-version-neutral shape of a message received on a Connection.

use std::time::{SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use serde::Serialize;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize)]
pub struct ConnectionId(pub u32);

#[derive(Debug, Clone)]
pub struct ReceivedMessage {
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
