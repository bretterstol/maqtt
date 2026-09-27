//! The Message Bus: every Connection publishes onto it, and everything downstream
//! (UI Bridge, later the History Store and processing) consumes from it. See ADR 0002.

use std::sync::Arc;

use serde::Serialize;
use tokio::sync::broadcast;

use crate::message::{ConnectionId, ReceivedMessage};

/// Enough headroom to absorb a few seconds of heavy traffic before a slow consumer lags.
const CAPACITY: usize = 65_536;

#[derive(Debug, Clone)]
pub enum BusEvent {
    Message(Arc<ReceivedMessage>),
    ConnectionState(ConnectionStatus),
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ConnectionStatus {
    pub connection_id: ConnectionId,
    pub name: String,
    pub state: ConnectionState,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "camelCase")]
pub enum ConnectionState {
    Connecting,
    Connected,
    Disconnected { reason: String },
}

#[derive(Clone)]
pub struct Bus {
    tx: broadcast::Sender<BusEvent>,
}

impl Bus {
    pub fn new() -> Self {
        let (tx, _) = broadcast::channel(CAPACITY);
        Self { tx }
    }

    pub fn publish(&self, event: BusEvent) {
        // No receivers is fine: nobody is listening yet.
        let _ = self.tx.send(event);
    }

    pub fn subscribe(&self) -> broadcast::Receiver<BusEvent> {
        self.tx.subscribe()
    }
}
