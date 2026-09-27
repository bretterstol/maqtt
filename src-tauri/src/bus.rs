//! The Message Bus: every Connection publishes onto it, and everything downstream
//! (UI Bridge, History Store, later processing) consumes from it. See ADR 0002.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

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
    next_seq: Arc<AtomicU64>,
}

impl Bus {
    pub fn new() -> Self {
        let (tx, _) = broadcast::channel(CAPACITY);
        Self {
            tx,
            next_seq: Arc::new(AtomicU64::new(1)),
        }
    }

    /// The seq for the next message a Connection puts on the bus.
    pub fn next_seq(&self) -> u64 {
        self.next_seq.fetch_add(1, Ordering::Relaxed)
    }

    pub fn publish(&self, event: BusEvent) {
        // No receivers is fine: nobody is listening yet.
        let _ = self.tx.send(event);
    }

    pub fn subscribe(&self) -> broadcast::Receiver<BusEvent> {
        self.tx.subscribe()
    }
}
