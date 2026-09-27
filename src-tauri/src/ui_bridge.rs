//! The UI Bridge: drains the Message Bus and streams it to the frontend in batches, so the
//! webview gets a steady ~20 updates per second no matter how busy the Brokers are.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde::Serialize;
use tauri::ipc::Channel;
use tokio::sync::broadcast::error::RecvError;
use tokio::sync::{broadcast, watch};

use crate::bus::{Bus, BusEvent};
use crate::connection::{ConnectionStatus, ConnectionStatuses};
use crate::history::{HistoryStore, LogFilter};
use crate::message::{ConnectionId, ReceivedMessage};
use crate::topics::TopicSnapshot;

const FLUSH_INTERVAL: Duration = Duration::from_millis(50);
/// Beyond this many messages per batch the frontend can't show them meaningfully anyway;
/// the oldest are dropped and counted.
const MAX_BATCH: usize = 2_000;
const PREVIEW_CHARS: usize = 200;
const PREVIEW_BYTES_BINARY: usize = 32;

#[derive(Debug, Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Batch {
    /// New Message Log entries matching the filter of `filter_generation`, oldest first.
    pub messages: Vec<LogEntry>,
    pub filter_generation: u64,
    pub connection_states: Vec<ConnectionStatus>,
    /// Topics that changed since the previous batch, each with its full current stats.
    pub topics: Vec<TopicUpdate>,
    /// Every message received since the previous batch, matching the filter or not.
    pub received: u64,
    /// Matching messages that never reached the frontend since the previous batch.
    pub dropped: u64,
}

/// The Message Log filter the live stream applies. Each change gets a new generation so the
/// frontend can tell batches filtered the old way from those filtered the new way.
#[derive(Debug, Clone, Default)]
pub struct LiveFilter {
    pub generation: u64,
    pub filter: LogFilter,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct LogEntry {
    pub seq: u64,
    pub connection_id: ConnectionId,
    pub topic: String,
    pub preview: String,
    pub binary: bool,
    pub payload_size: usize,
    pub qos: u8,
    pub retain: bool,
    pub received_at_ms: u64,
}

/// A topic's full current state for the Topic Tree. Absolute, not a delta, so updates can be
/// applied in any order: the one with the newest `latest.seq` wins.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TopicUpdate {
    pub connection_id: ConnectionId,
    pub topic: String,
    pub count: u64,
    pub retained: bool,
    pub latest: LogEntry,
}

impl From<TopicSnapshot<'_>> for TopicUpdate {
    fn from(s: TopicSnapshot<'_>) -> Self {
        Self {
            connection_id: s.connection_id,
            topic: s.topic.to_string(),
            count: s.stats.count,
            retained: s.stats.retained,
            latest: LogEntry::from(s.stats.latest.as_ref()),
        }
    }
}

impl From<&ReceivedMessage> for LogEntry {
    fn from(m: &ReceivedMessage) -> Self {
        let (preview, binary) = preview(&m.payload);
        Self {
            seq: m.seq,
            connection_id: m.connection_id,
            topic: m.topic.clone(),
            preview,
            binary,
            payload_size: m.payload.len(),
            qos: m.qos,
            retain: m.retain,
            received_at_ms: m.received_at_ms,
        }
    }
}

/// A single-line summary of a payload: the start of the text, or the first bytes as hex.
fn preview(payload: &[u8]) -> (String, bool) {
    match std::str::from_utf8(payload) {
        Ok(text) => {
            let mut preview: String = text
                .chars()
                .take(PREVIEW_CHARS)
                .map(|c| if c.is_control() { ' ' } else { c })
                .collect();
            if text.chars().nth(PREVIEW_CHARS).is_some() {
                preview.push('…');
            }
            (preview, false)
        }
        Err(_) => {
            let mut preview = payload
                .iter()
                .take(PREVIEW_BYTES_BINARY)
                .map(|b| format!("{b:02x}"))
                .collect::<Vec<_>>()
                .join(" ");
            if payload.len() > PREVIEW_BYTES_BINARY {
                preview.push_str(" …");
            }
            (preview, true)
        }
    }
}

pub fn spawn(
    bus: &Bus,
    statuses: &ConnectionStatuses,
    history: Arc<Mutex<HistoryStore>>,
    filter: watch::Receiver<LiveFilter>,
    channel: Channel<Batch>,
) -> tauri::async_runtime::JoinHandle<()> {
    // Subscribe before taking the snapshot so no state change falls between the two.
    let rx = bus.subscribe();
    let initial = statuses.snapshot();
    tauri::async_runtime::spawn(run(rx, initial, history, filter, channel))
}

async fn run(
    mut rx: broadcast::Receiver<BusEvent>,
    initial: Vec<ConnectionStatus>,
    history: Arc<Mutex<HistoryStore>>,
    mut filter: watch::Receiver<LiveFilter>,
    channel: Channel<Batch>,
) {
    let mut live = filter.borrow_and_update().clone();
    // Kept as messages and only turned into entries on flush, so dropped ones cost nothing.
    let mut pending: VecDeque<Arc<ReceivedMessage>> = VecDeque::new();
    let mut connection_states = initial;
    let mut received = 0u64;
    let mut dropped = 0u64;
    let mut tick = tokio::time::interval(FLUSH_INTERVAL);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        tokio::select! {
            event = rx.recv() => match event {
                Ok(BusEvent::Message(m)) => {
                    received += 1;
                    if !live.filter.matches(&m) {
                        continue;
                    }
                    if pending.len() == MAX_BATCH {
                        pending.pop_front();
                        dropped += 1;
                    }
                    pending.push_back(m);
                }
                Ok(BusEvent::ConnectionState(status)) => connection_states.push(status),
                Err(RecvError::Lagged(n)) => {
                    received += n;
                    dropped += n;
                }
                Err(RecvError::Closed) => return,
            },
            Ok(()) = filter.changed() => {
                live = filter.borrow_and_update().clone();
                // Filtered the old way; the frontend reloads from the History Store instead.
                pending.clear();
            },
            _ = tick.tick() => {
                let topics: Vec<TopicUpdate> = history
                    .lock()
                    .unwrap()
                    .topics
                    .take_changed()
                    .into_iter()
                    .map(TopicUpdate::from)
                    .collect();
                let idle = pending.is_empty()
                    && connection_states.is_empty()
                    && topics.is_empty()
                    && received == 0;
                if idle {
                    continue;
                }
                let batch = Batch {
                    messages: pending.drain(..).map(|m| LogEntry::from(m.as_ref())).collect(),
                    filter_generation: live.generation,
                    connection_states: std::mem::take(&mut connection_states),
                    topics,
                    received: std::mem::take(&mut received),
                    dropped: std::mem::take(&mut dropped),
                };
                if channel.send(batch).is_err() {
                    return;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn text_preview_is_single_line() {
        assert_eq!(preview(b"a\nb\tc"), ("a b c".to_string(), false));
    }

    #[test]
    fn long_text_is_truncated_on_a_char_boundary() {
        let text = "å".repeat(PREVIEW_CHARS + 5);
        let (preview, binary) = preview(text.as_bytes());
        assert!(!binary);
        assert_eq!(preview.chars().count(), PREVIEW_CHARS + 1);
        assert!(preview.ends_with('…'));
    }

    #[test]
    fn text_at_exact_limit_is_not_marked_truncated() {
        let text = "x".repeat(PREVIEW_CHARS);
        assert_eq!(preview(text.as_bytes()).0, text);
    }

    #[test]
    fn binary_preview_is_hex() {
        assert_eq!(preview(&[0xff, 0x00, 0x10]), ("ff 00 10".to_string(), true));
    }

    #[test]
    fn long_binary_preview_is_truncated() {
        let (preview, binary) = preview(&[0xffu8; PREVIEW_BYTES_BINARY + 1]);
        assert!(binary);
        assert!(preview.ends_with(" …"));
        assert_eq!(preview.matches("ff").count(), PREVIEW_BYTES_BINARY);
    }
}
