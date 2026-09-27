//! The UI Bridge: drains the Message Bus and streams it to the frontend in batches, so the
//! webview gets a steady ~20 updates per second no matter how busy the Brokers are.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use base64::Engine;
use serde::Serialize;
use tauri::ipc::Channel;
use tokio::sync::broadcast::error::RecvError;
use tokio::sync::{broadcast, watch};

use crate::bus::{Bus, BusEvent};
use crate::connection::{ConnectionStatus, ConnectionStatuses};
use crate::history::{HistoryStore, LogFilter, LogItem};
use crate::message::{ConnectionId, MarkerEvent, ReceivedMessage};
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
    /// New Message Log rows matching the filter of `filter_generation`, oldest first.
    pub messages: Vec<LogRow>,
    pub filter_generation: u64,
    pub connection_states: Vec<ConnectionStatus>,
    /// Topic prefixes cleared since the previous batch; apply these before `topics`.
    pub cleared: Vec<ClearedTopics>,
    /// Topics that changed since the previous batch, each with its full current stats.
    pub topics: Vec<TopicUpdate>,
    /// Every message received since the previous batch, matching the filter or not.
    pub received: u64,
    /// Matching messages that never reached the frontend since the previous batch.
    pub dropped: u64,
}

/// The topics at and under `prefix` on a Connection are gone.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ClearedTopics {
    pub connection_id: ConnectionId,
    pub prefix: String,
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

/// One Message Log row, tagged by `kind`.
#[derive(Debug, Serialize)]
#[serde(tag = "kind", rename_all = "camelCase")]
pub enum LogRow {
    Message(LogEntry),
    Marker(MarkerEntry),
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct MarkerEntry {
    pub seq: u64,
    pub connection_id: ConnectionId,
    pub at_ms: u64,
    pub event: MarkerEvent,
}

impl From<&LogItem> for LogRow {
    fn from(item: &LogItem) -> Self {
        match item {
            LogItem::Message(m) => LogRow::Message(LogEntry::from(m.as_ref())),
            LogItem::Marker(m) => LogRow::Marker(MarkerEntry {
                seq: m.seq,
                connection_id: m.connection_id,
                at_ms: m.at_ms,
                event: m.event.clone(),
            }),
        }
    }
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

/// Payloads larger than this are cut for display.
const MAX_DETAIL_PAYLOAD: usize = 4 * 1024 * 1024;

/// One message in full, for the topic detail panel.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct MessageDetail {
    #[serde(flatten)]
    pub entry: LogEntry,
    pub payload: PayloadData,
    /// The payload was longer than `MAX_DETAIL_PAYLOAD` and was cut.
    pub truncated: bool,
    pub properties: Option<PropertiesView>,
}

/// Text when the bytes are UTF-8, base64 otherwise.
#[derive(Debug, Serialize)]
// `rename_all_fields` too: `rename_all` alone renames variants, not their fields.
#[serde(
    tag = "kind",
    rename_all = "camelCase",
    rename_all_fields = "camelCase"
)]
pub enum PayloadData {
    Text { text: String },
    Binary { base64: String },
}

impl PayloadData {
    fn from_bytes(bytes: &[u8]) -> Self {
        match std::str::from_utf8(bytes) {
            Ok(text) => Self::Text { text: text.into() },
            // A cut in the middle of a character still reads as text.
            Err(err) if err.error_len().is_none() && err.valid_up_to() > 0 => Self::Text {
                text: String::from_utf8_lossy(&bytes[..err.valid_up_to()]).into_owned(),
            },
            Err(_) => Self::Binary {
                base64: base64::engine::general_purpose::STANDARD.encode(bytes),
            },
        }
    }
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PropertiesView {
    pub payload_format_indicator: Option<u8>,
    pub message_expiry_interval: Option<u32>,
    pub content_type: Option<String>,
    pub response_topic: Option<String>,
    pub correlation_data: Option<PayloadData>,
    pub user_properties: Vec<(String, String)>,
}

impl From<&ReceivedMessage> for MessageDetail {
    fn from(m: &ReceivedMessage) -> Self {
        let truncated = m.payload.len() > MAX_DETAIL_PAYLOAD;
        let shown = &m.payload[..m.payload.len().min(MAX_DETAIL_PAYLOAD)];
        Self {
            entry: LogEntry::from(m),
            payload: PayloadData::from_bytes(shown),
            truncated,
            properties: m.properties.as_ref().map(|p| PropertiesView {
                payload_format_indicator: p.payload_format_indicator,
                message_expiry_interval: p.message_expiry_interval,
                content_type: p.content_type.clone(),
                response_topic: p.response_topic.clone(),
                correlation_data: p.correlation_data.as_deref().map(PayloadData::from_bytes),
                user_properties: p.user_properties.clone(),
            }),
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
    let mut pending: VecDeque<LogItem> = VecDeque::new();
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
                    let item = LogItem::Message(m);
                    if live.filter.matches_item(&item) {
                        push_capped(&mut pending, item, &mut dropped);
                    }
                }
                Ok(BusEvent::Marker(m)) => {
                    let item = LogItem::Marker(m);
                    if live.filter.matches_item(&item) {
                        push_capped(&mut pending, item, &mut dropped);
                    }
                }
                Ok(BusEvent::ConnectionState(status)) => connection_states.push(status),
                // The History Store applies these; the tick passes them on in step with it.
                Ok(BusEvent::TopicsCleared { .. }) => {}
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
                let (cleared, topics) = {
                    let mut history = history.lock().unwrap();
                    let cleared: Vec<ClearedTopics> = history
                        .topics
                        .take_cleared()
                        .into_iter()
                        .map(|(connection_id, prefix)| ClearedTopics { connection_id, prefix })
                        .collect();
                    let topics: Vec<TopicUpdate> = history
                        .topics
                        .take_changed()
                        .into_iter()
                        .map(TopicUpdate::from)
                        .collect();
                    (cleared, topics)
                };
                let idle = pending.is_empty()
                    && cleared.is_empty()
                    && connection_states.is_empty()
                    && topics.is_empty()
                    && received == 0;
                if idle {
                    continue;
                }
                let messages: Vec<LogRow> = pending.drain(..).map(|item| LogRow::from(&item)).collect();
                let batch = Batch {
                    messages,
                    filter_generation: live.generation,
                    connection_states: std::mem::take(&mut connection_states),
                    cleared,
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

fn push_capped(pending: &mut VecDeque<LogItem>, item: LogItem, dropped: &mut u64) {
    if pending.len() == MAX_BATCH {
        pending.pop_front();
        *dropped += 1;
    }
    pending.push_back(item);
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
    fn detail_payload_is_text_or_base64() {
        assert!(
            matches!(PayloadData::from_bytes(b"hi"), PayloadData::Text { text } if text == "hi")
        );
        assert!(matches!(
            PayloadData::from_bytes(&[0xff, 0x00]),
            PayloadData::Binary { base64 } if base64 == "/wA="
        ));
    }

    #[test]
    fn a_text_payload_cut_mid_character_stays_text() {
        let cut = &"aå".as_bytes()[..2];
        assert!(matches!(PayloadData::from_bytes(cut), PayloadData::Text { text } if text == "a"));
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
