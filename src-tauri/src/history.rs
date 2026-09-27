//! The History Store: the backend's bounded memory of received messages, and the source of
//! truth for the Message Log and each topic's history. The frontend asks it for pages instead
//! of mirroring everything.

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::iter::Peekable;
use std::sync::{Arc, Mutex};

use serde::{Deserialize, Serialize};
use tokio::sync::broadcast::error::RecvError;

use crate::bus::{Bus, BusEvent};
use crate::connection::ConnectionState;
use crate::message::{ConnectionId, ReceivedMessage};
use crate::topics::TopicIndex;

#[derive(Debug, Clone, Copy)]
pub struct HistoryLimits {
    pub per_topic: usize,
    pub per_connection: usize,
}

impl Default for HistoryLimits {
    fn default() -> Self {
        Self {
            per_topic: 100,
            per_connection: 100_000,
        }
    }
}

/// Which messages the Message Log shows.
#[derive(Debug, Clone, Default, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LogFilter {
    pub connection_id: Option<ConnectionId>,
    /// Matches the topic or the payload, ignoring ASCII case.
    pub text: Option<String>,
}

impl LogFilter {
    pub fn matches(&self, message: &ReceivedMessage) -> bool {
        if self
            .connection_id
            .is_some_and(|id| id != message.connection_id)
        {
            return false;
        }
        match self.text.as_deref() {
            None | Some("") => true,
            Some(text) => {
                contains_ignore_ascii_case(message.topic.as_bytes(), text.as_bytes())
                    || contains_ignore_ascii_case(&message.payload, text.as_bytes())
            }
        }
    }
}

fn contains_ignore_ascii_case(haystack: &[u8], needle: &[u8]) -> bool {
    needle.len() <= haystack.len()
        && haystack
            .windows(needle.len())
            .any(|window| window.eq_ignore_ascii_case(needle))
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LogQuery {
    #[serde(default)]
    pub filter: LogFilter,
    /// Only messages older than this; `None` starts from the newest.
    pub before_seq: Option<u64>,
    pub limit: usize,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct LogPage<T> {
    /// Oldest first.
    pub entries: Vec<T>,
    /// Whether older matching messages exist beyond this page.
    pub has_more: bool,
}

impl<T> LogPage<T> {
    pub fn map<U>(self, f: impl FnMut(T) -> U) -> LogPage<U> {
        LogPage {
            entries: self.entries.into_iter().map(f).collect(),
            has_more: self.has_more,
        }
    }
}

#[derive(Default)]
pub struct HistoryStore {
    /// Every topic ever seen; kept here so one lock covers both.
    pub topics: TopicIndex,
    default_limits: HistoryLimits,
    limits: HashMap<ConnectionId, HistoryLimits>,
    connections: HashMap<ConnectionId, ConnectionHistory>,
}

#[derive(Default)]
struct ConnectionHistory {
    messages: BTreeMap<u64, Arc<ReceivedMessage>>,
    /// Each topic's messages by seq, oldest first.
    topics: HashMap<String, VecDeque<u64>>,
}

impl HistoryStore {
    pub fn new(default_limits: HistoryLimits) -> Self {
        Self {
            topics: TopicIndex::default(),
            default_limits,
            limits: HashMap::new(),
            connections: HashMap::new(),
        }
    }

    /// Changes one Connection's per-topic cap; it applies as each topic next receives a message.
    pub fn set_per_topic_limit(&mut self, connection_id: ConnectionId, per_topic: usize) {
        let limits = self
            .limits
            .entry(connection_id)
            .or_insert(self.default_limits);
        limits.per_topic = per_topic.max(1);
    }

    pub fn insert(&mut self, message: Arc<ReceivedMessage>) {
        self.topics.record(&message);
        let limits = self
            .limits
            .get(&message.connection_id)
            .copied()
            .unwrap_or(self.default_limits);
        let history = self.connections.entry(message.connection_id).or_default();
        let seq = message.seq;

        let topic = history.topics.entry(message.topic.clone()).or_default();
        topic.push_back(seq);
        while topic.len() > limits.per_topic
            && let Some(evicted) = topic.pop_front()
        {
            history.messages.remove(&evicted);
        }
        history.messages.insert(seq, message);

        if history.messages.len() > limits.per_connection
            && let Some((evicted_seq, evicted)) = history.messages.pop_first()
            && let Some(topic) = history.topics.get_mut(&evicted.topic)
        {
            // Seqs rise within a Connection, so the Connection's oldest message is also the
            // oldest of its topic.
            debug_assert_eq!(topic.front(), Some(&evicted_seq));
            topic.pop_front();
            if topic.is_empty() {
                history.topics.remove(&evicted.topic);
            }
        }
    }

    /// The topic's retained history on one Connection, oldest first.
    pub fn topic_history(
        &self,
        connection_id: ConnectionId,
        topic: &str,
    ) -> Vec<Arc<ReceivedMessage>> {
        let Some(history) = self.connections.get(&connection_id) else {
            return Vec::new();
        };
        history
            .topics
            .get(topic)
            .into_iter()
            .flatten()
            .filter_map(|seq| history.messages.get(seq).cloned())
            .collect()
    }

    /// A page of the Message Log across all Connections, merged by seq.
    pub fn query(&self, query: &LogQuery) -> LogPage<Arc<ReceivedMessage>> {
        let before = query.before_seq.unwrap_or(u64::MAX);
        let mut sources: Vec<Peekable<_>> = self
            .connections
            .iter()
            .filter(|(id, _)| {
                query
                    .filter
                    .connection_id
                    .is_none_or(|wanted| wanted == **id)
            })
            .map(|(_, history)| history.messages.range(..before).rev().peekable())
            .collect();

        let mut newest_first = Vec::with_capacity(query.limit.min(1024));
        let mut has_more = false;
        loop {
            // Take the newest head among the Connections; there are only ever a handful.
            let Some(source) = sources
                .iter_mut()
                .filter_map(|source| source.peek().map(|(seq, _)| **seq).zip(Some(source)))
                .max_by_key(|(seq, _)| *seq)
                .map(|(_, source)| source)
            else {
                break;
            };
            let (_, message) = source.next().expect("peeked");
            if !query.filter.matches(message) {
                continue;
            }
            if newest_first.len() == query.limit {
                has_more = true;
                break;
            }
            newest_first.push(message.clone());
        }

        newest_first.reverse();
        LogPage {
            entries: newest_first,
            has_more,
        }
    }
}

/// Records everything on the Message Bus into the store.
pub fn spawn_recorder(bus: &Bus, store: Arc<Mutex<HistoryStore>>) {
    let mut rx = bus.subscribe();
    tauri::async_runtime::spawn(async move {
        loop {
            match rx.recv().await {
                Ok(event) => {
                    let mut store = store.lock().unwrap();
                    store.record(event);
                    // Drain whatever else is ready under the same lock.
                    while let Ok(event) = rx.try_recv() {
                        store.record(event);
                    }
                }
                Err(RecvError::Lagged(n)) => {
                    eprintln!("maqtt: History Store fell behind and missed {n} messages");
                }
                Err(RecvError::Closed) => return,
            }
        }
    });
}

impl HistoryStore {
    fn record(&mut self, event: BusEvent) {
        match event {
            BusEvent::Message(message) => self.insert(message),
            BusEvent::ConnectionState(status) if status.state == ConnectionState::Connected => {
                self.topics.on_connected(status.connection_id);
            }
            BusEvent::ConnectionState(_) => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    fn message(seq: u64, connection: u32, topic: &str, payload: &str) -> Arc<ReceivedMessage> {
        Arc::new(ReceivedMessage {
            seq,
            connection_id: ConnectionId(connection),
            topic: topic.into(),
            payload: Bytes::copy_from_slice(payload.as_bytes()),
            qos: 0,
            retain: false,
            received_at_ms: 0,
        })
    }

    fn seqs(messages: &[Arc<ReceivedMessage>]) -> Vec<u64> {
        messages.iter().map(|m| m.seq).collect()
    }

    fn store(per_topic: usize, per_connection: usize) -> HistoryStore {
        HistoryStore::new(HistoryLimits {
            per_topic,
            per_connection,
        })
    }

    fn latest(
        store: &HistoryStore,
        filter: LogFilter,
        limit: usize,
    ) -> LogPage<Arc<ReceivedMessage>> {
        store.query(&LogQuery {
            filter,
            before_seq: None,
            limit,
        })
    }

    #[test]
    fn per_topic_cap_evicts_that_topics_oldest() {
        let mut s = store(2, 100);
        for (seq, topic) in [(1, "a"), (2, "b"), (3, "a"), (4, "a")] {
            s.insert(message(seq, 1, topic, ""));
        }
        assert_eq!(seqs(&s.topic_history(ConnectionId(1), "a")), [3, 4]);
        assert_eq!(seqs(&s.topic_history(ConnectionId(1), "b")), [2]);
        assert_eq!(
            seqs(&latest(&s, LogFilter::default(), 10).entries),
            [2, 3, 4]
        );
    }

    #[test]
    fn per_connection_cap_evicts_the_connections_oldest() {
        let mut s = store(100, 3);
        for (seq, topic) in [(1, "a"), (2, "b"), (3, "a"), (4, "c")] {
            s.insert(message(seq, 1, topic, ""));
        }
        assert_eq!(
            seqs(&latest(&s, LogFilter::default(), 10).entries),
            [2, 3, 4]
        );
        assert_eq!(seqs(&s.topic_history(ConnectionId(1), "a")), [3]);
    }

    #[test]
    fn a_topic_fully_evicted_by_the_connection_cap_is_forgotten() {
        let mut s = store(100, 1);
        s.insert(message(1, 1, "a", ""));
        s.insert(message(2, 1, "b", ""));
        assert!(s.topic_history(ConnectionId(1), "a").is_empty());
        assert!(!s.connections[&ConnectionId(1)].topics.contains_key("a"));
    }

    #[test]
    fn a_lowered_per_topic_cap_trims_the_topic_on_its_next_message() {
        let mut s = store(5, 100);
        for seq in 1..=5 {
            s.insert(message(seq, 1, "t", ""));
        }
        s.set_per_topic_limit(ConnectionId(1), 2);
        s.insert(message(6, 1, "t", ""));
        assert_eq!(seqs(&s.topic_history(ConnectionId(1), "t")), [5, 6]);
        assert_eq!(seqs(&latest(&s, LogFilter::default(), 10).entries), [5, 6]);
    }

    #[test]
    fn caps_apply_per_connection() {
        let mut s = store(100, 2);
        for seq in 1..=4 {
            s.insert(message(seq, 1, "t", ""));
        }
        s.insert(message(5, 2, "t", ""));
        assert_eq!(
            seqs(&latest(&s, LogFilter::default(), 10).entries),
            [3, 4, 5]
        );
    }

    #[test]
    fn query_merges_connections_by_seq_and_pages_backwards() {
        let mut s = store(100, 100);
        for (seq, connection) in [(1, 1), (2, 2), (3, 1), (4, 2), (5, 2), (6, 1)] {
            s.insert(message(seq, connection, "t", ""));
        }
        let page = latest(&s, LogFilter::default(), 4);
        assert_eq!(seqs(&page.entries), [3, 4, 5, 6]);
        assert!(page.has_more);

        let older = s.query(&LogQuery {
            filter: LogFilter::default(),
            before_seq: Some(3),
            limit: 4,
        });
        assert_eq!(seqs(&older.entries), [1, 2]);
        assert!(!older.has_more);
    }

    #[test]
    fn has_more_is_false_when_the_page_is_exactly_full() {
        let mut s = store(100, 100);
        for seq in 1..=3 {
            s.insert(message(seq, 1, "t", ""));
        }
        assert!(!latest(&s, LogFilter::default(), 3).has_more);
    }

    #[test]
    fn query_filters_by_connection() {
        let mut s = store(100, 100);
        for (seq, connection) in [(1, 1), (2, 2), (3, 1)] {
            s.insert(message(seq, connection, "t", ""));
        }
        let filter = LogFilter {
            connection_id: Some(ConnectionId(1)),
            text: None,
        };
        assert_eq!(seqs(&latest(&s, filter, 10).entries), [1, 3]);
    }

    #[test]
    fn query_filters_by_text_in_topic_or_payload() {
        let mut s = store(100, 100);
        s.insert(message(1, 1, "home/Kitchen/temp", "21"));
        s.insert(message(2, 1, "home/office/temp", r#"{"room":"kitchen"}"#));
        s.insert(message(3, 1, "home/office/temp", "22"));
        let filter = LogFilter {
            connection_id: None,
            text: Some("KITCHEN".into()),
        };
        assert_eq!(seqs(&latest(&s, filter, 10).entries), [1, 2]);
    }

    #[test]
    fn empty_text_matches_everything() {
        let filter = LogFilter {
            connection_id: None,
            text: Some(String::new()),
        };
        assert!(filter.matches(&message(1, 1, "t", "")));
    }

    #[test]
    fn text_filter_handles_binary_payloads_and_long_needles() {
        let binary = Arc::new(ReceivedMessage {
            payload: Bytes::from_static(&[0xff, b'o', b'K', 0x00]),
            ..(*message(1, 1, "t", "")).clone()
        });
        let text = |t: &str| LogFilter {
            connection_id: None,
            text: Some(t.into()),
        };
        assert!(text("ok").matches(&binary));
        assert!(!text("a needle longer than both").matches(&binary));
    }
}
