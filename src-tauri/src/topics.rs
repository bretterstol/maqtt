//! The Topic Index: every topic seen on each Connection, with its message count, latest message
//! and whether the Broker holds a Retained Message for it. Unlike the History Store it forgets
//! nothing, so the Topic Tree keeps every topic however much history is evicted.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use crate::message::{ConnectionId, ReceivedMessage};

pub struct TopicStats {
    pub count: u64,
    pub retained: bool,
    pub latest: Arc<ReceivedMessage>,
}

/// A topic's current stats, as handed to the frontend.
pub struct TopicSnapshot<'a> {
    pub connection_id: ConnectionId,
    pub topic: &'a str,
    pub stats: &'a TopicStats,
}

#[derive(Default)]
pub struct TopicIndex {
    connections: HashMap<ConnectionId, HashMap<String, TopicStats>>,
    /// Topics changed since the last `take_changed`.
    changed: HashSet<(ConnectionId, String)>,
}

impl TopicIndex {
    pub fn record(&mut self, message: &Arc<ReceivedMessage>) {
        let topics = self.connections.entry(message.connection_id).or_default();
        let stats = topics
            .entry(message.topic.clone())
            .or_insert_with(|| TopicStats {
                count: 0,
                retained: false,
                latest: message.clone(),
            });
        stats.count += 1;
        stats.latest = message.clone();
        // Only a message flagged retain says anything about the Broker's Retained Message: the
        // replay on subscribe, or (MQTT 5, Retain As Published) a live retained publish. An empty
        // retained payload is how a Retained Message is cleared.
        if message.retain {
            stats.retained = !message.payload.is_empty();
        }
        self.changed
            .insert((message.connection_id, message.topic.clone()));
    }

    /// A (re)connect replays every Retained Message, so forget what we knew and let the replay
    /// tell us again; topics cleared while we were away lose their badge this way.
    pub fn on_connected(&mut self, connection_id: ConnectionId) {
        let Some(topics) = self.connections.get_mut(&connection_id) else {
            return;
        };
        for (topic, stats) in topics.iter_mut().filter(|(_, s)| s.retained) {
            stats.retained = false;
            self.changed.insert((connection_id, topic.clone()));
        }
    }

    /// The latest message of some topic, if it has this seq.
    pub fn latest_with_seq(
        &self,
        connection_id: ConnectionId,
        seq: u64,
    ) -> Option<Arc<ReceivedMessage>> {
        self.connections
            .get(&connection_id)?
            .values()
            .find(|stats| stats.latest.seq == seq)
            .map(|stats| stats.latest.clone())
    }

    /// Every topic changed since the previous call.
    pub fn take_changed(&mut self) -> Vec<TopicSnapshot<'_>> {
        let changed = std::mem::take(&mut self.changed);
        changed
            .into_iter()
            .filter_map(|(connection_id, topic)| {
                let (topic, stats) = self
                    .connections
                    .get(&connection_id)?
                    .get_key_value(&topic)?;
                Some(TopicSnapshot {
                    connection_id,
                    topic,
                    stats,
                })
            })
            .collect()
    }

    pub fn snapshot(&self) -> Vec<TopicSnapshot<'_>> {
        self.connections
            .iter()
            .flat_map(|(connection_id, topics)| {
                topics.iter().map(|(topic, stats)| TopicSnapshot {
                    connection_id: *connection_id,
                    topic,
                    stats,
                })
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    fn message(seq: u64, topic: &str, payload: &str, retain: bool) -> Arc<ReceivedMessage> {
        Arc::new(ReceivedMessage {
            seq,
            connection_id: ConnectionId(1),
            topic: topic.into(),
            payload: Bytes::copy_from_slice(payload.as_bytes()),
            qos: 0,
            retain,
            received_at_ms: 0,
            properties: None,
        })
    }

    fn stats<'a>(index: &'a TopicIndex, topic: &str) -> &'a TopicStats {
        &index.connections[&ConnectionId(1)][topic]
    }

    fn changed_topics(index: &mut TopicIndex) -> Vec<String> {
        let mut topics: Vec<_> = index
            .take_changed()
            .into_iter()
            .map(|s| s.topic.to_string())
            .collect();
        topics.sort();
        topics
    }

    #[test]
    fn counts_messages_and_keeps_the_latest() {
        let mut index = TopicIndex::default();
        index.record(&message(1, "a", "x", false));
        index.record(&message(2, "a", "y", false));
        assert_eq!(stats(&index, "a").count, 2);
        assert_eq!(stats(&index, "a").latest.seq, 2);
    }

    #[test]
    fn a_retained_message_sets_the_badge_and_a_live_one_leaves_it() {
        let mut index = TopicIndex::default();
        index.record(&message(1, "a", "on", true));
        assert!(stats(&index, "a").retained);
        index.record(&message(2, "a", "live", false));
        assert!(stats(&index, "a").retained);
    }

    #[test]
    fn an_empty_retained_message_clears_the_badge() {
        let mut index = TopicIndex::default();
        index.record(&message(1, "a", "on", true));
        index.record(&message(2, "a", "", true));
        assert!(!stats(&index, "a").retained);
    }

    #[test]
    fn reconnecting_resets_retained_until_the_replay_sets_it_again() {
        let mut index = TopicIndex::default();
        index.record(&message(1, "kept", "on", true));
        index.record(&message(2, "cleared", "on", true));
        index.record(&message(3, "plain", "x", false));
        index.take_changed();

        index.on_connected(ConnectionId(1));
        assert_eq!(changed_topics(&mut index), ["cleared", "kept"]);
        index.record(&message(4, "kept", "on", true));
        assert!(stats(&index, "kept").retained);
        assert!(!stats(&index, "cleared").retained);
    }

    #[test]
    fn take_changed_reports_each_topic_once_until_it_changes_again() {
        let mut index = TopicIndex::default();
        index.record(&message(1, "a", "x", false));
        index.record(&message(2, "a", "y", false));
        index.record(&message(3, "b", "z", false));
        assert_eq!(changed_topics(&mut index), ["a", "b"]);
        assert!(changed_topics(&mut index).is_empty());
        assert_eq!(index.snapshot().len(), 2);
    }
}
