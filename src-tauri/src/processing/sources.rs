//! Where a Pipeline's messages come from: the one place that knows each kind of Source
//! (Brokers, other Pipelines' Derived Topics, files), both read live by a running Pipeline and
//! sampled for Preview.
//!
//! Which topics a Pipeline takes is the Stages' business (its leading `filter topic` lines);
//! this module only decides which Connection, Pipeline or file a message is from.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use serde::Serialize;
use tokio::sync::broadcast::{Receiver, error::RecvError};

use super::file_source::{FileReader, Next};
use super::lang::{FileSource, Source};
use super::preview::RecentMessages;
use super::runner::PIPELINES;
use crate::bus::BusEvent;
use crate::message::{ConnectionId, ReceivedMessage, now_ms};

/// `$connection` for messages read from files.
pub const FILE_CONNECTION: &str = "file";

/// How many messages a Preview runs over.
pub const SAMPLE: usize = 100;
/// How far back to look per Connection for them.
const SCAN: usize = 20_000;

/// Whether a Pipeline reads the Connection with this name.
pub fn reads_connection(from: &Source, connection: &str) -> bool {
    match from {
        Source::All => true,
        Source::Named(names) => names.iter().any(|n| n == connection),
        Source::Files(_) | Source::Pipelines(_) => false,
    }
}

/// A Derived Topic of one of `sources`, as a message of that Pipeline's own: its topic without
/// the Pipeline's name in front, and the name (which is `$connection`).
pub fn derived_from(
    message: &ReceivedMessage,
    sources: &[String],
) -> Option<(Arc<ReceivedMessage>, String)> {
    let (pipeline, topic) = message.topic.split_once('/')?;
    if !sources.iter().any(|s| s == pipeline) {
        return None;
    }
    let mut own = message.clone();
    own.topic = topic.to_string();
    Some((Arc::new(own), pipeline.to_string()))
}

/// What arrives from a live Source.
#[derive(Debug)]
pub enum Arrival {
    /// A message for the Pipeline, with its `$connection`. `from_broker` says it came from a
    /// Connection (and so may be the Pipeline's own republished message coming back); `problem`
    /// is something wrong with it that didn't stop it.
    Message {
        message: Arc<ReceivedMessage>,
        connection: String,
        from_broker: bool,
        problem: Option<String>,
    },
    /// Messages missed because the Pipeline fell behind the bus.
    Missed(u64),
    /// Something that couldn't be read; the rest carries on.
    Problem(String),
    /// Nothing more will come: the files are read to the end.
    Done,
    /// The bus closed: måqtt is shutting down.
    Closed,
}

/// A Source read as a running Pipeline reads it: from the bus as messages arrive, or from
/// files at their pace.
pub enum Live {
    Bus {
        from: Source,
        rx: Receiver<BusEvent>,
        names: HashMap<ConnectionId, String>,
    },
    Files {
        reader: FileReader,
        reading: Vec<PathBuf>,
        seq: u64,
    },
}

impl Live {
    /// `names` are the Connections' names now; later changes arrive on the bus.
    pub fn new(
        from: &Source,
        rx: Receiver<BusEvent>,
        names: HashMap<ConnectionId, String>,
        files_dir: &Path,
    ) -> Result<Self, String> {
        Ok(match from {
            Source::Files(source) => {
                let reader = FileReader::new(source, files_dir)?;
                Live::Files {
                    reading: reader.paths(),
                    reader,
                    seq: 0,
                }
            }
            _ => Live::Bus {
                from: from.clone(),
                rx,
                names,
            },
        })
    }

    /// The files it reads, which `to file` mustn't write.
    pub fn reading(&self) -> &[PathBuf] {
        match self {
            Live::Files { reading, .. } => reading,
            Live::Bus { .. } => &[],
        }
    }

    /// The next arrival, or None when `until` comes first (so the caller can tick).
    pub async fn next(&mut self, until: Instant) -> Option<Arrival> {
        let until = tokio::time::Instant::from_std(until);
        match self {
            Live::Bus { from, rx, names } => loop {
                let event = tokio::select! {
                    event = rx.recv() => event,
                    _ = tokio::time::sleep_until(until) => return None,
                };
                match event {
                    Ok(BusEvent::Message(message)) => {
                        if let Some(arrival) = take(from, names, message) {
                            return Some(arrival);
                        }
                    }
                    Ok(BusEvent::ConnectionState(s)) => {
                        names.insert(s.connection_id, s.name);
                    }
                    Ok(BusEvent::Marker(_) | BusEvent::TopicsCleared { .. }) => {}
                    Err(RecvError::Lagged(n)) => return Some(Arrival::Missed(n)),
                    Err(RecvError::Closed) => return Some(Arrival::Closed),
                }
            },
            Live::Files { reader, seq, .. } => loop {
                let now = Instant::now();
                match reader.next(now) {
                    Next::Record(record) => {
                        *seq += 1;
                        let message = file_message(*seq, record.name, record.payload, now_ms());
                        return Some(Arrival::Message {
                            message,
                            connection: FILE_CONNECTION.into(),
                            from_broker: false,
                            problem: record.problem,
                        });
                    }
                    Next::Problem(problem) => return Some(Arrival::Problem(problem)),
                    Next::Done => return Some(Arrival::Done),
                    Next::Wait(wait) => {
                        let due = tokio::time::Instant::from_std(now + wait);
                        if due >= until {
                            tokio::time::sleep_until(until).await;
                            return None;
                        }
                        tokio::time::sleep_until(due).await;
                    }
                }
            },
        }
    }
}

/// A bus message, if it's this Source's: from a Connection it reads, or a Derived Topic of a
/// Pipeline it reads. Other Pipelines' Derived Topics are never taken, so `from all` can't
/// feed a Pipeline its own output.
fn take(
    from: &Source,
    names: &HashMap<ConnectionId, String>,
    message: Arc<ReceivedMessage>,
) -> Option<Arrival> {
    if message.connection_id == PIPELINES {
        let Source::Pipelines(sources) = from else {
            return None;
        };
        let (message, connection) = derived_from(&message, sources)?;
        return Some(Arrival::Message {
            message,
            connection,
            from_broker: false,
            problem: None,
        });
    }
    // A Connection not named yet reads as "", as it always has: `from all` still takes it.
    let connection = names
        .get(&message.connection_id)
        .cloned()
        .unwrap_or_default();
    reads_connection(from, &connection).then_some(Arrival::Message {
        message,
        connection,
        from_broker: true,
        problem: None,
    })
}

fn file_message(seq: u64, name: String, payload: Vec<u8>, at_ms: u64) -> Arc<ReceivedMessage> {
    Arc::new(ReceivedMessage {
        seq,
        connection_id: PIPELINES,
        topic: name,
        payload: payload.into(),
        qos: 0,
        retain: false,
        received_at_ms: at_ms,
        properties: None,
    })
}

/// What a Preview ran over, which is how the tab words it.
#[derive(Debug, Clone, Copy, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum Sampled {
    /// Recent messages from Brokers.
    Messages,
    /// The first records of files.
    Records,
    /// Recent Derived Topics of other Pipelines.
    Derived,
}

/// The messages a Preview runs over, oldest first, each with its `$connection`, timed as the
/// Pipeline would meet them.
pub struct Sample {
    pub messages: Vec<(Arc<ReceivedMessage>, String)>,
    pub sampled: Sampled,
    /// A file that couldn't be read.
    pub note: Option<String>,
}

/// Up to [`SAMPLE`] messages for a Preview: the newest from the Brokers it reads (those
/// `takes_topic` lets through), the newest Derived Topics of the Pipelines it reads, or the
/// first records of its files, paced as they'd be read.
pub fn sample(
    from: &Source,
    names: &HashMap<ConnectionId, String>,
    recent: &RecentMessages,
    files_dir: &Path,
    takes_topic: impl Fn(&str) -> bool,
) -> Sample {
    match from {
        Source::Files(source) => {
            let (messages, note) = file_sample(source, files_dir);
            Sample {
                messages,
                sampled: Sampled::Records,
                note,
            }
        }
        Source::Pipelines(sources) => {
            let messages = newest(
                recent(SCAN)
                    .into_iter()
                    .filter(|m| m.connection_id == PIPELINES)
                    .filter_map(|m| derived_from(&m, sources))
                    .collect(),
            );
            Sample {
                messages,
                sampled: Sampled::Derived,
                note: None,
            }
        }
        Source::All | Source::Named(_) => {
            let name = |id: &ConnectionId| names.get(id).map_or("", String::as_str);
            let messages = newest(
                recent(SCAN)
                    .into_iter()
                    .filter(|m| {
                        m.connection_id != PIPELINES
                            && reads_connection(from, name(&m.connection_id))
                            && takes_topic(&m.topic)
                    })
                    .map(|m| {
                        let connection = name(&m.connection_id).to_string();
                        (m, connection)
                    })
                    .collect(),
            );
            Sample {
                messages,
                sampled: Sampled::Messages,
                note: None,
            }
        }
    }
}

/// The newest [`SAMPLE`] of them, oldest first.
fn newest(
    mut messages: Vec<(Arc<ReceivedMessage>, String)>,
) -> Vec<(Arc<ReceivedMessage>, String)> {
    messages.sort_by_key(|(m, _)| m.seq);
    let excess = messages.len().saturating_sub(SAMPLE);
    messages.drain(..excess);
    messages
}

/// The first records of a file Source, timed as it would hand them out, and the first problem
/// reading them.
fn file_sample(
    source: &FileSource,
    files_dir: &Path,
) -> (Vec<(Arc<ReceivedMessage>, String)>, Option<String>) {
    let reader = match FileReader::new(source, files_dir) {
        Ok(reader) => reader,
        Err(problem) => return (Vec::new(), Some(problem)),
    };
    let mut reader = reader.unpaced();
    let (mut sample, mut note) = (Vec::new(), None);
    let mut first_time = None;
    let now = Instant::now();
    while sample.len() < SAMPLE {
        match reader.next(now) {
            Next::Record(record) => {
                let offset_ms = match (reader.replay_time(&record), reader.rate_every()) {
                    (Some(t), _) => {
                        let t0 = *first_time.get_or_insert(t);
                        ((t - t0).max(0.0) * 1000.0) as u64
                    }
                    (None, Some(every)) => (every * sample.len() as u32).as_millis() as u64,
                    (None, None) => 0,
                };
                let message =
                    file_message(sample.len() as u64, record.name, record.payload, offset_ms);
                sample.push((message, FILE_CONNECTION.to_string()));
            }
            Next::Problem(problem) => {
                note.get_or_insert(problem);
            }
            Next::Wait(_) | Next::Done => break,
        }
    }
    (sample, note)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::processing::lang::parse;

    fn from(source: &str) -> Source {
        parse(&format!("{source} |> inspect")).unwrap().from
    }

    fn message(connection: u32, topic: &str) -> Arc<ReceivedMessage> {
        Arc::new(ReceivedMessage {
            seq: 1,
            connection_id: ConnectionId(connection),
            topic: topic.into(),
            payload: bytes::Bytes::from_static(b"1"),
            qos: 0,
            retain: false,
            received_at_ms: 0,
            properties: None,
        })
    }

    #[test]
    fn from_picks_connections() {
        assert!(reads_connection(&from("from all"), "anything"));
        let named = from(r#"from "A", "B""#);
        assert!(reads_connection(&named, "B") && !reads_connection(&named, "C"));
        assert!(!reads_connection(&from(r#"from pipeline "p""#), "p"));
    }

    #[test]
    fn a_bus_message_is_taken_only_by_the_source_it_belongs_to() {
        let names = HashMap::from([(ConnectionId(1), "Dev".to_string())]);
        let taken = |source: &str, m: Arc<ReceivedMessage>| match take(&from(source), &names, m) {
            Some(Arrival::Message {
                message,
                connection,
                from_broker,
                ..
            }) => Some((message.topic.clone(), connection, from_broker)),
            _ => None,
        };
        assert_eq!(
            taken(r#"from "Dev""#, message(1, "a/b")),
            Some(("a/b".into(), "Dev".into(), true))
        );
        assert_eq!(taken(r#"from "Other""#, message(1, "a/b")), None);
        // Derived Topics: only by `from pipeline`, with the Pipeline's name taken off.
        assert_eq!(taken("from all", message(0, "clean/rooms/x")), None);
        assert_eq!(
            taken(r#"from pipeline "clean""#, message(0, "clean/rooms/x")),
            Some(("rooms/x".into(), "clean".into(), false))
        );
        assert_eq!(taken(r#"from pipeline "clean""#, message(1, "a/b")), None);
    }

    #[test]
    fn a_sample_is_the_newest_from_what_the_source_reads() {
        let mut recent: Vec<Arc<ReceivedMessage>> = Vec::new();
        for seq in 0..150u64 {
            let mut m = (*message(1, if seq % 2 == 0 { "home/a" } else { "office/a" })).clone();
            m.seq = seq;
            recent.push(Arc::new(m));
        }
        let recent: RecentMessages = Arc::new(move |_| recent.clone());
        let names = HashMap::from([(ConnectionId(1), "Dev".to_string())]);
        let sample = sample(
            &from("from all"),
            &names,
            &recent,
            Path::new("/nowhere"),
            |t| t.starts_with("home/"),
        );
        assert_eq!(sample.sampled, Sampled::Messages);
        assert_eq!(sample.messages.len(), 75);
        assert!(
            sample
                .messages
                .iter()
                .all(|(m, c)| m.topic == "home/a" && c == "Dev")
        );
        assert!(sample.messages.windows(2).all(|w| w[0].0.seq < w[1].0.seq));
    }
}
