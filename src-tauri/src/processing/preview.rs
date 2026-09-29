//! Preview: runs a draft over recent messages from the History Store, without starting it and
//! without sending anything anywhere, and reports what each Stage did.

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use serde::Serialize;

use super::file_source::{FileReader, Next};
use super::lang::Source;
use super::lang::{self, Diagnostic};
use super::markers::Markers;
use super::runner::{FILE_CONNECTION, PIPELINES};
use super::stages::{self, Effect, Origin, Runtime, StageTrace};
use crate::message::{ConnectionId, ReceivedMessage};

/// How many recent matching messages a Preview runs over.
pub const SAMPLE: usize = 100;
/// How far back to look per Connection for them.
const SCAN: usize = 20_000;
/// How many results and inspected values to send back.
const SHOWN: usize = 50;

/// Hands out the newest `n` messages of each Connection. `lib.rs` backs it with the History
/// Store.
pub type RecentMessages = Arc<dyn Fn(usize) -> Vec<Arc<ReceivedMessage>> + Send + Sync>;

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Preview {
    /// When not empty, nothing ran.
    pub problems: Vec<Diagnostic>,
    /// The sample messages that went in.
    pub messages: usize,
    /// The newest sample's payload as JSON, for suggesting field names.
    pub payload: Option<String>,
    /// Where the `from` line and each Stage end in the source (byte offsets).
    pub ends: Vec<usize>,
    /// One per Stage, after `from`.
    pub stages: Vec<StageTrace>,
    /// The latest values that reached a Sink, oldest first. Nothing is sent or written: `to
    /// broker` and `to file` values only show here.
    pub results: Vec<PreviewValue>,
    pub inspected: Vec<PreviewValue>,
    /// Why a file Source gave fewer records than it might: a file that couldn't be read.
    pub note: Option<String>,
    /// Where its `to geo` values would put their markers.
    pub markers: Markers,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PreviewValue {
    /// For results: the Sink, as written (`to app`, `to broker "Office"`).
    pub sink: Option<String>,
    pub label: Option<String>,
    pub topic: String,
    pub value: String,
}

/// `files_dir` is where a file Source's relative names are.
pub fn preview(
    source: &str,
    names: &HashMap<ConnectionId, String>,
    recent: &RecentMessages,
    files_dir: &Path,
) -> Preview {
    let mut preview = Preview {
        problems: Vec::new(),
        messages: 0,
        payload: None,
        ends: Vec::new(),
        stages: Vec::new(),
        results: Vec::new(),
        inspected: Vec::new(),
        note: None,
        markers: Markers::default(),
    };
    let pipeline = match lang::parse(source) {
        Ok(pipeline) => pipeline,
        Err(problems) => {
            preview.problems = problems;
            return preview;
        }
    };
    preview.ends = pipeline.ends.clone();
    let mut runtime = match Runtime::new(&pipeline) {
        Ok(runtime) => runtime.traced(),
        Err(message) => {
            preview.problems = vec![Diagnostic {
                message,
                line: 1,
                column: 1,
                start: 0,
                end: 0,
            }];
            return preview;
        }
    };
    let name = |id: ConnectionId| names.get(&id).map_or("", String::as_str);

    let sample: Vec<(Arc<ReceivedMessage>, String)> = match &pipeline.from {
        Source::Files(source) => {
            let (sample, note) = file_sample(source, files_dir);
            preview.note = note;
            sample
        }
        _ => {
            let mut sample: Vec<_> = recent(SCAN)
                .into_iter()
                .filter(|m| {
                    m.connection_id != PIPELINES && runtime.wants(name(m.connection_id), &m.topic)
                })
                .collect();
            sample.sort_by_key(|m| m.seq);
            let excess = sample.len().saturating_sub(SAMPLE);
            sample.drain(..excess);
            sample
                .into_iter()
                .map(|m| {
                    let connection = name(m.connection_id).to_string();
                    (m, connection)
                })
                .collect()
        }
    };
    preview.messages = sample.len();
    preview.payload = sample
        .last()
        .map(|(m, _)| stages::payload_value(&m.payload).to_string());

    // Replay on the messages' own clock, so `throttle` behaves as it did live.
    let start = Instant::now();
    let first_ms = sample.first().map_or(0, |(m, _)| m.received_at_ms);
    let at = |ms: u64| start + Duration::from_millis(ms.saturating_sub(first_ms));
    let mut effects = Vec::new();
    let mut last = start;
    for (message, connection) in sample {
        let now = at(message.received_at_ms).max(last);
        last = now;
        runtime.tick(now, &mut effects);
        runtime.push(Origin::new(message, &connection), now, &mut effects);
    }
    // What later Stages still hold (throttled values, open windows) would come out later: show
    // it now.
    runtime.flush(last, &mut effects);

    for effect in effects {
        match effect {
            Effect::Derived { topic, payload } => preview.results.push(PreviewValue {
                sink: Some("to app".into()),
                label: None,
                topic,
                value: String::from_utf8_lossy(&payload).into_owned(),
            }),
            Effect::Republish {
                broker,
                topic,
                payload,
            } => preview.results.push(PreviewValue {
                sink: Some(format!("to broker \"{broker}\"")),
                label: None,
                topic,
                value: String::from_utf8_lossy(&payload).into_owned(),
            }),
            Effect::Inspected {
                label,
                topic,
                value,
            } => preview.inspected.push(PreviewValue {
                sink: None,
                label,
                topic,
                value,
            }),
            Effect::Point {
                key,
                label,
                lat,
                lon,
                at_ms,
            } => preview.markers.place(key, label, lat, lon, at_ms),
            Effect::Snapshot { points, at_ms } => preview.markers.replace(points, at_ms),
            Effect::Write { path, record, .. } => preview.results.push(PreviewValue {
                sink: Some("to file".into()),
                label: None,
                topic: path,
                value: record.to_string(),
            }),
            Effect::Error(_) => {}
        }
    }
    keep_last(&mut preview.results, SHOWN);
    keep_last(&mut preview.inspected, SHOWN);
    preview.stages = runtime.trace().to_vec();
    preview
}

/// The first records of a file Source, timed as it would hand them out, and the first problem
/// reading them.
fn file_sample(
    source: &lang::FileSource,
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
                let message = Arc::new(ReceivedMessage {
                    seq: sample.len() as u64,
                    connection_id: PIPELINES,
                    topic: record.name,
                    payload: record.payload.into(),
                    qos: 0,
                    retain: false,
                    received_at_ms: offset_ms,
                    properties: None,
                });
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

fn keep_last<T>(values: &mut Vec<T>, n: usize) {
    let excess = values.len().saturating_sub(n);
    values.drain(..excess);
}

#[cfg(test)]
mod tests {
    use super::*;

    fn message(
        seq: u64,
        connection: u32,
        topic: &str,
        payload: &str,
        at_ms: u64,
    ) -> Arc<ReceivedMessage> {
        Arc::new(ReceivedMessage {
            seq,
            connection_id: ConnectionId(connection),
            topic: topic.into(),
            payload: bytes::Bytes::copy_from_slice(payload.as_bytes()),
            qos: 0,
            retain: false,
            received_at_ms: at_ms,
            properties: None,
        })
    }

    fn history(messages: Vec<Arc<ReceivedMessage>>) -> RecentMessages {
        Arc::new(move |_| messages.clone())
    }

    fn names() -> HashMap<ConnectionId, String> {
        HashMap::from([
            (ConnectionId(1), "Dev".to_string()),
            (ConnectionId(2), "Office".to_string()),
            (PIPELINES, "Pipelines".to_string()),
        ])
    }

    #[test]
    fn runs_the_draft_over_matching_recent_messages() {
        let recent = history(vec![
            message(1, 1, "home/a", r#"{"t": 20}"#, 0),
            message(2, 2, "home/a", r#"{"t": 99}"#, 10),
            message(3, 1, "office/a", r#"{"t": 99}"#, 20),
            message(4, 1, "home/b", r#"{"t": 25}"#, 30),
            message(5, 0, "home/c", r#"{"t": 99}"#, 40),
        ]);
        let p = preview(
            r#"from "Dev"
|> filter topic "home/#"
|> map .t
|> filter . > 21
|> to app "hot/{$levels[1]}""#,
            &names(),
            &recent,
            Path::new("/nowhere"),
        );
        assert!(p.problems.is_empty());
        assert_eq!(
            p.messages, 2,
            "only Dev's home/ messages, never Derived Topics"
        );
        assert_eq!(p.ends.len(), 5);
        let passed: Vec<usize> = p.stages.iter().map(|s| s.passed).collect();
        assert_eq!(passed, vec![2, 2, 1, 1]);
        assert_eq!(
            p.results,
            vec![PreviewValue {
                sink: Some("to app".into()),
                label: None,
                topic: "hot/b".into(),
                value: "25".into()
            }]
        );
    }

    #[test]
    fn replays_throttle_on_the_messages_own_clock() {
        let recent = history(
            (0..10)
                .map(|i| message(i, 1, "t", &i.to_string(), i * 1_000))
                .collect(),
        );
        let p = preview(
            r#"from all |> throttle 3s |> to app "x""#,
            &names(),
            &recent,
            Path::new("/nowhere"),
        );
        // 0 passes; then every 3 s the latest held back comes out, and 9 at the end.
        let values: Vec<&str> = p.results.iter().map(|r| r.value.as_str()).collect();
        assert_eq!(values, vec!["0", "2", "5", "8", "9"]);
    }

    #[test]
    fn to_broker_shows_what_it_would_send_and_refuses_a_loop() {
        let recent = history(vec![
            message(1, 1, "home/a", r#"{"t": 20}"#, 0),
            message(2, 2, "home/a", r#"{"t": 21}"#, 10),
        ]);
        let p = preview(
            r#"from "Dev" |> filter topic "home/#" |> map .t |> to broker "Office" "copy/{$levels[1]}""#,
            &names(),
            &recent,
            Path::new("/nowhere"),
        );
        assert_eq!(
            p.results,
            vec![PreviewValue {
                sink: Some(r#"to broker "Office""#.into()),
                label: None,
                topic: "copy/a".into(),
                value: "20".into()
            }]
        );

        // Reading Office too, `copy/a` would come straight back in.
        let p = preview(
            r#"from all |> map .t |> to broker "Office" "copy/{$levels[1]}""#,
            &names(),
            &recent,
            Path::new("/nowhere"),
        );
        assert!(p.results.is_empty());
        let sink = p.stages.last().unwrap();
        assert_eq!(sink.errors, 2);
        assert!(
            sink.first_error
                .as_deref()
                .unwrap()
                .contains("would come back into this Pipeline"),
            "{:?}",
            sink.first_error
        );
    }

    #[test]
    fn several_topic_filters_pick_samples_and_guard_several_outputs() {
        let recent = history(vec![
            message(1, 1, "home/a", "1", 0),
            message(2, 1, "office/a", "2", 10),
            message(3, 1, "garage/a", "3", 20),
        ]);
        let p = preview(
            r#"from "Dev" |> filter topic "home/#", "office/#" |> to app "x""#,
            &names(),
            &recent,
            Path::new("/nowhere"),
        );
        assert_eq!(p.messages, 2, "home and office, not garage");

        // Reading Dev but not out/ or alerts/: both outputs go, anything else is refused.
        let p = preview(
            r#"from "Dev"
|> filter not topic "out/#", "alerts/#"
|> to broker "Dev" "out/{$topic}"
|> to broker "Dev" "alerts/{$topic}"
|> to broker "Dev" "other/{$topic}""#,
            &names(),
            &recent,
            Path::new("/nowhere"),
        );
        let errors: Vec<usize> = p.stages.iter().map(|s| s.errors).collect();
        assert_eq!(errors, vec![0, 0, 0, 3]);
        assert_eq!(p.results.len(), 6);
    }

    #[test]
    fn a_draft_with_problems_does_not_run() {
        let p = preview(
            "from all |> sort",
            &names(),
            &history(vec![message(1, 1, "t", "1", 0)]),
            Path::new("/nowhere"),
        );
        assert_eq!(p.problems.len(), 1);
        assert_eq!(p.messages, 0);
    }

    #[test]
    fn a_file_source_previews_its_first_records() {
        let dir = std::env::temp_dir().join(format!("maqtt-preview-files-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("t.csv"), "room,t\nkitchen,21\noffice,19\n").unwrap();
        let p = preview(
            r#"from file "t.csv", "gone.jsonl" |> map .t |> to app "{$topic}""#,
            &names(),
            &history(Vec::new()),
            &dir,
        );
        assert_eq!(p.messages, 2);
        let values: Vec<&str> = p.results.iter().map(|r| r.value.as_str()).collect();
        assert_eq!(values, ["21", "19"]);
        assert!(p.note.unwrap().contains("gone.jsonl"));
    }

    #[test]
    fn a_windowed_snapshot_drops_trucks_that_stopped_reporting() {
        let recent = history(vec![
            message(1, 1, "gps", r#"{"id": "A", "lat": 1, "lon": 1}"#, 0),
            message(2, 1, "gps", r#"{"id": "B", "lat": 5, "lon": 5}"#, 500),
            message(3, 1, "gps", r#"{"id": "A", "lat": 2, "lon": 1}"#, 4_000),
        ]);
        let p = preview(
            r#"from all |> map { lat: .lat, lon: .lon, label: .id } |> window 3s |> to geo"#,
            &names(),
            &recent,
            Path::new("/nowhere"),
        );
        let markers: Vec<(String, f64, usize)> = p
            .markers
            .list()
            .iter()
            .map(|m| (m.key.clone(), m.lat, m.trail.len()))
            .collect();
        assert_eq!(markers, [("A".to_string(), 2.0, 1)]);
    }
}
