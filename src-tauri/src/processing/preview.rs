//! Preview: runs a draft over recent messages from the History Store, without starting it and
//! without sending anything anywhere, and reports what each Stage did.

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use serde::Serialize;

use super::lang::{self, Diagnostic};
use super::outputs::{Delivery, Outputs};
use super::sources::{self, Sample, Sampled};
use super::stages::{self, Origin, Runtime, StageTrace};
use crate::message::{ConnectionId, ReceivedMessage, now_ms};

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
    /// The Sinks the draft uses (`app`, `geo`, `chart`, …), so the tab offers their views.
    pub sinks: Vec<String>,
    /// What the sample is: recent messages, records from files, or Derived Topics.
    pub sampled: Sampled,
    /// Why a file Source gave fewer records than it might: a file that couldn't be read.
    pub note: Option<String>,
    /// What its Sinks would show: `inspect` values, markers, chart lines, counts.
    pub outputs: Outputs,
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
        sinks: Vec::new(),
        sampled: Sampled::Messages,
        note: None,
        outputs: Outputs::default(),
    };
    let pipeline = match lang::parse(source) {
        Ok(pipeline) => pipeline,
        Err(problems) => {
            preview.problems = problems;
            return preview;
        }
    };
    preview.ends = pipeline.ends.clone();
    preview.sinks = pipeline.sinks().into_iter().map(String::from).collect();
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

    let Sample {
        messages: sample,
        sampled,
        note,
    } = sources::sample(&pipeline.from, names, recent, files_dir, |topic| {
        runtime.takes_topic(topic)
    });
    preview.sampled = sampled;
    preview.note = note;
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

    // The same outputs a running Pipeline keeps; what it would deliver is only listed.
    for effect in effects {
        if let Some(delivery) = preview.outputs.record(effect, now_ms()) {
            preview.results.push(would_send(delivery));
        }
    }
    keep_last(&mut preview.results, SHOWN);
    preview.stages = runtime.trace().to_vec();
    preview
}

/// A row for what a Sink would send, which Preview shows instead of sending it.
fn would_send(delivery: Delivery) -> PreviewValue {
    let (sink, topic, value) = match delivery {
        Delivery::Derived { topic, payload } => (
            "to app".to_string(),
            topic,
            String::from_utf8_lossy(&payload).into_owned(),
        ),
        Delivery::Republish {
            broker,
            topic,
            payload,
        } => (
            format!("to broker \"{broker}\""),
            topic,
            String::from_utf8_lossy(&payload).into_owned(),
        ),
        Delivery::Write { path, record, .. } => ("to file".to_string(), path, record.to_string()),
    };
    PreviewValue {
        sink: Some(sink),
        label: None,
        topic,
        value,
    }
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
            (
                crate::processing::runner::PIPELINES,
                "Pipelines".to_string(),
            ),
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
        assert_eq!(p.sampled, Sampled::Records);
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
            .outputs
            .markers
            .list()
            .iter()
            .map(|m| (m.key.clone(), m.lat, m.trail.len()))
            .collect();
        assert_eq!(markers, [("A".to_string(), 2.0, 1)]);
    }

    #[test]
    fn from_pipeline_previews_that_pipelines_derived_topics() {
        let recent = history(vec![
            message(1, 0, "clean/rooms/kitchen", r#"{"t": 21}"#, 0),
            message(2, 0, "other/rooms/kitchen", r#"{"t": 99}"#, 10),
            message(3, 1, "clean/rooms/office", r#"{"t": 99}"#, 20),
            message(4, 0, "clean/rooms/office", r#"{"t": 19}"#, 30),
        ]);
        let p = preview(
            r#"from pipeline "clean" |> filter topic "rooms/+room" |> map { t: .t, from: $connection, room: $room } |> to app "x""#,
            &names(),
            &recent,
            Path::new("/nowhere"),
        );
        assert_eq!(
            p.messages, 2,
            "only clean's Derived Topics, not a Broker's topic that looks alike"
        );
        assert_eq!(p.sampled, Sampled::Derived);
        let values: Vec<&str> = p.results.iter().map(|r| r.value.as_str()).collect();
        assert_eq!(
            values,
            [
                r#"{"t":21,"from":"clean","room":"kitchen"}"#,
                r#"{"t":19,"from":"clean","room":"office"}"#
            ]
        );
    }

    #[test]
    fn a_preview_says_which_sinks_the_draft_uses() {
        // In a comment or a string, `to geo` isn't a Sink.
        let p = preview(
            "from all # to geo\n|> map \"to chart\" |> to app \"x\" |> to geo |> to app \"y\"",
            &names(),
            &history(Vec::new()),
            Path::new("/nowhere"),
        );
        assert_eq!(p.sinks, ["app", "geo"]);
    }
}
