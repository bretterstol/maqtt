//! Preview: runs a draft over recent messages from the History Store, without starting it and
//! without sending anything anywhere, and reports what each Stage did.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use serde::Serialize;

use super::lang::{self, Diagnostic};
use super::runner::PIPELINES;
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
    /// The latest values that reached `to app`, oldest first.
    pub results: Vec<PreviewValue>,
    pub inspected: Vec<PreviewValue>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PreviewValue {
    pub label: Option<String>,
    pub topic: String,
    pub value: String,
}

pub fn preview(
    source: &str,
    names: &HashMap<ConnectionId, String>,
    recent: &RecentMessages,
) -> Preview {
    let mut preview = Preview {
        problems: Vec::new(),
        messages: 0,
        payload: None,
        ends: Vec::new(),
        stages: Vec::new(),
        results: Vec::new(),
        inspected: Vec::new(),
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

    let mut sample: Vec<_> = recent(SCAN)
        .into_iter()
        .filter(|m| m.connection_id != PIPELINES && runtime.wants(name(m.connection_id), &m.topic))
        .collect();
    sample.sort_by_key(|m| m.seq);
    let excess = sample.len().saturating_sub(SAMPLE);
    sample.drain(..excess);
    preview.messages = sample.len();
    preview.payload = sample
        .last()
        .map(|m| stages::payload_value(&m.payload).to_string());

    // Replay on the messages' own clock, so `throttle` behaves as it did live.
    let start = Instant::now();
    let first_ms = sample.first().map_or(0, |m| m.received_at_ms);
    let at = |ms: u64| start + Duration::from_millis(ms.saturating_sub(first_ms));
    let mut effects = Vec::new();
    let mut last = start;
    for message in sample {
        let now = at(message.received_at_ms).max(last);
        last = now;
        runtime.tick(now, &mut effects);
        let connection = name(message.connection_id).to_string();
        runtime.push(Origin::new(message, &connection), now, &mut effects);
    }
    // Whatever `throttle` still holds would come out later; let it.
    runtime.tick(last + Duration::from_secs(24 * 3600), &mut effects);

    for effect in effects {
        match effect {
            Effect::Derived { topic, payload } => preview.results.push(PreviewValue {
                label: None,
                topic,
                value: String::from_utf8_lossy(&payload).into_owned(),
            }),
            Effect::Inspected {
                label,
                topic,
                value,
            } => preview.inspected.push(PreviewValue {
                label,
                topic,
                value,
            }),
            Effect::Error(_) => {}
        }
    }
    keep_last(&mut preview.results, SHOWN);
    keep_last(&mut preview.inspected, SHOWN);
    preview.stages = runtime.trace().to_vec();
    preview
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
        );
        // 0 passes; then every 3 s the latest held back comes out, and 9 at the end.
        let values: Vec<&str> = p.results.iter().map(|r| r.value.as_str()).collect();
        assert_eq!(values, vec!["0", "2", "5", "8", "9"]);
    }

    #[test]
    fn a_draft_with_problems_does_not_run() {
        let p = preview(
            "from all |> sort",
            &names(),
            &history(vec![message(1, 1, "t", "1", 0)]),
        );
        assert_eq!(p.problems.len(), 1);
        assert_eq!(p.messages, 0);
    }
}
