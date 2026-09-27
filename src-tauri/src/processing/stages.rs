//! Running a parsed Pipeline: each message becomes a value that flows through the Stages, and
//! whatever reaches a Sink, an `inspect` or an error becomes an [`Effect`] for the runner.
//! No threads or clocks here; the caller passes the time, which keeps it easy to test.

use std::collections::HashMap;
use std::rc::Rc;
use std::sync::Arc;
use std::time::{Duration, Instant};

use jaq_json::Val;

use super::jq;
use super::lang::{Connections, Pipeline, Stage};
use crate::message::ReceivedMessage;
use crate::profile::topic_matches;

/// What running a message (or a tick) produced.
#[derive(Debug, Clone, PartialEq)]
pub enum Effect {
    /// A message for the app Sink: a Derived Topic path (within the Pipeline) and its payload.
    Derived {
        topic: String,
        payload: Vec<u8>,
    },
    Inspected {
        label: Option<String>,
        topic: String,
        value: String,
    },
    Error(String),
}

/// The original message a value came from, with its jq variables worked out once.
pub struct Origin {
    pub message: Arc<ReceivedMessage>,
    vars: [Val; 8],
}

impl Origin {
    pub fn new(message: Arc<ReceivedMessage>, connection: &str) -> Self {
        let payload = payload_value(&message.payload);
        let levels: Val = message
            .topic
            .split('/')
            .map(|level| Val::from(level.to_string()))
            .collect();
        let properties = message
            .properties
            .as_ref()
            .map(properties_value)
            .unwrap_or(Val::Null);
        let vars = [
            payload,
            Val::from(message.topic.clone()),
            levels,
            Val::from(connection.to_string()),
            Val::from(message.received_at_ms as f64 / 1000.0),
            Val::from(usize::from(message.qos)),
            Val::Bool(message.retain),
            properties,
        ];
        Self { message, vars }
    }

    fn payload(&self) -> Val {
        self.vars[0].clone()
    }

    fn topic(&self) -> &str {
        &self.message.topic
    }
}

/// A value on its way through the Stages.
struct Item {
    value: Val,
    origin: Rc<Origin>,
}

enum Running {
    FilterTopic {
        filter: String,
        negate: bool,
    },
    Filter(jq::Filter),
    Map(jq::Filter),
    Changes {
        key: Option<jq::Filter>,
        last: HashMap<String, Val>,
    },
    Throttle {
        interval: Duration,
        key: Option<jq::Filter>,
        keys: HashMap<String, Throttled>,
    },
    Inspect(Option<String>),
    ToApp(jq::Filter),
}

struct Throttled {
    open_at: Instant,
    pending: Option<Item>,
}

/// A Pipeline ready to run; not `Send` (jq values are reference-counted), so it lives on one
/// thread.
pub struct Runtime {
    from: Connections,
    stages: Vec<Running>,
}

impl Runtime {
    pub fn new(pipeline: &Pipeline) -> Result<Self, String> {
        let compile = |code: &str| jq::compile(code).map_err(|e| e.message);
        let stages = pipeline
            .stages
            .iter()
            .map(|stage| {
                Ok(match stage {
                    Stage::FilterTopic { filter, negate } => Running::FilterTopic {
                        filter: filter.clone(),
                        negate: *negate,
                    },
                    Stage::Filter(code) => Running::Filter(compile(code)?),
                    Stage::Map(code) => Running::Map(compile(code)?),
                    Stage::Changes(key) => Running::Changes {
                        key: key.as_deref().map(compile).transpose()?,
                        last: HashMap::new(),
                    },
                    Stage::Throttle(interval, key) => Running::Throttle {
                        interval: *interval,
                        key: key.as_deref().map(compile).transpose()?,
                        keys: HashMap::new(),
                    },
                    Stage::Inspect(label) => Running::Inspect(label.clone()),
                    Stage::ToApp(code) => Running::ToApp(compile(code)?),
                })
            })
            .collect::<Result<_, String>>()?;
        Ok(Self {
            from: pipeline.from.clone(),
            stages,
        })
    }

    /// Whether messages from this Connection are the Pipeline's to handle.
    pub fn reads(&self, connection: &str) -> bool {
        match &self.from {
            Connections::All => true,
            Connections::Named(names) => names.iter().any(|n| n == connection),
        }
    }

    pub fn push(&mut self, origin: Origin, now: Instant, out: &mut Vec<Effect>) {
        let origin = Rc::new(origin);
        let item = Item {
            value: origin.payload(),
            origin,
        };
        self.run_from(0, vec![item], now, out);
    }

    /// Lets time-based Stages (throttle) release what they held back.
    pub fn tick(&mut self, now: Instant, out: &mut Vec<Effect>) {
        for i in 0..self.stages.len() {
            let released = match &mut self.stages[i] {
                Running::Throttle { interval, keys, .. } => {
                    let mut released = Vec::new();
                    for state in keys.values_mut() {
                        if now >= state.open_at
                            && let Some(item) = state.pending.take()
                        {
                            state.open_at = now + *interval;
                            released.push(item);
                        }
                    }
                    released
                }
                _ => continue,
            };
            if !released.is_empty() {
                self.run_from(i + 1, released, now, out);
            }
        }
    }

    fn run_from(
        &mut self,
        start: usize,
        mut items: Vec<Item>,
        now: Instant,
        out: &mut Vec<Effect>,
    ) {
        for stage in &mut self.stages[start..] {
            let mut next = Vec::with_capacity(items.len());
            for item in items {
                apply(stage, item, now, &mut next, out);
            }
            items = next;
            if items.is_empty() {
                return;
            }
        }
    }
}

fn apply(
    stage: &mut Running,
    item: Item,
    now: Instant,
    next: &mut Vec<Item>,
    out: &mut Vec<Effect>,
) {
    match stage {
        Running::FilterTopic { filter, negate } => {
            if topic_matches(filter, item.origin.topic()) != *negate {
                next.push(item);
            }
        }
        Running::Filter(filter) => match jq::run(filter, item.value.clone(), &item.origin.vars) {
            Ok(results) => {
                if results.first().is_some_and(truthy) {
                    next.push(item);
                }
            }
            Err(err) => out.push(error(&item, "filter", err)),
        },
        Running::Map(filter) => match jq::run(filter, item.value.clone(), &item.origin.vars) {
            // Each result is its own message downstream; none drops it.
            Ok(results) => next.extend(results.into_iter().map(|value| Item {
                value,
                origin: item.origin.clone(),
            })),
            Err(err) => out.push(error(&item, "map", err)),
        },
        Running::Changes { key, last } => {
            let Some(key) = state_key(key.as_ref(), &item, "changes", out) else {
                return;
            };
            if last.get(&key) != Some(&item.value) {
                last.insert(key, item.value.clone());
                next.push(item);
            }
        }
        Running::Throttle {
            interval,
            key,
            keys,
        } => {
            let Some(key) = state_key(key.as_ref(), &item, "throttle", out) else {
                return;
            };
            let state = keys.entry(key).or_insert(Throttled {
                open_at: now,
                pending: None,
            });
            if now >= state.open_at {
                state.open_at = now + *interval;
                state.pending = None;
                next.push(item);
            } else {
                // Latest wins; `tick` releases it when the interval is up.
                state.pending = Some(item);
            }
        }
        Running::Inspect(label) => {
            out.push(Effect::Inspected {
                label: label.clone(),
                topic: item.origin.topic().to_string(),
                value: item.value.to_string(),
            });
            next.push(item);
        }
        Running::ToApp(filter) => {
            match jq::run(filter, item.value.clone(), &item.origin.vars) {
                Ok(results) => match results.first() {
                    Some(Val::TStr(topic)) => {
                        match derived_topic(&String::from_utf8_lossy(topic)) {
                            Ok(topic) => out.push(Effect::Derived {
                                topic,
                                payload: payload_bytes(&item.value),
                            }),
                            Err(err) => out.push(error(&item, "to app", err)),
                        }
                    }
                    other => out.push(error(
                        &item,
                        "to app",
                        format!(
                            "the topic must be a string, not {}",
                            other.map_or("nothing".into(), |v| v.to_string())
                        ),
                    )),
                },
                Err(err) => out.push(error(&item, "to app", err)),
            }
            next.push(item);
        }
    }
}

/// Which state a stateful Stage keeps for this item: its `by` key, or else its incoming topic.
/// `None` (after reporting an error) when the key can't be worked out.
fn state_key(
    key: Option<&jq::Filter>,
    item: &Item,
    stage: &str,
    out: &mut Vec<Effect>,
) -> Option<String> {
    let Some(key) = key else {
        return Some(item.origin.topic().to_string());
    };
    match jq::run(key, item.value.clone(), &item.origin.vars) {
        Ok(results) => match results.into_iter().next() {
            Some(Val::TStr(text)) => Some(String::from_utf8_lossy(&text).into_owned()),
            Some(value) => Some(value.to_string()),
            None => {
                out.push(error(item, stage, "the `by` key gave no value".into()));
                None
            }
        },
        Err(err) => {
            out.push(error(item, stage, err));
            None
        }
    }
}

fn error(item: &Item, stage: &str, message: String) -> Effect {
    Effect::Error(format!("{stage} on {}: {message}", item.origin.topic()))
}

fn truthy(value: &Val) -> bool {
    !matches!(value, Val::Null | Val::Bool(false))
}

fn derived_topic(topic: &str) -> Result<String, String> {
    let topic = topic.trim_matches('/');
    if topic.is_empty() {
        return Err("the topic is empty".into());
    }
    if topic.contains(['+', '#']) {
        return Err(format!("the topic `{topic}` can't contain `+` or `#`"));
    }
    Ok(topic.to_string())
}

/// JSON when the payload is JSON, text when it's text, bytes otherwise.
fn payload_value(payload: &[u8]) -> Val {
    if let Ok(value) = jaq_json::read::parse_single(payload) {
        return value;
    }
    match std::str::from_utf8(payload) {
        Ok(text) => Val::from(text.to_string()),
        Err(_) => Val::byte_str(bytes::Bytes::copy_from_slice(payload)),
    }
}

/// Strings are sent as their text; everything else as JSON.
fn payload_bytes(value: &Val) -> Vec<u8> {
    match value {
        Val::TStr(bytes) | Val::BStr(bytes) => bytes.to_vec(),
        other => other.to_string().into_bytes(),
    }
}

fn properties_value(p: &crate::message::MessageProperties) -> Val {
    let json = serde_json::json!({
        "contentType": p.content_type,
        "responseTopic": p.response_topic,
        "correlationData": p.correlation_data.as_ref().map(|c| String::from_utf8_lossy(c).into_owned()),
        "messageExpiryInterval": p.message_expiry_interval,
        "payloadFormatIndicator": p.payload_format_indicator,
        "userProperties": p.user_properties.iter().map(|(k, v)| serde_json::json!([k, v])).collect::<Vec<_>>(),
    });
    jaq_json::read::parse_single(json.to_string().as_bytes()).unwrap_or(Val::Null)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::message::ConnectionId;
    use crate::processing::lang::parse;

    fn runtime(source: &str) -> Runtime {
        Runtime::new(&parse(source).unwrap_or_else(|e| panic!("{e:?}"))).unwrap()
    }

    fn origin(topic: &str, payload: &str) -> Origin {
        Origin::new(
            Arc::new(ReceivedMessage {
                seq: 1,
                connection_id: ConnectionId(1),
                topic: topic.into(),
                payload: bytes::Bytes::copy_from_slice(payload.as_bytes()),
                qos: 1,
                retain: false,
                received_at_ms: 1_000,
                properties: None,
            }),
            "Dev Broker",
        )
    }

    fn run(rt: &mut Runtime, messages: &[(&str, &str)]) -> Vec<Effect> {
        let now = Instant::now();
        let mut out = Vec::new();
        for (topic, payload) in messages {
            rt.push(origin(topic, payload), now, &mut out);
        }
        out
    }

    fn derived(topic: &str, payload: &str) -> Effect {
        Effect::Derived {
            topic: topic.into(),
            payload: payload.as_bytes().to_vec(),
        }
    }

    #[test]
    fn the_plan_example_end_to_end() {
        let mut rt = runtime(
            r#"from "Dev Broker"
            |> filter topic "home/+/climate-1"
            |> map { temp: .temperature, room: $levels[1] }
            |> filter .temp > 22
            |> to app "hot/{.room}""#,
        );
        let out = run(
            &mut rt,
            &[
                ("home/kitchen/climate-1", r#"{"temperature": 23}"#),
                ("home/office/climate-1", r#"{"temperature": 19}"#),
                ("home/office/power-1", r#"{"temperature": 99}"#),
            ],
        );
        assert_eq!(
            out,
            [derived("hot/kitchen", r#"{"temp":23,"room":"kitchen"}"#)]
        );
    }

    #[test]
    fn from_picks_connections() {
        assert!(runtime("from all").reads("anything"));
        let named = runtime(r#"from "A", "B""#);
        assert!(named.reads("B") && !named.reads("C"));
    }

    #[test]
    fn negated_topic_filter() {
        let mut rt = runtime(r#"from all |> filter not topic "+/status" |> to app "x""#);
        let out = run(&mut rt, &[("a/status", "1"), ("a/temp", "2")]);
        assert_eq!(out, [derived("x", "2")]);
    }

    #[test]
    fn map_fans_out_and_payload_stays_available() {
        let mut rt = runtime(
            r#"from all |> map .items[] |> to app "item/{.}/of/{$payload.items | length}""#,
        );
        let out = run(&mut rt, &[("t", r#"{"items": ["a", "b"]}"#)]);
        assert_eq!(
            out,
            [derived("item/a/of/2", "a"), derived("item/b/of/2", "b")]
        );
    }

    #[test]
    fn text_payloads_are_strings() {
        let mut rt = runtime(r#"from all |> filter . == "online" |> map "is {.}" |> to app "s""#);
        let out = run(&mut rt, &[("t", "online"), ("t", "offline")]);
        assert_eq!(out, [derived("s", "is online")]);
    }

    #[test]
    fn changes_is_per_topic() {
        let mut rt = runtime(r#"from all |> changes |> to app "{$topic}""#);
        let out = run(
            &mut rt,
            &[("a", "1"), ("a", "1"), ("b", "1"), ("a", "2"), ("a", "2")],
        );
        assert_eq!(
            out,
            [derived("a", "1"), derived("b", "1"), derived("a", "2")]
        );
    }

    #[test]
    fn throttle_lets_the_first_through_and_releases_the_latest_later() {
        let mut rt = runtime(r#"from all |> throttle 1s |> to app "{$topic}""#);
        let t0 = Instant::now();
        let mut out = Vec::new();
        rt.push(origin("a", "1"), t0, &mut out);
        rt.push(origin("a", "2"), t0 + Duration::from_millis(100), &mut out);
        rt.push(origin("a", "3"), t0 + Duration::from_millis(200), &mut out);
        rt.push(origin("b", "9"), t0 + Duration::from_millis(200), &mut out);
        assert_eq!(out, [derived("a", "1"), derived("b", "9")]);
        rt.tick(t0 + Duration::from_millis(500), &mut out);
        assert_eq!(out.len(), 2, "nothing released before the interval");
        rt.tick(t0 + Duration::from_millis(1000), &mut out);
        assert_eq!(out[2..], [derived("a", "3")]);
        rt.tick(t0 + Duration::from_millis(3000), &mut out);
        assert_eq!(out.len(), 3, "nothing pending, nothing released");
    }

    #[test]
    fn by_keys_share_state_across_topics() {
        // Two sensors in one room: keyed by room, they count as one.
        let mut rt = runtime(r#"from all |> changes by $levels[1] |> to app "{$levels[1]}""#);
        let out = run(
            &mut rt,
            &[("r/bed/s1", "20"), ("r/bed/s2", "20"), ("r/bed/s2", "21")],
        );
        assert_eq!(out, [derived("bed", "20"), derived("bed", "21")]);

        let mut rt = runtime(r#"from all |> throttle 1s by .room |> to app "{.room}""#);
        let t0 = Instant::now();
        let mut out = Vec::new();
        rt.push(origin("a/1", r#"{"room":"bed","t":1}"#), t0, &mut out);
        rt.push(
            origin("a/2", r#"{"room":"bed","t":2}"#),
            t0 + Duration::from_millis(10),
            &mut out,
        );
        rt.push(
            origin("a/3", r#"{"room":"hall","t":3}"#),
            t0 + Duration::from_millis(20),
            &mut out,
        );
        assert_eq!(out.len(), 2, "bed once, hall once");
        rt.tick(t0 + Duration::from_secs(1), &mut out);
        assert_eq!(out[2], derived("bed", r#"{"room":"bed","t":2}"#));
    }

    #[test]
    fn a_key_that_fails_skips_the_message() {
        let mut rt = runtime(r#"from all |> changes by .a.b |> to app "x""#);
        let out = run(&mut rt, &[("t", r#"{"a": 5}"#)]);
        assert!(
            matches!(&out[0], Effect::Error(e) if e.starts_with("changes on t:")),
            "{out:?}"
        );
    }

    #[test]
    fn inspect_reports_and_passes_on() {
        let mut rt = runtime(r#"from all |> map .a |> inspect "a" |> to app "x""#);
        let out = run(&mut rt, &[("t", r#"{"a": [1]}"#)]);
        assert_eq!(
            out,
            [
                Effect::Inspected {
                    label: Some("a".into()),
                    topic: "t".into(),
                    value: "[1]".into()
                },
                derived("x", "[1]")
            ]
        );
    }

    #[test]
    fn several_sinks_along_one_pipeline() {
        let mut rt = runtime(r#"from all |> to app "raw" |> filter . > 1 |> to app "big""#);
        let out = run(&mut rt, &[("t", "1"), ("t", "2")]);
        assert_eq!(
            out,
            [
                derived("raw", "1"),
                derived("raw", "2"),
                derived("big", "2")
            ]
        );
    }

    #[test]
    fn errors_skip_the_message_and_say_where() {
        let mut rt = runtime(r#"from all |> map .a + 1 |> to app "x""#);
        let out = run(&mut rt, &[("t", r#"{"a": "text"}"#), ("t", r#"{"a": 1}"#)]);
        assert!(
            matches!(&out[0], Effect::Error(e) if e.starts_with("map on t: ")),
            "{out:?}"
        );
        assert_eq!(out[1], derived("x", "2"));
    }

    #[test]
    fn derived_topics_must_be_usable() {
        let mut rt = runtime(r#"from all |> to app .name"#);
        let out = run(
            &mut rt,
            &[
                ("t", r#"{"name": "a/+"}"#),
                ("t", r#"{"name": 5}"#),
                ("t", r#"{"name": "/ok/"}"#),
            ],
        );
        assert!(matches!(&out[0], Effect::Error(e) if e.contains("can't contain")));
        assert!(matches!(&out[1], Effect::Error(e) if e.contains("must be a string")));
        assert_eq!(
            out[2],
            Effect::Derived {
                topic: "ok".into(),
                payload: br#"{"name":"/ok/"}"#.to_vec()
            }
        );
    }

    #[test]
    fn message_variables() {
        let mut rt = runtime(
            r#"from all |> map [$topic, $levels, $connection, $time, $qos, $retained] |> to app "v""#,
        );
        let out = run(&mut rt, &[("a/b", "x")]);
        assert_eq!(
            out,
            [derived(
                "v",
                r#"["a/b",["a","b"],"Dev Broker",1.0,1,false]"#
            )]
        );
    }
}
