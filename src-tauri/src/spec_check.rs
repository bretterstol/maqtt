//! The spec check, opt-in per Broker Profile: each message received on a Connection is compared
//! with the Profile's API Specs. A topic no Channel describes, or a payload its message schemas
//! don't allow, counts as a mismatch, and the latest one says why.
//!
//! It runs on its own task beside the History Store's, so checking never holds up recording.
//! What each Connection is checked against is looked up again every few seconds, so edits to a
//! spec, and the check being switched on or off, are followed.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use serde::Serialize;
use serde_json::Value;
use tokio::sync::broadcast::error::RecvError;

use crate::bus::{Bus, BusEvent, under_prefix};
use crate::message::{ConnectionId, ReceivedMessage};
use crate::spec::{self, SpecView};

/// How long what a Connection is checked against is trusted before it's looked up again.
const LOOK_AGAIN: Duration = Duration::from_secs(2);

/// How a topic's messages compare with the spec.
#[derive(Debug, Clone, Default, PartialEq, Serialize, ts_rs::TS)]
#[serde(rename_all = "camelCase")]
#[ts(export, export_to = "generated/")]
pub struct TopicCheck {
    /// The Channel the topic is on; None when the spec describes no such topic.
    pub channel: Option<String>,
    /// Messages checked.
    pub messages: u64,
    /// Messages that didn't match.
    pub mismatched: u64,
    /// Why the latest mismatch didn't match.
    pub problem: Option<String>,
}

/// A topic's check changed; None when it's no longer checked (the check was switched off).
#[derive(Debug, Clone, PartialEq, Serialize, ts_rs::TS)]
#[serde(rename_all = "camelCase")]
#[ts(export, export_to = "generated/")]
pub struct CheckUpdate {
    pub connection_id: ConnectionId,
    pub topic: String,
    pub check: Option<TopicCheck>,
}

/// What one Connection's messages are checked against.
struct Rules {
    looked_at: Instant,
    /// The specs as read, to see when they change.
    specs: Vec<SpecView>,
    channels: Vec<Channel>,
}

struct Channel {
    address: String,
    /// Each level: Some(literal), or None for a parameter.
    levels: Vec<Option<String>>,
    literal: usize,
    /// The message schemas, ready to check with. Empty when none can be checked (no schema,
    /// or Avro and the like): any payload goes.
    validators: Vec<jsonschema::Validator>,
}

impl Channel {
    fn new(address: &str, schemas: &[Value]) -> Self {
        let levels: Vec<Option<String>> = address
            .split('/')
            .map(|level| {
                let param = level.starts_with('{') && level.ends_with('}') && level.len() > 2;
                (!param).then(|| level.to_string())
            })
            .collect();
        Self {
            address: address.to_string(),
            literal: levels.iter().filter(|l| l.is_some()).count(),
            levels,
            validators: schemas.iter().filter_map(spec::compile).collect(),
        }
    }

    fn fits(&self, topic: &str) -> bool {
        let levels: Vec<&str> = topic.split('/').collect();
        levels.len() == self.levels.len()
            && self
                .levels
                .iter()
                .zip(&levels)
                .all(|(want, got)| match want {
                    Some(literal) => literal == got,
                    None => !got.is_empty(),
                })
    }
}

impl Rules {
    fn new(specs: Vec<SpecView>, now: Instant) -> Self {
        let channels = specs
            .iter()
            .filter_map(|view| view.spec.as_ref())
            .flat_map(|spec| &spec.channels)
            .map(|c| {
                let schemas: Vec<Value> = c
                    .messages
                    .iter()
                    .filter_map(|m| m.payload.clone())
                    .collect();
                Channel::new(&c.address, &schemas)
            })
            .collect();
        Self {
            looked_at: now,
            specs,
            channels,
        }
    }

    /// The Channel a topic is on, the one with the most literal levels first.
    fn channel(&self, topic: &str) -> Option<&Channel> {
        self.channels
            .iter()
            .filter(|c| c.fits(topic))
            .max_by_key(|c| c.literal)
    }
}

/// The spec check's state: what each Connection is checked against, and each topic's result.
#[derive(Default)]
pub struct SpecChecks {
    rules: HashMap<ConnectionId, Rules>,
    checks: HashMap<(ConnectionId, String), TopicCheck>,
    changed: HashSet<(ConnectionId, String)>,
    /// Topics no longer checked since the last `take_changed`.
    dropped: Vec<(ConnectionId, String)>,
}

impl SpecChecks {
    /// Checks a received message. `specs_for` gives the specs a Connection is checked against,
    /// or None when its Profile doesn't check; it's asked again every [`LOOK_AGAIN`].
    pub fn check(
        &mut self,
        message: &ReceivedMessage,
        now: Instant,
        specs_for: &dyn Fn(ConnectionId) -> Option<Vec<SpecView>>,
    ) {
        let connection = message.connection_id;
        let stale = self
            .rules
            .get(&connection)
            .is_none_or(|r| now.duration_since(r.looked_at) >= LOOK_AGAIN);
        if stale {
            match specs_for(connection) {
                Some(specs) => {
                    let same = self
                        .rules
                        .get(&connection)
                        .is_some_and(|r| r.specs == specs);
                    if same {
                        self.rules.get_mut(&connection).unwrap().looked_at = now;
                    } else {
                        // What it's checked against changed: the counts start over.
                        self.forget(connection);
                        self.rules.insert(connection, Rules::new(specs, now));
                    }
                }
                None => {
                    self.forget(connection);
                    // Remember that it isn't checked, until it's time to look again.
                    self.rules.insert(connection, Rules::new(Vec::new(), now));
                }
            }
        }
        let rules = &self.rules[&connection];
        if rules.specs.is_empty() {
            return;
        }
        // An empty payload clears a Retained Message; it isn't a message to check.
        if message.payload.is_empty() {
            return;
        }
        let channel = rules.channel(&message.topic);
        let problem = match channel {
            None => Some("No channel in the spec describes this topic".to_string()),
            Some(c) if c.validators.is_empty() => None,
            Some(c) => match serde_json::from_slice::<Value>(&message.payload) {
                Err(err) => Some(format!("The payload isn't JSON: {err}")),
                Ok(instance) => spec::best_fit(&c.validators, &instance).first().map(|p| {
                    if p.path.is_empty() {
                        p.message.clone()
                    } else {
                        format!("{}: {}", p.path.join("/"), p.message)
                    }
                }),
            },
        };
        let key = (connection, message.topic.clone());
        let check = self.checks.entry(key.clone()).or_default();
        check.channel = channel.map(|c| c.address.clone());
        check.messages += 1;
        if problem.is_some() {
            check.mismatched += 1;
            check.problem = problem;
        }
        self.changed.insert(key);
    }

    /// Stops checking a Connection's topics, telling the frontend so.
    fn forget(&mut self, connection: ConnectionId) {
        let gone: Vec<_> = self
            .checks
            .keys()
            .filter(|(c, _)| *c == connection)
            .cloned()
            .collect();
        for key in gone {
            self.checks.remove(&key);
            self.changed.remove(&key);
            self.dropped.push(key);
        }
    }

    /// The topics at and under `prefix` were cleared: their counts start over. The frontend
    /// drops them with the topics.
    pub fn clear(&mut self, connection: ConnectionId, prefix: &str) {
        self.checks
            .retain(|(c, topic), _| *c != connection || !under_prefix(topic, prefix));
        self.changed
            .retain(|(c, topic)| *c != connection || !under_prefix(topic, prefix));
    }

    /// What changed since the last call.
    pub fn take_changed(&mut self) -> Vec<CheckUpdate> {
        let dropped = self
            .dropped
            .drain(..)
            .map(|(connection_id, topic)| CheckUpdate {
                connection_id,
                topic,
                check: None,
            });
        let changed: Vec<_> = std::mem::take(&mut self.changed)
            .into_iter()
            .map(|key| CheckUpdate {
                check: self.checks.get(&key).cloned(),
                connection_id: key.0,
                topic: key.1,
            })
            .collect();
        dropped.chain(changed).collect()
    }

    /// Every topic's check, for a frontend starting over.
    pub fn snapshot(&self) -> Vec<CheckUpdate> {
        self.checks
            .iter()
            .map(|((connection_id, topic), check)| CheckUpdate {
                connection_id: *connection_id,
                topic: topic.clone(),
                check: Some(check.clone()),
            })
            .collect()
    }
}

/// Checks what arrives on the Message Bus, on its own task.
pub fn spawn_checker(
    bus: &Bus,
    checks: Arc<Mutex<SpecChecks>>,
    specs_for: impl Fn(ConnectionId) -> Option<Vec<SpecView>> + Send + 'static,
) {
    let mut rx = bus.subscribe();
    tauri::async_runtime::spawn(async move {
        loop {
            let event = match rx.recv().await {
                Ok(event) => event,
                // Missed messages simply go unchecked.
                Err(RecvError::Lagged(_)) => continue,
                Err(RecvError::Closed) => return,
            };
            let mut checks = checks.lock().unwrap();
            let mut handle = |event: BusEvent| match event {
                BusEvent::Message(m) => checks.check(&m, Instant::now(), &specs_for),
                BusEvent::TopicsCleared {
                    connection_id,
                    prefix,
                } => checks.clear(connection_id, &prefix),
                _ => {}
            };
            handle(event);
            while let Ok(event) = rx.try_recv() {
                handle(event);
            }
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spec::{Spec, SpecChannel, SpecMessage};
    use bytes::Bytes;
    use serde_json::json;
    use std::cell::RefCell;

    fn message(connection: u32, topic: &str, payload: &str) -> ReceivedMessage {
        ReceivedMessage {
            seq: 0,
            connection_id: ConnectionId(connection),
            topic: topic.into(),
            payload: Bytes::copy_from_slice(payload.as_bytes()),
            qos: 0,
            retain: false,
            received_at_ms: 0,
            properties: None,
        }
    }

    fn spec(title: &str) -> Vec<SpecView> {
        let channel = |address: &str, payload: Option<Value>| SpecChannel {
            address: address.into(),
            description: None,
            parameters: Vec::new(),
            messages: vec![SpecMessage {
                name: "m".into(),
                summary: None,
                payload,
                examples: Vec::new(),
            }],
        };
        vec![SpecView {
            path: "spec.yaml".into(),
            spec: Some(Spec {
                title: title.into(),
                version: "1".into(),
                asyncapi: "2.6.0".into(),
                channels: vec![
                    channel(
                        "home/{room}/climate",
                        Some(json!({
                            "type": "object",
                            "required": ["t"],
                            "properties": { "t": { "type": "number" } }
                        })),
                    ),
                    channel("home/status", None),
                ],
            }),
            problem: None,
        }]
    }

    fn update(checks: &mut SpecChecks, topic: &str) -> Option<TopicCheck> {
        checks
            .take_changed()
            .into_iter()
            .find(|u| u.topic == topic)
            .and_then(|u| u.check)
    }

    #[test]
    fn counts_what_matches_the_spec_and_says_why_the_rest_doesnt() {
        let mut checks = SpecChecks::default();
        let now = Instant::now();
        let specs = |_| Some(spec("Home"));
        checks.check(
            &message(1, "home/kitchen/climate", r#"{"t": 21}"#),
            now,
            &specs,
        );
        checks.check(
            &message(1, "home/kitchen/climate", r#"{"t": "hot"}"#),
            now,
            &specs,
        );
        let check = update(&mut checks, "home/kitchen/climate").unwrap();
        assert_eq!(check.channel.as_deref(), Some("home/{room}/climate"));
        assert_eq!((check.messages, check.mismatched), (2, 1));
        assert!(
            check.problem.as_deref().unwrap().starts_with("t: "),
            "{check:?}"
        );

        checks.check(&message(1, "home/kitchen/climate", "warm"), now, &specs);
        let check = update(&mut checks, "home/kitchen/climate").unwrap();
        assert!(check.problem.unwrap().contains("isn't JSON"));

        checks.check(&message(1, "home/garage", "1"), now, &specs);
        let check = update(&mut checks, "home/garage").unwrap();
        assert_eq!(check.channel, None);
        assert_eq!(check.mismatched, 1);
        assert!(check.problem.unwrap().contains("No channel"));

        // A channel without a schema takes anything; an empty payload isn't checked.
        checks.check(&message(1, "home/status", "online"), now, &specs);
        checks.check(&message(1, "home/kitchen/climate", ""), now, &specs);
        let changed = checks.take_changed();
        assert_eq!(changed.len(), 1);
        assert_eq!(changed[0].check.as_ref().unwrap().mismatched, 0);
    }

    #[test]
    fn follows_the_check_being_switched_off_and_the_spec_changing() {
        let mut checks = SpecChecks::default();
        let now = Instant::now();
        let current = RefCell::new(Some(spec("One")));
        let specs = |_| current.borrow().clone();
        let bad = message(1, "home/kitchen/climate", "{}");
        checks.check(&bad, now, &specs);
        checks.check(&bad, now, &specs);
        assert_eq!(
            update(&mut checks, "home/kitchen/climate")
                .unwrap()
                .mismatched,
            2
        );

        // Within the look-again time, an edit isn't seen yet.
        *current.borrow_mut() = Some(spec("Two"));
        checks.check(&bad, now + Duration::from_millis(100), &specs);
        assert_eq!(
            update(&mut checks, "home/kitchen/climate")
                .unwrap()
                .messages,
            3
        );
        // Then it is, and the counts start over.
        checks.check(&bad, now + LOOK_AGAIN, &specs);
        let changed = checks.take_changed();
        assert_eq!(changed[0].check, None, "the old counts are dropped first");
        assert_eq!(changed[1].check.as_ref().unwrap().messages, 1);

        // Switched off: the topics are no longer checked, and nothing more is counted.
        *current.borrow_mut() = None;
        let later = now + LOOK_AGAIN * 2;
        checks.check(&bad, later, &specs);
        let changed = checks.take_changed();
        assert_eq!(changed.len(), 1);
        assert_eq!(changed[0].check, None);
        assert!(checks.snapshot().is_empty());
        // Another Connection is looked up on its own.
        checks.check(&message(2, "home/kitchen/climate", "{}"), later, &|c| {
            (c == ConnectionId(2)).then(|| spec("Other"))
        });
        assert_eq!(checks.snapshot().len(), 1);
    }

    #[test]
    fn cleared_topics_start_over() {
        let mut checks = SpecChecks::default();
        let now = Instant::now();
        let specs = |_| Some(spec("Home"));
        checks.check(&message(1, "home/kitchen/climate", "{}"), now, &specs);
        checks.check(&message(1, "home/status", "x"), now, &specs);
        checks.clear(ConnectionId(1), "home/kitchen");
        let topics: Vec<String> = checks.snapshot().into_iter().map(|u| u.topic).collect();
        assert_eq!(topics, ["home/status"]);
        assert!(
            checks
                .take_changed()
                .iter()
                .all(|u| u.topic == "home/status")
        );
    }
}
