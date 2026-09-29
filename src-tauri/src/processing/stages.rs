//! Running a parsed Pipeline: each message becomes a value that flows through the Stages, and
//! whatever reaches a Sink, an `inspect` or an error becomes an [`Effect`] for the runner.
//! No threads or clocks here; the caller passes the time, which keeps it easy to test.

use std::rc::Rc;
use std::sync::Arc;
use std::time::{Duration, Instant};

use jaq_json::Val;

use super::jq;
use super::keyed::Keyed;
use super::lang::{FileFormat, Pipeline, Rotate, Source, Stage, TopicFilter, WindowSize};
use super::markers::Point;
use crate::message::ReceivedMessage;
use crate::profile::validate_topic_name;

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
    /// A message to republish on a Connection, named as in `from`.
    Republish {
        broker: String,
        topic: String,
        payload: Vec<u8>,
    },
    /// A value for `to chart`, on the line for its series.
    Sample {
        series: String,
        value: f64,
        at_ms: u64,
    },
    /// A point for `to geo`, kept per key.
    Point {
        key: String,
        label: Option<String>,
        lat: f64,
        lon: f64,
        at_ms: u64,
    },
    /// A list for `to geo`: the whole map, replacing what was there.
    Snapshot {
        points: Vec<Point>,
        at_ms: u64,
    },
    /// A value for `to file`: the path as the expression gave it, and the value as JSON.
    Write {
        path: String,
        format: FileFormat,
        rotate: Option<Rotate>,
        record: serde_json::Value,
    },
    Error(String),
}

/// What went through one Stage, kept only when Preview asks for it.
#[derive(Debug, Clone, Default, PartialEq, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct StageTrace {
    /// Values the Stage passed on.
    pub passed: usize,
    /// The latest of them, as JSON.
    pub last: Option<String>,
    pub errors: usize,
    pub first_error: Option<String>,
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
#[derive(Clone)]
struct Item {
    value: Val,
    origin: Rc<Origin>,
    /// `$key`: what the latest Stage that keeps state kept it under; null before any.
    key: Val,
    /// The Pipeline's named wildcards, as the `filter topic` Stages it passed set them; null
    /// until then.
    captured: Rc<[Val]>,
}

/// The jq variables for an item: its message's, `$key`, then the named wildcards.
fn vars(item: &Item) -> Vec<Val> {
    let mut vars = Vec::with_capacity(item.origin.vars.len() + 1 + item.captured.len());
    vars.extend(item.origin.vars.iter().cloned());
    vars.push(item.key.clone());
    vars.extend(item.captured.iter().cloned());
    vars
}

enum Running {
    FilterTopic {
        filter: TopicFilter,
        /// Per name the filter declares, where its value goes in [`Item::captured`].
        slots: Vec<(String, usize)>,
    },
    Filter(jq::Filter),
    Map(jq::Filter),
    Changes {
        key: Option<jq::Filter>,
        last: Keyed<Val>,
    },
    Join {
        key: Option<jq::Filter>,
        name: Option<jq::Filter>,
        needs: Vec<String>,
        within: Option<Duration>,
        /// Per key: each part's latest value and when it came, in the order parts first came.
        parts: Keyed<Vec<(String, Val, Instant)>>,
    },
    /// `delta` and `pairwise`: the previous value per key, and which of the two to send.
    Previous {
        pairwise: bool,
        key: Option<jq::Filter>,
        last: Keyed<Val>,
    },
    Throttle {
        interval: Duration,
        key: Option<jq::Filter>,
        keys: Keyed<Throttled>,
    },
    Debounce {
        interval: Duration,
        key: Option<jq::Filter>,
        /// Per key: when to send, and the latest value.
        pending: Keyed<(Instant, Item)>,
    },
    Scan {
        start: jq::Filter,
        update: jq::Filter,
        key: Option<jq::Filter>,
        results: Keyed<Val>,
    },
    Window {
        size: WindowSize,
        key: Option<jq::Filter>,
        open: Keyed<Window>,
    },
    Take {
        count: usize,
        key: Option<jq::Filter>,
        taken: Keyed<usize>,
    },
    Inspect(Option<String>),
    ToApp(jq::Filter),
    ToBroker {
        broker: String,
        topic: jq::Filter,
        /// When the Pipeline reads this Broker: the `filter topic` Stages at its start. A topic
        /// that gets past them would come back into the Pipeline, so it's refused.
        reads_back: Option<Vec<TopicFilter>>,
    },
    ToFile {
        path: jq::Filter,
        format: FileFormat,
        rotate: Option<Rotate>,
    },
    ToGeo,
    ToChart,
}

struct Throttled {
    open_at: Instant,
    pending: Option<Item>,
}

/// The values collected so far in one key's window.
struct Window {
    /// For a time window: which interval since 1970 it covers.
    index: u64,
    items: Vec<Item>,
    key: Val,
}

impl Window {
    /// The window's values as one list, sent on as a value of the newest message.
    fn close(self) -> Item {
        let newest = self.items.last().expect("windows are never empty");
        let (origin, captured) = (newest.origin.clone(), newest.captured.clone());
        Item {
            value: self.items.into_iter().map(|item| item.value).collect(),
            origin,
            key: self.key,
            captured,
        }
    }
}

/// A Pipeline ready to run; not `Send` (jq values are reference-counted), so it lives on one
/// thread.
pub struct Runtime {
    from: Source,
    /// How many named wildcards the Pipeline declares.
    names: usize,
    stages: Vec<Running>,
    trace: Option<Vec<StageTrace>>,
    /// A moment on both clocks, from the first message: time windows follow the messages'
    /// wall-clock times, while the caller passes monotonic time.
    epoch: Option<(Instant, u64)>,
}

impl Runtime {
    pub fn new(pipeline: &Pipeline) -> Result<Self, String> {
        let compile = |code: &str| jq::compile(code, &pipeline.names).map_err(|e| e.message);
        let slot = |name: &str| {
            let at = pipeline.names.iter().position(|n| n == name);
            (name.to_string(), at.expect("the parser lists every name"))
        };
        let leading_filters: Vec<TopicFilter> = pipeline
            .stages
            .iter()
            .map_while(|stage| match stage {
                Stage::FilterTopic(filter) => Some(filter.clone()),
                _ => None,
            })
            .collect();
        let stages = pipeline
            .stages
            .iter()
            .map(|stage| {
                Ok(match stage {
                    Stage::FilterTopic(filter) => Running::FilterTopic {
                        slots: filter.names().into_iter().map(slot).collect(),
                        filter: filter.clone(),
                    },
                    Stage::Filter(code) => Running::Filter(compile(code)?),
                    Stage::Map(code) => Running::Map(compile(code)?),
                    Stage::Join(join) => Running::Join {
                        key: join.key.as_deref().map(compile).transpose()?,
                        name: join.name.as_deref().map(compile).transpose()?,
                        needs: join.needs.clone(),
                        within: join.within,
                        parts: Keyed::default(),
                    },
                    Stage::Delta(key) | Stage::Pairwise(key) => Running::Previous {
                        pairwise: matches!(stage, Stage::Pairwise(_)),
                        key: key.as_deref().map(compile).transpose()?,
                        last: Keyed::default(),
                    },
                    Stage::Changes(key) => Running::Changes {
                        key: key.as_deref().map(compile).transpose()?,
                        last: Keyed::default(),
                    },
                    Stage::Throttle(interval, key) => Running::Throttle {
                        interval: *interval,
                        key: key.as_deref().map(compile).transpose()?,
                        keys: Keyed::default(),
                    },
                    Stage::Debounce(interval, key) => Running::Debounce {
                        interval: *interval,
                        key: key.as_deref().map(compile).transpose()?,
                        pending: Keyed::default(),
                    },
                    Stage::Scan { start, update, key } => {
                        let (prefix, suffix) = jq::SCAN_WRAP;
                        Running::Scan {
                            start: compile(start)?,
                            update: compile(&format!("{prefix}{update}{suffix}"))?,
                            key: key.as_deref().map(compile).transpose()?,
                            results: Keyed::default(),
                        }
                    }
                    Stage::Window(size, key) => Running::Window {
                        size: *size,
                        key: key.as_deref().map(compile).transpose()?,
                        open: Keyed::default(),
                    },
                    Stage::Take(count, key) => Running::Take {
                        count: *count,
                        key: key.as_deref().map(compile).transpose()?,
                        taken: Keyed::default(),
                    },
                    Stage::Inspect(label) => Running::Inspect(label.clone()),
                    Stage::ToApp(code) => Running::ToApp(compile(code)?),
                    Stage::ToGeo => Running::ToGeo,
                    Stage::ToChart => Running::ToChart,
                    Stage::ToFile {
                        path,
                        format,
                        rotate,
                    } => Running::ToFile {
                        path: compile(path)?,
                        format: *format,
                        rotate: *rotate,
                    },
                    Stage::ToBroker { broker, topic } => Running::ToBroker {
                        broker: broker.clone(),
                        topic: compile(topic)?,
                        reads_back: reads(&pipeline.from, broker).then(|| leading_filters.clone()),
                    },
                })
            })
            .collect::<Result<_, String>>()?;
        Ok(Self {
            from: pipeline.from.clone(),
            names: pipeline.names.len(),
            stages,
            trace: None,
            epoch: None,
        })
    }

    /// Keeps a [`StageTrace`] per Stage from now on.
    pub fn traced(mut self) -> Self {
        self.trace = Some(vec![StageTrace::default(); self.stages.len()]);
        self
    }

    pub fn trace(&self) -> &[StageTrace] {
        self.trace.as_deref().unwrap_or_default()
    }

    /// Whether a message gets past `from` and the `filter topic` Stages at the start: the cheap
    /// test Preview uses to pick sample messages.
    pub fn wants(&self, connection: &str, topic: &str) -> bool {
        self.reads(connection)
            && self
                .stages
                .iter()
                .map_while(|stage| match stage {
                    Running::FilterTopic { filter, .. } => Some(filter),
                    _ => None,
                })
                .all(|filter| filter.passes(topic))
    }

    /// Whether messages from this Connection are the Pipeline's to handle.
    pub fn reads(&self, connection: &str) -> bool {
        reads(&self.from, connection)
    }

    pub fn push(&mut self, origin: Origin, now: Instant, out: &mut Vec<Effect>) {
        if self.finished() {
            return;
        }
        self.epoch
            .get_or_insert((now, origin.message.received_at_ms));
        let origin = Rc::new(origin);
        let item = Item {
            value: origin.payload(),
            origin,
            key: Val::Null,
            captured: vec![Val::Null; self.names].into(),
        };
        self.run_from(0, vec![item], now, out);
    }

    /// Whether a `take` without `by` has passed all it will: the Pipeline is done.
    pub fn finished(&self) -> bool {
        self.stages.iter().any(|stage| match stage {
            Running::Take {
                count,
                key: None,
                taken,
            } => taken.iter().map(|(_, n)| n).sum::<usize>() >= *count,
            _ => false,
        })
    }

    /// Lets time-based Stages send what is due: throttled and debounced values, and time
    /// windows that have closed.
    pub fn tick(&mut self, now: Instant, out: &mut Vec<Effect>) {
        self.release(now, false, out);
    }

    /// Sends everything still held back, due or not, including windows not yet full. For the
    /// end of a Preview, or a Pipeline that finished.
    pub fn flush(&mut self, now: Instant, out: &mut Vec<Effect>) {
        self.release(now, true, out);
    }

    fn wall_ms(&self, now: Instant) -> u64 {
        self.epoch.map_or(0, |(at, ms)| {
            ms + now.saturating_duration_since(at).as_millis() as u64
        })
    }

    fn release(&mut self, now: Instant, all: bool, out: &mut Vec<Effect>) {
        let wall = self.wall_ms(now);
        for i in 0..self.stages.len() {
            let released = match &mut self.stages[i] {
                Running::Throttle { interval, keys, .. } => {
                    let mut released = Vec::new();
                    for state in keys.values_mut() {
                        if (all || now >= state.open_at)
                            && let Some(item) = state.pending.take()
                        {
                            state.open_at = now + *interval;
                            released.push(item);
                        }
                    }
                    released
                }
                Running::Debounce { pending, .. } => {
                    let due: Vec<String> = pending
                        .iter()
                        .filter(|(_, (at, _))| all || now >= *at)
                        .map(|(key, _)| key.clone())
                        .collect();
                    let mut released: Vec<(Instant, Item)> =
                        due.iter().filter_map(|key| pending.remove(key)).collect();
                    released.sort_by_key(|(at, _)| *at);
                    released.into_iter().map(|(_, item)| item).collect()
                }
                Running::Window { size, open, .. } => {
                    let closed: Vec<String> = open
                        .iter()
                        .filter(|(_, window)| {
                            all || matches!(size, WindowSize::Time(interval)
                                if wall >= (window.index + 1) * interval.as_millis() as u64)
                        })
                        .map(|(key, _)| key.clone())
                        .collect();
                    let mut released: Vec<Window> =
                        closed.iter().filter_map(|key| open.remove(key)).collect();
                    released.sort_by_key(|w| w.items.last().map(|i| i.origin.message.seq));
                    released.into_iter().map(Window::close).collect()
                }
                _ => continue,
            };
            if !released.is_empty() {
                if let Some(trace) = &mut self.trace {
                    record(&mut trace[i], &released, &[]);
                }
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
        for (i, stage) in self.stages.iter_mut().enumerate().skip(start) {
            let mut next = Vec::with_capacity(items.len());
            let before = out.len();
            for item in items {
                apply(stage, item, now, &mut next, out);
            }
            if let Some(trace) = &mut self.trace {
                record(&mut trace[i], &next, &out[before..]);
            }
            items = next;
            if items.is_empty() {
                return;
            }
        }
    }
}

fn record(trace: &mut StageTrace, passed: &[Item], effects: &[Effect]) {
    trace.passed += passed.len();
    if let Some(item) = passed.last() {
        trace.last = Some(item.value.to_string());
    }
    for effect in effects {
        if let Effect::Error(message) = effect {
            trace.errors += 1;
            trace.first_error.get_or_insert_with(|| message.clone());
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
        Running::FilterTopic { filter, slots } => {
            if !filter.passes(item.origin.topic()) {
                return;
            }
            let mut item = item;
            if !slots.is_empty() {
                let mut captured = item.captured.to_vec();
                // A name only some of the listed filters have is null when another matched.
                for (_, at) in slots.iter() {
                    captured[*at] = Val::Null;
                }
                if let Some(pattern) = filter.matching(item.origin.topic()) {
                    for (name, value) in pattern.captured(item.origin.topic()) {
                        if let Some((_, at)) = slots.iter().find(|(n, _)| n == name) {
                            captured[*at] = value.map_or(Val::Null, Val::from);
                        }
                    }
                }
                item.captured = captured.into();
            }
            next.push(item);
        }
        Running::Filter(filter) => match jq::run(filter, item.value.clone(), &vars(&item)) {
            Ok(results) => {
                if results.first().is_some_and(truthy) {
                    next.push(item);
                }
            }
            Err(err) => out.push(error(&item, "filter", err)),
        },
        Running::Map(filter) => match jq::run(filter, item.value.clone(), &vars(&item)) {
            // Each result is its own message downstream; none drops it.
            Ok(results) => next.extend(results.into_iter().map(|value| Item {
                value,
                ..item.clone()
            })),
            Err(err) => out.push(error(&item, "map", err)),
        },
        Running::Changes { key, last } => {
            let Some((key, item)) = keyed(key.as_ref(), item, "changes", out) else {
                return;
            };
            if last.get(&key) != Some(&item.value) {
                last.insert(key, item.value.clone());
                next.push(item);
            }
        }
        Running::Join {
            key,
            name,
            needs,
            within,
            parts,
        } => {
            let part = match name {
                None => item.origin.topic().to_string(),
                Some(name) => match jq::run(name, item.value.clone(), &vars(&item)) {
                    Ok(results) => match results.into_iter().next() {
                        Some(Val::TStr(text)) => String::from_utf8_lossy(&text).into_owned(),
                        Some(other) => other.to_string(),
                        None => {
                            out.push(error(&item, "join", "`as` gave no name".into()));
                            return;
                        }
                    },
                    Err(err) => {
                        out.push(error(&item, "join", err));
                        return;
                    }
                },
            };
            // Without `by`, everything joins into one object: per topic, each would be one part.
            let (key, mut item) = match key {
                Some(_) => match keyed(key.as_ref(), item, "join", out) {
                    Some(keyed) => keyed,
                    None => return,
                },
                None => (String::new(), item),
            };
            let held = parts.entry(key, Vec::new);
            match held.iter_mut().find(|(n, _, _)| *n == part) {
                Some(slot) => (slot.1, slot.2) = (item.value.clone(), now),
                None => held.push((part, item.value.clone(), now)),
            }
            // A part that has gone quiet drops out.
            if let Some(within) = within {
                held.retain(|(_, _, at)| now.saturating_duration_since(*at) <= *within);
            }
            if !needs
                .iter()
                .all(|need| held.iter().any(|(n, _, _)| n == need))
            {
                return;
            }
            let mut object = jaq_json::Map::default();
            for (name, value, _) in held.iter() {
                object.insert(Val::from(name.clone()), value.clone());
            }
            item.value = Val::obj(object);
            next.push(item);
        }
        Running::Previous {
            pairwise,
            key,
            last,
        } => {
            let stage = if *pairwise { "pairwise" } else { "delta" };
            let Some((key, mut item)) = keyed(key.as_ref(), item, stage, out) else {
                return;
            };
            if !*pairwise && !matches!(item.value, Val::Num(_)) {
                out.push(error(
                    &item,
                    stage,
                    format!("`delta` takes numbers, not {}", item.value),
                ));
                return;
            }
            let previous = last.get(&key).cloned();
            last.insert(key, item.value.clone());
            let Some(previous) = previous else {
                return;
            };
            item.value = if *pairwise {
                [previous, item.value].into_iter().collect()
            } else {
                // jq's own arithmetic, so whole numbers stay whole.
                match item.value.clone() - previous {
                    Ok(change) => change,
                    Err(err) => {
                        out.push(error(&item, stage, err.to_string()));
                        return;
                    }
                }
            };
            next.push(item);
        }
        Running::Debounce {
            interval,
            key,
            pending,
        } => {
            let Some((key, item)) = keyed(key.as_ref(), item, "debounce", out) else {
                return;
            };
            // Latest wins, and the quiet interval starts over.
            pending.insert(key, (now + *interval, item));
        }
        Running::Scan {
            start,
            update,
            key,
            results,
        } => {
            let Some((key, item)) = keyed(key.as_ref(), item, "scan", out) else {
                return;
            };
            let previous = match results.get(&key) {
                Some(previous) => previous.clone(),
                None => match jq::run(start, Val::Null, &vars(&item)) {
                    Ok(values) => values.into_iter().next().unwrap_or(Val::Null),
                    Err(err) => return out.push(error(&item, "scan", err)),
                },
            };
            let input: Val = [previous, item.value.clone()].into_iter().collect();
            match jq::run(update, input, &vars(&item)) {
                Ok(values) => {
                    if let Some(value) = values.into_iter().next() {
                        results.insert(key, value.clone());
                        next.push(Item { value, ..item });
                    }
                }
                Err(err) => out.push(error(&item, "scan", err)),
            }
        }
        Running::Window { size, key, open } => {
            let Some((key, item)) = keyed(key.as_ref(), item, "window", out) else {
                return;
            };
            let index = match size {
                WindowSize::Time(interval) => {
                    item.origin.message.received_at_ms / (interval.as_millis() as u64).max(1)
                }
                WindowSize::Count(_) => 0,
            };
            // A value from a later interval closes the window it finds open.
            if open.get(&key).is_some_and(|window| index > window.index) {
                next.push(open.remove(&key).expect("checked").close());
            }
            let window = open.entry(key.clone(), || Window {
                index,
                items: Vec::new(),
                key: item.key.clone(),
            });
            window.items.push(item);
            if matches!(size, WindowSize::Count(n) if window.items.len() >= *n) {
                next.push(open.remove(&key).expect("just filled").close());
            }
        }
        Running::Take { count, key, taken } => {
            let limited_per_key = key.is_some();
            let Some((key, item)) = keyed(key.as_ref(), item, "take", out) else {
                return;
            };
            // Without `by`, one count for everything.
            let slot = if limited_per_key { key } else { String::new() };
            let n = taken.entry(slot, || 0);
            if *n < *count {
                *n += 1;
                next.push(item);
            }
        }
        Running::Throttle {
            interval,
            key,
            keys,
        } => {
            let Some((key, item)) = keyed(key.as_ref(), item, "throttle", out) else {
                return;
            };
            let state = keys.entry(key, || Throttled {
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
            match sink_topic(filter, &item, "topic").and_then(|topic| derived_topic(&topic)) {
                Ok(topic) => out.push(Effect::Derived {
                    topic,
                    payload: payload_bytes(&item.value),
                }),
                Err(err) => out.push(error(&item, "to app", err)),
            }
            next.push(item);
        }
        Running::ToChart => {
            match sample(&item) {
                Ok(effect) => out.push(effect),
                Err(err) => out.push(error(&item, "to chart", err)),
            }
            next.push(item);
        }
        Running::ToGeo => {
            map_effects(&item, out);
            next.push(item);
        }
        Running::ToFile {
            path,
            format,
            rotate,
        } => {
            match sink_topic(path, &item, "file name") {
                Ok(path) if path.trim().is_empty() => {
                    out.push(error(&item, "to file", "the file name is empty".into()))
                }
                Ok(path) => out.push(Effect::Write {
                    path,
                    format: *format,
                    rotate: *rotate,
                    record: json_record(&item.value),
                }),
                Err(err) => out.push(error(&item, "to file", err)),
            }
            next.push(item);
        }
        Running::ToBroker {
            broker,
            topic,
            reads_back,
        } => {
            let stage = format!("to broker \"{broker}\"");
            let checked = sink_topic(topic, &item, "topic").and_then(|topic| {
                validate_topic_name(&topic).map_err(|e| {
                    if topic.is_empty() { e } else { format!("{e}: `{topic}`") }
                })?;
                match reads_back {
                    Some(filters) if filters.iter().all(|filter| filter.passes(&topic)) =>
                    {
                        Err(format!(
                            "`{topic}` would come back into this Pipeline, which reads {broker}. Keep it out with `filter not topic` right after `from`"
                        ))
                    }
                    _ => Ok(topic),
                }
            });
            match checked {
                Ok(topic) => out.push(Effect::Republish {
                    broker: broker.clone(),
                    topic,
                    payload: payload_bytes(&item.value),
                }),
                Err(err) => out.push(error(&item, &stage, err)),
            }
            next.push(item);
        }
    }
}

/// What a value for `to geo` stands for: one point `{lat, lon, label?}`, kept under `$key` (or
/// the topic before any `by`); or a list of them, a snapshot of the whole map, each kept under
/// its label (or its place in the list). A snapshot's bad points are reported and left out.
fn map_effects(item: &Item, out: &mut Vec<Effect>) {
    let at_ms = item.origin.message.received_at_ms;
    let record = json_record(&item.value);
    let serde_json::Value::Array(values) = record else {
        let key = match &item.key {
            Val::Null => item.origin.topic().to_string(),
            Val::TStr(text) => String::from_utf8_lossy(text).into_owned(),
            other => other.to_string(),
        };
        match coordinates(&record) {
            Ok((lat, lon, label)) => out.push(Effect::Point {
                key,
                label,
                lat,
                lon,
                at_ms,
            }),
            Err(err) => out.push(error(item, "to geo", err)),
        }
        return;
    };
    let mut points = Vec::with_capacity(values.len());
    for (i, value) in values.iter().enumerate() {
        match coordinates(value) {
            Ok((lat, lon, label)) => points.push(Point {
                key: label.clone().unwrap_or_else(|| (i + 1).to_string()),
                label,
                lat,
                lon,
            }),
            Err(err) => out.push(error(item, "to geo", format!("point {}: {err}", i + 1))),
        }
    }
    out.push(Effect::Snapshot { points, at_ms });
}

/// A value for `to chart`: a number on the line for `$key` (or the topic before any `by`), or
/// `{value, series?}` to name the line.
fn sample(item: &Item) -> Result<Effect, String> {
    let record = json_record(&item.value);
    let (value, series) = match &record {
        serde_json::Value::Number(n) => (n.as_f64(), None),
        serde_json::Value::Object(fields) => {
            let value = match fields.get("value") {
                Some(serde_json::Value::Number(n)) => n.as_f64(),
                Some(other) => return Err(format!("`value` must be a number, not {other}")),
                None => None,
            };
            let series = match fields.get("series") {
                None | Some(serde_json::Value::Null) => None,
                Some(serde_json::Value::String(text)) => Some(text.clone()),
                Some(other) => Some(other.to_string()),
            };
            (value, series)
        }
        _ => (None, None),
    };
    let Some(value) = value.filter(|v| v.is_finite()) else {
        return Err(format!(
            "`to chart` takes a number, or `{{ value, series }}`, not {}",
            short(&record.to_string())
        ));
    };
    let series = series.unwrap_or_else(|| match &item.key {
        Val::Null => item.origin.topic().to_string(),
        Val::TStr(text) => String::from_utf8_lossy(text).into_owned(),
        other => other.to_string(),
    });
    Ok(Effect::Sample {
        series,
        value,
        at_ms: item.origin.message.received_at_ms,
    })
}

/// A point's latitude, longitude and label.
fn coordinates(record: &serde_json::Value) -> Result<(f64, f64, Option<String>), String> {
    let coordinate = |name: &str, limit: f64| match record.get(name) {
        Some(serde_json::Value::Number(n)) => n
            .as_f64()
            .filter(|v| v.abs() <= limit)
            .ok_or_else(|| format!("`{name}` must be between -{limit} and {limit}, not {n}")),
        Some(other) => Err(format!("`{name}` must be a number, not {other}")),
        None => Err(format!(
            "the value needs `lat` and `lon`, like `map {{ lat: .lat, lon: .lng }}`, not {}",
            short(&record.to_string())
        )),
    };
    let (lat, lon) = (coordinate("lat", 90.0)?, coordinate("lon", 180.0)?);
    let label = match record.get("label") {
        None | Some(serde_json::Value::Null) => None,
        Some(serde_json::Value::String(text)) => Some(text.clone()),
        Some(other) => Some(other.to_string()),
    };
    Ok((lat, lon, label))
}

fn short(text: &str) -> String {
    match text.char_indices().nth(80) {
        Some((at, _)) => format!("{}…", &text[..at]),
        None => text.to_string(),
    }
}

/// A jq value as JSON for `to file`; one that isn't JSON (raw bytes) is written as text.
fn json_record(value: &Val) -> serde_json::Value {
    let text = value.to_string();
    serde_json::from_str(&text).unwrap_or(serde_json::Value::String(text))
}

/// The topic (or file name, `what`) a Sink's expression gives for this item: its first result,
/// which must be a string.
fn sink_topic(filter: &jq::Filter, item: &Item, what: &str) -> Result<String, String> {
    match jq::run(filter, item.value.clone(), &vars(item))?.first() {
        Some(Val::TStr(topic)) => Ok(String::from_utf8_lossy(topic).into_owned()),
        other => Err(format!(
            "the {what} must be a string, not {}",
            other.map_or("nothing".into(), |v| v.to_string())
        )),
    }
}

fn reads(from: &Source, connection: &str) -> bool {
    match from {
        Source::All => true,
        Source::Named(names) => names.iter().any(|n| n == connection),
        // Files and Pipelines aren't Connections; the runner reads them itself.
        Source::Files(_) | Source::Pipelines(_) => false,
    }
}

/// Which state a stateful Stage keeps for this item: its `by` key, or else its incoming topic.
/// The item comes back with that key as its `$key`; `None` (after reporting an error) when the
/// key can't be worked out.
fn keyed(
    key: Option<&jq::Filter>,
    mut item: Item,
    stage: &str,
    out: &mut Vec<Effect>,
) -> Option<(String, Item)> {
    let Some(key) = key else {
        let topic = item.origin.topic().to_string();
        item.key = Val::from(topic.clone());
        return Some((topic, item));
    };
    match jq::run(key, item.value.clone(), &vars(&item)) {
        Ok(results) => match results.into_iter().next() {
            Some(value) => {
                let text = match &value {
                    Val::TStr(text) => String::from_utf8_lossy(text).into_owned(),
                    other => other.to_string(),
                };
                item.key = value;
                Some((text, item))
            }
            None => {
                out.push(error(&item, stage, "the `by` key gave no value".into()));
                None
            }
        },
        Err(err) => {
            out.push(error(&item, stage, err));
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

/// Deeper JSON than this is taken as text: parsing (and later printing or dropping) nests once
/// per level, and a payload deep enough would overflow the stack and take the app down.
const MAX_JSON_DEPTH: usize = 256;

/// JSON when the payload is JSON, text when it's text, bytes otherwise.
pub fn payload_value(payload: &[u8]) -> Val {
    if nesting_at_most(payload, MAX_JSON_DEPTH)
        && let Ok(value) = jaq_json::read::parse_single(payload)
    {
        return value;
    }
    match std::str::from_utf8(payload) {
        Ok(text) => Val::from(text.to_string()),
        Err(_) => Val::byte_str(bytes::Bytes::copy_from_slice(payload)),
    }
}

/// Whether brackets in `json` (outside strings) nest no deeper than `max`.
fn nesting_at_most(json: &[u8], max: usize) -> bool {
    let (mut depth, mut in_string, mut escaped) = (0usize, false, false);
    for &b in json {
        if in_string {
            match b {
                _ if escaped => escaped = false,
                b'\\' => escaped = true,
                b'"' => in_string = false,
                _ => {}
            }
            continue;
        }
        match b {
            b'"' => in_string = true,
            b'[' | b'{' => {
                depth += 1;
                if depth > max {
                    return false;
                }
            }
            b']' | b'}' => depth = depth.saturating_sub(1),
            _ => {}
        }
    }
    true
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

    #[test]
    fn a_trace_counts_what_each_stage_passed() {
        let mut rt =
            runtime(r#"from all |> filter .t > 1 |> map .t - 1 |> to app "x/{.}""#).traced();
        run(
            &mut rt,
            &[
                ("a", r#"{"t": 1}"#),
                ("a", r#"{"t": 2}"#),
                ("a", r#"{"t": "hi"}"#),
            ],
        );
        let trace = rt.trace();
        assert_eq!(trace.len(), 3);
        // "hi" > 1 is true in jq, so it passes the filter and fails in map.
        assert_eq!(trace[0].passed, 2);
        assert_eq!((trace[1].passed, trace[1].errors), (1, 1));
        assert!(
            trace[1]
                .first_error
                .as_deref()
                .unwrap()
                .starts_with("map on a: ")
        );
        assert_eq!(trace[1].last.as_deref(), Some("1"));
        assert_eq!((trace[2].passed, trace[2].errors), (1, 0));
    }

    #[test]
    fn untraced_runtimes_keep_no_trace() {
        let mut rt = runtime("from all |> inspect");
        run(&mut rt, &[("a", "1")]);
        assert!(rt.trace().is_empty());
    }

    #[test]
    fn wants_checks_from_and_the_leading_topic_filters() {
        let rt = runtime(
            r#"from "Dev" |> filter topic "home/#" |> filter not topic "+/status" |> filter .x |> filter topic "never""#,
        );
        assert!(rt.wants("Dev", "home/kitchen"));
        assert!(!rt.wants("Dev", "home/status"));
        assert!(!rt.wants("Dev", "office/kitchen"));
        assert!(!rt.wants("Other", "home/kitchen"));
    }

    #[test]
    fn a_trace_counts_what_throttle_releases_later() {
        let mut rt = runtime(r#"from all |> throttle 1s |> to app "x""#).traced();
        let start = Instant::now();
        let mut out = Vec::new();
        rt.push(origin("a", "1"), start, &mut out);
        rt.push(origin("a", "2"), start, &mut out);
        rt.tick(start + Duration::from_secs(2), &mut out);
        assert_eq!(rt.trace()[0].passed, 2);
        assert_eq!(rt.trace()[1].passed, 2);
    }

    fn origin_at(topic: &str, payload: &str, at_ms: u64) -> Origin {
        let o = origin(topic, payload);
        let mut message = (*o.message).clone();
        message.received_at_ms = at_ms;
        Origin::new(Arc::new(message), "Dev Broker")
    }

    fn values(out: &[Effect]) -> Vec<String> {
        out.iter()
            .map(|e| match e {
                Effect::Derived { payload, .. } => String::from_utf8_lossy(payload).into_owned(),
                other => format!("{other:?}"),
            })
            .collect()
    }

    #[test]
    fn scan_carries_a_result_per_topic_or_key() {
        let mut rt = runtime(r#"from all |> scan 0 ($acc + .) |> to app "x""#);
        let out = run(&mut rt, &[("a", "1"), ("a", "2"), ("b", "10"), ("a", "3")]);
        assert_eq!(values(&out), ["1", "3", "10", "6"]);

        let mut rt = runtime(r#"from all |> scan 0 ($acc + 1) by .room |> to app "{$key}""#);
        let out = run(
            &mut rt,
            &[
                ("s1", r#"{"room":"k"}"#),
                ("s2", r#"{"room":"k"}"#),
                ("s3", r#"{"room":"o"}"#),
            ],
        );
        assert_eq!(values(&out), ["1", "2", "1"]);
        assert!(matches!(&out[2], Effect::Derived { topic, .. } if topic == "o"));
    }

    #[test]
    fn scan_can_build_objects_from_the_previous_result() {
        let mut rt = runtime(
            r#"from all |> scan null { t: ., change: (. - ($acc.t // .)) } |> map .change |> to app "x""#,
        );
        let out = run(&mut rt, &[("a", "20"), ("a", "22"), ("a", "21")]);
        assert_eq!(values(&out), ["0", "2", "-1"]);
    }

    #[test]
    fn debounce_sends_the_latest_once_quiet() {
        let mut rt = runtime(r#"from all |> debounce 1s |> to app "x""#);
        let start = Instant::now();
        let mut out = Vec::new();
        rt.push(origin("a", "1"), start, &mut out);
        rt.push(
            origin("a", "2"),
            start + Duration::from_millis(500),
            &mut out,
        );
        rt.tick(start + Duration::from_millis(1_200), &mut out);
        assert!(
            out.is_empty(),
            "the second value restarted the quiet interval"
        );
        rt.tick(start + Duration::from_millis(1_600), &mut out);
        assert_eq!(values(&out), ["2"]);
    }

    #[test]
    fn time_windows_close_on_the_clock_and_send_a_list() {
        let mut rt = runtime(
            r#"from all |> window 1m by .room |> map { room: $key, avg: avg(.t), n: count } |> to app "x""#,
        );
        let start = Instant::now();
        let mut out = Vec::new();
        // 12:00:50, 12:00:55 and 12:01:05 (in ms since 1970, on a whole minute boundary).
        let minute = 60_000 * 1_000_000;
        rt.push(
            origin_at("a", r#"{"room":"k","t":20}"#, minute + 50_000),
            start,
            &mut out,
        );
        rt.push(
            origin_at("b", r#"{"room":"k","t":22}"#, minute + 55_000),
            start + Duration::from_secs(5),
            &mut out,
        );
        rt.tick(start + Duration::from_secs(9), &mut out);
        assert!(out.is_empty(), "12:00:59 — the window is still open");
        rt.tick(start + Duration::from_secs(10), &mut out);
        assert_eq!(values(&out), [r#"{"room":"k","avg":21.0,"n":2}"#]);

        // A value from a later minute closes an open window at once.
        rt.push(
            origin_at("a", r#"{"room":"k","t":1}"#, minute + 65_000),
            start + Duration::from_secs(15),
            &mut out,
        );
        rt.push(
            origin_at("a", r#"{"room":"k","t":2}"#, minute + 125_000),
            start + Duration::from_secs(75),
            &mut out,
        );
        assert_eq!(values(&out)[1], r#"{"room":"k","avg":1.0,"n":1}"#);
    }

    #[test]
    fn count_windows_send_every_n_values() {
        let mut rt = runtime(r#"from all |> window 2 |> map sum(.) |> to app "x""#);
        let out = run(
            &mut rt,
            &[("a", "1"), ("a", "2"), ("a", "3"), ("b", "5"), ("a", "4")],
        );
        assert_eq!(values(&out), ["3", "7"]);

        // A flush sends what an unfinished window holds.
        let mut out = Vec::new();
        rt.flush(Instant::now(), &mut out);
        assert_eq!(values(&out), ["5"]);
    }

    #[test]
    fn take_passes_the_first_values_then_finishes() {
        let mut rt = runtime(r#"from all |> take 2 |> to app "x""#);
        let out = run(&mut rt, &[("a", "1"), ("b", "2"), ("a", "3")]);
        assert_eq!(values(&out), ["1", "2"]);
        assert!(rt.finished());

        let mut rt = runtime(r#"from all |> take 1 by .room |> to app "x""#);
        let out = run(
            &mut rt,
            &[
                ("a", r#"{"room":"k"}"#),
                ("b", r#"{"room":"k"}"#),
                ("c", r#"{"room":"o"}"#),
            ],
        );
        assert_eq!(out.len(), 2);
        assert!(!rt.finished(), "with `by` it keeps running");
    }

    #[test]
    fn key_is_the_topic_without_by_and_null_before_any_state() {
        let mut rt = runtime(r#"from all |> map $key |> changes |> map $key |> to app "x""#);
        let out = run(&mut rt, &[("home/k", "1")]);
        assert_eq!(values(&out), ["home/k"]);
        let mut rt = runtime(r#"from all |> map ($key == null) |> to app "x""#);
        assert_eq!(values(&run(&mut rt, &[("a", "1")])), ["true"]);
    }

    #[test]
    fn a_deeply_nested_payload_is_text_not_a_crash() {
        let deep = "[".repeat(200_000) + &"]".repeat(200_000);
        let value = payload_value(deep.as_bytes());
        assert!(matches!(value, Val::TStr(_)));
        assert!(nesting_at_most(br#"{"a": [1, {"b": "[[[[["}]}"#, 3));
        assert!(!nesting_at_most(b"[[[[]]]]", 3));
        assert!(
            nesting_at_most(br#""\"[[[[""#, 1),
            "escaped quotes stay in the string"
        );
    }

    #[test]
    fn named_wildcards_key_a_window_and_survive_it() {
        let mut rt = runtime(
            r#"from all
|> filter topic "maqtt/home/+room/#"
|> window 2 by $room
|> map { room: $room, key: $key, sum: sum(.t) }
|> to app "rooms/{$room}""#,
        );
        let out = run(
            &mut rt,
            &[
                ("maqtt/home/kitchen/climate", r#"{"t": 1}"#),
                ("maqtt/home/office/climate", r#"{"t": 10}"#),
                ("maqtt/home/kitchen/other", r#"{"t": 2}"#),
                ("maqtt/garage/x/climate", r#"{"t": 99}"#),
            ],
        );
        assert_eq!(
            out,
            vec![derived(
                "rooms/kitchen",
                r#"{"room":"kitchen","key":"kitchen","sum":3}"#
            )]
        );
    }

    #[test]
    fn a_name_the_matching_filter_lacks_is_null() {
        let mut rt = runtime(
            r#"from all |> filter topic "home/+room/+sensor", "office/+room" |> map [$room, $sensor] |> to app "x""#,
        );
        let out = run(&mut rt, &[("home/kitchen/t1", "1"), ("office/desk", "1")]);
        assert_eq!(values(&out), [r#"["kitchen","t1"]"#, r#"["desk",null]"#]);
    }

    #[test]
    fn to_map_places_points_by_topic_or_key_and_reports_bad_ones() {
        let mut rt = runtime(r#"from all |> map { lat: .la, lon: .lo, label: .n } |> to geo"#);
        let out = run(
            &mut rt,
            &[
                ("trucks/1", r#"{"la": 59.9, "lo": 10.7, "n": "T1"}"#),
                ("trucks/2", r#"{"la": 91, "lo": 0}"#),
                ("trucks/3", r#"{"la": "x", "lo": 0}"#),
            ],
        );
        assert_eq!(
            out[0],
            Effect::Point {
                key: "trucks/1".into(),
                label: Some("T1".into()),
                lat: 59.9,
                lon: 10.7,
                at_ms: 1_000,
            }
        );
        let errors: Vec<String> = out[1..]
            .iter()
            .map(|e| match e {
                Effect::Error(e) => e.clone(),
                other => format!("{other:?}"),
            })
            .collect();
        assert_eq!(
            errors,
            [
                "to geo on trucks/2: `lat` must be between -90 and 90, not 91",
                "to geo on trucks/3: `lat` must be a number, not \"x\"",
            ]
        );

        let mut rt = runtime(r#"from all |> changes by .id |> to geo"#);
        let out = run(
            &mut rt,
            &[
                ("gps", r#"{"id": "A", "lat": 1, "lon": 2}"#),
                ("gps", r#"{"id": "B"}"#),
            ],
        );
        assert!(matches!(&out[0], Effect::Point { key, .. } if key == "A"));
        assert!(matches!(&out[1], Effect::Error(e) if e.contains("needs `lat` and `lon`")));
    }

    #[test]
    fn a_list_for_to_map_is_a_snapshot_with_bad_points_left_out() {
        let mut rt = runtime(r#"from all |> window 3 |> to geo"#);
        let out = run(
            &mut rt,
            &[
                ("gps", r#"{"lat": 1, "lon": 2, "label": "A"}"#),
                ("gps", r#"{"lat": 3, "lon": 4}"#),
                ("gps", r#"{"lat": 500, "lon": 0, "label": "C"}"#),
            ],
        );
        assert_eq!(
            out,
            vec![
                Effect::Error(
                    "to geo on gps: point 3: `lat` must be between -90 and 90, not 500".into()
                ),
                Effect::Snapshot {
                    points: vec![
                        Point {
                            key: "A".into(),
                            label: Some("A".into()),
                            lat: 1.0,
                            lon: 2.0,
                        },
                        Point {
                            key: "2".into(),
                            label: None,
                            lat: 3.0,
                            lon: 4.0,
                        },
                    ],
                    at_ms: 1_000,
                },
            ]
        );
    }

    #[test]
    fn to_chart_takes_numbers_or_named_values() {
        let mut rt = runtime(r#"from all |> map .t |> to chart"#);
        let out = run(
            &mut rt,
            &[("home/a", r#"{"t": 21.5}"#), ("home/b", r#"{"t": "hot"}"#)],
        );
        assert_eq!(
            out[0],
            Effect::Sample {
                series: "home/a".into(),
                value: 21.5,
                at_ms: 1_000,
            }
        );
        assert!(matches!(&out[1], Effect::Error(e) if e.contains("takes a number")));

        let mut rt = runtime(r#"from all |> map { value: .t, series: $levels[1] } |> to chart"#);
        let out = run(&mut rt, &[("home/kitchen", r#"{"t": 3}"#)]);
        assert!(
            matches!(&out[0], Effect::Sample { series, value, .. } if series == "kitchen" && *value == 3.0)
        );

        let mut rt = runtime(r#"from all |> window 2 by .room |> map avg(.t) |> to chart"#);
        let out = run(
            &mut rt,
            &[
                ("t", r#"{"room": "a", "t": 1}"#),
                ("t", r#"{"room": "a", "t": 3}"#),
            ],
        );
        assert!(
            matches!(&out[0], Effect::Sample { series, value, .. } if series == "a" && *value == 2.0)
        );
    }

    #[test]
    fn delta_and_pairwise_compare_with_the_previous_value_per_key() {
        let mut rt = runtime(r#"from all |> map .kwh |> delta |> to app "x""#);
        let out = run(
            &mut rt,
            &[
                ("meter/a", r#"{"kwh": 10}"#),
                ("meter/b", r#"{"kwh": 100}"#),
                ("meter/a", r#"{"kwh": 12}"#),
                ("meter/a", r#"{"kwh": 12.5}"#),
                ("meter/a", r#"{"kwh": "?"}"#),
            ],
        );
        assert_eq!(
            values(&out),
            [
                "2",
                "0.5",
                r#"Error("delta on meter/a: `delta` takes numbers, not \"?\"")"#
            ]
        );

        let mut rt = runtime(r#"from all |> pairwise by .room |> map [.[].t] |> to app "{$key}""#);
        let out = run(
            &mut rt,
            &[
                ("t", r#"{"room": "a", "t": 1}"#),
                ("t", r#"{"room": "b", "t": 5}"#),
                ("t", r#"{"room": "a", "t": 2}"#),
            ],
        );
        assert_eq!(out, vec![derived("a", "[1,2]")]);
    }

    #[test]
    fn pairwise_gives_a_rate_per_second_as_the_reference_shows() {
        let mut rt = runtime(
            r#"from all |> map { v: ., t: $time } |> pairwise |> map ((.[1].v - .[0].v) / (.[1].t - .[0].t)) |> to app "rate""#,
        );
        let start = Instant::now();
        let mut out = Vec::new();
        rt.push(origin_at("meter", "100", 10_000), start, &mut out);
        rt.push(
            origin_at("meter", "130", 12_000),
            start + Duration::from_secs(2),
            &mut out,
        );
        assert_eq!(values(&out), ["15.0"]);
    }

    #[test]
    fn join_combines_the_latest_part_values_per_key() {
        let mut rt = runtime(
            r#"from all |> filter topic "home/+room/+sensor" |> join by $room as $sensor needs "t", "h" |> to app "{$key}""#,
        );
        let out = run(
            &mut rt,
            &[
                ("home/kitchen/t", "21"),
                ("home/office/t", "19"),
                ("home/kitchen/h", "40"),
                ("home/kitchen/t", "22"),
                ("home/office/h", "55"),
            ],
        );
        assert_eq!(
            out,
            vec![
                derived("kitchen", r#"{"t":21,"h":40}"#),
                derived("kitchen", r#"{"t":22,"h":40}"#),
                derived("office", r#"{"t":19,"h":55}"#),
            ]
        );

        // Without `needs`, every part sends what there is so far; without `as`, parts are named
        // by topic.
        let mut rt = runtime(r#"from all |> join |> to app "all""#);
        let out = run(&mut rt, &[("a", "1"), ("b", "2")]);
        assert_eq!(values(&out), [r#"{"a":1}"#, r#"{"a":1,"b":2}"#]);
    }

    #[test]
    fn join_within_drops_parts_that_went_quiet() {
        let mut rt = runtime(r#"from all |> join as $topic within 5s |> to app "x""#);
        let start = Instant::now();
        let mut out = Vec::new();
        rt.push(origin("a", "1"), start, &mut out);
        rt.push(origin("b", "2"), start + Duration::from_secs(3), &mut out);
        rt.push(origin("b", "3"), start + Duration::from_secs(7), &mut out);
        assert_eq!(
            values(&out),
            [r#"{"a":1}"#, r#"{"a":1,"b":2}"#, r#"{"b":3}"#]
        );
    }
}
