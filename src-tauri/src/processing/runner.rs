//! Running Pipelines. Each started Pipeline gets its own OS thread with its own small async
//! runtime, so however slow its code is, it can't take time from the Connections or the UI. It
//! reads the Message Bus through its own receiver: if it falls behind, it misses messages and
//! counts them, instead of holding anyone else up.

use std::collections::{HashMap, VecDeque};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, mpsc};
use std::time::{Duration, Instant};

use serde::Serialize;
use tokio::sync::Notify;
use tokio::sync::broadcast::error::RecvError;

use super::file_source::{FileReader, Next};
use super::files::FileSink;
use super::lang::{FileSource, Pipeline, Source};
use super::lines::Lines;
use super::markers::Markers;
use super::stages::{Effect, Origin, Runtime};
use crate::bus::{Bus, BusEvent};
use crate::connection::{ConnectionState, ConnectionStatus, ConnectionStatuses};
use crate::message::{ConnectionId, ReceivedMessage, now_ms};
use crate::profile::ProfileId;

/// Sends a payload to a topic on the Connection with this name, without waiting, for
/// `to broker`. `lib.rs` backs it with the live Connections.
pub type Republish = Arc<dyn Fn(&str, &str, Vec<u8>) -> Result<(), String> + Send + Sync>;

/// `$connection` for messages read from files.
pub const FILE_CONNECTION: &str = "file";

/// The pseudo-Connection that Derived Topics come from; real Connections start at 1.
pub const PIPELINES: ConnectionId = ConnectionId(0);
const TICK: Duration = Duration::from_millis(50);
const INSPECTED_KEPT: usize = 50;
/// How long stopping waits for a Pipeline's thread before giving up on it.
const STOP_WAIT: Duration = Duration::from_secs(1);
/// How long a republished message is remembered, to recognise it when a Broker sends it back.
const ECHO_WINDOW: Duration = Duration::from_secs(10);
/// At most this many are remembered; past that, the oldest are forgotten.
const ECHO_KEPT: usize = 10_000;

#[derive(Debug, Clone, Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PipelineStatus {
    pub name: String,
    pub running: bool,
    /// It stopped by itself: a `take` passed all it would.
    pub finished: bool,
    /// Messages from the Pipeline's Connections that entered it.
    pub received: u64,
    /// Messages its Sinks sent: Derived Topics and republished messages.
    pub emitted: u64,
    /// Messages it missed because it fell behind.
    pub missed: u64,
    /// Its own republished messages that came back from a Broker and were left out.
    pub returned: u64,
    pub errors: u64,
    pub last_error: Option<String>,
    /// The latest `inspect` values, newest last.
    pub inspected: VecDeque<Inspected>,
    /// Where its `to geo` values put their markers.
    pub markers: Markers,
    /// Its `to chart` values, a line per series.
    pub chart: Lines,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Inspected {
    pub label: Option<String>,
    pub topic: String,
    pub value: String,
    pub at_ms: u64,
}

struct Handle {
    /// The Pipelines whose Derived Topics it reads (`from pipeline`), for the loop check.
    reads: Vec<String>,
    stop: Arc<Notify>,
    /// Set on stop; the thread sends nothing once it's set.
    halted: Arc<AtomicBool>,
    status: Arc<Mutex<PipelineStatus>>,
    /// Disconnects when the thread ends.
    done: mpsc::Receiver<()>,
}

pub struct Runner {
    bus: Bus,
    statuses: ConnectionStatuses,
    republish: Republish,
    /// Where `to file` writes names that aren't absolute.
    files_dir: PathBuf,
    running: Mutex<HashMap<String, Handle>>,
    /// Status of Pipelines that ran and stopped, kept so their counts stay visible.
    stopped: Mutex<HashMap<String, PipelineStatus>>,
}

impl Runner {
    pub fn new(
        bus: Bus,
        statuses: ConnectionStatuses,
        republish: Republish,
        files_dir: PathBuf,
    ) -> Self {
        Self {
            bus,
            statuses,
            republish,
            files_dir,
            running: Mutex::new(HashMap::new()),
            stopped: Mutex::new(HashMap::new()),
        }
    }

    /// Starts a parsed Pipeline under `name`; a Pipeline already running under that name is
    /// stopped first.
    pub fn start(&self, name: &str, pipeline: Pipeline) -> Result<(), String> {
        // Compile once here so a problem is reported now rather than from the thread.
        Runtime::new(&pipeline)?;
        let reads = match &pipeline.from {
            Source::Pipelines(names) => names.clone(),
            _ => Vec::new(),
        };
        self.check_loops(name, &reads)?;
        self.stop(name);
        // Its Derived Topics start over with this run.
        self.clear_topics(name);
        self.announce();

        let stop = Arc::new(Notify::new());
        let status = Arc::new(Mutex::new(PipelineStatus {
            name: name.to_string(),
            running: true,
            ..PipelineStatus::default()
        }));
        let halted = Arc::new(AtomicBool::new(false));
        let (done_tx, done) = mpsc::channel();
        let job = Job {
            name: name.to_string(),
            reads: reads.clone(),
            halted: halted.clone(),
            _done: done_tx,
            pipeline,
            bus: self.bus.clone(),
            republish: self.republish.clone(),
            sent: Sent::default(),
            files: FileSink::new(self.files_dir.clone()),
            reading: Vec::new(),
            names: self
                .statuses
                .snapshot()
                .into_iter()
                .map(|s| (s.connection_id, s.name))
                .collect(),
            rx: self.bus.subscribe(),
            stop: stop.clone(),
            status: status.clone(),
        };
        std::thread::Builder::new()
            .name(format!("pipeline {name}"))
            .spawn(move || job.run())
            .map_err(|e| format!("Couldn't start {name}: {e}"))?;
        self.running.lock().unwrap().insert(
            name.to_string(),
            Handle {
                reads,
                stop,
                halted,
                status,
                done,
            },
        );
        self.stopped.lock().unwrap().remove(name);
        Ok(())
    }

    /// Refuses a start that would let Pipelines feed each other: reading itself, or reading a
    /// running Pipeline that (through others) reads it. Checked on every start, so a loop can
    /// never be running.
    fn check_loops(&self, name: &str, reads: &[String]) -> Result<(), String> {
        if reads.iter().any(|r| r == name) {
            return Err(format!("{name} can't read its own Derived Topics"));
        }
        let running = self.running.lock().unwrap();
        // Each Pipeline to look into, with the one this Pipeline reads that led to it.
        let mut todo: Vec<(&str, &str)> = reads.iter().map(|r| (r.as_str(), r.as_str())).collect();
        let mut seen = std::collections::HashSet::new();
        while let Some((pipeline, via)) = todo.pop() {
            if pipeline == name || !seen.insert(pipeline) {
                continue;
            }
            let Some(handle) = running.get(pipeline) else {
                continue;
            };
            if !handle.status.lock().unwrap().running {
                continue;
            }
            if handle.reads.iter().any(|r| r == name) {
                return Err(if pipeline == via {
                    format!("{name} and {via} would read each other in a loop")
                } else {
                    format!("{name} would read itself through {via}, as {pipeline} reads {name}")
                });
            }
            todo.extend(handle.reads.iter().map(|r| (r.as_str(), via)));
        }
        Ok(())
    }

    /// Stops a Pipeline: nothing it sends arrives after this. Waits briefly for its thread; one
    /// stuck in an endless expression is left to finish on its own, sending nothing.
    pub fn stop(&self, name: &str) {
        let handle = self.running.lock().unwrap().remove(name);
        if let Some(handle) = handle {
            handle.halted.store(true, Ordering::SeqCst);
            handle.stop.notify_one();
            let stuck = matches!(
                handle.done.recv_timeout(STOP_WAIT),
                Err(mpsc::RecvTimeoutError::Timeout)
            );
            let mut status = handle.status.lock().unwrap().clone();
            status.running = false;
            if stuck {
                status.last_error = Some(format!(
                    "It didn't stop within {} s, most likely stuck in an expression that never ends. It sends nothing more.",
                    STOP_WAIT.as_secs()
                ));
            }
            self.stopped
                .lock()
                .unwrap()
                .insert(name.to_string(), status);
        }
    }

    /// Notes why a Pipeline that should be running couldn't start, for its panel.
    pub fn failed(&self, name: &str, error: String) {
        self.stopped.lock().unwrap().insert(
            name.to_string(),
            PipelineStatus {
                name: name.to_string(),
                last_error: Some(error),
                ..PipelineStatus::default()
            },
        );
    }

    /// Stops a Pipeline for good (deleted or renamed), and clears its Derived Topics.
    pub fn forget(&self, name: &str) {
        self.stop(name);
        self.stopped.lock().unwrap().remove(name);
        self.clear_topics(name);
    }

    fn clear_topics(&self, name: &str) {
        self.bus.publish(BusEvent::TopicsCleared {
            connection_id: PIPELINES,
            prefix: name.to_string(),
        });
    }

    pub fn is_running(&self, name: &str) -> bool {
        self.running
            .lock()
            .unwrap()
            .get(name)
            .is_some_and(|handle| handle.status.lock().unwrap().running)
    }

    pub fn status(&self, name: &str) -> Option<PipelineStatus> {
        if let Some(handle) = self.running.lock().unwrap().get(name) {
            return Some(handle.status.lock().unwrap().clone());
        }
        self.stopped.lock().unwrap().get(name).cloned()
    }

    /// Makes the Pipelines pseudo-Connection known, so the Topic Tree can name its root.
    fn announce(&self) {
        let known = self
            .statuses
            .snapshot()
            .iter()
            .any(|s| s.connection_id == PIPELINES);
        if !known {
            self.statuses.publish(
                &self.bus,
                ConnectionStatus {
                    connection_id: PIPELINES,
                    profile_id: ProfileId("pipelines".into()),
                    name: "Pipelines".into(),
                    state: ConnectionState::Connected,
                    subscriptions: Vec::new(),
                },
            );
        }
    }
}

/// What a Pipeline republished lately, as fingerprints of topic and payload.
#[derive(Default)]
struct Sent {
    order: VecDeque<(Instant, u64)>,
    counts: HashMap<u64, usize>,
}

impl Sent {
    fn fingerprint(topic: &str, payload: &[u8]) -> u64 {
        use std::hash::{Hash, Hasher};
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        (topic, payload).hash(&mut hasher);
        hasher.finish()
    }

    fn remember(&mut self, topic: &str, payload: &[u8], now: Instant) {
        if self.order.len() == ECHO_KEPT {
            self.forget_oldest();
        }
        let print = Self::fingerprint(topic, payload);
        self.order.push_back((now, print));
        *self.counts.entry(print).or_default() += 1;
    }

    /// Whether this is a message it republished within [`ECHO_WINDOW`]. Every copy counts, as
    /// one message can come back on several Connections.
    fn recognises(&mut self, topic: &str, payload: &[u8], now: Instant) -> bool {
        while self
            .order
            .front()
            .is_some_and(|(at, _)| now.duration_since(*at) > ECHO_WINDOW)
        {
            self.forget_oldest();
        }
        !self.counts.is_empty() && self.counts.contains_key(&Self::fingerprint(topic, payload))
    }

    fn forget_oldest(&mut self) {
        if let Some((_, print)) = self.order.pop_front()
            && let Some(n) = self.counts.get_mut(&print)
        {
            *n -= 1;
            if *n == 0 {
                self.counts.remove(&print);
            }
        }
    }
}

/// A Derived Topic of one of `sources`, as a message of that Pipeline's own: its topic without
/// the Pipeline's name in front, and the name (which is `$connection`).
pub(super) fn derived_from(
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

/// Everything a Pipeline's thread needs.
struct Job {
    name: String,
    /// The Pipelines it reads with `from pipeline`.
    reads: Vec<String>,
    pipeline: Pipeline,
    bus: Bus,
    republish: Republish,
    sent: Sent,
    files: FileSink,
    /// The files a file Source reads, which `to file` mustn't write.
    reading: Vec<PathBuf>,
    names: HashMap<ConnectionId, String>,
    rx: tokio::sync::broadcast::Receiver<BusEvent>,
    stop: Arc<Notify>,
    halted: Arc<AtomicBool>,
    status: Arc<Mutex<PipelineStatus>>,
    /// Dropped when the thread ends, which tells `stop` it has.
    _done: mpsc::Sender<()>,
}

impl Job {
    fn run(self) {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .expect("a Pipeline's runtime");
        runtime.block_on(self.run_async());
    }

    async fn run_async(mut self) {
        // jq values aren't Send, so the Pipeline is compiled on its own thread.
        let mut pipeline = match Runtime::new(&self.pipeline) {
            Ok(p) => p,
            Err(err) => {
                let mut status = self.status.lock().unwrap();
                status.running = false;
                status.last_error = Some(err);
                return;
            }
        };
        if let Source::Files(source) = &self.pipeline.from {
            let source = source.clone();
            return self.run_files(pipeline, source).await;
        }
        let mut tick = tokio::time::interval(TICK);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut effects = Vec::new();
        loop {
            tokio::select! {
                biased;
                _ = self.stop.notified() => break,
                event = self.rx.recv() => match event {
                    Ok(BusEvent::Message(message)) => {
                        // Derived Topics only from the Pipelines named in `from pipeline`; the
                        // loop check at start keeps a Pipeline from feeding itself.
                        if message.connection_id == PIPELINES {
                            if let Some((message, source)) = derived_from(&message, &self.reads) {
                                self.status.lock().unwrap().received += 1;
                                pipeline.push(Origin::new(message, &source), Instant::now(), &mut effects);
                            }
                            continue;
                        }
                        let connection = self.names.get(&message.connection_id).map_or("", String::as_str);
                        if !pipeline.reads(connection) {
                            continue;
                        }
                        // What it republished comes back on any Connection to that Broker, and
                        // several profiles can reach one Broker: taking it in again could loop.
                        if self.sent.recognises(&message.topic, &message.payload, Instant::now()) {
                            self.status.lock().unwrap().returned += 1;
                            continue;
                        }
                        let origin = Origin::new(message, connection);
                        self.status.lock().unwrap().received += 1;
                        pipeline.push(origin, Instant::now(), &mut effects);
                    }
                    Ok(BusEvent::ConnectionState(s)) => {
                        self.names.insert(s.connection_id, s.name);
                    }
                    Ok(BusEvent::Marker(_) | BusEvent::TopicsCleared { .. }) => {}
                    Err(RecvError::Lagged(n)) => self.status.lock().unwrap().missed += n,
                    Err(RecvError::Closed) => break,
                },
                _ = tick.tick() => pipeline.tick(Instant::now(), &mut effects),
            }
            if pipeline.finished() {
                // A `take` is done: send what later Stages still hold, and stop.
                pipeline.flush(Instant::now(), &mut effects);
                self.handle(&mut effects);
                self.status.lock().unwrap().finished = true;
                break;
            }
            self.handle(&mut effects);
            if self.halted.load(Ordering::SeqCst) {
                break;
            }
        }
        self.status.lock().unwrap().running = false;
    }

    /// A Pipeline reading files: records as they're due, until the files end (unless
    /// followed) or it's stopped.
    async fn run_files(mut self, mut pipeline: Runtime, source: FileSource) {
        let mut reader = match FileReader::new(&source, self.files.dir()) {
            Ok(reader) => reader,
            Err(err) => {
                let mut status = self.status.lock().unwrap();
                status.running = false;
                status.last_error = Some(err);
                return;
            }
        };
        self.reading = source
            .names
            .iter()
            .map(|name| self.files.resolve(name))
            .collect();
        let mut effects = Vec::new();
        let mut next_tick = Instant::now() + TICK;
        let mut seq = 0;
        loop {
            if self.halted.load(Ordering::SeqCst) {
                break;
            }
            let now = Instant::now();
            if now >= next_tick {
                pipeline.tick(now, &mut effects);
                next_tick = now + TICK;
            }
            match reader.next(now) {
                Next::Record(record) => {
                    let mut status = self.status.lock().unwrap();
                    status.received += 1;
                    if let Some(problem) = record.problem {
                        status.errors += 1;
                        status.last_error = Some(problem);
                    }
                    drop(status);
                    seq += 1;
                    let message = Arc::new(ReceivedMessage {
                        seq,
                        connection_id: PIPELINES,
                        topic: record.name,
                        payload: record.payload.into(),
                        qos: 0,
                        retain: false,
                        received_at_ms: now_ms(),
                        properties: None,
                    });
                    pipeline.push(Origin::new(message, FILE_CONNECTION), now, &mut effects);
                }
                Next::Problem(problem) => {
                    let mut status = self.status.lock().unwrap();
                    status.errors += 1;
                    status.last_error = Some(problem);
                }
                Next::Wait(wait) => {
                    let wait = wait
                        .min(next_tick.saturating_duration_since(now))
                        .max(Duration::from_millis(1));
                    tokio::select! {
                        biased;
                        _ = self.stop.notified() => break,
                        _ = tokio::time::sleep(wait) => {}
                    }
                }
                Next::Done => {
                    pipeline.flush(Instant::now(), &mut effects);
                    self.handle(&mut effects);
                    self.status.lock().unwrap().finished = true;
                    break;
                }
            }
            if pipeline.finished() {
                pipeline.flush(Instant::now(), &mut effects);
                self.handle(&mut effects);
                self.status.lock().unwrap().finished = true;
                break;
            }
            self.handle(&mut effects);
            // Records that are due at once still let a stop through.
            tokio::task::yield_now().await;
        }
        self.status.lock().unwrap().running = false;
    }

    fn handle(&mut self, effects: &mut Vec<Effect>) {
        if effects.is_empty() {
            return;
        }
        if self.halted.load(Ordering::SeqCst) {
            effects.clear();
            return;
        }
        let status = self.status.clone();
        let mut status = status.lock().unwrap();
        let mut wrote = false;
        for effect in effects.drain(..) {
            match effect {
                Effect::Derived { topic, payload } => {
                    status.emitted += 1;
                    self.bus
                        .publish(BusEvent::Message(Arc::new(ReceivedMessage {
                            seq: self.bus.next_seq(),
                            connection_id: PIPELINES,
                            topic: format!("{}/{topic}", self.name),
                            payload: payload.into(),
                            qos: 0,
                            retain: false,
                            received_at_ms: now_ms(),
                            properties: None,
                        })));
                }
                Effect::Republish {
                    broker,
                    topic,
                    payload,
                } => match (self.republish)(&broker, &topic, payload.clone()) {
                    Ok(()) => {
                        status.emitted += 1;
                        self.sent.remember(&topic, &payload, Instant::now());
                    }
                    Err(err) => {
                        status.errors += 1;
                        status.last_error =
                            Some(format!("to broker \"{broker}\" on {topic}: {err}"));
                    }
                },
                Effect::Write {
                    path,
                    format,
                    rotate,
                    record,
                } => {
                    wrote = true;
                    if self.reading.contains(&self.files.resolve(&path)) {
                        status.errors += 1;
                        status.last_error = Some(format!(
                            "to file: not written to {path}: this Pipeline reads it, so it would read its own output"
                        ));
                        continue;
                    }
                    match self
                        .files
                        .write(&path, format, rotate, &record, &jiff::Zoned::now())
                    {
                        Ok(_) => status.emitted += 1,
                        Err(err) => {
                            status.errors += 1;
                            status.last_error = Some(format!("to file: {err}"));
                        }
                    }
                }
                Effect::Point {
                    key,
                    label,
                    lat,
                    lon,
                    at_ms,
                } => {
                    status.emitted += 1;
                    status.markers.place(key, label, lat, lon, at_ms);
                }
                Effect::Sample {
                    series,
                    value,
                    at_ms,
                } => {
                    status.emitted += 1;
                    status.chart.add(&series, at_ms, value);
                }
                Effect::Snapshot { points, at_ms } => {
                    status.emitted += 1;
                    status.markers.replace(points, at_ms);
                }
                Effect::Inspected {
                    label,
                    topic,
                    value,
                } => {
                    if status.inspected.len() == INSPECTED_KEPT {
                        status.inspected.pop_front();
                    }
                    status.inspected.push_back(Inspected {
                        label,
                        topic,
                        value,
                        at_ms: now_ms(),
                    });
                }
                Effect::Error(err) => {
                    status.errors += 1;
                    status.last_error = Some(err);
                }
            }
        }
        if wrote && let Err(err) = self.files.flush() {
            status.errors += 1;
            status.last_error = Some(format!("to file: {err}"));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::processing::lang::parse;
    use bytes::Bytes;

    fn message(bus: &Bus, connection: u32, topic: &str, payload: &str) -> BusEvent {
        BusEvent::Message(Arc::new(ReceivedMessage {
            seq: bus.next_seq(),
            connection_id: ConnectionId(connection),
            topic: topic.into(),
            payload: Bytes::copy_from_slice(payload.as_bytes()),
            qos: 0,
            retain: false,
            received_at_ms: now_ms(),
            properties: None,
        }))
    }

    type SentLog = Arc<Mutex<Vec<(String, String, Vec<u8>)>>>;

    fn setup() -> (Bus, Runner) {
        let (bus, runner, _) = setup_sending();
        (bus, runner)
    }

    /// Also records what `to broker` sends; only "Office" is connected.
    fn setup_sending() -> (Bus, Runner, SentLog) {
        let bus = Bus::new();
        let statuses = ConnectionStatuses::default();
        statuses.publish(
            &bus,
            ConnectionStatus {
                connection_id: ConnectionId(1),
                profile_id: ProfileId("p".into()),
                name: "Dev Broker".into(),
                state: ConnectionState::Connected,
                subscriptions: Vec::new(),
            },
        );
        let sent: SentLog = Arc::default();
        let log = sent.clone();
        let republish: Republish = Arc::new(move |broker, topic, payload| {
            if broker != "Office" {
                return Err(format!("{broker} isn't connected"));
            }
            log.lock()
                .unwrap()
                .push((broker.into(), topic.into(), payload));
            Ok(())
        });
        (
            bus.clone(),
            Runner::new(bus, statuses, republish, std::env::temp_dir()),
            sent,
        )
    }

    fn wait_for(what: &str, mut done: impl FnMut() -> bool) {
        let end = Instant::now() + Duration::from_secs(5);
        while Instant::now() < end {
            if done() {
                return;
            }
            std::thread::sleep(Duration::from_millis(20));
        }
        panic!("timed out waiting for {what}");
    }

    #[test]
    fn derived_topics_come_back_on_the_bus_and_never_feed_the_pipeline() {
        let (bus, runner) = setup();
        let mut rx = bus.subscribe();
        let pipeline =
            parse(r#"from all |> filter .t > 20 |> map { hot: .t } |> to app "hot/{$levels[1]}""#)
                .unwrap();
        runner.start("warm", pipeline).unwrap();
        std::thread::sleep(Duration::from_millis(100));

        bus.publish(message(&bus, 1, "home/kitchen", r#"{"t": 23}"#));
        bus.publish(message(&bus, 1, "home/office", r#"{"t": 18}"#));

        let derived = loop {
            match rx.blocking_recv().unwrap() {
                BusEvent::Message(m) if m.connection_id == PIPELINES => break m,
                _ => {}
            }
        };
        assert_eq!(derived.topic, "warm/hot/kitchen");
        assert_eq!(&derived.payload[..], br#"{"hot":23}"#);

        wait_for("counts", || {
            let s = runner.status("warm").unwrap();
            s.received == 2 && s.emitted == 1
        });
        runner.stop("warm");
        wait_for("stopped", || !runner.status("warm").unwrap().running);
    }

    #[test]
    fn errors_and_inspect_show_in_the_status() {
        let (bus, runner) = setup();
        runner
            .start(
                "check",
                parse(r#"from "Dev Broker" |> inspect "raw" |> map .a + 1"#).unwrap(),
            )
            .unwrap();
        std::thread::sleep(Duration::from_millis(100));
        bus.publish(message(&bus, 1, "t", r#"{"a": "x"}"#));
        // From another Connection: not this Pipeline's.
        bus.publish(message(&bus, 2, "t", r#"{"a": 1}"#));
        wait_for("error", || runner.status("check").unwrap().errors == 1);
        let status = runner.status("check").unwrap();
        assert_eq!(status.received, 1);
        assert_eq!(status.inspected[0].value, r#"{"a":"x"}"#);
        assert!(status.last_error.unwrap().starts_with("map on t:"));
    }

    #[test]
    fn a_slow_pipeline_misses_messages_instead_of_holding_up_the_bus() {
        let (bus, runner) = setup();
        // Deliberately expensive per message.
        let slow = parse("from all |> map [range(20000)] | length").unwrap();
        runner.start("slow", slow).unwrap();
        std::thread::sleep(Duration::from_millis(100));

        let started = Instant::now();
        for _ in 0..200_000 {
            bus.publish(message(&bus, 1, "t", "1"));
        }
        let publishing = started.elapsed();
        // Publishing never waits for the Pipeline.
        assert!(
            publishing < Duration::from_secs(2),
            "publishing took {publishing:?}"
        );
        wait_for("missed messages", || {
            runner.status("slow").unwrap().missed > 0
        });
        runner.stop("slow");
    }
    #[test]
    fn a_stuck_pipeline_stops_quickly_and_sends_nothing_more() {
        let (bus, runner) = setup();
        let mut rx = bus.subscribe();
        runner
            .start(
                "stuck",
                parse(r#"from all |> map last(range(1; infinite)) |> to app "x""#).unwrap(),
            )
            .unwrap();
        std::thread::sleep(Duration::from_millis(100));
        bus.publish(message(&bus, 1, "t", "1"));
        std::thread::sleep(Duration::from_millis(100));

        let asked = Instant::now();
        runner.stop("stuck");
        assert!(asked.elapsed() < STOP_WAIT + Duration::from_millis(500));
        let status = runner.status("stuck").unwrap();
        assert!(!status.running);
        assert!(status.last_error.unwrap().contains("didn't stop"));
        while let Ok(event) = rx.try_recv() {
            assert!(
                !matches!(event, BusEvent::Message(m) if m.connection_id == PIPELINES),
                "nothing from the stuck Pipeline"
            );
        }
    }

    #[test]
    fn a_restart_clears_the_old_topics_and_nothing_old_follows() {
        let (bus, runner) = setup();
        let pipeline = || parse(r#"from all |> to app "x""#).unwrap();
        runner.start("p", pipeline()).unwrap();
        std::thread::sleep(Duration::from_millis(100));
        for _ in 0..200 {
            bus.publish(message(&bus, 1, "t", "1"));
        }
        let mut rx = bus.subscribe();
        runner.start("p", pipeline()).unwrap();

        // After the clear, only the new run's output: here, none, as nothing new came in.
        let mut after_clear = None;
        while let Ok(event) = rx.try_recv() {
            match event {
                BusEvent::TopicsCleared {
                    connection_id,
                    prefix,
                } => {
                    assert_eq!((connection_id, prefix.as_str()), (PIPELINES, "p"));
                    after_clear = Some(0);
                }
                BusEvent::Message(m) if m.connection_id == PIPELINES => {
                    if let Some(n) = after_clear.as_mut() {
                        *n += 1;
                    }
                }
                _ => {}
            }
        }
        assert_eq!(after_clear, Some(0), "cleared once, nothing old after it");

        runner.forget("p");
        assert!(matches!(
            rx.try_recv(),
            Ok(BusEvent::TopicsCleared { prefix, .. }) if prefix == "p"
        ));
    }

    #[test]
    fn take_finishes_the_pipeline() {
        let (bus, runner) = setup();
        runner
            .start("few", parse(r#"from all |> take 2 |> to app "x""#).unwrap())
            .unwrap();
        std::thread::sleep(Duration::from_millis(100));
        for _ in 0..5 {
            bus.publish(message(&bus, 1, "t", "1"));
        }
        wait_for("finished", || runner.status("few").unwrap().finished);
        let status = runner.status("few").unwrap();
        assert!(!status.running && status.emitted == 2, "{status:?}");
        assert!(!runner.is_running("few"));
    }

    #[test]
    fn to_broker_republishes_and_reports_what_it_couldnt_send() {
        let (bus, runner, sent) = setup_sending();
        runner
            .start(
                "copy",
                parse(
                    r#"from "Dev Broker" |> to broker "Office" "copy/{$topic}" |> to broker "Home" "x""#,
                )
                .unwrap(),
            )
            .unwrap();
        std::thread::sleep(Duration::from_millis(100));
        bus.publish(message(&bus, 1, "a/b", r#"{"t": 1}"#));
        wait_for("sent", || runner.status("copy").unwrap().errors == 1);
        assert_eq!(
            *sent.lock().unwrap(),
            vec![("Office".into(), "copy/a/b".into(), br#"{"t":1}"#.to_vec())]
        );
        let status = runner.status("copy").unwrap();
        assert_eq!(status.emitted, 1);
        assert_eq!(
            status.last_error.as_deref(),
            Some(r#"to broker "Home" on x: Home isn't connected"#)
        );
        runner.stop("copy");
    }

    #[test]
    fn its_own_messages_coming_back_on_another_connection_are_left_out() {
        // "Office" sends back what it gets as Connection 1, as when two profiles reach one
        // Broker: the name check can't see that loop.
        let (bus, runner, sent) = setup_sending();
        runner
            .start(
                "echo",
                parse(r#"from "Dev Broker" |> to broker "Office" "copy/{$topic}""#).unwrap(),
            )
            .unwrap();
        std::thread::sleep(Duration::from_millis(100));
        bus.publish(message(&bus, 1, "a", "1"));
        wait_for("sent", || sent.lock().unwrap().len() == 1);
        let (_, topic, payload) = sent.lock().unwrap()[0].clone();
        for _ in 0..2 {
            bus.publish(message(
                &bus,
                1,
                &topic,
                std::str::from_utf8(&payload).unwrap(),
            ));
        }
        // Another message with the same payload on another topic is still taken in.
        bus.publish(message(&bus, 1, "b", "1"));
        wait_for("counts", || {
            let s = runner.status("echo").unwrap();
            s.returned == 2 && s.received == 2
        });
        let topics: Vec<String> = sent.lock().unwrap().iter().map(|s| s.1.clone()).collect();
        assert_eq!(topics, ["copy/a", "copy/b"]);
        runner.stop("echo");
    }

    #[test]
    fn sent_messages_are_forgotten_after_the_window_or_past_the_limit() {
        let now = Instant::now();
        let mut sent = Sent::default();
        sent.remember("t", b"1", now);
        assert!(sent.recognises("t", b"1", now + Duration::from_secs(1)));
        assert!(!sent.recognises("t", b"2", now));
        assert!(!sent.recognises("t", b"1", now + ECHO_WINDOW + Duration::from_secs(1)));
        assert!(sent.counts.is_empty() && sent.order.is_empty());

        for i in 0..=ECHO_KEPT {
            sent.remember(&i.to_string(), b"", now);
        }
        assert_eq!(sent.order.len(), ECHO_KEPT);
        assert!(!sent.recognises("0", b"", now));
        assert!(sent.recognises("1", b"", now));
    }

    #[test]
    fn to_file_writes_what_a_started_pipeline_sends() {
        let dir = std::env::temp_dir().join(format!("maqtt-runner-files-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let (bus, _) = setup();
        let statuses = ConnectionStatuses::default();
        let runner = Runner::new(
            bus.clone(),
            statuses,
            Arc::new(|_, _, _| Ok(())),
            dir.clone(),
        );
        runner
            .start(
                "log",
                parse(r#"from all |> filter topic "home/+room" |> map { room: $room, t: .t } |> to file "{$room}.csv""#)
                    .unwrap(),
            )
            .unwrap();
        std::thread::sleep(Duration::from_millis(100));
        bus.publish(message(&bus, 1, "home/kitchen", r#"{"t": 21}"#));
        bus.publish(message(&bus, 1, "home/kitchen", r#"{"t": 22}"#));
        bus.publish(message(&bus, 1, "home/office", r#"{"t": 19}"#));
        wait_for("written", || runner.status("log").unwrap().emitted == 3);
        runner.stop("log");
        assert_eq!(
            std::fs::read_to_string(dir.join("kitchen.csv")).unwrap(),
            "room,t\nkitchen,21\nkitchen,22\n"
        );
        assert_eq!(
            std::fs::read_to_string(dir.join("office.csv")).unwrap(),
            "room,t\noffice,19\n"
        );
    }

    #[test]
    fn a_recording_replays_from_a_file_and_the_pipeline_finishes() {
        let dir = std::env::temp_dir().join(format!("maqtt-runner-replay-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(
            dir.join("rec.jsonl"),
            "{\"topic\":\"home/a\",\"payload\":{\"t\":1}}\n{\"topic\":\"home/b\",\"payload\":{\"t\":2}}\n",
        )
        .unwrap();
        let (bus, _, sent) = setup_sending();
        let republish: Republish = {
            let sent = sent.clone();
            Arc::new(move |broker, topic, payload| {
                sent.lock()
                    .unwrap()
                    .push((broker.into(), topic.into(), payload));
                Ok(())
            })
        };
        let runner = Runner::new(bus, ConnectionStatuses::default(), republish, dir.clone());
        runner
            .start(
                "replay",
                parse(
                    r#"from file "rec.jsonl" rate 100/s
|> map .payload
|> to broker "Office" "{$topic}/{$connection}"
|> to file "rec.jsonl""#,
                )
                .unwrap(),
            )
            .unwrap();
        wait_for("finished", || runner.status("replay").unwrap().finished);
        let status = runner.status("replay").unwrap();
        assert_eq!((status.received, status.emitted), (2, 2), "{status:?}");
        // Writing back into the file it reads is refused.
        assert_eq!(status.errors, 2);
        assert!(
            status
                .last_error
                .unwrap()
                .contains("would read its own output")
        );
        let sent: Vec<_> = sent
            .lock()
            .unwrap()
            .iter()
            .map(|s| (s.1.clone(), s.2.clone()))
            .collect();
        assert_eq!(
            sent,
            [
                ("rec.jsonl/file".to_string(), br#"{"t":1}"#.to_vec()),
                ("rec.jsonl/file".to_string(), br#"{"t":2}"#.to_vec()),
            ]
        );
        assert_eq!(
            std::fs::read_to_string(dir.join("rec.jsonl"))
                .unwrap()
                .lines()
                .count(),
            2
        );
    }

    #[test]
    fn a_pipeline_reads_another_pipelines_derived_topics() {
        let (bus, runner) = setup();
        let mut rx = bus.subscribe();
        runner
            .start(
                "clean",
                parse(r#"from all |> map .t |> to app "t/{$topic}""#).unwrap(),
            )
            .unwrap();
        runner
            .start(
                "hot",
                parse(
                    r#"from pipeline "clean" |> filter . > 20 |> to app "{$connection}/{$topic}""#,
                )
                .unwrap(),
            )
            .unwrap();
        std::thread::sleep(Duration::from_millis(100));
        bus.publish(message(&bus, 1, "kitchen", r#"{"t": 23}"#));
        bus.publish(message(&bus, 1, "office", r#"{"t": 18}"#));
        let hot = loop {
            match rx.blocking_recv().unwrap() {
                BusEvent::Message(m) if m.topic.starts_with("hot/") => break m,
                _ => {}
            }
        };
        assert_eq!(hot.topic, "hot/clean/t/kitchen");
        assert_eq!(&hot.payload[..], b"23");
        wait_for("counts", || {
            let s = runner.status("hot").unwrap();
            s.received == 2 && s.emitted == 1
        });
        runner.stop("hot");
        runner.stop("clean");
    }

    #[test]
    fn pipelines_that_would_feed_each_other_are_refused() {
        let (_, runner) = setup();
        let reads =
            |name: &str| parse(&format!(r#"from pipeline "{name}" |> to app "x""#)).unwrap();
        assert_eq!(
            runner.start("a", reads("a")).unwrap_err(),
            "a can't read its own Derived Topics"
        );
        runner.start("a", reads("b")).unwrap();
        assert_eq!(
            runner.start("b", reads("a")).unwrap_err(),
            "b and a would read each other in a loop"
        );
        runner.start("c", reads("a")).unwrap();
        assert_eq!(
            runner.start("b", reads("c")).unwrap_err(),
            "b would read itself through c, as a reads b"
        );
        // Once a stops, nothing loops any more.
        runner.stop("a");
        runner.start("b", reads("c")).unwrap();
        runner.stop("b");
        runner.stop("c");
    }
}
