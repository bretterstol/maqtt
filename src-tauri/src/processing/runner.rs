//! Running Pipelines. Each started Pipeline gets its own OS thread with its own small async
//! runtime, so however slow its code is, it can't take time from the Connections or the UI. It
//! reads the Message Bus through its own receiver: if it falls behind, it misses messages and
//! counts them, instead of holding anyone else up.

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, mpsc};
use std::time::{Duration, Instant};

use serde::Serialize;
use tokio::sync::Notify;
use tokio::sync::broadcast::error::RecvError;

use super::lang::Pipeline;
use super::stages::{Effect, Origin, Runtime};
use crate::bus::{Bus, BusEvent};
use crate::connection::{ConnectionState, ConnectionStatus, ConnectionStatuses};
use crate::message::{ConnectionId, ReceivedMessage, now_ms};
use crate::profile::ProfileId;

/// The pseudo-Connection that Derived Topics come from; real Connections start at 1.
pub const PIPELINES: ConnectionId = ConnectionId(0);
const TICK: Duration = Duration::from_millis(50);
const INSPECTED_KEPT: usize = 50;
/// How long stopping waits for a Pipeline's thread before giving up on it.
const STOP_WAIT: Duration = Duration::from_secs(1);

#[derive(Debug, Clone, Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PipelineStatus {
    pub name: String,
    pub running: bool,
    /// Messages from the Pipeline's Connections that entered it.
    pub received: u64,
    /// Messages its Sinks sent.
    pub emitted: u64,
    /// Messages it missed because it fell behind.
    pub missed: u64,
    pub errors: u64,
    pub last_error: Option<String>,
    /// The latest `inspect` values, newest last.
    pub inspected: VecDeque<Inspected>,
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
    running: Mutex<HashMap<String, Handle>>,
    /// Status of Pipelines that ran and stopped, kept so their counts stay visible.
    stopped: Mutex<HashMap<String, PipelineStatus>>,
}

impl Runner {
    pub fn new(bus: Bus, statuses: ConnectionStatuses) -> Self {
        Self {
            bus,
            statuses,
            running: Mutex::new(HashMap::new()),
            stopped: Mutex::new(HashMap::new()),
        }
    }

    /// Starts a parsed Pipeline under `name`; a Pipeline already running under that name is
    /// stopped first.
    pub fn start(&self, name: &str, pipeline: Pipeline) -> Result<(), String> {
        // Compile once here so a problem is reported now rather than from the thread.
        Runtime::new(&pipeline)?;
        self.stop(name);
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
            halted: halted.clone(),
            _done: done_tx,
            pipeline,
            bus: self.bus.clone(),
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
                stop,
                halted,
                status,
                done,
            },
        );
        self.stopped.lock().unwrap().remove(name);
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

    pub fn forget(&self, name: &str) {
        self.stop(name);
        self.stopped.lock().unwrap().remove(name);
    }

    pub fn is_running(&self, name: &str) -> bool {
        self.running.lock().unwrap().contains_key(name)
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

/// Everything a Pipeline's thread needs.
struct Job {
    name: String,
    pipeline: Pipeline,
    bus: Bus,
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
        let mut tick = tokio::time::interval(TICK);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut effects = Vec::new();
        loop {
            tokio::select! {
                biased;
                _ = self.stop.notified() => break,
                event = self.rx.recv() => match event {
                    Ok(BusEvent::Message(message)) => {
                        // Never read Derived Topics: a Pipeline can't feed itself.
                        if message.connection_id == PIPELINES {
                            continue;
                        }
                        let connection = self.names.get(&message.connection_id).map_or("", String::as_str);
                        if !pipeline.reads(connection) {
                            continue;
                        }
                        let origin = Origin::new(message, connection);
                        self.status.lock().unwrap().received += 1;
                        pipeline.push(origin, Instant::now(), &mut effects);
                    }
                    Ok(BusEvent::ConnectionState(s)) => {
                        self.names.insert(s.connection_id, s.name);
                    }
                    Ok(BusEvent::Marker(_)) => {}
                    Err(RecvError::Lagged(n)) => self.status.lock().unwrap().missed += n,
                    Err(RecvError::Closed) => break,
                },
                _ = tick.tick() => pipeline.tick(Instant::now(), &mut effects),
            }
            self.handle(&mut effects);
            if self.halted.load(Ordering::SeqCst) {
                break;
            }
        }
        self.status.lock().unwrap().running = false;
    }

    fn handle(&self, effects: &mut Vec<Effect>) {
        if effects.is_empty() {
            return;
        }
        if self.halted.load(Ordering::SeqCst) {
            effects.clear();
            return;
        }
        let mut status = self.status.lock().unwrap();
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

    fn setup() -> (Bus, Runner) {
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
        (bus.clone(), Runner::new(bus, statuses))
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
}
