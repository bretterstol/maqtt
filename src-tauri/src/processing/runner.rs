//! Running Pipelines. Each started Pipeline gets its own OS thread with its own small async
//! runtime, so however slow its code is, it can't take time from the Connections or the UI. It
//! reads the Message Bus through its own receiver: if it falls behind, it misses messages and
//! counts them, instead of holding anyone else up.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, mpsc};
use std::time::{Duration, Instant};

use serde::Serialize;
use tokio::sync::Notify;

use super::files::FileSink;
use super::lang::{Pipeline, Source};
use super::outputs::Outputs;
use super::run::{Outlets, Run, Step};
use super::sources::Live;
use super::stages::Runtime;
use crate::bus::{Bus, BusEvent};
use crate::connection::{ConnectionState, ConnectionStatus, ConnectionStatuses};
use crate::message::ConnectionId;
use crate::profile::ProfileId;

/// Sends a payload to a topic on the Connection with this name, without waiting, for
/// `to broker`. `lib.rs` backs it with the live Connections.
pub type Republish = Arc<dyn Fn(&str, &str, Vec<u8>) -> Result<(), String> + Send + Sync>;

/// The pseudo-Connection that Derived Topics come from; real Connections start at 1.
pub const PIPELINES: ConnectionId = ConnectionId(0);
/// How long stopping waits for a Pipeline's thread before giving up on it.
const STOP_WAIT: Duration = Duration::from_secs(1);

#[derive(Debug, Clone, Default, Serialize, ts_rs::TS)]
#[serde(rename_all = "camelCase")]
#[ts(export, export_to = "processing/generated/")]
pub struct PipelineStatus {
    pub name: String,
    pub running: bool,
    /// It stopped by itself: a `take` passed all it would.
    pub finished: bool,
    /// Messages from the Pipeline's Connections that entered it.
    pub received: u64,
    /// Messages it missed because it fell behind.
    pub missed: u64,
    /// Its own republished messages that came back from a Broker and were left out.
    pub returned: u64,
    /// What its Sinks show: counts, errors, `inspect` values, markers, chart lines.
    pub outputs: Outputs,
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
            pipeline,
            bus: self.bus.clone(),
            republish: self.republish.clone(),
            files_dir: self.files_dir.clone(),
            names: self
                .statuses
                .snapshot()
                .into_iter()
                .map(|s| (s.connection_id, s.name))
                .collect(),
            rx: self.bus.subscribe(),
            stop: stop.clone(),
            halted: halted.clone(),
            status: status.clone(),
            _done: done_tx,
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
                status.outputs.last_error = Some(format!(
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

    /// Stops every running Pipeline.
    pub fn stop_all(&self) {
        let names: Vec<String> = self.running.lock().unwrap().keys().cloned().collect();
        for name in names {
            self.stop(&name);
        }
    }

    /// Notes why a Pipeline that should be running couldn't start, for its panel.
    pub fn failed(&self, name: &str, error: String) {
        self.stopped.lock().unwrap().insert(
            name.to_string(),
            PipelineStatus {
                name: name.to_string(),
                outputs: Outputs {
                    last_error: Some(error),
                    ..Outputs::default()
                },
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

/// Everything a Pipeline's thread needs. The thread waits for the next arrival (or a stop)
/// and hands it to the [`Run`], which decides everything else.
struct Job {
    name: String,
    pipeline: Pipeline,
    bus: Bus,
    republish: Republish,
    files_dir: PathBuf,
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

    async fn run_async(self) {
        // jq values aren't Send, so the Pipeline (and a file Source's reader) are made on its
        // own thread.
        let started = Runtime::new(&self.pipeline).and_then(|pipeline| {
            let source = Live::new(&self.pipeline.from, self.rx, self.names, &self.files_dir)?;
            Ok((pipeline, source))
        });
        let (pipeline, mut source) = match started {
            Ok(started) => started,
            Err(err) => {
                let mut status = self.status.lock().unwrap();
                status.running = false;
                status.outputs.last_error = Some(err);
                return;
            }
        };
        let outlets = Outlets {
            name: self.name,
            bus: self.bus,
            republish: self.republish,
            files: FileSink::new(self.files_dir),
            reading: source.reading().to_vec(),
        };
        let mut run = Run::new(
            pipeline,
            outlets,
            self.status.clone(),
            self.halted,
            Instant::now(),
        );
        loop {
            let arrival = tokio::select! {
                biased;
                _ = self.stop.notified() => break,
                arrival = source.next(run.next_tick()) => arrival,
            };
            if run.step(arrival, Instant::now()) == Step::Ended {
                break;
            }
        }
        self.status.lock().unwrap().running = false;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::message::{ReceivedMessage, now_ms};
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
            s.received == 2 && s.outputs.emitted == 1
        });
        runner.stop("warm");
        wait_for("stopped", || !runner.status("warm").unwrap().running);
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
        assert!(status.outputs.last_error.unwrap().contains("didn't stop"));
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
        assert!(!status.running && status.outputs.emitted == 2, "{status:?}");
        assert!(!runner.is_running("few"));
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
        assert_eq!(
            (status.received, status.outputs.emitted),
            (2, 2),
            "{status:?}"
        );
        // Writing back into the file it reads is refused.
        assert_eq!(status.outputs.errors, 2);
        assert!(
            status
                .outputs
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
            s.received == 2 && s.outputs.emitted == 1
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
