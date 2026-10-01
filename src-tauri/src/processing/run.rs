//! A running Pipeline's decisions, one arrival at a time with the time passed in: what it
//! takes in (and what it recognises as its own republished messages coming back), when it
//! ticks, when it's done, and where its Sinks' values go. The runner's thread only waits for
//! the next arrival or a stop and hands it here, so this is tested by stepping it, without
//! threads or sleeps.

use std::collections::{HashMap, VecDeque};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use super::files::FileSink;
use super::outputs::Delivery;
use super::runner::{PIPELINES, PipelineStatus, Republish};
use super::sources::Arrival;
use super::stages::{Effect, Origin, Runtime};
use crate::bus::{Bus, BusEvent};
use crate::message::{ReceivedMessage, now_ms};

/// How often time-based Stages are let send what is due.
pub const TICK: Duration = Duration::from_millis(50);
/// How long a republished message is remembered, to recognise it when a Broker sends it back.
const ECHO_WINDOW: Duration = Duration::from_secs(10);
/// At most this many are remembered; past that, the oldest are forgotten.
const ECHO_KEPT: usize = 10_000;

/// Where a running Pipeline's values leave it.
pub struct Outlets {
    /// The Pipeline's name, which its Derived Topics start with.
    pub name: String,
    /// Derived Topics go onto it.
    pub bus: Bus,
    /// `to broker` sends through it.
    pub republish: Republish,
    /// `to file` writes through it.
    pub files: FileSink,
    /// The files its Source reads, which `to file` mustn't write.
    pub reading: Vec<PathBuf>,
}

/// Whether a run goes on after a step.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Step {
    Going,
    /// Done: it finished, the bus closed, or it was stopped.
    Ended,
}

pub struct Run {
    pipeline: Runtime,
    outlets: Outlets,
    sent: Sent,
    status: Arc<Mutex<PipelineStatus>>,
    /// Set when it's stopped: from then on it sends nothing.
    halted: Arc<AtomicBool>,
    next_tick: Instant,
    effects: Vec<Effect>,
}

impl Run {
    pub fn new(
        pipeline: Runtime,
        outlets: Outlets,
        status: Arc<Mutex<PipelineStatus>>,
        halted: Arc<AtomicBool>,
        now: Instant,
    ) -> Self {
        Self {
            pipeline,
            outlets,
            sent: Sent::default(),
            status,
            halted,
            next_tick: now + TICK,
            effects: Vec::new(),
        }
    }

    /// When it wants a step even if nothing arrives, to tick.
    pub fn next_tick(&self) -> Instant {
        self.next_tick
    }

    /// Takes in what arrived at `now`, or, given nothing, ticks if it's time.
    pub fn step(&mut self, arrival: Option<Arrival>, now: Instant) -> Step {
        match arrival {
            None => {}
            Some(Arrival::Message {
                message,
                connection,
                from_broker,
                problem,
            }) => {
                // What it republished comes back on any Connection to that Broker, and several
                // profiles can reach one Broker: taking it in again could loop.
                if from_broker && self.sent.recognises(&message.topic, &message.payload, now) {
                    self.status.lock().unwrap().returned += 1;
                    return Step::Going;
                }
                let mut status = self.status.lock().unwrap();
                status.received += 1;
                if let Some(problem) = problem {
                    status.outputs.failed(problem);
                }
                drop(status);
                self.pipeline
                    .push(Origin::new(message, &connection), now, &mut self.effects);
            }
            Some(Arrival::Missed(n)) => self.status.lock().unwrap().missed += n,
            Some(Arrival::Problem(problem)) => {
                self.status.lock().unwrap().outputs.failed(problem);
            }
            Some(Arrival::Done) => return self.finish(now),
            Some(Arrival::Closed) => return Step::Ended,
        }
        if now >= self.next_tick {
            self.pipeline.tick(now, &mut self.effects);
            self.next_tick = now + TICK;
        }
        if self.pipeline.finished() {
            // A `take` is done.
            return self.finish(now);
        }
        self.handle(now);
        if self.halted.load(Ordering::SeqCst) {
            Step::Ended
        } else {
            Step::Going
        }
    }

    /// It has read all it will: what later Stages still hold is sent, and it's done.
    fn finish(&mut self, now: Instant) -> Step {
        self.pipeline.flush(now, &mut self.effects);
        self.handle(now);
        self.status.lock().unwrap().finished = true;
        Step::Ended
    }

    fn handle(&mut self, now: Instant) {
        if self.effects.is_empty() {
            return;
        }
        if self.halted.load(Ordering::SeqCst) {
            self.effects.clear();
            return;
        }
        let status = self.status.clone();
        let mut status = status.lock().unwrap();
        let outputs = &mut status.outputs;
        let mut wrote = false;
        for effect in std::mem::take(&mut self.effects) {
            let Some(delivery) = outputs.record(effect, now_ms()) else {
                continue;
            };
            wrote |= matches!(delivery, Delivery::Write { .. });
            match self.deliver(delivery, now) {
                Ok(()) => outputs.delivered(),
                Err(error) => outputs.failed(error),
            }
        }
        if wrote && let Err(err) = self.outlets.files.flush() {
            outputs.failed(format!("to file: {err}"));
        }
    }

    /// Sends a Sink's value out: onto the bus, to a Broker, into a file.
    fn deliver(&mut self, delivery: Delivery, now: Instant) -> Result<(), String> {
        let out = &mut self.outlets;
        match delivery {
            Delivery::Derived { topic, payload } => {
                out.bus.publish(BusEvent::Message(Arc::new(ReceivedMessage {
                    seq: out.bus.next_seq(),
                    connection_id: PIPELINES,
                    topic: format!("{}/{topic}", out.name),
                    payload: payload.into(),
                    qos: 0,
                    retain: false,
                    received_at_ms: now_ms(),
                    properties: None,
                })));
                Ok(())
            }
            Delivery::Republish {
                broker,
                topic,
                payload,
            } => {
                (out.republish)(&broker, &topic, payload.clone())
                    .map_err(|err| format!("to broker \"{broker}\" on {topic}: {err}"))?;
                self.sent.remember(&topic, &payload, now);
                Ok(())
            }
            Delivery::Write {
                path,
                format,
                rotate,
                record,
            } => {
                if out.reading.contains(&out.files.resolve(&path)) {
                    return Err(format!(
                        "to file: not written to {path}: this Pipeline reads it, so it would read its own output"
                    ));
                }
                out.files
                    .write(&path, format, rotate, &record, &jiff::Zoned::now())
                    .map(|_| ())
                    .map_err(|err| format!("to file: {err}"))
            }
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::message::ConnectionId;
    use crate::processing::lang::parse;
    use bytes::Bytes;

    type SentLog = Arc<Mutex<Vec<(String, String, Vec<u8>)>>>;

    /// A run of `source`, started at `t0`, as "p". Only "Office" can be republished to; files
    /// go in `dir`, and the Source reads `reading` there.
    struct Fixture {
        run: Run,
        bus: Bus,
        status: Arc<Mutex<PipelineStatus>>,
        halted: Arc<AtomicBool>,
        sent: SentLog,
        t0: Instant,
    }

    fn start_in(source: &str, dir: PathBuf, reading: &[&str]) -> Fixture {
        let bus = Bus::new();
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
        let files = FileSink::new(dir);
        let reading = reading.iter().map(|name| files.resolve(name)).collect();
        let status: Arc<Mutex<PipelineStatus>> = Arc::default();
        let halted: Arc<AtomicBool> = Arc::default();
        let t0 = Instant::now();
        let run = Run::new(
            Runtime::new(&parse(source).unwrap()).unwrap(),
            Outlets {
                name: "p".into(),
                bus: bus.clone(),
                republish,
                files,
                reading,
            },
            status.clone(),
            halted.clone(),
            t0,
        );
        Fixture {
            run,
            bus,
            status,
            halted,
            sent,
            t0,
        }
    }

    fn start(source: &str) -> Fixture {
        start_in(source, std::env::temp_dir().join("maqtt-run-unused"), &[])
    }

    fn arrived(topic: &str, payload: &str) -> Option<Arrival> {
        Some(Arrival::Message {
            message: Arc::new(ReceivedMessage {
                seq: 0,
                connection_id: ConnectionId(1),
                topic: topic.into(),
                payload: Bytes::copy_from_slice(payload.as_bytes()),
                qos: 0,
                retain: false,
                received_at_ms: 0,
                properties: None,
            }),
            connection: "Dev Broker".into(),
            from_broker: true,
            problem: None,
        })
    }

    fn after(f: &Fixture, ms: u64) -> Instant {
        f.t0 + Duration::from_millis(ms)
    }

    impl Fixture {
        fn status(&self) -> PipelineStatus {
            self.status.lock().unwrap().clone()
        }
    }

    #[test]
    fn values_go_out_as_derived_topics_under_its_name() {
        let mut f =
            start(r#"from all |> filter .t > 20 |> map { hot: .t } |> to app "hot/{$levels[1]}""#);
        let mut rx = f.bus.subscribe();
        let now = after(&f, 0);
        assert_eq!(
            f.run.step(arrived("home/kitchen", r#"{"t": 23}"#), now),
            Step::Going
        );
        f.run.step(arrived("home/office", r#"{"t": 18}"#), now);
        let Ok(BusEvent::Message(derived)) = rx.try_recv() else {
            panic!("a Derived Topic")
        };
        assert_eq!(derived.connection_id, PIPELINES);
        assert_eq!(derived.topic, "p/hot/kitchen");
        assert_eq!(&derived.payload[..], br#"{"hot":23}"#);
        assert!(rx.try_recv().is_err());
        let status = f.status();
        assert_eq!((status.received, status.outputs.emitted), (2, 1));
    }

    #[test]
    fn errors_inspect_misses_and_problems_show_in_the_status() {
        let mut f = start(r#"from "Dev Broker" |> inspect "raw" |> map .a + 1"#);
        let now = after(&f, 0);
        f.run.step(arrived("t", r#"{"a": "x"}"#), now);
        f.run.step(Some(Arrival::Missed(3)), now);
        f.run
            .step(Some(Arrival::Problem("unreadable line".into())), now);
        let status = f.status();
        assert_eq!((status.received, status.missed), (1, 3));
        assert_eq!(status.outputs.inspected[0].value, r#"{"a":"x"}"#);
        assert_eq!(status.outputs.errors, 2);
        assert_eq!(
            status.outputs.last_error.as_deref(),
            Some("unreadable line")
        );
    }

    #[test]
    fn time_based_stages_send_when_a_tick_comes_due() {
        let mut f = start(r#"from all |> debounce 1s |> to app "x""#);
        f.run.step(arrived("t", "1"), after(&f, 0));
        assert_eq!(f.run.next_tick(), after(&f, TICK.as_millis() as u64));
        // Not yet quiet for a second.
        f.run.step(None, after(&f, 500));
        assert_eq!(f.status().outputs.emitted, 0);
        assert_eq!(f.run.next_tick(), after(&f, 500) + TICK);
        // Quiet for a second, at a tick.
        f.run.step(None, after(&f, 1010));
        assert_eq!(f.status().outputs.emitted, 1);
    }

    #[test]
    fn take_ends_the_run_once_it_has_passed_all_it_will() {
        let mut f = start(r#"from all |> take 2 |> to app "x""#);
        let now = after(&f, 0);
        assert_eq!(f.run.step(arrived("t", "1"), now), Step::Going);
        assert_eq!(f.run.step(arrived("t", "2"), now), Step::Ended);
        let status = f.status();
        assert!(status.finished);
        assert_eq!(status.outputs.emitted, 2);
    }

    #[test]
    fn the_end_of_its_files_sends_what_stages_still_hold() {
        let mut f = start(r#"from all |> debounce 1h |> to app "x""#);
        f.run.step(arrived("t", "1"), after(&f, 0));
        assert_eq!(f.run.step(Some(Arrival::Done), after(&f, 10)), Step::Ended);
        let status = f.status();
        assert!(status.finished);
        assert_eq!(status.outputs.emitted, 1);
    }

    #[test]
    fn the_bus_closing_ends_it_without_finishing() {
        let mut f = start(r#"from all |> debounce 1h |> to app "x""#);
        f.run.step(arrived("t", "1"), after(&f, 0));
        assert_eq!(
            f.run.step(Some(Arrival::Closed), after(&f, 10)),
            Step::Ended
        );
        let status = f.status();
        assert!(!status.finished);
        assert_eq!(status.outputs.emitted, 0);
    }

    #[test]
    fn once_halted_it_sends_nothing_and_ends() {
        let mut f = start(r#"from all |> to app "x""#);
        let mut rx = f.bus.subscribe();
        f.halted.store(true, Ordering::SeqCst);
        assert_eq!(f.run.step(arrived("t", "1"), after(&f, 0)), Step::Ended);
        assert!(rx.try_recv().is_err());
        assert_eq!(f.status().outputs.emitted, 0);
    }

    #[test]
    fn to_broker_republishes_and_reports_what_it_couldnt_send() {
        let mut f = start(
            r#"from "Dev Broker" |> to broker "Office" "copy/{$topic}" |> to broker "Home" "x""#,
        );
        f.run.step(arrived("a/b", r#"{"t": 1}"#), after(&f, 0));
        assert_eq!(
            *f.sent.lock().unwrap(),
            vec![("Office".into(), "copy/a/b".into(), br#"{"t":1}"#.to_vec())]
        );
        let status = f.status();
        assert_eq!((status.outputs.emitted, status.outputs.errors), (1, 1));
        assert_eq!(
            status.outputs.last_error.as_deref(),
            Some(r#"to broker "Home" on x: Home isn't connected"#)
        );
    }

    #[test]
    fn its_own_messages_coming_back_are_left_out_until_the_window_passes() {
        let mut f = start(r#"from "Dev Broker" |> to broker "Office" "copy/{$topic}""#);
        f.run.step(arrived("a", "1"), after(&f, 0));
        // Back on two Connections; the same payload on another topic is still taken in.
        f.run.step(arrived("copy/a", "1"), after(&f, 10));
        f.run.step(arrived("copy/a", "1"), after(&f, 20));
        f.run.step(arrived("b", "1"), after(&f, 30));
        let status = f.status();
        assert_eq!((status.returned, status.received), (2, 2));

        // A message from a file can't be an echo.
        let Some(Arrival::Message { message, .. }) = arrived("copy/b", "1") else {
            unreachable!()
        };
        f.run.step(
            Some(Arrival::Message {
                message,
                connection: "file".into(),
                from_broker: false,
                problem: None,
            }),
            after(&f, 40),
        );
        assert_eq!(f.status().received, 3);

        // Long after, the same message is someone else's.
        let later = after(&f, 0) + ECHO_WINDOW + Duration::from_secs(1);
        f.run.step(arrived("copy/a", "1"), later);
        assert_eq!(f.status().received, 4);
        let topics: Vec<String> = f.sent.lock().unwrap().iter().map(|s| s.1.clone()).collect();
        assert_eq!(topics, ["copy/a", "copy/b", "copy/copy/b", "copy/copy/a"]);
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
    fn to_file_writes_but_never_into_a_file_it_reads() {
        let dir = std::env::temp_dir().join(format!("maqtt-run-files-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let mut f = start_in(
            r#"from all |> filter topic "home/+room" |> map { room: $room, t: .t } |> to file "{$room}.csv""#,
            dir.clone(),
            &["office.csv"],
        );
        let now = after(&f, 0);
        f.run.step(arrived("home/kitchen", r#"{"t": 21}"#), now);
        f.run.step(arrived("home/kitchen", r#"{"t": 22}"#), now);
        f.run.step(arrived("home/office", r#"{"t": 19}"#), now);
        assert_eq!(
            std::fs::read_to_string(dir.join("kitchen.csv")).unwrap(),
            "room,t\nkitchen,21\nkitchen,22\n"
        );
        assert!(!dir.join("office.csv").exists());
        let status = f.status();
        assert_eq!((status.outputs.emitted, status.outputs.errors), (2, 1));
        assert!(
            status
                .outputs
                .last_error
                .unwrap()
                .contains("would read its own output")
        );
        let _ = std::fs::remove_dir_all(&dir);
    }
}
