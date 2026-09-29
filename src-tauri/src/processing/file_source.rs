//! The file Source: `from file` reads JSON Lines or CSV records, one message each, at a steady
//! rate or on the times the records carry, and with `follow` keeps reading what's added.
//!
//! No threads or clocks of its own: the caller asks for the next record with the time now, and
//! is told to wait when it isn't due yet.

use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use jaq_json::Val;
use serde_json::Value;

use super::files::{home, parse_csv_record, resolve};
use super::jq;
use super::lang::{FileSource, Pace};
use super::stages::payload_value;

/// How often a followed file is checked for more.
const FOLLOW_EVERY: Duration = Duration::from_millis(200);

/// One record, as a message payload from the named file.
#[derive(Debug, Clone, PartialEq)]
pub struct Record {
    /// The file's name as written in `from file`: the message's topic.
    pub name: String,
    pub payload: Vec<u8>,
    /// Something wrong with it that didn't stop it, such as a missing time for `replay`.
    pub problem: Option<String>,
}

#[derive(Debug, PartialEq)]
pub enum Next {
    Record(Record),
    /// Nothing is due before this long.
    Wait(Duration),
    /// A file couldn't be read; the others carry on.
    Problem(String),
    /// Every file is read to the end, and none is followed.
    Done,
}

pub struct FileReader {
    files: Vec<OneFile>,
    /// The file being read; with `follow`, the one checked next once all are at their end.
    current: usize,
    follow: bool,
    pacing: Pacing,
    /// A record read but not yet due.
    held: Option<(Record, Instant)>,
    /// When the followed files were last found at their end.
    idle_since: Option<Instant>,
    unpaced: bool,
}

enum Pacing {
    Rate {
        every: Duration,
        next: Option<Instant>,
    },
    Replay {
        time: jq::Filter,
        /// The first record's time, and when it was handed out.
        start: Option<(f64, Instant)>,
    },
}

struct OneFile {
    name: String,
    path: PathBuf,
    csv: bool,
    reader: Option<BufReader<File>>,
    /// Didn't open: reported once, then skipped.
    failed: bool,
    /// Text read that doesn't end a record yet: a line still being written, or a CSV record
    /// open inside quotes.
    partial: String,
    columns: Option<Vec<String>>,
    at_end: bool,
}

impl FileReader {
    pub fn new(source: &FileSource, dir: &Path) -> Result<Self, String> {
        let home = home();
        let files = source
            .names
            .iter()
            .map(|name| OneFile {
                name: name.clone(),
                path: resolve(dir, home.as_deref(), name),
                csv: name.ends_with(".csv"),
                reader: None,
                failed: false,
                partial: String::new(),
                columns: None,
                at_end: false,
            })
            .collect();
        let pacing = match &source.pace {
            Pace::Rate(n) => Pacing::Rate {
                every: Duration::from_secs(1) / *n,
                next: None,
            },
            Pace::Replay(time) => Pacing::Replay {
                time: jq::compile(time, &[]).map_err(|e| e.message)?,
                start: None,
            },
        };
        Ok(Self {
            files,
            current: 0,
            follow: source.follow,
            pacing,
            held: None,
            idle_since: None,
            unpaced: false,
        })
    }

    /// Hands records out as fast as they're asked for, and stops at the end, as Preview wants.
    /// The pace stays known, for [`Self::replay_time`] and [`Self::rate_every`].
    pub fn unpaced(mut self) -> Self {
        self.unpaced = true;
        self.follow = false;
        self
    }

    /// For Preview, which replays records on a clock of its own: the time `replay` reads from
    /// this record, in seconds.
    pub fn replay_time(&self, record: &Record) -> Option<f64> {
        match &self.pacing {
            Pacing::Replay { time, .. } => seconds(time, &record.payload),
            _ => None,
        }
    }

    /// For Preview: the gap between records at a steady rate.
    pub fn rate_every(&self) -> Option<Duration> {
        match &self.pacing {
            Pacing::Rate { every, .. } => Some(*every),
            _ => None,
        }
    }

    pub fn next(&mut self, now: Instant) -> Next {
        let (record, due) = match self.held.take() {
            Some(held) => held,
            None => match self.read(now) {
                Ok(Some(record)) => {
                    let due = self.due(&record, now);
                    (record, due)
                }
                Ok(None) if self.follow => return Next::Wait(FOLLOW_EVERY),
                Ok(None) => return Next::Done,
                Err(problem) => return Next::Problem(problem),
            },
        };
        if due > now {
            self.held = Some((record, due));
            return Next::Wait(due - now);
        }
        Next::Record(record)
    }

    /// When a freshly read record is due, noting a missing time on it.
    fn due(&mut self, record: &Record, now: Instant) -> Instant {
        if self.unpaced {
            return now;
        }
        match &mut self.pacing {
            Pacing::Rate { every, next } => {
                // Behind (a slow Stage, or idle while following): carry on from now, no burst.
                let due = next.map_or(now, |n| n.max(now - *every));
                *next = Some(due + *every);
                due
            }
            Pacing::Replay { time, start } => match seconds(time, &record.payload) {
                Some(t) => {
                    let (t0, at) = *start.get_or_insert((t, now));
                    at + Duration::try_from_secs_f64(t - t0).unwrap_or_default()
                }
                None => now,
            },
        }
    }

    /// The next whole record from any file, or none for now.
    fn read(&mut self, now: Instant) -> Result<Option<Record>, String> {
        // With `follow`, don't hammer files that are at their end.
        if self.follow
            && self.files.iter().all(|f| f.at_end || f.failed)
            && self
                .idle_since
                .is_some_and(|at| now.duration_since(at) < FOLLOW_EVERY)
        {
            return Ok(None);
        }
        let count = self.files.len();
        for _ in 0..count {
            let file = &mut self.files[self.current];
            match file.next_record(self.follow) {
                Ok(Some(payload)) => {
                    self.idle_since = None;
                    let problem = match &self.pacing {
                        Pacing::Replay { time, .. } if seconds(time, &payload).is_none() => Some(
                            format!("replay: a record in {} has no time in seconds", file.name),
                        ),
                        _ => None,
                    };
                    return Ok(Some(Record {
                        name: file.name.clone(),
                        payload,
                        problem,
                    }));
                }
                Ok(None) => {}
                Err(problem) => return Err(problem),
            }
            // Read the files one after the other; once all are done, go round for more.
            if self.current + 1 < count || self.follow {
                self.current = (self.current + 1) % count;
            }
        }
        self.idle_since = Some(now);
        Ok(None)
    }
}

impl OneFile {
    /// The next complete record's payload, if one is there. A last line without a line break
    /// is taken as it is at the end, unless the file is followed: then it may still be being
    /// written, and waits for its line break.
    fn next_record(&mut self, follow: bool) -> Result<Option<Vec<u8>>, String> {
        if self.failed {
            return Ok(None);
        }
        if self.reader.is_none() {
            match File::open(&self.path) {
                Ok(file) => self.reader = Some(BufReader::new(file)),
                Err(e) => {
                    self.failed = true;
                    return Err(format!("Couldn't read {}: {e}", self.path.display()));
                }
            }
        }
        loop {
            let mut line = String::new();
            let read = self
                .reader
                .as_mut()
                .unwrap()
                .read_line(&mut line)
                .map_err(|e| format!("Couldn't read {}: {e}", self.path.display()))?;
            if read == 0 {
                self.at_end = true;
                if follow || self.partial.is_empty() {
                    return Ok(None);
                }
                let text = std::mem::take(&mut self.partial);
                return Ok(self.record(text.trim_end_matches(['\r', '\n'])));
            }
            self.at_end = false;
            self.partial.push_str(&line);
            if !self.partial.ends_with('\n') {
                continue;
            }
            // A CSV record goes on while a quote is open.
            if self.csv && self.partial.matches('"').count() % 2 == 1 {
                continue;
            }
            let text = std::mem::take(&mut self.partial);
            if let Some(payload) = self.record(text.trim_end_matches(['\r', '\n'])) {
                return Ok(Some(payload));
            }
        }
    }

    /// The payload for one record's text; none for blank lines and the CSV header.
    fn record(&mut self, text: &str) -> Option<Vec<u8>> {
        if text.trim().is_empty() {
            return None;
        }
        if !self.csv {
            return Some(text.as_bytes().to_vec());
        }
        let cells = parse_csv_record(text);
        let Some(columns) = &self.columns else {
            self.columns = Some(cells);
            return None;
        };
        let row: serde_json::Map<String, Value> = columns
            .iter()
            .enumerate()
            .map(|(i, column)| {
                (
                    column.clone(),
                    cells.get(i).map_or(Value::Null, |c| cell(c)),
                )
            })
            .collect();
        Some(Value::Object(row).to_string().into_bytes())
    }
}

/// A CSV cell as a value: empty is null; numbers, true, false and JSON lists and objects come
/// back as they were written; anything else is text.
fn cell(text: &str) -> Value {
    if text.is_empty() {
        return Value::Null;
    }
    match serde_json::from_str::<Value>(text) {
        Ok(value) if !value.is_string() => value,
        _ => Value::String(text.to_string()),
    }
}

/// A `replay` expression's time for a record, in seconds since 1970.
fn seconds(time: &jq::Filter, payload: &[u8]) -> Option<f64> {
    let vars: Vec<Val> = (0..jq::VARIABLES.len()).map(|_| Val::Null).collect();
    match jq::run(time, payload_value(payload), &vars).ok()?.first()? {
        number @ Val::Num(_) => number.to_string().parse().ok().map(from_number),
        Val::TStr(text) => time_in_text(&String::from_utf8_lossy(text)),
        _ => None,
    }
}

/// Past this, a number is taken as milliseconds: as seconds it would be after the year 5000.
const MILLISECONDS_FROM: f64 = 1e11;

fn from_number(n: f64) -> f64 {
    if n.abs() >= MILLISECONDS_FROM {
        n / 1000.0
    } else {
        n
    }
}

/// A time written as text: a number (seconds, or milliseconds when large), a timestamp with
/// an offset (`2026-09-29T12:00:00.250Z`, `+02:00`), or a date and time without one, taken as
/// local time. A space may stand for the `T`.
fn time_in_text(text: &str) -> Option<f64> {
    let text = text.trim();
    if let Ok(n) = text.parse::<f64>() {
        return Some(from_number(n));
    }
    let timestamp = match text.parse::<jiff::Timestamp>() {
        Ok(timestamp) => timestamp,
        Err(_) => text
            .parse::<jiff::civil::DateTime>()
            .ok()?
            .to_zoned(jiff::tz::TimeZone::system())
            .ok()?
            .timestamp(),
    };
    Some(timestamp.as_second() as f64 + f64::from(timestamp.subsec_nanosecond()) / 1e9)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    fn dir(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("maqtt-source-{}-{name}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn source(names: &[&str], follow: bool, pace: Pace) -> FileSource {
        FileSource {
            names: names.iter().map(|n| n.to_string()).collect(),
            follow,
            pace,
        }
    }

    fn payloads(reader: &mut FileReader, now: Instant) -> Vec<String> {
        let mut out = Vec::new();
        loop {
            match reader.next(now) {
                Next::Record(r) => out.push(format!(
                    "{}: {}",
                    r.name,
                    String::from_utf8_lossy(&r.payload)
                )),
                Next::Problem(p) => out.push(format!(
                    "problem: {}",
                    p.rsplit('/').next().unwrap().split(':').next().unwrap()
                )),
                Next::Wait(_) | Next::Done => return out,
            }
        }
    }

    #[test]
    fn json_lines_then_csv_rows_as_typed_objects() {
        let dir = dir("formats");
        std::fs::write(dir.join("a.jsonl"), "{\"t\":1}\n\n\"text\"\nnot json").unwrap();
        std::fs::write(
            dir.join("b.csv"),
            "room,t,note,tags\nkitchen,21.5,,\"[1,2]\"\n\"a, b\",true,\"two\nlines\",x\n",
        )
        .unwrap();
        let mut reader = FileReader::new(
            &source(
                &["a.jsonl", "missing.jsonl", "b.csv"],
                false,
                Pace::Rate(10),
            ),
            &dir,
        )
        .unwrap()
        .unpaced();
        assert_eq!(
            payloads(&mut reader, Instant::now()),
            [
                "a.jsonl: {\"t\":1}",
                "a.jsonl: \"text\"",
                "a.jsonl: not json",
                "problem: missing.jsonl",
                "b.csv: {\"room\":\"kitchen\",\"t\":21.5,\"note\":null,\"tags\":[1,2]}",
                "b.csv: {\"room\":\"a, b\",\"t\":true,\"note\":\"two\\nlines\",\"tags\":\"x\"}",
            ]
        );
        assert_eq!(reader.next(Instant::now()), Next::Done);
    }

    #[test]
    fn a_rate_spaces_records_out() {
        let dir = dir("rate");
        std::fs::write(dir.join("a.jsonl"), "1\n2\n3\n").unwrap();
        let mut reader =
            FileReader::new(&source(&["a.jsonl"], false, Pace::Rate(4)), &dir).unwrap();
        let start = Instant::now();
        assert!(matches!(reader.next(start), Next::Record(_)));
        assert_eq!(reader.next(start), Next::Wait(Duration::from_millis(250)));
        assert!(matches!(
            reader.next(start + Duration::from_millis(250)),
            Next::Record(_)
        ));
        assert!(matches!(
            reader.next(start + Duration::from_millis(400)),
            Next::Wait(_)
        ));
    }

    #[test]
    fn replay_keeps_the_gaps_between_recorded_times() {
        let dir = dir("replay");
        std::fs::write(
            dir.join("a.jsonl"),
            "{\"at\":100}\n{\"at\":100.5}\n{\"no\":1}\n{\"at\":102}\n",
        )
        .unwrap();
        let mut reader = FileReader::new(
            &source(&["a.jsonl"], false, Pace::Replay(".at".into())),
            &dir,
        )
        .unwrap();
        let start = Instant::now();
        assert!(matches!(reader.next(start), Next::Record(_)));
        assert_eq!(reader.next(start), Next::Wait(Duration::from_millis(500)));
        let at = |ms| start + Duration::from_millis(ms);
        assert!(matches!(reader.next(at(500)), Next::Record(_)));
        // No time: sent right away, and said so.
        match reader.next(at(500)) {
            Next::Record(r) => assert!(r.problem.unwrap().contains("no time")),
            other => panic!("{other:?}"),
        }
        assert_eq!(
            reader.next(at(500)),
            Next::Wait(Duration::from_millis(1500))
        );
        assert!(matches!(reader.next(at(2000)), Next::Record(_)));
        assert_eq!(reader.next(at(2000)), Next::Done);
    }

    #[test]
    fn follow_picks_up_lines_added_later_and_waits_for_whole_ones() {
        let dir = dir("follow");
        let path = dir.join("log.jsonl");
        std::fs::write(&path, "1\n").unwrap();
        let mut reader =
            FileReader::new(&source(&["log.jsonl"], true, Pace::Rate(1000)), &dir).unwrap();
        let mut now = Instant::now();
        assert_eq!(payloads(&mut reader, now), ["log.jsonl: 1"]);
        assert_eq!(reader.next(now), Next::Wait(FOLLOW_EVERY));

        let mut file = std::fs::OpenOptions::new()
            .append(true)
            .open(&path)
            .unwrap();
        write!(file, "2\n3").unwrap();
        now += FOLLOW_EVERY;
        assert_eq!(payloads(&mut reader, now), ["log.jsonl: 2"]);
        writeln!(file).unwrap();
        now += FOLLOW_EVERY;
        assert_eq!(payloads(&mut reader, now), ["log.jsonl: 3"]);
    }

    #[test]
    fn replay_times_can_be_timestamps_or_milliseconds() {
        assert_eq!(
            time_in_text("2026-09-29T12:00:00.250Z"),
            Some(1_790_683_200.25)
        );
        assert_eq!(
            time_in_text("2026-09-29 14:00:00+02:00"),
            Some(1_790_683_200.0)
        );
        assert_eq!(time_in_text("1790683200.5"), Some(1_790_683_200.5));
        assert_eq!(time_in_text("1790683200500"), Some(1_790_683_200.5));
        assert_eq!(time_in_text("soon"), None);
        let local = jiff::civil::date(2026, 9, 29)
            .at(12, 0, 0, 0)
            .to_zoned(jiff::tz::TimeZone::system())
            .unwrap()
            .timestamp()
            .as_second() as f64;
        assert_eq!(time_in_text("2026-09-29 12:00:00"), Some(local));

        let dir = dir("timestamps");
        std::fs::write(
            dir.join("t.csv"),
            "recordTimestamp,v\n2026-09-29T12:00:00Z,1\n2026-09-29T12:00:01.5Z,2\n1790683203000,3\n",
        )
        .unwrap();
        let mut reader = FileReader::new(
            &source(&["t.csv"], false, Pace::Replay(".recordTimestamp".into())),
            &dir,
        )
        .unwrap();
        let start = Instant::now();
        assert!(matches!(reader.next(start), Next::Record(_)));
        assert_eq!(reader.next(start), Next::Wait(Duration::from_millis(1500)));
        let at = start + Duration::from_millis(1500);
        assert!(matches!(reader.next(at), Next::Record(_)));
        assert_eq!(reader.next(at), Next::Wait(Duration::from_millis(1500)));
    }

    #[test]
    fn a_time_written_with_todate_reads_back() {
        let filter = jq::compile("todate", &[]).unwrap();
        let vars: Vec<Val> = (0..jq::VARIABLES.len()).map(|_| Val::Null).collect();
        let written = jq::run(&filter, payload_value(b"1790683200.25"), &vars).unwrap();
        let Val::TStr(text) = &written[0] else {
            panic!("{written:?}");
        };
        let text = String::from_utf8_lossy(text).into_owned();
        assert_eq!(
            time_in_text(&text).map(|t| (t * 1000.0).round()),
            Some(1_790_683_200_250.0)
        );
    }
}
