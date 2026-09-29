//! The file Sink: `to file` writes JSON Lines or CSV, into the Pipeline files folder unless the
//! name is absolute or starts with `~/`, and starts new files by day, hour or size.
//!
//! Writes are buffered and flushed after each batch, on the Pipeline's own thread.

use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};

use serde_json::Value;

use super::lang::{FileFormat, Rotate};

/// Files kept open at once; past that, the least recently written is closed.
const MAX_OPEN: usize = 32;

pub struct FileSink {
    dir: PathBuf,
    home: Option<PathBuf>,
    /// By the path as named (before rotation adds a date).
    open: HashMap<PathBuf, Open>,
    /// Counts writes, to find the least recently written file.
    clock: u64,
}

struct Open {
    /// The file written now: the named path, or it with a date for daily and hourly files.
    actual: PathBuf,
    writer: BufWriter<File>,
    size: u64,
    /// CSV columns, from the file's first line.
    columns: Option<Vec<String>>,
    last_write: u64,
}

impl FileSink {
    pub fn new(dir: PathBuf) -> Self {
        Self {
            dir,
            home: home(),
            open: HashMap::new(),
            clock: 0,
        }
    }

    /// The folder relative names are in.
    pub fn dir(&self) -> &Path {
        &self.dir
    }

    /// Where a name from `to file` points.
    pub fn resolve(&self, name: &str) -> PathBuf {
        resolve(&self.dir, self.home.as_deref(), name)
    }

    /// Appends one value; says which file it went to, or what went wrong. A CSV value with
    /// keys the file has no columns for is written without them, and reported.
    pub fn write(
        &mut self,
        name: &str,
        format: FileFormat,
        rotate: Option<Rotate>,
        record: &Value,
        now: &jiff::Zoned,
    ) -> Result<PathBuf, String> {
        let named = self.resolve(name);
        let actual = match rotate {
            Some(Rotate::Daily) => dated(&named, &now.strftime("%Y-%m-%d").to_string()),
            Some(Rotate::Hourly) => dated(&named, &now.strftime("%Y-%m-%dT%H").to_string()),
            Some(Rotate::Size(_)) | None => named.clone(),
        };
        if self.open.get(&named).is_some_and(|o| o.actual != actual) {
            self.close(&named);
        }
        if !self.open.contains_key(&named) {
            if self.open.len() >= MAX_OPEN {
                self.close_oldest();
            }
            let open = Open::new(&actual, format)?;
            self.open.insert(named.clone(), open);
        }

        let (line, missing) = self.line(&named, format, record);
        if let Some(Rotate::Size(limit)) = rotate {
            let open = &self.open[&named];
            if open.size > 0 && open.size + line.len() as u64 > limit {
                self.close(&named);
                let stamp = now.strftime("%Y-%m-%dT%H-%M-%S").to_string();
                let mut full = dated(&actual, &stamp);
                for n in 1.. {
                    if !full.exists() {
                        break;
                    }
                    full = dated(&actual, &format!("{stamp}-{n}"));
                }
                std::fs::rename(&actual, &full)
                    .map_err(|e| format!("Couldn't rotate {}: {e}", actual.display()))?;
                self.open.insert(named.clone(), Open::new(&actual, format)?);
            }
        }
        // A fresh CSV file starts with its columns.
        let (line, missing) = if format == FileFormat::Csv && self.open[&named].columns.is_none() {
            let columns = columns_of(record);
            let header = csv_row(columns.iter().map(|c| Value::String(c.clone())));
            self.open.get_mut(&named).unwrap().columns = Some(columns);
            let (row, missing) = self.line(&named, format, record);
            (format!("{header}{row}"), missing)
        } else {
            (line, missing)
        };

        self.clock += 1;
        let open = self.open.get_mut(&named).unwrap();
        open.writer
            .write_all(line.as_bytes())
            .map_err(|e| format!("Couldn't write {}: {e}", actual.display()))?;
        open.size += line.len() as u64;
        open.last_write = self.clock;
        if missing.is_empty() {
            Ok(actual)
        } else {
            Err(format!(
                "{} has no column for {}; the row was written without it",
                actual.display(),
                missing.join(", ")
            ))
        }
    }

    /// The text for one value, and for CSV, the keys the file has no columns for.
    fn line(&self, named: &Path, format: FileFormat, record: &Value) -> (String, Vec<String>) {
        match format {
            FileFormat::JsonLines => (format!("{record}\n"), Vec::new()),
            FileFormat::Csv => {
                let Some(columns) = self.open[named].columns.as_ref() else {
                    return (String::new(), Vec::new());
                };
                match record {
                    Value::Object(fields) => {
                        let row = csv_row(
                            columns
                                .iter()
                                .map(|c| fields.get(c).cloned().unwrap_or(Value::Null)),
                        );
                        let missing = fields
                            .keys()
                            .filter(|k| !columns.contains(k))
                            .cloned()
                            .collect();
                        (row, missing)
                    }
                    other => {
                        // Not an object: it goes in the first column.
                        let row = csv_row(
                            std::iter::once(other.clone())
                                .chain((1..columns.len()).map(|_| Value::Null)),
                        );
                        (row, Vec::new())
                    }
                }
            }
        }
    }

    pub fn flush(&mut self) -> Result<(), String> {
        let mut failed = None;
        for (named, open) in &mut self.open {
            if let Err(e) = open.writer.flush() {
                failed = Some(format!("Couldn't write {}: {e}", named.display()));
            }
        }
        failed.map_or(Ok(()), Err)
    }

    fn close(&mut self, named: &Path) {
        if let Some(mut open) = self.open.remove(named) {
            let _ = open.writer.flush();
        }
    }

    fn close_oldest(&mut self) {
        if let Some(oldest) = self
            .open
            .iter()
            .min_by_key(|(_, o)| o.last_write)
            .map(|(p, _)| p.clone())
        {
            self.close(&oldest);
        }
    }
}

impl Open {
    fn new(path: &Path, format: FileFormat) -> Result<Self, String> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .map_err(|e| format!("Couldn't create {}: {e}", parent.display()))?;
        }
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .map_err(|e| format!("Couldn't open {}: {e}", path.display()))?;
        let size = file.metadata().map(|m| m.len()).unwrap_or(0);
        // Carrying on with a CSV file: keep its columns.
        let columns = (format == FileFormat::Csv && size > 0)
            .then(|| {
                let mut first = String::new();
                BufReader::new(File::open(path).ok()?)
                    .read_line(&mut first)
                    .ok()?;
                Some(parse_csv_record(first.trim_end_matches(['\r', '\n'])))
            })
            .flatten();
        Ok(Self {
            actual: path.to_path_buf(),
            writer: BufWriter::new(file),
            size,
            columns,
            last_write: 0,
        })
    }
}

/// Where a file name in a Pipeline points: into `dir`, unless it's absolute or starts with `~/`.
pub fn resolve(dir: &Path, home: Option<&Path>, name: &str) -> PathBuf {
    match (name.strip_prefix("~/"), home) {
        (Some(rest), Some(home)) => home.join(rest),
        _ if Path::new(name).is_absolute() => PathBuf::from(name),
        _ => dir.join(name),
    }
}

/// The user's home folder, for `~/` in file names.
pub fn home() -> Option<PathBuf> {
    directories::BaseDirs::new().map(|d| d.home_dir().to_path_buf())
}

/// `temps.jsonl` with `2026-09-29` is `temps.2026-09-29.jsonl`.
fn dated(path: &Path, date: &str) -> PathBuf {
    let stem = path
        .file_stem()
        .map_or_else(String::new, |s| s.to_string_lossy().into_owned());
    let name = match path.extension() {
        Some(ext) => format!("{stem}.{date}.{}", ext.to_string_lossy()),
        None => format!("{stem}.{date}"),
    };
    path.with_file_name(name)
}

/// A new CSV file's columns: an object's keys, or `value` for anything else.
fn columns_of(record: &Value) -> Vec<String> {
    match record {
        Value::Object(fields) => fields.keys().cloned().collect(),
        _ => vec!["value".into()],
    }
}

/// One CSV row (RFC 4180): text as is, null empty, lists and objects as JSON.
fn csv_row(cells: impl Iterator<Item = Value>) -> String {
    let cells: Vec<String> = cells
        .map(|cell| {
            let text = match cell {
                Value::Null => String::new(),
                Value::String(s) => s,
                other => other.to_string(),
            };
            if text.contains([',', '"', '\n', '\r']) {
                format!("\"{}\"", text.replace('"', "\"\""))
            } else {
                text
            }
        })
        .collect();
    format!("{}\n", cells.join(","))
}

/// The cells of one CSV record (RFC 4180), which may span lines inside quotes.
pub(super) fn parse_csv_record(line: &str) -> Vec<String> {
    let mut columns = Vec::new();
    let mut cell = String::new();
    let mut quoted = false;
    let mut chars = line.chars().peekable();
    while let Some(c) = chars.next() {
        match (quoted, c) {
            (true, '"') if chars.peek() == Some(&'"') => {
                chars.next();
                cell.push('"');
            }
            (true, '"') => quoted = false,
            (false, '"') if cell.is_empty() => quoted = true,
            (false, ',') => columns.push(std::mem::take(&mut cell)),
            (_, c) => cell.push(c),
        }
    }
    columns.push(cell);
    columns
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn at(time: &str) -> jiff::Zoned {
        format!("{time}[Europe/Oslo]").parse().unwrap()
    }

    fn dir(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("maqtt-files-{}-{name}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        dir
    }

    fn read(path: &Path) -> String {
        std::fs::read_to_string(path).unwrap()
    }

    #[test]
    fn json_lines_append_one_value_per_line_in_the_files_folder() {
        let dir = dir("jsonl");
        let mut sink = FileSink::new(dir.clone());
        let now = at("2026-09-29T12:00:00");
        for value in [json!({"t": 21.5}), json!("text"), json!([1, 2])] {
            sink.write(
                "rooms/kitchen.jsonl",
                FileFormat::JsonLines,
                None,
                &value,
                &now,
            )
            .unwrap();
        }
        sink.flush().unwrap();
        assert_eq!(
            read(&dir.join("rooms/kitchen.jsonl")),
            "{\"t\":21.5}\n\"text\"\n[1,2]\n"
        );
    }

    #[test]
    fn csv_takes_its_columns_from_the_first_object_and_keeps_them() {
        let dir = dir("csv");
        let now = at("2026-09-29T12:00:00");
        let mut sink = FileSink::new(dir.clone());
        let write = |sink: &mut FileSink, value: Value| {
            sink.write("t.csv", FileFormat::Csv, None, &value, &now)
        };
        write(&mut sink, json!({"room": "kitchen", "t": 21.5})).unwrap();
        write(&mut sink, json!({"t": 3, "room": "a, \"b\""})).unwrap();
        let extra = write(&mut sink, json!({"room": "x", "hum": 40, "t": null}));
        assert!(extra.unwrap_err().contains("no column for hum"));
        sink.flush().unwrap();
        drop(sink);

        // A later run carries on with the same columns.
        let mut sink = FileSink::new(dir.clone());
        write(&mut sink, json!({"t": 1, "room": "y", "tags": [1]})).ok();
        write(&mut sink, json!(7)).unwrap();
        sink.flush().unwrap();
        assert_eq!(
            read(&dir.join("t.csv")),
            "room,t\nkitchen,21.5\n\"a, \"\"b\"\"\",3\nx,\ny,1\n7,\n"
        );
    }

    #[test]
    fn daily_and_hourly_files_carry_the_local_date() {
        let dir = dir("daily");
        let mut sink = FileSink::new(dir.clone());
        let write = |sink: &mut FileSink, rotate, time| {
            sink.write(
                "log.jsonl",
                FileFormat::JsonLines,
                Some(rotate),
                &json!(time),
                &at(time),
            )
            .unwrap()
        };
        assert_eq!(
            write(&mut sink, Rotate::Daily, "2026-09-29T23:59:00"),
            dir.join("log.2026-09-29.jsonl")
        );
        assert_eq!(
            write(&mut sink, Rotate::Daily, "2026-09-30T00:01:00"),
            dir.join("log.2026-09-30.jsonl")
        );
        assert_eq!(
            write(&mut sink, Rotate::Hourly, "2026-09-30T00:01:00"),
            dir.join("log.2026-09-30T00.jsonl")
        );
        sink.flush().unwrap();
        assert_eq!(
            read(&dir.join("log.2026-09-29.jsonl")),
            "\"2026-09-29T23:59:00\"\n"
        );
    }

    #[test]
    fn a_full_file_is_renamed_with_a_timestamp_and_a_new_one_started() {
        let dir = dir("size");
        let mut sink = FileSink::new(dir.clone());
        let rotate = Some(Rotate::Size(10));
        for (i, time) in [
            "2026-09-29T12:00:00",
            "2026-09-29T12:00:01",
            "2026-09-29T12:00:02",
        ]
        .into_iter()
        .enumerate()
        {
            sink.write(
                "s.jsonl",
                FileFormat::JsonLines,
                rotate,
                &json!(format!("{i}{i}{i}")),
                &at(time),
            )
            .unwrap();
        }
        sink.flush().unwrap();
        // Each line is 6 bytes: two don't fit in 10.
        assert_eq!(read(&dir.join("s.2026-09-29T12-00-01.jsonl")), "\"000\"\n");
        assert_eq!(read(&dir.join("s.2026-09-29T12-00-02.jsonl")), "\"111\"\n");
        assert_eq!(read(&dir.join("s.jsonl")), "\"222\"\n");
    }

    #[test]
    fn names_resolve_into_the_folder_home_or_as_given() {
        let sink = FileSink::new(PathBuf::from("/data/files"));
        assert_eq!(
            sink.resolve("a/b.csv"),
            PathBuf::from("/data/files/a/b.csv")
        );
        assert_eq!(sink.resolve("/tmp/x.csv"), PathBuf::from("/tmp/x.csv"));
        if let Some(home) = &sink.home {
            assert_eq!(sink.resolve("~/x.csv"), home.join("x.csv"));
        }
    }

    #[test]
    fn only_so_many_files_stay_open() {
        let dir = dir("many");
        let mut sink = FileSink::new(dir.clone());
        let now = at("2026-09-29T12:00:00");
        for i in 0..MAX_OPEN + 5 {
            sink.write(
                &format!("{i}.jsonl"),
                FileFormat::JsonLines,
                None,
                &json!(i),
                &now,
            )
            .unwrap();
        }
        assert_eq!(sink.open.len(), MAX_OPEN);
        sink.flush().unwrap();
        drop(sink);
        assert_eq!(read(&dir.join("0.jsonl")), "0\n");
    }
}
