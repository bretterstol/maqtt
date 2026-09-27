//! Getting data out of måqtt: the Message Log as JSON Lines, and portable paths for Broker
//! Profile files moved between machines.

use std::collections::HashMap;
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};

use base64::Engine;
use serde_json::{Value, json};

use crate::history::LogItem;
use crate::message::{ConnectionId, MessageProperties};

/// Writes one JSON object per line, oldest first; returns how many lines were written.
pub fn write_json_lines(
    path: &Path,
    items: &[LogItem],
    names: &HashMap<ConnectionId, String>,
) -> Result<usize, String> {
    let file = std::fs::File::create(path)
        .map_err(|err| format!("Couldn't create {}: {err}", path.display()))?;
    let mut out = BufWriter::new(file);
    for item in items {
        serde_json::to_writer(&mut out, &log_line(item, names)).map_err(|err| err.to_string())?;
        out.write_all(b"\n").map_err(|err| err.to_string())?;
    }
    out.flush()
        .map_err(|err| format!("Couldn't write {}: {err}", path.display()))?;
    Ok(items.len())
}

pub fn log_line(item: &LogItem, names: &HashMap<ConnectionId, String>) -> Value {
    let name = |id: &ConnectionId| {
        names
            .get(id)
            .cloned()
            .unwrap_or_else(|| format!("#{}", id.0))
    };
    match item {
        LogItem::Message(m) => {
            let (payload, encoding) = match std::str::from_utf8(&m.payload) {
                Ok(text) => (text.to_string(), "utf8"),
                Err(_) => (
                    base64::engine::general_purpose::STANDARD.encode(&m.payload),
                    "base64",
                ),
            };
            let mut line = json!({
                "kind": "message",
                "time": iso_utc(m.received_at_ms),
                "connection": name(&m.connection_id),
                "topic": m.topic,
                "qos": m.qos,
                "retain": m.retain,
                "payload": payload,
                "payloadEncoding": encoding,
            });
            if let Some(properties) = &m.properties {
                line["properties"] = properties_json(properties);
            }
            line
        }
        LogItem::Marker(m) => json!({
            "kind": "marker",
            "time": iso_utc(m.at_ms),
            "connection": name(&m.connection_id),
            "event": m.event,
        }),
    }
}

fn properties_json(p: &MessageProperties) -> Value {
    let mut out = serde_json::Map::new();
    if let Some(v) = &p.content_type {
        out.insert("contentType".into(), v.clone().into());
    }
    if let Some(v) = p.payload_format_indicator {
        out.insert("payloadFormatIndicator".into(), v.into());
    }
    if let Some(v) = &p.response_topic {
        out.insert("responseTopic".into(), v.clone().into());
    }
    if let Some(v) = &p.correlation_data {
        let text = std::str::from_utf8(v)
            .map(str::to_string)
            .unwrap_or_else(|_| base64::engine::general_purpose::STANDARD.encode(v));
        out.insert("correlationData".into(), text.into());
    }
    if let Some(v) = p.message_expiry_interval {
        out.insert("messageExpiryInterval".into(), v.into());
    }
    if !p.user_properties.is_empty() {
        out.insert("userProperties".into(), json!(p.user_properties));
    }
    Value::Object(out)
}

/// `2026-09-27T12:53:37.245Z` from Unix milliseconds.
pub fn iso_utc(ms: u64) -> String {
    let days = (ms / 86_400_000) as i64;
    let rem = ms % 86_400_000;
    let (year, month, day) = civil_from_days(days);
    format!(
        "{year:04}-{month:02}-{day:02}T{:02}:{:02}:{:02}.{:03}Z",
        rem / 3_600_000,
        rem / 60_000 % 60,
        rem / 1000 % 60,
        rem % 1000
    )
}

/// Days since 1970-01-01 to a proleptic Gregorian date (Howard Hinnant's algorithm).
fn civil_from_days(days: i64) -> (i64, u32, u32) {
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let month = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    let year = yoe + era * 400 + i64::from(month <= 2);
    (year, month, day)
}

/// `/home/me/certs/ca.crt` becomes `~/certs/ca.crt`, so a file exported on Linux finds the same
/// certificates under `/Users/me` on macOS.
pub fn to_portable(path: &Path, home: &Path) -> PathBuf {
    match path.strip_prefix(home) {
        Ok(rest) => Path::new("~").join(rest),
        Err(_) => path.to_path_buf(),
    }
}

pub fn from_portable(path: &Path, home: &Path) -> PathBuf {
    match path.strip_prefix("~") {
        Ok(rest) => home.join(rest),
        Err(_) => path.to_path_buf(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::message::{ConnectionMarker, MarkerEvent, ReceivedMessage};
    use bytes::Bytes;
    use std::sync::Arc;

    #[test]
    fn iso_times() {
        assert_eq!(iso_utc(0), "1970-01-01T00:00:00.000Z");
        assert_eq!(iso_utc(951_782_400_000), "2000-02-29T00:00:00.000Z");
        assert_eq!(iso_utc(1_790_505_217_245), "2026-09-27T10:33:37.245Z");
    }

    fn names() -> HashMap<ConnectionId, String> {
        HashMap::from([(ConnectionId(1), "Home".to_string())])
    }

    fn message(payload: &[u8]) -> LogItem {
        LogItem::Message(Arc::new(ReceivedMessage {
            seq: 1,
            connection_id: ConnectionId(1),
            topic: "a/b".into(),
            payload: Bytes::copy_from_slice(payload),
            qos: 1,
            retain: true,
            received_at_ms: 0,
            properties: Some(MessageProperties {
                content_type: Some("text/plain".into()),
                user_properties: vec![("k".into(), "v".into())],
                ..MessageProperties::default()
            }),
        }))
    }

    #[test]
    fn a_text_message_line() {
        assert_eq!(
            log_line(&message(b"on"), &names()),
            json!({
                "kind": "message",
                "time": "1970-01-01T00:00:00.000Z",
                "connection": "Home",
                "topic": "a/b",
                "qos": 1,
                "retain": true,
                "payload": "on",
                "payloadEncoding": "utf8",
                "properties": {"contentType": "text/plain", "userProperties": [["k", "v"]]},
            })
        );
    }

    #[test]
    fn a_binary_payload_is_base64() {
        let line = log_line(&message(&[0xff, 0x00]), &names());
        assert_eq!(line["payload"], "/wA=");
        assert_eq!(line["payloadEncoding"], "base64");
    }

    #[test]
    fn a_marker_line() {
        let marker = LogItem::Marker(Arc::new(ConnectionMarker {
            seq: 2,
            connection_id: ConnectionId(9),
            at_ms: 1000,
            event: MarkerEvent::Reconnected { offline_ms: 5 },
        }));
        assert_eq!(
            log_line(&marker, &names()),
            json!({
                "kind": "marker",
                "time": "1970-01-01T00:00:01.000Z",
                "connection": "#9",
                "event": {"kind": "reconnected", "offlineMs": 5},
            })
        );
    }

    #[test]
    fn json_lines_file_has_one_object_per_line() {
        let path = std::env::temp_dir().join(format!("maqtt-export-{}.jsonl", std::process::id()));
        let written = write_json_lines(&path, &[message(b"1"), message(b"2")], &names()).unwrap();
        assert_eq!(written, 2);
        let text = std::fs::read_to_string(&path).unwrap();
        let payloads: Vec<String> = text
            .lines()
            .map(|l| {
                serde_json::from_str::<Value>(l).unwrap()["payload"]
                    .as_str()
                    .unwrap()
                    .into()
            })
            .collect();
        assert_eq!(payloads, ["1", "2"]);
    }

    #[test]
    fn home_paths_become_portable_and_back() {
        let linux = Path::new("/home/me");
        let mac = Path::new("/Users/me");
        let portable = to_portable(Path::new("/home/me/certs/ca.crt"), linux);
        assert_eq!(portable, Path::new("~/certs/ca.crt"));
        assert_eq!(
            from_portable(&portable, mac),
            Path::new("/Users/me/certs/ca.crt")
        );
        assert_eq!(
            to_portable(Path::new("/etc/ssl/ca.crt"), linux),
            Path::new("/etc/ssl/ca.crt")
        );
        assert_eq!(
            from_portable(Path::new("/etc/ssl/ca.crt"), mac),
            Path::new("/etc/ssl/ca.crt")
        );
    }
}
