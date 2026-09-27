//! Publishing from måqtt: what a message to send looks like, and the Publish History of
//! recent sends, kept in the config directory so it survives restarts.

use std::collections::VecDeque;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::message::now_ms;
use crate::profile::{ProfileId, validate_topic_name};

const MAX_HISTORY: usize = 50;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OutgoingMessage {
    pub topic: String,
    pub payload: String,
    pub qos: u8,
    pub retain: bool,
    /// MQTT 5 only.
    #[serde(default)]
    pub properties: Option<OutgoingProperties>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct OutgoingProperties {
    pub content_type: Option<String>,
    pub response_topic: Option<String>,
    pub correlation_data: Option<String>,
    pub message_expiry_interval: Option<u32>,
    /// Marks the payload as UTF-8 text.
    pub payload_is_utf8: bool,
    pub user_properties: Vec<(String, String)>,
}

impl OutgoingProperties {
    pub fn is_empty(&self) -> bool {
        *self == Self::default()
    }
}

impl OutgoingMessage {
    pub fn validate(&self) -> Result<(), String> {
        validate_topic_name(&self.topic)?;
        if self.qos > 2 {
            return Err("QoS must be 0, 1 or 2".into());
        }
        if let Some(response_topic) = self
            .properties
            .as_ref()
            .and_then(|p| p.response_topic.as_deref())
        {
            validate_topic_name(response_topic).map_err(|err| format!("Response topic: {err}"))?;
        }
        Ok(())
    }
}

/// One entry in the Publish History.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PublishRecord {
    pub profile_id: ProfileId,
    pub message: OutgoingMessage,
    pub at_ms: u64,
}

pub struct PublishHistory {
    path: PathBuf,
    records: VecDeque<PublishRecord>,
}

impl PublishHistory {
    /// Loads from `path`; a missing or unreadable file starts an empty history.
    pub fn load(path: PathBuf) -> Self {
        let records = std::fs::read_to_string(&path)
            .ok()
            .and_then(|text| serde_json::from_str(&text).ok())
            .unwrap_or_default();
        Self { path, records }
    }

    /// Newest first.
    pub fn list(&self) -> Vec<PublishRecord> {
        self.records.iter().cloned().collect()
    }

    /// Records a send; sending the same thing again moves it to the top instead of repeating it.
    pub fn record(
        &mut self,
        profile_id: ProfileId,
        message: OutgoingMessage,
    ) -> Result<(), String> {
        self.records
            .retain(|r| !(r.profile_id == profile_id && r.message == message));
        self.records.push_front(PublishRecord {
            profile_id,
            message,
            at_ms: now_ms(),
        });
        self.records.truncate(MAX_HISTORY);
        self.write()
    }

    pub fn clear(&mut self) -> Result<(), String> {
        self.records.clear();
        self.write()
    }

    fn write(&self) -> Result<(), String> {
        let text = serde_json::to_string_pretty(&self.records).map_err(|err| err.to_string())?;
        write_atomically(&self.path, &text)
            .map_err(|err| format!("Couldn't write {}: {err}", self.path.display()))
    }
}

fn write_atomically(path: &Path, text: &str) -> std::io::Result<()> {
    if let Some(dir) = path.parent() {
        std::fs::create_dir_all(dir)?;
    }
    let tmp = path.with_extension("json.tmp");
    std::fs::write(&tmp, text)?;
    std::fs::rename(&tmp, path)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn message(topic: &str, payload: &str) -> OutgoingMessage {
        OutgoingMessage {
            topic: topic.into(),
            payload: payload.into(),
            qos: 0,
            retain: false,
            properties: None,
        }
    }

    fn temp_path(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("maqtt-pub-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        dir.join("publish-history.json")
    }

    fn id(s: &str) -> ProfileId {
        ProfileId(s.into())
    }

    #[test]
    fn records_newest_first_and_survive_a_reload() {
        let path = temp_path("reload");
        let mut history = PublishHistory::load(path.clone());
        history.record(id("p"), message("a", "1")).unwrap();
        history.record(id("p"), message("b", "2")).unwrap();
        let topics: Vec<_> = PublishHistory::load(path)
            .list()
            .into_iter()
            .map(|r| r.message.topic)
            .collect();
        assert_eq!(topics, ["b", "a"]);
    }

    #[test]
    fn sending_the_same_again_moves_it_to_the_top() {
        let mut history = PublishHistory::load(temp_path("dedupe"));
        history.record(id("p"), message("a", "1")).unwrap();
        history.record(id("p"), message("b", "2")).unwrap();
        history.record(id("p"), message("a", "1")).unwrap();
        history.record(id("q"), message("a", "1")).unwrap();
        let list = history.list();
        assert_eq!(list.len(), 3);
        assert_eq!(
            (
                list[0].profile_id.0.as_str(),
                list[1].message.topic.as_str()
            ),
            ("q", "a")
        );
    }

    #[test]
    fn keeps_the_newest_fifty() {
        let mut history = PublishHistory::load(temp_path("cap"));
        for i in 0..60 {
            history
                .record(id("p"), message("t", &i.to_string()))
                .unwrap();
        }
        let list = history.list();
        assert_eq!(list.len(), MAX_HISTORY);
        assert_eq!(list[0].message.payload, "59");
    }

    #[test]
    fn a_corrupt_file_starts_empty() {
        let path = temp_path("corrupt");
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, "not json").unwrap();
        assert!(PublishHistory::load(path).list().is_empty());
    }

    #[test]
    fn validation_rejects_wildcards_and_bad_qos() {
        assert!(message("a/b", "").validate().is_ok());
        assert!(message("a/#", "").validate().is_err());
        let mut bad_qos = message("a", "");
        bad_qos.qos = 3;
        assert!(bad_qos.validate().is_err());
        let mut bad_response = message("a", "");
        bad_response.properties = Some(OutgoingProperties {
            response_topic: Some("reply/+".into()),
            ..OutgoingProperties::default()
        });
        assert!(bad_response.validate().is_err());
    }
}
