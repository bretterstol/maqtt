//! Broker Profiles: the saved settings for reaching one Broker as one identity.
//! Secrets (the password) are not part of a Profile; they live in the OS keyring.

use std::path::PathBuf;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, ts_rs::TS)]
#[ts(export, export_to = "generated/")]
pub struct ProfileId(pub String);

impl ProfileId {
    pub fn generate() -> Self {
        Self(uuid::Uuid::new_v4().to_string())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ts_rs::TS)]
#[serde(rename_all = "camelCase")]
#[ts(export, export_to = "generated/")]
pub enum Protocol {
    Mqtt5,
    Mqtt311,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ts_rs::TS)]
#[serde(rename_all = "camelCase")]
#[ts(export, export_to = "generated/")]
pub struct Profile {
    pub id: ProfileId,
    pub name: String,
    pub host: String,
    pub port: u16,
    pub protocol: Protocol,
    pub client_id: String,
    #[serde(default)]
    pub username: Option<String>,
    #[serde(default)]
    pub tls: Option<TlsSettings>,
    /// Start without a stored session; when false, the Broker keeps the session across
    /// disconnects.
    #[serde(default = "default_true")]
    pub clean_start: bool,
    #[serde(default = "default_keep_alive")]
    pub keep_alive_secs: u16,
    #[serde(default)]
    pub subscriptions: Vec<Subscription>,
    #[serde(default = "default_history_per_topic")]
    pub history_per_topic: usize,
    /// Connect when måqtt starts.
    #[serde(default)]
    pub connect_at_startup: bool,
    /// API Specs (AsyncAPI files) describing the Broker's topics, by path.
    #[serde(default)]
    pub specs: Vec<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize, ts_rs::TS)]
#[serde(rename_all = "camelCase")]
#[ts(export, export_to = "generated/")]
pub struct TlsSettings {
    /// Trust only this CA (PEM). Without it, the system's trusted roots are used.
    #[serde(default)]
    pub ca_file: Option<PathBuf>,
    #[serde(default)]
    pub client_cert_file: Option<PathBuf>,
    #[serde(default)]
    pub client_key_file: Option<PathBuf>,
    /// Accept any server certificate. Only for Brokers you trust on networks you trust.
    #[serde(default)]
    pub insecure_skip_verify: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ts_rs::TS)]
#[serde(rename_all = "camelCase")]
#[ts(export, export_to = "generated/")]
pub struct Subscription {
    pub filter: String,
    #[serde(default)]
    pub qos: u8,
}

fn default_true() -> bool {
    true
}

fn default_keep_alive() -> u16 {
    30
}

fn default_history_per_topic() -> usize {
    100
}

impl Profile {
    /// A new Profile with sensible defaults: MQTT 5, a generated client ID, subscribed to `#`.
    pub fn new(name: impl Into<String>, host: impl Into<String>, port: u16) -> Self {
        let id = ProfileId::generate();
        Self {
            client_id: format!("maqtt-{}", &id.0[..8]),
            id,
            name: name.into(),
            host: host.into(),
            port,
            protocol: Protocol::Mqtt5,
            username: None,
            tls: None,
            clean_start: true,
            keep_alive_secs: default_keep_alive(),
            subscriptions: vec![Subscription {
                filter: "#".into(),
                qos: 0,
            }],
            history_per_topic: default_history_per_topic(),
            connect_at_startup: false,
            specs: Vec::new(),
        }
    }

    pub fn validate(&self) -> Result<(), String> {
        if self.name.trim().is_empty() {
            return Err("Name is required".into());
        }
        if self.host.trim().is_empty() {
            return Err("Host is required".into());
        }
        if self.port == 0 {
            return Err("Port must be between 1 and 65535".into());
        }
        if self.client_id.trim().is_empty() {
            return Err("Client ID is required".into());
        }
        if self.history_per_topic == 0 {
            return Err("History per topic must be at least 1".into());
        }
        if let Some(tls) = &self.tls
            && tls.client_cert_file.is_some() != tls.client_key_file.is_some()
        {
            return Err("A client certificate needs both a certificate and a key file".into());
        }
        for subscription in &self.subscriptions {
            subscription.validate()?;
        }
        Ok(())
    }
}

impl Subscription {
    pub fn validate(&self) -> Result<(), String> {
        if self.qos > 2 {
            return Err(format!("QoS for {} must be 0, 1 or 2", self.filter));
        }
        validate_topic_filter(&self.filter)
    }
}

/// Checks that `topic` can be published to: not empty, and no wildcards.
pub fn validate_topic_name(topic: &str) -> Result<(), String> {
    if topic.is_empty() {
        return Err("A topic can't be empty".into());
    }
    if topic.contains(['+', '#']) {
        return Err("A topic to publish to can't contain `+` or `#`".into());
    }
    if topic.contains('\0') {
        return Err("A topic can't contain a null character".into());
    }
    Ok(())
}

/// MQTT topic filter matching: `+` matches one level, `#` the rest (including none), and topics
/// starting with `$` are not matched by a filter starting with a wildcard.
pub fn topic_matches(filter: &str, topic: &str) -> bool {
    if topic.starts_with('$') && (filter.starts_with('+') || filter.starts_with('#')) {
        return false;
    }
    let mut topic_levels = topic.split('/');
    for level in filter.split('/') {
        if level == "#" {
            return true;
        }
        match topic_levels.next() {
            Some(t) if level == "+" || level == t => {}
            _ => return false,
        }
    }
    topic_levels.next().is_none()
}

/// Checks MQTT topic filter syntax: `#` only as the whole last level, `+` only as a whole level.
pub fn validate_topic_filter(filter: &str) -> Result<(), String> {
    if filter.is_empty() {
        return Err("A topic filter can't be empty".into());
    }
    let levels: Vec<&str> = filter.split('/').collect();
    for (i, level) in levels.iter().enumerate() {
        let is_last = i == levels.len() - 1;
        if level.contains('#') && (*level != "#" || !is_last) {
            return Err(format!("`#` must be the whole last level in {filter}"));
        }
        if level.contains('+') && *level != "+" {
            return Err(format!("`+` must be a whole level in {filter}"));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn valid_topic_filters() {
        for filter in ["#", "a/b", "a/+/c", "+", "a/#", "+/+/#", "/a", "a//b"] {
            assert!(validate_topic_filter(filter).is_ok(), "{filter}");
        }
    }

    #[test]
    fn invalid_topic_filters() {
        for filter in ["", "a/#/b", "a#", "#/a", "a+/b", "a/b+", "++"] {
            assert!(validate_topic_filter(filter).is_err(), "{filter}");
        }
    }

    #[test]
    fn topic_matching() {
        for (filter, topic) in [
            ("a/b", "a/b"),
            ("#", "a/b/c"),
            ("a/#", "a"),
            ("a/+", "a/b"),
            ("+/+", "/a"),
            ("+", ""),
            ("a/+/b", "a//b"),
            ("$SYS/#", "$SYS/x"),
        ] {
            assert!(
                topic_matches(filter, topic),
                "{filter} should match {topic}"
            );
        }
        for (filter, topic) in [
            ("a/b", "a/b/c"),
            ("a/+", "a"),
            ("+", "/a"),
            ("A/b", "a/b"),
            ("#", "$SYS/x"),
            ("+/x", "$SYS/x"),
        ] {
            assert!(
                !topic_matches(filter, topic),
                "{filter} should not match {topic}"
            );
        }
    }

    #[test]
    fn topic_names_for_publishing() {
        for topic in ["a", "a/b", "/a", "a//b", "$SYS/x"] {
            assert!(validate_topic_name(topic).is_ok(), "{topic}");
        }
        for topic in ["", "a/+", "a/#", "#", "a\0b"] {
            assert!(validate_topic_name(topic).is_err(), "{topic:?}");
        }
    }

    #[test]
    fn new_profile_is_valid_and_subscribes_to_everything() {
        let profile = Profile::new("Home", "localhost", 1883);
        assert!(profile.validate().is_ok());
        assert_eq!(profile.subscriptions[0].filter, "#");
        assert!(profile.client_id.starts_with("maqtt-"));
    }

    #[test]
    fn client_cert_without_key_is_rejected() {
        let mut profile = Profile::new("Home", "localhost", 8883);
        profile.tls = Some(TlsSettings {
            client_cert_file: Some("client.crt".into()),
            ..TlsSettings::default()
        });
        assert!(profile.validate().is_err());
    }

    #[test]
    fn missing_optional_fields_get_defaults_when_parsed() {
        let profile: Profile = toml::from_str(
            r#"
            id = "abc"
            name = "Home"
            host = "localhost"
            port = 1883
            protocol = "mqtt311"
            clientId = "me"
            "#,
        )
        .unwrap();
        assert!(profile.clean_start);
        assert_eq!(profile.keep_alive_secs, 30);
        assert_eq!(profile.history_per_topic, 100);
        assert!(profile.subscriptions.is_empty());
    }
}
