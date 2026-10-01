//! Settings: how måqtt behaves, in `settings.toml` beside `profiles.toml`. A missing file or
//! key means today's behaviour, so nobody's setup changes when a setting is added.

use std::path::PathBuf;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct Settings {
    pub pipelines: PipelinesSettings,
    pub history: HistorySettings,
    pub message_log: MessageLogSettings,
    pub subscriptions: SubscriptionSettings,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct HistorySettings {
    /// Messages kept per Connection, oldest dropped first. (Per topic is on each Profile.)
    pub per_connection: usize,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct MessageLogSettings {
    /// Messages the Message Log keeps while following live traffic.
    pub live: usize,
    /// How far back it can be scrolled, in messages.
    pub scrollback: usize,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct SubscriptionSettings {
    /// The QoS new Subscriptions start with.
    pub default_qos: u8,
}

impl Default for HistorySettings {
    fn default() -> Self {
        Self {
            per_connection: 100_000,
        }
    }
}

impl Default for MessageLogSettings {
    fn default() -> Self {
        Self {
            live: 5_000,
            scrollback: 100_000,
        }
    }
}

impl Settings {
    /// Refuses values that would make måqtt misbehave, saying what's allowed.
    pub fn check(&self) -> Result<(), String> {
        let within = |what: &str, v: usize, lo: usize, hi: usize| {
            if (lo..=hi).contains(&v) {
                Ok(())
            } else {
                Err(format!("{what} must be from {lo} to {hi}"))
            }
        };
        within(
            "History per Connection",
            self.history.per_connection,
            1_000,
            10_000_000,
        )?;
        within(
            "Message Log live messages",
            self.message_log.live,
            100,
            100_000,
        )?;
        within(
            "Message Log scrollback",
            self.message_log.scrollback,
            self.message_log.live,
            1_000_000,
        )?;
        if self.subscriptions.default_qos > 2 {
            return Err("The default QoS must be 0, 1 or 2".into());
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct PipelinesSettings {
    /// The Pipelines tab and Topic Tree branch are shown, and Pipelines may run.
    pub enabled: bool,
}

impl Default for PipelinesSettings {
    fn default() -> Self {
        Self { enabled: true }
    }
}

/// The settings as the Settings view sees them.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SettingsView {
    pub settings: Settings,
    /// Where the file is, to show it.
    pub path: String,
    /// Why the file couldn't be read, when it couldn't: the defaults are in use, and saving
    /// replaces it.
    pub problem: Option<String>,
}

pub struct SettingsStore {
    path: PathBuf,
    settings: Settings,
    problem: Option<String>,
}

impl SettingsStore {
    pub fn load(path: PathBuf) -> Self {
        let (settings, problem) = match std::fs::read_to_string(&path) {
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => (Settings::default(), None),
            Err(e) => (
                Settings::default(),
                Some(format!("Couldn't read {}: {e}", path.display())),
            ),
            Ok(text) => match toml::from_str(&text) {
                Ok(settings) => (settings, None),
                Err(e) => (
                    Settings::default(),
                    Some(format!("{} isn't valid: {}", path.display(), e.message())),
                ),
            },
        };
        Self {
            path,
            settings,
            problem,
        }
    }

    pub fn get(&self) -> &Settings {
        &self.settings
    }

    pub fn view(&self) -> SettingsView {
        SettingsView {
            settings: self.settings.clone(),
            path: self.path.display().to_string(),
            problem: self.problem.clone(),
        }
    }

    /// Saves new settings, written atomically, unless they're out of range.
    pub fn set(&mut self, settings: Settings) -> Result<(), String> {
        settings.check()?;
        if let Some(dir) = self.path.parent() {
            std::fs::create_dir_all(dir).map_err(|e| e.to_string())?;
        }
        let text = toml::to_string_pretty(&settings).map_err(|e| e.to_string())?;
        let tmp = self.path.with_extension("toml.tmp");
        std::fs::write(&tmp, text)
            .and_then(|()| std::fs::rename(&tmp, &self.path))
            .map_err(|e| format!("Couldn't save {}: {e}", self.path.display()))?;
        self.settings = settings;
        self.problem = None;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn path(name: &str) -> PathBuf {
        let dir =
            std::env::temp_dir().join(format!("maqtt-settings-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        dir.join("settings.toml")
    }

    #[test]
    fn missing_file_means_todays_behaviour() {
        let store = SettingsStore::load(path("missing"));
        assert!(store.get().pipelines.enabled);
        assert!(store.view().problem.is_none());
    }

    #[test]
    fn settings_are_saved_and_read_back() {
        let p = path("roundtrip");
        let mut store = SettingsStore::load(p.clone());
        store
            .set(Settings {
                pipelines: PipelinesSettings { enabled: false },
                ..Settings::default()
            })
            .unwrap();
        assert!(
            std::fs::read_to_string(&p)
                .unwrap()
                .starts_with("[pipelines]\nenabled = false\n")
        );
        assert!(!SettingsStore::load(p).get().pipelines.enabled);
    }

    #[test]
    fn a_broken_file_says_so_and_uses_the_defaults() {
        let p = path("broken");
        std::fs::create_dir_all(p.parent().unwrap()).unwrap();
        std::fs::write(&p, "[pipelines\nenabled = nope").unwrap();
        let store = SettingsStore::load(p);
        assert!(store.get().pipelines.enabled);
        assert!(store.view().problem.unwrap().contains("isn't valid"));
    }

    #[test]
    fn keys_it_doesnt_know_are_left_alone_when_reading() {
        let p = path("unknown");
        std::fs::create_dir_all(p.parent().unwrap()).unwrap();
        std::fs::write(&p, "[pipelines]\nenabled = false\n\n[later]\nthing = 1\n").unwrap();
        let store = SettingsStore::load(p);
        assert!(!store.get().pipelines.enabled);
        assert!(store.view().problem.is_none());
    }

    #[test]
    fn out_of_range_values_are_refused_and_not_saved() {
        let p = path("range");
        let mut store = SettingsStore::load(p.clone());
        let mut bad = Settings::default();
        bad.message_log.scrollback = 10;
        assert_eq!(
            store.set(bad).unwrap_err(),
            "Message Log scrollback must be from 5000 to 1000000"
        );
        let mut bad = Settings::default();
        bad.subscriptions.default_qos = 3;
        assert!(store.set(bad).is_err());
        assert!(!p.exists());
    }

    #[test]
    fn an_older_file_gets_todays_defaults_for_new_settings() {
        let p = path("older");
        std::fs::create_dir_all(p.parent().unwrap()).unwrap();
        std::fs::write(&p, "[pipelines]\nenabled = false\n").unwrap();
        let settings = SettingsStore::load(p).get().clone();
        assert_eq!(
            (
                settings.history.per_connection,
                settings.message_log.live,
                settings.subscriptions.default_qos
            ),
            (100_000, 5_000, 0)
        );
    }
}
