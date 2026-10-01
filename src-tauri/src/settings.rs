//! Settings: how måqtt behaves, in `settings.toml` beside `profiles.toml`. A missing file or
//! key means today's behaviour, so nobody's setup changes when a setting is added.

use std::path::PathBuf;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct Settings {
    pub pipelines: PipelinesSettings,
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

    /// Saves new settings, written atomically.
    pub fn set(&mut self, settings: Settings) -> Result<(), String> {
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
            })
            .unwrap();
        assert_eq!(
            std::fs::read_to_string(&p).unwrap(),
            "[pipelines]\nenabled = false\n"
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
}
