//! Where Broker Profiles live: a TOML file in the platform config directory, with passwords
//! kept in the OS keyring (Secret Service on Linux, Keychain on macOS).

use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::profile::{Profile, ProfileId};

const KEYRING_SERVICE: &str = "maqtt";

pub trait SecretStore: Send + Sync {
    fn get(&self, key: &str) -> Result<Option<String>, String>;
    fn set(&self, key: &str, secret: &str) -> Result<(), String>;
    fn delete(&self, key: &str) -> Result<(), String>;
}

pub struct KeyringSecrets;

impl SecretStore for KeyringSecrets {
    fn get(&self, key: &str) -> Result<Option<String>, String> {
        match entry(key)?.get_password() {
            Ok(secret) => Ok(Some(secret)),
            Err(keyring::Error::NoEntry) => Ok(None),
            Err(err) => Err(format!("Couldn't read from the keyring: {err}")),
        }
    }

    fn set(&self, key: &str, secret: &str) -> Result<(), String> {
        entry(key)?
            .set_password(secret)
            .map_err(|err| format!("Couldn't write to the keyring: {err}"))
    }

    fn delete(&self, key: &str) -> Result<(), String> {
        match entry(key)?.delete_credential() {
            Ok(()) | Err(keyring::Error::NoEntry) => Ok(()),
            Err(err) => Err(format!("Couldn't delete from the keyring: {err}")),
        }
    }
}

fn entry(key: &str) -> Result<keyring::Entry, String> {
    keyring::Entry::new(KEYRING_SERVICE, key).map_err(|err| format!("Keyring unavailable: {err}"))
}

/// Keeps secrets in memory; for tests.
#[cfg(test)]
#[derive(Default)]
pub struct MemorySecrets(std::sync::Mutex<std::collections::HashMap<String, String>>);

#[cfg(test)]
impl SecretStore for MemorySecrets {
    fn get(&self, key: &str) -> Result<Option<String>, String> {
        Ok(self.0.lock().unwrap().get(key).cloned())
    }

    fn set(&self, key: &str, secret: &str) -> Result<(), String> {
        self.0.lock().unwrap().insert(key.into(), secret.into());
        Ok(())
    }

    fn delete(&self, key: &str) -> Result<(), String> {
        self.0.lock().unwrap().remove(key);
        Ok(())
    }
}

/// What the frontend sees of a Profile: the Profile itself plus whether a password is stored,
/// never the password.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ProfileView {
    #[serde(flatten)]
    pub profile: Profile,
    pub has_password: bool,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "kind", content = "value", rename_all = "camelCase")]
pub enum PasswordChange {
    Keep,
    Set(String),
    Clear,
}

#[derive(Default, Serialize, Deserialize)]
struct ProfilesFile {
    #[serde(default)]
    profiles: Vec<Profile>,
}

pub struct ProfileStore {
    path: PathBuf,
    profiles: Vec<Profile>,
    secrets: Box<dyn SecretStore>,
}

impl ProfileStore {
    /// The Profiles file: `$MAQTT_CONFIG_DIR/profiles.toml` if set, otherwise
    /// `~/.config/maqtt/profiles.toml` (Linux) or `~/Library/Application Support/maqtt/profiles.toml` (macOS).
    pub fn default_path() -> Result<PathBuf, String> {
        let dir = match std::env::var_os("MAQTT_CONFIG_DIR") {
            Some(dir) => PathBuf::from(dir),
            None => directories::BaseDirs::new()
                .ok_or("Couldn't find the home directory")?
                .config_dir()
                .join("maqtt"),
        };
        Ok(dir.join("profiles.toml"))
    }

    /// Loads Profiles from `path`; a missing file means no Profiles yet.
    pub fn load(path: PathBuf, secrets: Box<dyn SecretStore>) -> Result<Self, String> {
        let profiles = match std::fs::read_to_string(&path) {
            Ok(text) => {
                toml::from_str::<ProfilesFile>(&text)
                    .map_err(|err| format!("Couldn't parse {}: {err}", path.display()))?
                    .profiles
            }
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Vec::new(),
            Err(err) => return Err(format!("Couldn't read {}: {err}", path.display())),
        };
        Ok(Self {
            path,
            profiles,
            secrets,
        })
    }

    pub fn exists(&self) -> bool {
        self.path.exists()
    }

    pub fn list(&self) -> Vec<ProfileView> {
        self.profiles
            .iter()
            .map(|profile| ProfileView {
                has_password: matches!(self.secrets.get(&profile.id.0), Ok(Some(_))),
                profile: profile.clone(),
            })
            .collect()
    }

    pub fn get(&self, id: &ProfileId) -> Option<&Profile> {
        self.profiles.iter().find(|p| &p.id == id)
    }

    pub fn password(&self, id: &ProfileId) -> Result<Option<String>, String> {
        self.secrets.get(&id.0)
    }

    /// Creates or replaces a Profile.
    pub fn save(&mut self, profile: Profile, password: PasswordChange) -> Result<(), String> {
        profile.validate()?;
        match &password {
            PasswordChange::Keep => {}
            PasswordChange::Set(secret) => self.secrets.set(&profile.id.0, secret)?,
            PasswordChange::Clear => self.secrets.delete(&profile.id.0)?,
        }
        match self.profiles.iter_mut().find(|p| p.id == profile.id) {
            Some(existing) => *existing = profile,
            None => self.profiles.push(profile),
        }
        self.write()
    }

    pub fn delete(&mut self, id: &ProfileId) -> Result<(), String> {
        self.profiles.retain(|p| &p.id != id);
        self.secrets.delete(&id.0)?;
        self.write()
    }

    fn write(&self) -> Result<(), String> {
        let text = toml::to_string_pretty(&ProfilesFile {
            profiles: self.profiles.clone(),
        })
        .map_err(|err| format!("Couldn't serialize Profiles: {err}"))?;
        write_atomically(&self.path, &text)
            .map_err(|err| format!("Couldn't write {}: {err}", self.path.display()))
    }
}

fn write_atomically(path: &Path, text: &str) -> std::io::Result<()> {
    if let Some(dir) = path.parent() {
        std::fs::create_dir_all(dir)?;
    }
    let tmp = path.with_extension("toml.tmp");
    std::fs::write(&tmp, text)?;
    std::fs::rename(&tmp, path)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::profile::TlsSettings;

    fn temp_path(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("maqtt-test-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        dir.join("profiles.toml")
    }

    fn store(path: &Path) -> ProfileStore {
        ProfileStore::load(path.to_path_buf(), Box::new(MemorySecrets::default())).unwrap()
    }

    #[test]
    fn missing_file_means_no_profiles() {
        let path = temp_path("missing");
        let store = store(&path);
        assert!(store.list().is_empty());
        assert!(!store.exists());
    }

    #[test]
    fn profiles_round_trip_through_the_file() {
        let path = temp_path("round-trip");
        let mut profile = Profile::new("TLS", "broker.local", 8883);
        profile.tls = Some(TlsSettings {
            ca_file: Some("/certs/ca.crt".into()),
            ..TlsSettings::default()
        });
        store(&path)
            .save(profile.clone(), PasswordChange::Keep)
            .unwrap();

        let reloaded = store(&path);
        assert_eq!(reloaded.get(&profile.id), Some(&profile));
    }

    #[test]
    fn passwords_go_to_the_secret_store_not_the_file() {
        let path = temp_path("secrets");
        let mut store = store(&path);
        let mut profile = Profile::new("Auth", "localhost", 1884);
        profile.username = Some("maqtt".into());
        store
            .save(profile.clone(), PasswordChange::Set("hunter2".into()))
            .unwrap();

        assert!(!std::fs::read_to_string(&path).unwrap().contains("hunter2"));
        assert_eq!(
            store.password(&profile.id).unwrap().as_deref(),
            Some("hunter2")
        );
        assert!(store.list()[0].has_password);

        store.save(profile.clone(), PasswordChange::Keep).unwrap();
        assert_eq!(
            store.password(&profile.id).unwrap().as_deref(),
            Some("hunter2")
        );

        store.save(profile.clone(), PasswordChange::Clear).unwrap();
        assert_eq!(store.password(&profile.id).unwrap(), None);
    }

    #[test]
    fn saving_replaces_by_id_and_delete_removes_the_secret() {
        let path = temp_path("replace");
        let mut store = store(&path);
        let mut profile = Profile::new("One", "localhost", 1883);
        store
            .save(profile.clone(), PasswordChange::Set("pw".into()))
            .unwrap();
        profile.name = "Renamed".into();
        store.save(profile.clone(), PasswordChange::Keep).unwrap();
        assert_eq!(store.list().len(), 1);
        assert_eq!(store.list()[0].profile.name, "Renamed");

        store.delete(&profile.id).unwrap();
        assert!(store.list().is_empty());
        assert_eq!(store.password(&profile.id).unwrap(), None);
    }

    #[test]
    fn invalid_profiles_are_not_saved() {
        let path = temp_path("invalid");
        let mut store = store(&path);
        let mut profile = Profile::new("", "localhost", 1883);
        profile.name.clear();
        assert!(store.save(profile, PasswordChange::Keep).is_err());
        assert!(!path.exists());
    }
}
