//! Pipelines on disk: one `.pipe` file each in the `pipelines` folder beside `profiles.toml`.

use std::path::{Path, PathBuf};

const EXTENSION: &str = "pipe";
/// Beside the `.pipe` files: which Pipelines start with mÅqtt.
const SETTINGS: &str = "settings.json";

#[derive(Default, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase", default)]
struct Settings {
    start_with_app: Vec<String>,
}

pub struct PipelineFiles {
    dir: PathBuf,
}

impl PipelineFiles {
    pub fn new(dir: PathBuf) -> Self {
        Self { dir }
    }

    /// Every Pipeline as (name, source), sorted by name.
    pub fn list(&self) -> Vec<(String, String)> {
        let Ok(entries) = std::fs::read_dir(&self.dir) else {
            return Vec::new();
        };
        let mut pipelines: Vec<_> = entries
            .flatten()
            .map(|e| e.path())
            .filter(|p| p.extension().is_some_and(|x| x == EXTENSION))
            .filter_map(|p| {
                let name = p.file_stem()?.to_str()?.to_string();
                let source = std::fs::read_to_string(&p).ok()?;
                Some((name, source))
            })
            .collect();
        pipelines.sort();
        pipelines
    }

    pub fn read(&self, name: &str) -> Result<String, String> {
        check_name(name)?;
        std::fs::read_to_string(self.path(name))
            .map_err(|_| format!("There is no Pipeline called {name}"))
    }

    pub fn save(&self, name: &str, source: &str) -> Result<(), String> {
        check_name(name)?;
        std::fs::create_dir_all(&self.dir).map_err(|e| e.to_string())?;
        let path = self.path(name);
        let tmp = path.with_extension("pipe.tmp");
        std::fs::write(&tmp, source)
            .and_then(|()| std::fs::rename(&tmp, &path))
            .map_err(|e| format!("Couldn't save {}: {e}", path.display()))
    }

    pub fn delete(&self, name: &str) -> Result<(), String> {
        check_name(name)?;
        match std::fs::remove_file(self.path(name)) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e.to_string()),
        }
    }

    fn path(&self, name: &str) -> PathBuf {
        self.dir.join(format!("{name}.{EXTENSION}"))
    }

    pub fn dir(&self) -> &Path {
        &self.dir
    }

    /// The Pipelines marked to start with mÅqtt, sorted.
    pub fn starts_with_app(&self) -> Vec<String> {
        self.settings().start_with_app
    }

    /// Marks a Pipeline to start with mÅqtt, or unmarks it.
    pub fn set_starts_with_app(&self, name: &str, on: bool) -> Result<(), String> {
        let mut settings = self.settings();
        settings.start_with_app.retain(|n| n != name);
        if on {
            check_name(name)?;
            settings.start_with_app.push(name.to_string());
            settings.start_with_app.sort();
        }
        self.save_settings(&settings)
    }

    /// What `settings.json` says; nothing marked when it's missing or unreadable.
    fn settings(&self) -> Settings {
        std::fs::read_to_string(self.dir.join(SETTINGS))
            .ok()
            .and_then(|text| serde_json::from_str(&text).ok())
            .unwrap_or_default()
    }

    fn save_settings(&self, settings: &Settings) -> Result<(), String> {
        std::fs::create_dir_all(&self.dir).map_err(|e| e.to_string())?;
        let path = self.dir.join(SETTINGS);
        let tmp = path.with_extension("json.tmp");
        let text = serde_json::to_string_pretty(settings).map_err(|e| e.to_string())?;
        std::fs::write(&tmp, text + "\n")
            .and_then(|()| std::fs::rename(&tmp, &path))
            .map_err(|e| format!("Couldn't save {}: {e}", path.display()))
    }
}

/// A Pipeline's name becomes a file name and a Derived Topic level, so keep it tame.
pub fn check_name(name: &str) -> Result<(), String> {
    let ok = !name.trim().is_empty()
        && name.len() <= 64
        && !name.starts_with('.')
        && name
            .chars()
            .all(|c| c.is_alphanumeric() || matches!(c, ' ' | '-' | '_' | '.'));
    if ok {
        Ok(())
    } else {
        Err("A Pipeline name uses letters, digits, spaces, `-`, `_` and `.` (up to 64)".into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn files(name: &str) -> PipelineFiles {
        let dir = std::env::temp_dir().join(format!("maqtt-pipes-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        PipelineFiles::new(dir)
    }

    #[test]
    fn save_list_read_delete() {
        let f = files("crud");
        assert!(f.list().is_empty());
        f.save("hot rooms", "from all").unwrap();
        f.save("b", "from all |> changes").unwrap();
        assert_eq!(
            f.list(),
            [
                ("b".into(), "from all |> changes".into()),
                ("hot rooms".into(), "from all".into())
            ]
        );
        assert_eq!(f.read("b").unwrap(), "from all |> changes");
        f.delete("b").unwrap();
        f.delete("b").unwrap();
        assert!(f.read("b").is_err());
    }

    #[test]
    fn names_stay_safe() {
        for bad in [
            "",
            " ",
            "a/b",
            "a+b",
            "#",
            ".hidden",
            "../x",
            &"x".repeat(65),
        ] {
            assert!(check_name(bad).is_err(), "{bad:?}");
        }
        for good in ["hot rooms", "fleet-rssi_v2", "Temperatur ÆØÅ"] {
            assert!(check_name(good).is_ok(), "{good:?}");
        }
    }

    #[test]
    fn pipelines_can_be_marked_to_start_with_the_app() {
        let f = files("autostart");
        assert!(f.starts_with_app().is_empty());
        f.set_starts_with_app("rec", true).unwrap();
        f.set_starts_with_app("a", true).unwrap();
        f.set_starts_with_app("rec", true).unwrap();
        assert_eq!(f.starts_with_app(), ["a", "rec"]);
        f.set_starts_with_app("a", false).unwrap();
        assert_eq!(f.starts_with_app(), ["rec"]);
        assert!(f.set_starts_with_app("../x", true).is_err());
        // It lives beside the Pipelines but isn't one.
        assert!(f.list().is_empty());
        let text = std::fs::read_to_string(f.dir().join("settings.json")).unwrap();
        assert_eq!(text, "{\n  \"startWithApp\": [\n    \"rec\"\n  ]\n}\n");
    }
}
