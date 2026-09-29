//! Pipelines: user code that transforms live messages. See docs/processing-plan.md.
//!
//! Per ADR 0004 this module is removable: it reaches the rest of måqtt only through the Message
//! Bus, the Connection names it reads, a read-only view of recent messages for Preview, a way to
//! send on a Connection for `to broker`, and the commands below, which `lib.rs` registers.

pub mod file_source;
pub mod files;
pub mod jq;
pub mod keyed;
pub mod lang;
pub mod lines;
pub mod markers;
pub mod preview;
#[cfg(test)]
mod reference_tests;
pub mod runner;
pub mod stages;
pub mod store;

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use serde::Serialize;
use tauri::State;

use crate::bus::Bus;
use crate::connection::ConnectionStatuses;
use lang::Diagnostic;
use preview::{Preview, RecentMessages};
use runner::{PipelineStatus, Republish, Runner};
use store::PipelineFiles;

pub struct Processing {
    runner: Runner,
    files: PipelineFiles,
    /// Where `to file` writes names that aren't absolute.
    files_dir: PathBuf,
    statuses: ConnectionStatuses,
    recent: RecentMessages,
    /// Previews running now. A Preview stuck on an endless expression never ends, so this
    /// caps how many can pile up.
    previewing: Arc<AtomicUsize>,
}

impl Processing {
    pub fn new(
        bus: Bus,
        statuses: ConnectionStatuses,
        dir: PathBuf,
        recent: RecentMessages,
        republish: Republish,
        files_dir: PathBuf,
    ) -> Self {
        Self {
            runner: Runner::new(bus, statuses.clone(), republish, files_dir.clone()),
            files: PipelineFiles::new(dir),
            files_dir,
            statuses,
            recent,
            previewing: Arc::new(AtomicUsize::new(0)),
        }
    }
}

impl Processing {
    /// Starts the Pipelines marked to start with måqtt: once, as the app starts. One that can't
    /// start says why in its panel; one whose file is gone is unmarked.
    pub fn start_marked(&self) {
        for name in self.files.starts_with_app() {
            let source = match self.files.read(&name) {
                Ok(source) => source,
                Err(_) => {
                    let _ = self.files.set_starts_with_app(&name, false);
                    continue;
                }
            };
            let started = lang::parse(&source)
                .map_err(|problems| {
                    let first = &problems[0];
                    format!(
                        "Line {}, column {}: {}",
                        first.line, first.column, first.message
                    )
                })
                .and_then(|pipeline| self.runner.start(&name, pipeline));
            if let Err(err) = started {
                self.runner
                    .failed(&name, format!("Didn't start with måqtt: {err}"));
            }
        }
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PipelineView {
    name: String,
    source: String,
    status: PipelineStatus,
    starts_with_app: bool,
}

/// Every saved Pipeline with its source and status.
#[tauri::command]
pub fn list_pipelines(state: State<'_, Processing>) -> Vec<PipelineView> {
    let marked = state.files.starts_with_app();
    state
        .files
        .list()
        .into_iter()
        .map(|(name, source)| PipelineView {
            starts_with_app: marked.contains(&name),
            status: state
                .runner
                .status(&name)
                .unwrap_or_else(|| PipelineStatus {
                    name: name.clone(),
                    ..PipelineStatus::default()
                }),
            name,
            source,
        })
        .collect()
}

/// Saves a Pipeline if it has no problems, and restarts it if it's running. Returns the
/// problems; when there are any, nothing is saved and a running Pipeline keeps its old code.
#[tauri::command]
pub fn save_pipeline(
    state: State<'_, Processing>,
    name: String,
    source: String,
) -> Result<Vec<Diagnostic>, String> {
    store::check_name(&name)?;
    let pipeline = match lang::parse(&source) {
        Ok(pipeline) => pipeline,
        Err(problems) => return Ok(problems),
    };
    state.files.save(&name, &source)?;
    if state.runner.is_running(&name) {
        state.runner.start(&name, pipeline)?;
    }
    Ok(Vec::new())
}

#[tauri::command]
pub fn delete_pipeline(state: State<'_, Processing>, name: String) -> Result<(), String> {
    state.runner.forget(&name);
    state.files.set_starts_with_app(&name, false)?;
    state.files.delete(&name)
}

/// Marks a Pipeline to start when måqtt starts, or unmarks it. It doesn't start or stop it now.
#[tauri::command]
pub fn set_pipeline_starts_with_app(
    state: State<'_, Processing>,
    name: String,
    on: bool,
) -> Result<(), String> {
    state.files.read(&name)?;
    state.files.set_starts_with_app(&name, on)
}

/// Renames a Pipeline's file; a running Pipeline carries on under the new name.
#[tauri::command]
pub fn rename_pipeline(
    state: State<'_, Processing>,
    from: String,
    to: String,
) -> Result<(), String> {
    store::check_name(&to)?;
    if from == to {
        return Ok(());
    }
    if state.files.read(&to).is_ok() {
        return Err(format!("There is already a Pipeline called {to}"));
    }
    let source = state.files.read(&from)?;
    let was_running = state.runner.is_running(&from);
    let marked = state.files.starts_with_app().contains(&from);
    state.files.save(&to, &source)?;
    state.files.delete(&from)?;
    if marked {
        state.files.set_starts_with_app(&from, false)?;
        state.files.set_starts_with_app(&to, true)?;
    }
    state.runner.forget(&from);
    if was_running && let Ok(pipeline) = lang::parse(&source) {
        state.runner.start(&to, pipeline)?;
    }
    Ok(())
}

#[tauri::command]
pub fn start_pipeline(state: State<'_, Processing>, name: String) -> Result<(), String> {
    let source = state.files.read(&name)?;
    let pipeline = lang::parse(&source).map_err(|problems| {
        let first = &problems[0];
        format!(
            "Line {}, column {}: {}",
            first.line, first.column, first.message
        )
    })?;
    state.runner.start(&name, pipeline)
}

#[tauri::command]
pub fn stop_pipeline(state: State<'_, Processing>, name: String) {
    state.runner.stop(&name);
}

/// Where the `.pipe` files live, for showing it to the user.
/// Counts a running Preview until dropped.
struct Busy(Arc<AtomicUsize>);

impl Drop for Busy {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

/// Previews that may run at once: one stuck on an endless expression still leaves room for the
/// fixed draft.
const MAX_PREVIEWS: usize = 2;

/// How long a Preview may take before it's given up on.
const PREVIEW_LIMIT: Duration = Duration::from_secs(3);

/// Runs a draft over recent messages without starting it; see [`preview::preview`]. It runs on
/// its own thread, so an expression that never ends can't hold up the app.
#[tauri::command]
pub async fn preview_pipeline(
    state: State<'_, Processing>,
    source: String,
) -> Result<Preview, String> {
    let busy = Busy(state.previewing.clone());
    if state.previewing.fetch_add(1, Ordering::SeqCst) >= MAX_PREVIEWS {
        return Err("Earlier Previews are stuck on expressions that never end, each keeping a CPU busy. Restart måqtt to free them.".into());
    }
    let names = state
        .statuses
        .snapshot()
        .into_iter()
        .map(|s| (s.connection_id, s.name))
        .collect();
    let recent = state.recent.clone();
    let files_dir = state.files_dir.clone();
    let (done, result) = tokio::sync::oneshot::channel();
    std::thread::Builder::new()
        .name("pipeline preview".into())
        .spawn(move || {
            // Counted down however the thread ends, even by a panic.
            let _busy = busy;
            let _ = done.send(preview::preview(&source, &names, &recent, &files_dir));
        })
        .map_err(|e| format!("Couldn't run the Preview: {e}"))?;
    match tokio::time::timeout(PREVIEW_LIMIT, result).await {
        Ok(Ok(preview)) => Ok(preview),
        Ok(Err(_)) => Err("The Preview stopped unexpectedly".into()),
        Err(_) => Err(format!(
            "The Preview didn't finish within {} s: an expression in it may never end",
            PREVIEW_LIMIT.as_secs()
        )),
    }
}

#[tauri::command]
pub fn pipelines_folder(state: State<'_, Processing>) -> String {
    state.files.dir().display().to_string()
}

/// The files in the Pipeline files folder.
#[tauri::command]
pub fn list_pipeline_files(state: State<'_, Processing>) -> Vec<files::FileEntry> {
    files::list(&state.files_dir)
}

/// The end of a file in the Pipeline files folder, for looking into it.
#[tauri::command]
pub fn tail_pipeline_file(state: State<'_, Processing>, name: String) -> Result<String, String> {
    files::tail(&state.files_dir, &name, TAIL_LINES)
}

/// How much of a file the tab shows.
const TAIL_LINES: usize = 50;

/// Where `to file` writes names that aren't absolute, for showing it to the user.
#[tauri::command]
pub fn pipeline_files_folder(state: State<'_, Processing>) -> String {
    state.files_dir.display().to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn marked_pipelines_start_with_the_app_or_say_why_not() {
        let dir = std::env::temp_dir().join(format!("maqtt-autostart-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let processing = Processing::new(
            Bus::new(),
            ConnectionStatuses::default(),
            dir.join("pipelines"),
            Arc::new(|_| Vec::new()),
            Arc::new(|_, _, _| Ok(())),
            dir.join("files"),
        );
        let files = &processing.files;
        files.save("good", "from all |> to app \"x\"").unwrap();
        // Edited outside måqtt into something that doesn't parse.
        std::fs::write(dir.join("pipelines/broken.pipe"), "from all |> sort").unwrap();
        for name in ["good", "broken", "gone"] {
            files.set_starts_with_app(name, true).unwrap();
        }

        processing.start_marked();

        assert!(processing.runner.is_running("good"));
        let broken = processing.runner.status("broken").unwrap();
        assert!(!broken.running);
        assert!(
            broken
                .last_error
                .unwrap()
                .starts_with("Didn't start with måqtt: Line 1, column 13: Unknown Stage `sort`")
        );
        assert_eq!(files.starts_with_app(), ["broken", "good"]);
        processing.runner.stop("good");
    }
}
