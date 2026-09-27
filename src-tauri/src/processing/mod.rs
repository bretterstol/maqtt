//! Pipelines: user code that transforms live messages. See docs/processing-plan.md.
//!
//! Per ADR 0004 this module is removable: it reaches the rest of måqtt only through the Message
//! Bus, the Connection names it reads, a read-only view of recent messages for Preview, and the
//! commands below, which `lib.rs` registers.

pub mod jq;
pub mod lang;
pub mod preview;
pub mod runner;
pub mod stages;
pub mod store;

use std::path::PathBuf;

use serde::Serialize;
use tauri::State;

use crate::bus::Bus;
use crate::connection::ConnectionStatuses;
use lang::Diagnostic;
use preview::{Preview, RecentMessages};
use runner::{PipelineStatus, Runner};
use store::PipelineFiles;

pub struct Processing {
    runner: Runner,
    files: PipelineFiles,
    statuses: ConnectionStatuses,
    recent: RecentMessages,
}

impl Processing {
    pub fn new(
        bus: Bus,
        statuses: ConnectionStatuses,
        dir: PathBuf,
        recent: RecentMessages,
    ) -> Self {
        Self {
            runner: Runner::new(bus, statuses.clone()),
            files: PipelineFiles::new(dir),
            statuses,
            recent,
        }
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PipelineView {
    name: String,
    source: String,
    status: PipelineStatus,
}

/// Every saved Pipeline with its source and status.
#[tauri::command]
pub fn list_pipelines(state: State<'_, Processing>) -> Vec<PipelineView> {
    state
        .files
        .list()
        .into_iter()
        .map(|(name, source)| PipelineView {
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
    state.files.delete(&name)
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
    state.files.save(&to, &source)?;
    state.files.delete(&from)?;
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
/// Runs a draft over recent messages without starting it; see [`preview::preview`].
#[tauri::command]
pub async fn preview_pipeline(
    state: State<'_, Processing>,
    source: String,
) -> Result<Preview, String> {
    let names = state
        .statuses
        .snapshot()
        .into_iter()
        .map(|s| (s.connection_id, s.name))
        .collect();
    Ok(preview::preview(&source, &names, &state.recent))
}

#[tauri::command]
pub fn pipelines_folder(state: State<'_, Processing>) -> String {
    state.files.dir().display().to_string()
}
