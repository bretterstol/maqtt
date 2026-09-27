mod bus;
mod connection;
mod history;
mod message;
mod ui_bridge;

use std::sync::{Arc, Mutex};

use tauri::State;
use tauri::async_runtime::JoinHandle;
use tauri::ipc::Channel;
use tokio::sync::watch;

use bus::Bus;
use connection::{ConnectionSettings, Connections};
use history::{HistoryLimits, HistoryStore, LogFilter, LogPage, LogQuery};
use message::ConnectionId;
use ui_bridge::{LiveFilter, LogEntry};

/// Upper bound for one `query_log` page, so a careless caller can't serialize the whole store.
const MAX_PAGE: usize = 5_000;

struct AppState {
    bus: Bus,
    connections: Connections,
    history: Arc<Mutex<HistoryStore>>,
    live_filter: watch::Sender<LiveFilter>,
    /// Only one UI Bridge at a time; a reloaded frontend replaces the previous stream.
    ui_bridge: Mutex<Option<JoinHandle<()>>>,
}

/// Starts streaming Message Bus traffic to the frontend over `on_batch`, with the Message Log
/// filter reset; returns that filter's generation.
#[tauri::command]
fn stream_messages(state: State<'_, AppState>, on_batch: Channel<ui_bridge::Batch>) -> u64 {
    let generation = replace_live_filter(&state, LogFilter::default());
    let handle = ui_bridge::spawn(
        &state.bus,
        &state.connections,
        state.live_filter.subscribe(),
        on_batch,
    );
    if let Some(previous) = state.ui_bridge.lock().unwrap().replace(handle) {
        previous.abort();
    }
    generation
}

/// Sets the filter for the live Message Log stream; returns its generation.
#[tauri::command]
fn set_log_filter(state: State<'_, AppState>, filter: LogFilter) -> u64 {
    replace_live_filter(&state, filter)
}

fn replace_live_filter(state: &AppState, filter: LogFilter) -> u64 {
    let mut generation = 0;
    state.live_filter.send_modify(|live| {
        live.generation += 1;
        live.filter = filter;
        generation = live.generation;
    });
    generation
}

#[tauri::command]
fn query_log(state: State<'_, AppState>, mut query: LogQuery) -> LogPage<LogEntry> {
    query.limit = query.limit.min(MAX_PAGE);
    let page = state.history.lock().unwrap().query(&query);
    page.map(|m| LogEntry::from(m.as_ref()))
}

#[tauri::command]
fn topic_history(
    state: State<'_, AppState>,
    connection_id: ConnectionId,
    topic: String,
) -> Vec<LogEntry> {
    let messages = state
        .history
        .lock()
        .unwrap()
        .topic_history(connection_id, &topic);
    messages
        .iter()
        .map(|m| LogEntry::from(m.as_ref()))
        .collect()
}

/// Hardcoded until Broker Profiles arrive in M3: the dev Broker from `dev/env.sh`.
fn dev_broker() -> ConnectionSettings {
    ConnectionSettings {
        name: "Dev Broker".into(),
        host: "localhost".into(),
        port: 21883,
        client_id: format!("maqtt-{}", std::process::id()),
        subscriptions: vec!["#".into()],
    }
}

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    let bus = Bus::new();
    let connections = Connections::default();
    let history = Arc::new(Mutex::new(HistoryStore::new(HistoryLimits::default())));
    let (live_filter, _) = watch::channel(LiveFilter::default());

    tauri::Builder::default()
        .setup({
            let bus = bus.clone();
            let connections = connections.clone();
            let history = history.clone();
            move |_| {
                history::spawn_recorder(&bus, history);
                connection::spawn(ConnectionId(1), dev_broker(), bus, connections);
                Ok(())
            }
        })
        .manage(AppState {
            bus,
            connections,
            history,
            live_filter,
            ui_bridge: Mutex::new(None),
        })
        .invoke_handler(tauri::generate_handler![
            stream_messages,
            set_log_filter,
            query_log,
            topic_history
        ])
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}
