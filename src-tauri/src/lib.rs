mod bus;
mod connection;
mod message;
mod ui_bridge;

use std::sync::Mutex;

use tauri::State;
use tauri::async_runtime::JoinHandle;
use tauri::ipc::Channel;

use bus::Bus;
use connection::{ConnectionSettings, Connections};
use message::ConnectionId;

struct AppState {
    bus: Bus,
    connections: Connections,
    /// Only one UI Bridge at a time; a reloaded frontend replaces the previous stream.
    ui_bridge: Mutex<Option<JoinHandle<()>>>,
}

/// Starts streaming Message Bus traffic to the frontend over `on_batch`.
#[tauri::command]
fn stream_messages(state: State<'_, AppState>, on_batch: Channel<ui_bridge::Batch>) {
    let handle = ui_bridge::spawn(&state.bus, &state.connections, on_batch);
    if let Some(previous) = state.ui_bridge.lock().unwrap().replace(handle) {
        previous.abort();
    }
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

    tauri::Builder::default()
        .setup({
            let bus = bus.clone();
            let connections = connections.clone();
            move |_| {
                connection::spawn(ConnectionId(1), dev_broker(), bus, connections);
                Ok(())
            }
        })
        .manage(AppState {
            bus,
            connections,
            ui_bridge: Mutex::new(None),
        })
        .invoke_handler(tauri::generate_handler![stream_messages])
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}
