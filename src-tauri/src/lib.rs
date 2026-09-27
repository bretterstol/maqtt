mod bus;
mod connection;
mod dev_profiles;
mod history;
mod message;
mod mqtt;
mod profile;
mod profile_store;
mod ui_bridge;

use std::sync::{Arc, Mutex};

use tauri::State;
use tauri::async_runtime::JoinHandle;
use tauri::ipc::Channel;
use tokio::sync::watch;

use bus::Bus;
use connection::{ConnectionManager, ConnectionStatuses};
use history::{HistoryLimits, HistoryStore, LogFilter, LogPage, LogQuery};
use message::ConnectionId;
use profile::{Profile, ProfileId, Subscription};
use profile_store::{KeyringSecrets, PasswordChange, ProfileStore, ProfileView};
use ui_bridge::{LiveFilter, LogEntry};

/// Upper bound for one `query_log` page, so a careless caller can't serialize the whole store.
const MAX_PAGE: usize = 5_000;

struct AppState {
    bus: Bus,
    statuses: ConnectionStatuses,
    connections: Mutex<ConnectionManager>,
    profiles: Mutex<ProfileStore>,
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
        &state.statuses,
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

#[tauri::command]
fn list_profiles(state: State<'_, AppState>) -> Vec<ProfileView> {
    state.profiles.lock().unwrap().list()
}

/// A new, unsaved Profile with defaults filled in.
#[tauri::command]
fn new_profile() -> Profile {
    Profile::new("New Broker", "localhost", 1883)
}

#[tauri::command]
fn save_profile(
    state: State<'_, AppState>,
    profile: Profile,
    password: PasswordChange,
) -> Result<Vec<ProfileView>, String> {
    let mut profiles = state.profiles.lock().unwrap();
    profiles.save(profile, password)?;
    Ok(profiles.list())
}

#[tauri::command]
fn delete_profile(
    state: State<'_, AppState>,
    profile_id: ProfileId,
) -> Result<Vec<ProfileView>, String> {
    state.connections.lock().unwrap().disconnect(&profile_id);
    let mut profiles = state.profiles.lock().unwrap();
    profiles.delete(&profile_id)?;
    Ok(profiles.list())
}

#[tauri::command]
fn connect(state: State<'_, AppState>, profile_id: ProfileId) -> Result<(), String> {
    let (profile, password) = {
        let profiles = state.profiles.lock().unwrap();
        let profile = profiles
            .get(&profile_id)
            .cloned()
            .ok_or("No such Profile")?;
        let password = profiles.password(&profile_id)?;
        (profile, password)
    };
    let id = state.connections.lock().unwrap().connect(
        &profile,
        password.as_deref(),
        &state.bus,
        &state.statuses,
    )?;
    state
        .history
        .lock()
        .unwrap()
        .set_per_topic_limit(id, profile.history_per_topic);
    Ok(())
}

#[tauri::command]
fn disconnect(state: State<'_, AppState>, profile_id: ProfileId) {
    state.connections.lock().unwrap().disconnect(&profile_id);
}

/// Subscribes the Profile's live Connection, for this session only.
#[tauri::command]
fn subscribe(
    state: State<'_, AppState>,
    profile_id: ProfileId,
    subscription: Subscription,
) -> Result<(), String> {
    let connections = state.connections.lock().unwrap();
    let connection = connections.get(&profile_id).ok_or("Not connected")?;
    connection.subscribe(subscription)
}

/// Unsubscribes the Profile's live Connection, for this session only.
#[tauri::command]
fn unsubscribe(
    state: State<'_, AppState>,
    profile_id: ProfileId,
    filter: String,
) -> Result<(), String> {
    let connections = state.connections.lock().unwrap();
    let connection = connections.get(&profile_id).ok_or("Not connected")?;
    connection.unsubscribe(&filter);
    Ok(())
}

fn load_profiles() -> Result<ProfileStore, String> {
    let mut store = ProfileStore::load(ProfileStore::default_path()?, Box::new(KeyringSecrets))?;
    if cfg!(debug_assertions) && !store.exists() {
        dev_profiles::seed(&mut store)?;
    }
    Ok(store)
}

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    let profiles = load_profiles().unwrap_or_else(|err| panic!("{err}"));
    let bus = Bus::new();
    let history = Arc::new(Mutex::new(HistoryStore::new(HistoryLimits::default())));
    let (live_filter, _) = watch::channel(LiveFilter::default());

    tauri::Builder::default()
        .plugin(tauri_plugin_dialog::init())
        .setup({
            let bus = bus.clone();
            let history = history.clone();
            move |_| {
                history::spawn_recorder(&bus, history);
                Ok(())
            }
        })
        .manage(AppState {
            bus,
            statuses: ConnectionStatuses::default(),
            connections: Mutex::new(ConnectionManager::default()),
            profiles: Mutex::new(profiles),
            history,
            live_filter,
            ui_bridge: Mutex::new(None),
        })
        .invoke_handler(tauri::generate_handler![
            stream_messages,
            set_log_filter,
            query_log,
            topic_history,
            list_profiles,
            new_profile,
            save_profile,
            delete_profile,
            connect,
            disconnect,
            subscribe,
            unsubscribe
        ])
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}
