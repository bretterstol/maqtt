mod bus;
mod connection;
mod dev_profiles;
mod export;
mod history;
mod message;
mod mqtt;
mod processing;
mod profile;
mod profile_store;
mod publish;
mod topics;
mod ui_bridge;

use std::sync::{Arc, Mutex};

use tauri::async_runtime::JoinHandle;
use tauri::ipc::Channel;
use tauri::{Manager, State};
use tokio::sync::watch;

use bus::Bus;
use connection::{ConnectionManager, ConnectionStatuses};
use history::{HistoryLimits, HistoryStore, LogFilter, LogPage, LogQuery};
use message::ConnectionId;
use profile::{Profile, ProfileId, Subscription};
use profile_store::{ImportReport, KeyringSecrets, PasswordChange, ProfileStore, ProfileView};
use publish::{OutgoingMessage, PublishHistory, PublishRecord};
use ui_bridge::{LiveFilter, LogEntry, LogRow, MessageDetail, TopicUpdate};

/// Upper bound for one `query_log` page, so a careless caller can't serialize the whole store.
const MAX_PAGE: usize = 5_000;

struct AppState {
    bus: Bus,
    statuses: ConnectionStatuses,
    connections: Arc<Mutex<ConnectionManager>>,
    profiles: Mutex<ProfileStore>,
    publish_history: Mutex<PublishHistory>,
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
        state.history.clone(),
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
fn query_log(state: State<'_, AppState>, mut query: LogQuery) -> LogPage<LogRow> {
    query.limit = query.limit.min(MAX_PAGE);
    let page = state.history.lock().unwrap().query(&query);
    page.map(|item| LogRow::from(&item))
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

/// One message in full: payload, metadata and MQTT 5 properties.
#[tauri::command]
fn message_detail(
    state: State<'_, AppState>,
    connection_id: ConnectionId,
    seq: u64,
) -> Option<MessageDetail> {
    let message = state.history.lock().unwrap().get(connection_id, seq)?;
    Some(MessageDetail::from(message.as_ref()))
}

/// Writes every message and marker the filter matches, oldest first, as JSON Lines to `path`;
/// returns how many lines were written.
#[tauri::command]
fn export_log(
    state: State<'_, AppState>,
    filter: LogFilter,
    path: std::path::PathBuf,
) -> Result<usize, String> {
    // Collect under the lock, write without it.
    let items = state
        .history
        .lock()
        .unwrap()
        .query(&LogQuery {
            filter,
            before_seq: None,
            limit: usize::MAX,
        })
        .entries;
    let names = state
        .statuses
        .snapshot()
        .into_iter()
        .map(|s| (s.connection_id, s.name))
        .collect();
    export::write_json_lines(&path, &items, &names)
}

fn home_dir() -> Result<std::path::PathBuf, String> {
    directories::BaseDirs::new()
        .map(|dirs| dirs.home_dir().to_path_buf())
        .ok_or_else(|| "Couldn't find the home directory".into())
}

/// Writes all Broker Profiles, without passwords, to `path`; returns how many.
#[tauri::command]
fn export_profiles(state: State<'_, AppState>, path: std::path::PathBuf) -> Result<usize, String> {
    state
        .profiles
        .lock()
        .unwrap()
        .export_to(&path, &home_dir()?)
}

#[derive(serde::Serialize)]
#[serde(rename_all = "camelCase")]
struct Imported {
    report: ImportReport,
    profiles: Vec<ProfileView>,
}

/// Adds the Profiles from an exported file that aren't here yet.
#[tauri::command]
fn import_profiles(
    state: State<'_, AppState>,
    path: std::path::PathBuf,
) -> Result<Imported, String> {
    let mut profiles = state.profiles.lock().unwrap();
    let report = profiles.import_from(&path, &home_dir()?)?;
    Ok(Imported {
        report,
        profiles: profiles.list(),
    })
}

/// Every topic in the Topic Tree, for a frontend starting from scratch.
#[tauri::command]
fn topic_snapshot(state: State<'_, AppState>) -> Vec<TopicUpdate> {
    let history = state.history.lock().unwrap();
    history
        .topics
        .snapshot()
        .into_iter()
        .map(TopicUpdate::from)
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
    connect_profile(&state, &profile_id)
}

fn connect_profile(state: &AppState, profile_id: &ProfileId) -> Result<(), String> {
    let (profile, password) = {
        let profiles = state.profiles.lock().unwrap();
        let profile = profiles.get(profile_id).cloned().ok_or("No such Profile")?;
        let password = profiles.password(profile_id)?;
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

/// `MAQTT_AUTOCONNECT=all` or a comma-separated list of Profile names
/// connects those Profiles at startup.
fn autoconnect(state: &AppState) {
    let Ok(wanted) = std::env::var("MAQTT_AUTOCONNECT") else {
        return;
    };
    let wanted: Vec<&str> = wanted.split(',').map(str::trim).collect();
    let profiles = state.profiles.lock().unwrap().list();
    for view in profiles {
        if (wanted.contains(&"all") || wanted.contains(&view.profile.name.as_str()))
            && let Err(err) = connect_profile(state, &view.profile.id)
        {
            eprintln!("maqtt: autoconnect {} failed: {err}", view.profile.name);
        }
    }
}

/// How long a publish may wait for room in the Connection's request queue.
const PUBLISH_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);

async fn send(
    state: &AppState,
    profile_id: &ProfileId,
    message: &OutgoingMessage,
) -> Result<(), String> {
    message.validate()?;
    let client = {
        let connections = state.connections.lock().unwrap();
        connections
            .get(profile_id)
            .ok_or("Not connected")?
            .publisher()?
    };
    tokio::time::timeout(PUBLISH_TIMEOUT, client.publish(message))
        .await
        .map_err(|_| "The Connection is too busy to send right now".to_string())?
}

/// Publishes a message on the Profile's Connection and adds it to the Publish History.
#[tauri::command]
async fn publish(
    state: State<'_, AppState>,
    profile_id: ProfileId,
    message: OutgoingMessage,
) -> Result<Vec<PublishRecord>, String> {
    send(&state, &profile_id, &message).await?;
    let mut history = state.publish_history.lock().unwrap();
    history.record(profile_id, message)?;
    Ok(history.list())
}

/// Clears the Broker's Retained Message for a topic by publishing an empty retained message.
#[tauri::command]
async fn clear_retained(
    state: State<'_, AppState>,
    profile_id: ProfileId,
    topic: String,
) -> Result<(), String> {
    let clear = OutgoingMessage {
        topic,
        payload: String::new(),
        qos: 1,
        retain: true,
        properties: None,
    };
    send(&state, &profile_id, &clear).await
}

#[tauri::command]
fn publish_history(state: State<'_, AppState>) -> Vec<PublishRecord> {
    state.publish_history.lock().unwrap().list()
}

#[tauri::command]
fn clear_publish_history(state: State<'_, AppState>) -> Result<(), String> {
    state.publish_history.lock().unwrap().clear()
}

/// Ends the backoff wait of a disconnected Profile and tries again now.
#[tauri::command]
fn retry_now(state: State<'_, AppState>, profile_id: ProfileId) {
    if let Some(connection) = state.connections.lock().unwrap().get(&profile_id) {
        connection.retry_now();
    }
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

/// After the computer sleeps, connections can look alive until their keep-alive times out.
/// Notice the sleep and make every Connection reconnect straight away instead.
fn spawn_sleep_watchdog(app: tauri::AppHandle) {
    const TICK: std::time::Duration = std::time::Duration::from_secs(5);
    tauri::async_runtime::spawn(async move {
        let mut monotonic = std::time::Instant::now();
        let mut wall = std::time::SystemTime::now();
        loop {
            tokio::time::sleep(TICK).await;
            let (now_monotonic, now_wall) =
                (std::time::Instant::now(), std::time::SystemTime::now());
            let wall_elapsed = now_wall.duration_since(wall).unwrap_or_default();
            if connection::slept(now_monotonic - monotonic, wall_elapsed) {
                eprintln!("maqtt: woke from sleep, reconnecting");
                app.state::<AppState>()
                    .connections
                    .lock()
                    .unwrap()
                    .reset_all();
            }
            (monotonic, wall) = (now_monotonic, now_wall);
        }
    });
}

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    let profiles = load_profiles().unwrap_or_else(|err| panic!("{err}"));
    let sent = PublishHistory::load(
        ProfileStore::default_path()
            .expect("config directory")
            .with_file_name("publish-history.json"),
    );
    let bus = Bus::new();
    let statuses = ConnectionStatuses::default();
    let processing_bus = bus.clone();
    let pipelines_dir = ProfileStore::default_path()
        .expect("config directory")
        .with_file_name("pipelines");
    let pipeline_files_dir = pipelines_dir.with_file_name("files");
    let history = Arc::new(Mutex::new(HistoryStore::new(HistoryLimits::default())));
    let (live_filter, _) = watch::channel(LiveFilter::default());
    let connections = Arc::new(Mutex::new(ConnectionManager::default()));
    // `to broker` sends on a Connection by name, as `from` reads one.
    let republish: processing::runner::Republish = {
        let (connections, statuses) = (connections.clone(), statuses.clone());
        Arc::new(move |name, topic, payload| {
            connections
                .lock()
                .unwrap()
                .try_publish_by_name(&statuses, name, topic, payload)
        })
    };
    let recent_messages: processing::preview::RecentMessages = {
        let history = history.clone();
        Arc::new(move |per_connection| history.lock().unwrap().newest(per_connection))
    };

    tauri::Builder::default()
        .plugin(tauri_plugin_dialog::init())
        .plugin(tauri_plugin_clipboard_manager::init())
        .setup({
            let bus = bus.clone();
            let history = history.clone();
            move |app| {
                history::spawn_recorder(&bus, history);
                autoconnect(&app.state::<AppState>());
                spawn_sleep_watchdog(app.handle().clone());
                Ok(())
            }
        })
        .manage(AppState {
            bus,
            statuses: statuses.clone(),
            connections: connections.clone(),
            profiles: Mutex::new(profiles),
            publish_history: Mutex::new(sent),
            history,
            live_filter,
            ui_bridge: Mutex::new(None),
        })
        // Pipelines (ADR 0004): this `manage` and the `processing::` commands below are the
        // whole of the feature's footprint here.
        .manage(processing::Processing::new(
            processing_bus,
            statuses,
            pipelines_dir,
            recent_messages,
            republish,
            pipeline_files_dir,
        ))
        .invoke_handler(tauri::generate_handler![
            stream_messages,
            set_log_filter,
            query_log,
            topic_history,
            topic_snapshot,
            message_detail,
            export_log,
            export_profiles,
            import_profiles,
            list_profiles,
            new_profile,
            save_profile,
            delete_profile,
            connect,
            retry_now,
            disconnect,
            publish,
            clear_retained,
            publish_history,
            clear_publish_history,
            processing::list_pipelines,
            processing::save_pipeline,
            processing::delete_pipeline,
            processing::rename_pipeline,
            processing::start_pipeline,
            processing::stop_pipeline,
            processing::preview_pipeline,
            processing::pipelines_folder,
            processing::pipeline_files_folder,
            processing::list_pipeline_files,
            processing::tail_pipeline_file,
            subscribe,
            unsubscribe
        ])
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}
