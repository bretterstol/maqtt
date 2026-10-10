// Settings (src-tauri/src/settings.rs): how mÅqtt behaves, in settings.toml beside the Profiles.

import { invoke } from "@tauri-apps/api/core";
import type { Settings } from "../generated/Settings";
import type { SettingsView } from "../generated/SettingsView";

// Generated from the backend's Rust types by ts-rs, when its tests run.
export type { Settings, SettingsView };

export const getSettings = (): Promise<SettingsView> => invoke("get_settings");

/** Saves and applies new settings. */
export const setSettings = (settings: Settings): Promise<SettingsView> =>
  invoke("set_settings", { settings });
