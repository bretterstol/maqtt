// Settings (src-tauri/src/settings.rs): how måqtt behaves, in settings.toml beside the Profiles.

import { invoke } from "@tauri-apps/api/core";

export interface Settings {
  pipelines: {
    /** The Pipelines tab and Topic Tree branch are shown, and Pipelines may run. */
    enabled: boolean;
  };
}

export interface SettingsView {
  settings: Settings;
  /** Where settings.toml is. */
  path: string;
  /** Why the file couldn't be read: the defaults are in use, and saving replaces it. */
  problem: string | null;
}

export const getSettings = (): Promise<SettingsView> => invoke("get_settings");

/** Saves and applies new settings. */
export const setSettings = (settings: Settings): Promise<SettingsView> =>
  invoke("set_settings", { settings });
