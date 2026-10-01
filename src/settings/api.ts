// Settings (src-tauri/src/settings.rs): how måqtt behaves, in settings.toml beside the Profiles.

import { invoke } from "@tauri-apps/api/core";
import type { ExpandMode } from "../topicTree/store";

export interface Settings {
  pipelines: {
    /** The Pipelines tab and Topic Tree branch are shown, and Pipelines may run. */
    enabled: boolean;
  };
  history: {
    /** Messages kept per Connection, oldest dropped first. */
    perConnection: number;
  };
  messageLog: {
    /** The Message Log tab is shown. */
    shown: boolean;
    /** Messages kept while following live traffic. */
    live: number;
    /** How far back the Message Log can be scrolled, in messages. */
    scrollback: number;
  };
  subscriptions: {
    /** The QoS new Subscriptions start with. */
    defaultQos: number;
  };
  appearance: {
    theme: "system" | "light" | "dark";
  };
  topicTree: {
    /** How the Topic Tree opens itself. */
    opens: ExpandMode;
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
