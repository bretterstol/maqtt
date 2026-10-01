import { createContext, useContext } from "react";
import type { Settings } from "./api";

/** Today's behaviour, for before the settings have loaded. */
export const DEFAULT_SETTINGS: Settings = {
  pipelines: { enabled: true },
  history: { perConnection: 100_000 },
  messageLog: { shown: true, live: 5_000, scrollback: 100_000 },
  subscriptions: { defaultQos: 0 },
  appearance: { theme: "system" },
  topicTree: { opens: "manual" },
};

export const SettingsContext = createContext<Settings>(DEFAULT_SETTINGS);

/** The settings in force, for the parts of the app they shape. */
export const useSettings = (): Settings => useContext(SettingsContext);
