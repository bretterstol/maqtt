import type { ExpandMode } from "./store";

// Before Settings, the Topic Tree remembered how it opens in browser storage. It now lives in
// settings.toml; this hands over a choice made the old way, once.
const KEY = "maqtt.expandMode";
const MODES: ExpandMode[] = ["manual", "groups", "all"];

/** A choice left in browser storage by an older måqtt, removed as it's read; null if none. */
export function takeOldExpandMode(): ExpandMode | null {
  try {
    const saved = localStorage.getItem(KEY);
    if (saved === null) return null;
    localStorage.removeItem(KEY);
    return MODES.find((mode) => mode === saved) ?? null;
  } catch {
    return null;
  }
}
