import type { ExpandMode } from "./store";

const KEY = "maqtt.expandMode";
const MODES: ExpandMode[] = ["manual", "groups", "all"];

/** The expand mode chosen last time, remembered on this machine. */
export function savedExpandMode(): ExpandMode {
  try {
    const saved = localStorage.getItem(KEY);
    return MODES.find((mode) => mode === saved) ?? "manual";
  } catch {
    return "manual";
  }
}

export function saveExpandMode(mode: ExpandMode): void {
  try {
    localStorage.setItem(KEY, mode);
  } catch {
    // Not remembering it is fine; the tree still works.
  }
}
