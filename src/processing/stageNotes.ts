import type { Preview } from "./api";
import { byteToIndex } from "./offsets";

/** A note shown after a Stage in the editor: what it did in the Preview. */
export interface StageNote {
  /** Index in the text (UTF-16) where the Stage ends. */
  at: number;
  text: string;
  /** The full text, when `text` was shortened. */
  title: string;
  error: boolean;
}

const MAX_VALUE = 60;

function shorten(value: string): string {
  return value.length > MAX_VALUE ? `${value.slice(0, MAX_VALUE - 1)}…` : value;
}

/** Errors read "map on <topic>: <why>"; the note sits on the Stage's line already, so keep the why. */
function reason(error: string): string {
  const match = /^[a-z ]+ on .*?: (.*)$/s.exec(error);
  return match ? match[1] : error;
}

/** One note per line of the Pipeline: the sample size after `from`, then each Stage's result. */
export function stageNotes(text: string, preview: Preview): StageNote[] {
  if (preview.problems.length > 0) return [];
  return preview.ends.map((end, i) => {
    const at = byteToIndex(text, end);
    if (i === 0) {
      const text =
        preview.messages === 0
          ? "no recent messages match yet"
          : `${preview.messages.toLocaleString()} recent message${preview.messages === 1 ? "" : "s"}`;
      return { at, text, title: text, error: false };
    }
    const stage = preview.stages[i - 1];
    if (!stage) return { at, text: "", title: "", error: false };
    if (stage.errors > 0) {
      const count = `${stage.errors.toLocaleString()} error${stage.errors === 1 ? "" : "s"}`;
      return {
        at,
        text: shorten(`${count}: ${reason(stage.firstError ?? "")}`),
        title: `${count}, the first: ${stage.firstError ?? ""}`,
        error: true,
      };
    }
    const full =
      stage.passed === 0
        ? "0"
        : `${stage.passed.toLocaleString()}  ${stage.last ?? ""}`;
    return { at, text: shorten(full), title: full, error: false };
  });
}
