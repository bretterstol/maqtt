import { BY_STAGES } from "./referenceData";
import { isTop, scan } from "./scanner";

/**
 * What can be written at the cursor in a Pipeline: the logic behind autocomplete, kept apart
 * from CodeMirror so it can be tested on plain text.
 */

export type Assist =
  | { kind: "none" }
  /** The first word of a Pipeline. */
  | { kind: "from"; from: number }
  /** A Stage name; `prefix` goes before it (" " after a bare `|>`, "|> " on a new line). */
  | { kind: "stage"; from: number; prefix: string; alsoFunctions: boolean }
  /** After `from`: `all` or a Broker's name. */
  | { kind: "sources"; from: number; first: boolean }
  /** After `to broker`: a Broker's name. */
  | { kind: "broker"; from: number }
  /** After `from pipeline`: a Pipeline's name. */
  | { kind: "pipelines"; from: number }
  | { kind: "variable"; from: number }
  /** A field of the value entering Stage `stage` (1 = the first after `from`), or of $payload. */
  | {
      kind: "field";
      from: number;
      base: "payload" | "value";
      path: string[];
      stage: number;
    }
  /** Right after `filter`: `topic`, `not topic`, or a condition. */
  | { kind: "filter"; from: number; negated: boolean }
  | { kind: "by"; from: number }
  | { kind: "sink"; from: number }
  /** A jq function name inside an expression; `word` may be empty. */
  | { kind: "function"; from: number; word: string };

/** What can follow `to`: every Sink, with a snippet to insert and what it does. */
export const SINKS: { name: string; snippet: string; detail: string }[] = [
  {
    name: "app",
    snippet: 'app "${topic}"',
    detail: "a Derived Topic in the Topic Tree",
  },
  {
    name: "broker",
    snippet: 'broker "${Broker}" "${topic}"',
    detail: "republish on a connected Broker",
  },
  {
    name: "file",
    snippet: 'file "${name}.jsonl"',
    detail: "append to a JSON Lines or CSV file",
  },
  {
    name: "geo",
    snippet: "geo",
    detail: "points { lat, lon, label } on a map",
  },
  {
    name: "chart",
    snippet: "chart",
    detail: "numbers, or { value, series }, as lines over time",
  },
];

type Mode = "code" | "object" | "string" | "fill" | "comment";

interface Scan {
  /** Modes still open at the end, innermost last. */
  modes: Mode[];
  /** Top-level `|>` seen. */
  pipes: number;
  /** Where the text after the last top-level `|>` starts. */
  segmentStart: number;
}

/** Where the text up to the cursor stands, by the shared scanner's rules (scanner.ts). */
function scanBefore(text: string): Scan {
  const { pieces, open } = scan(text);
  let pipes = 0;
  let segmentStart = 0;
  for (const piece of pieces.filter(isTop)) {
    const code = text.slice(piece.start, piece.end);
    for (
      let at = code.indexOf("|>");
      at !== -1;
      at = code.indexOf("|>", at + 2)
    ) {
      pipes++;
      segmentStart = piece.start + at + 2;
    }
  }
  const modes: Mode[] = [
    "code",
    ...open.map((o): Mode =>
      o.kind === "string" ? "string" : o.kind === "fill" ? "fill" : "object",
    ),
  ];
  const last = pieces[pieces.length - 1];
  if (last?.kind === "comment" && last.end === text.length)
    modes.push("comment");
  return { modes, pipes, segmentStart };
}

const WORD = /[A-Za-z_]\w*$/;

/** What fits at the end of `before` (the text up to the cursor). */
export function assistAt(before: string): Assist {
  const pos = before.length;
  const { modes, pipes, segmentStart } = scanBefore(before);
  const top = modes[modes.length - 1];
  if (pipes > 0 && modes.length <= 2) {
    const name = /^to\s+broker\s+("[^"]*)?$/.exec(
      before.slice(segmentStart).trimStart(),
    );
    if (name) return { kind: "broker", from: pos - (name[1]?.length ?? 0) };
  }
  // A string is no place for suggestions, except a Broker's name after `from`.
  const naming = top === "string" && modes.length === 2 && pipes === 0;
  if (top === "comment" || (top === "string" && !naming)) {
    return { kind: "none" };
  }

  const variable = /\$\w*$/.exec(before);
  if (variable) return { kind: "variable", from: variable.index };

  const field = /(\$payload)?((?:\.[A-Za-z_]\w*)*)\.([A-Za-z_]\w*)?$/.exec(
    before,
  );
  if (field && !/[\w)\]"]/.test(before[field.index - 1] ?? "")) {
    return {
      kind: "field",
      from: pos - (field[3]?.length ?? 0),
      base: field[1] ? "payload" : "value",
      path: field[2].split(".").filter(Boolean),
      stage: pipes,
    };
  }

  const word = WORD.exec(before)?.[0] ?? "";
  const from = pos - word.length;
  const fn: Assist = { kind: "function", from, word };
  if (modes.length > 1 && !naming) return fn;

  const segment = before.slice(segmentStart);
  const trimmed = segment.trimStart();
  if (pipes === 0) {
    // Comments often come before `from`.
    const code = trimmed.replace(/#[^\n]*/g, "").trimStart();
    if (/^\w*$/.test(code)) return { kind: "from", from };
    const pipelines = /^from\s+pipelines?\s+(.*)$/s.exec(code);
    if (pipelines) {
      const token = pipelines[1].split(",").pop()!.trimStart();
      return { kind: "pipelines", from: pos - token.length };
    }
    const sources = /^from\s+(.*)$/s.exec(code);
    if (sources) {
      const token = sources[1].split(",").pop()!.trimStart();
      return {
        kind: "sources",
        from: pos - token.length,
        first: !sources[1].includes(","),
      };
    }
    return { kind: "none" };
  }

  if (/^\w*$/.test(trimmed)) {
    return {
      kind: "stage",
      from,
      prefix: segment === "" ? " " : "",
      alsoFunctions: false,
    };
  }
  // A word alone on a new line after a Stage: most likely the start of the next one.
  const line = before.slice(before.lastIndexOf("\n") + 1);
  if (/^\s*[A-Za-z_]\w*$/.test(line) && trimmed.includes("\n")) {
    return { kind: "stage", from, prefix: "|> ", alsoFunctions: true };
  }

  const filter = /^filter\s+(not\s+)?\w*$/.exec(trimmed);
  if (filter) return { kind: "filter", from, negated: Boolean(filter[1]) };
  if (takesByHere(trimmed)) return { kind: "by", from };
  if (/^to\s+\w*$/.test(trimmed)) return { kind: "sink", from };
  if (/^throttle\s+\S*$/.test(trimmed)) return { kind: "none" };
  return fn;
}

/** The field names at `path` inside a JSON value, or none. */
export function fieldsAt(json: string | null, path: string[]): string[] {
  if (json === null) return [];
  let value: unknown;
  try {
    value = JSON.parse(json);
  } catch {
    return [];
  }
  for (const key of path) {
    if (value === null || typeof value !== "object" || Array.isArray(value)) {
      return [];
    }
    value = (value as Record<string, unknown>)[key];
  }
  return value !== null && typeof value === "object" && !Array.isArray(value)
    ? Object.keys(value)
    : [];
}

/** A named wildcard in a `filter topic`, such as `+room`, which is `$room` from there on. */
export interface WildcardName {
  name: string;
  /** As written in the filter: `+room` or `#rest`. */
  written: string;
  filter: string;
}

/** The named wildcards the `filter topic` Stages in `text` declare, each once, in order. */
export function wildcardNames(text: string): WildcardName[] {
  const found: WildcardName[] = [];
  const stage = /filter\s+topic\s+("[^"]*"(?:\s*,\s*"[^"]*")*)/g;
  for (const [, list] of text.matchAll(stage)) {
    for (const [, filter] of list.matchAll(/"([^"]*)"/g)) {
      for (const level of filter.split("/")) {
        const named = /^[+#]([A-Za-z_]\w*)$/.exec(level);
        if (named && !found.some((w) => w.name === named[1])) {
          found.push({ name: named[1], written: level, filter });
        }
      }
    }
  }
  return found;
}

/**
 * Whether `by` fits next: after a Stage that takes it, once its arguments are written
 * (`throttle 5s `, `scan 0 (.+1) `). An argument is taken to be one word; jq with spaces in
 * it can hide a `by`, which is only a missed suggestion.
 */
function takesByHere(segment: string): boolean {
  const words = segment.split(/\s+/);
  const args = BY_STAGES.get(words[0]);
  return (
    args !== undefined &&
    words.length === args + 2 &&
    /^\w*$/.test(words[words.length - 1])
  );
}
