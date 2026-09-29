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

type Mode = "code" | "object" | "string" | "fill" | "comment";

interface Scan {
  /** Modes still open at the end, innermost last. */
  modes: Mode[];
  /** Top-level `|>` seen. */
  pipes: number;
  /** Where the text after the last top-level `|>` starts. */
  segmentStart: number;
}

/** Follows strings, `{…}` fills, objects and comments, as the backend's parser does. */
function scan(text: string): Scan {
  const modes: Mode[] = ["code"];
  let pipes = 0;
  let segmentStart = 0;
  for (let i = 0; i < text.length; i++) {
    const c = text[i];
    const top = modes[modes.length - 1];
    if (top === "comment") {
      if (c === "\n") modes.pop();
      continue;
    }
    if (top === "string") {
      if (c === "\\") i++;
      else if (c === '"') modes.pop();
      else if (c === "{") {
        if (text[i + 1] === "{") i++;
        else modes.push("fill");
      } else if (c === "}" && text[i + 1] === "}") i++;
      continue;
    }
    // Code, an object or a fill.
    if (c === "#" && modes.length === 1) modes.push("comment");
    else if (c === '"') modes.push("string");
    else if (c === "{") modes.push("object");
    else if (c === "}" && modes.length > 1) modes.pop();
    else if (c === "|" && text[i + 1] === ">" && modes.length === 1) {
      pipes++;
      i++;
      segmentStart = i + 1;
    }
  }
  return { modes, pipes, segmentStart };
}

const WORD = /[A-Za-z_]\w*$/;

/** What fits at the end of `before` (the text up to the cursor). */
export function assistAt(before: string): Assist {
  const pos = before.length;
  const { modes, pipes, segmentStart } = scan(before);
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
  if (/^(throttle\s+\S+|changes|delta|pairwise|join)\s+\w*$/.test(trimmed)) {
    return { kind: "by", from };
  }
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
