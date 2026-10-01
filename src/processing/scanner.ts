/**
 * The editor's copy of the backend's scanner (src-tauri/src/processing/scanner.rs): how a
 * Pipeline's text divides into code, strings, `{…}` fills, comments. Both run
 * scanCases.json, so highlighting and suggestions follow the parser's own rules.
 *
 * The rules:
 * - `"` opens a string in code, and closes it inside one.
 * - In a string, `{…}` is a fill (code, which may hold strings of its own), as is jq's own
 *   `\(…)`. `{{` and `}}` are literal braces. `\` escapes the next character.
 * - In code, `( [ {` and `) ] }` nest; a bracket belongs to the level outside it.
 * - `#` in code (fills included) starts a comment, to the end of the line.
 * - Any text scans; what is still open at the end is reported, not fixed.
 *
 * Offsets are string indices (UTF-16), where the backend's are bytes.
 */

export type Kind =
  "code" | "comment" | "quote" | "text" | "escape" | "brace" | "fill" | "end";

export interface Piece {
  kind: Kind;
  start: number;
  end: number;
  /** How many strings it's inside: 0 is the Pipeline's own code. */
  strings: number;
  /** Bracket depth in its code; a bracket has the depth outside it. */
  depth: number;
}

export type Open =
  | { kind: "string"; at: number }
  | { kind: "fill"; at: number }
  | { kind: "bracket"; at: number };

type Context =
  { kind: "code"; depth: number; closes: string | null } | { kind: "string" };

/** Where a scan is, so it can carry on later: the highlighter keeps one per line. */
export interface ScanState {
  stack: Context[];
  open: Open[];
}

export const startState = (): ScanState => ({
  stack: [{ kind: "code", depth: 0, closes: null }],
  open: [],
});

export const copyState = (state: ScanState): ScanState => ({
  stack: state.stack.map((c) => ({ ...c })),
  open: state.open.map((o) => ({ ...o })),
});

/** Code at the Pipeline's top level: outside every string and bracket. */
export const isTop = (piece: Piece): boolean =>
  piece.kind === "code" && piece.strings === 0 && piece.depth === 0;

const BRACKET_OPEN = "([{";
const BRACKET_CLOSE = ")]}";

/**
 * Reads the one piece starting at `text[i]` (a character, or an escape, a `{{`, a `\(`, or a
 * comment to the end of the line), updating `state`.
 */
export function next(state: ScanState, text: string, i: number): Piece {
  const { stack, open } = state;
  const c = text[i];
  const after = text[i + 1];
  const strings = stack.filter((s) => s.kind === "string").length;
  const piece = (kind: Kind, end: number, s = strings, depth = 0): Piece => ({
    kind,
    start: i,
    end,
    strings: s,
    depth,
  });
  const top = stack[stack.length - 1];

  if (top.kind === "string") {
    if (c === '"') {
      stack.pop();
      open.pop();
      return piece("quote", i + 1);
    }
    if (c === "\\" && after === "(") {
      stack.push({ kind: "code", depth: 0, closes: ")" });
      open.push({ kind: "fill", at: i });
      return piece("fill", i + 2);
    }
    if (c === "\\") return piece("escape", Math.min(i + 2, text.length));
    if ((c === "{" || c === "}") && after === c) return piece("brace", i + 2);
    if (c === "{") {
      stack.push({ kind: "code", depth: 0, closes: "}" });
      open.push({ kind: "fill", at: i });
      return piece("fill", i + 1);
    }
    return piece("text", i + 1);
  }

  const { depth, closes } = top;
  if (c === "#") {
    const lineEnd = text.indexOf("\n", i);
    return piece(
      "comment",
      lineEnd === -1 ? text.length : lineEnd,
      strings,
      depth,
    );
  }
  if (c === '"') {
    stack.push({ kind: "string" });
    open.push({ kind: "string", at: i });
    return piece("quote", i + 1, strings + 1);
  }
  if (depth === 0 && closes === c) {
    stack.pop();
    open.pop();
    return piece("end", i + 1);
  }
  if (BRACKET_OPEN.includes(c)) {
    top.depth = depth + 1;
    open.push({ kind: "bracket", at: i });
    return piece("code", i + 1, strings, depth);
  }
  if (BRACKET_CLOSE.includes(c)) {
    const outside = Math.max(0, depth - 1);
    top.depth = outside;
    if (depth > 0) open.pop();
    return piece("code", i + 1, strings, outside);
  }
  return piece("code", i + 1, strings, depth);
}

export interface Scan {
  pieces: Piece[];
  /** What is still open at the end, innermost last. */
  open: Open[];
}

/** Divides `text` into pieces; runs of code and of text with the same nesting are joined. */
export function scan(text: string): Scan {
  const state = startState();
  const pieces: Piece[] = [];
  for (let i = 0; i < text.length;) {
    const p = next(state, text, i);
    const last = pieces[pieces.length - 1];
    if (
      last &&
      (p.kind === "code" || p.kind === "text") &&
      last.kind === p.kind &&
      last.end === p.start &&
      last.strings === p.strings &&
      last.depth === p.depth
    ) {
      last.end = p.end;
    } else {
      pieces.push(p);
    }
    i = p.end;
  }
  return { pieces, open: state.open };
}
