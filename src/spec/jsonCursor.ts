// Where things are in a JSON payload while it's being typed: what the cursor is completing (a
// key, a value) and at which path, and where each value sits, to underline a problem with it.
// A scanner rather than a parser, so half-written JSON reads as far as it goes.

export type Path = (string | number)[];

export interface CursorContext {
  /** A key of an object, a value, or neither (between a key and its colon, say). */
  kind: "key" | "value" | "none";
  /** For a key: the object's path. For a value: the value's own path. */
  path: Path;
  /** Where the text being completed starts: its opening quote, or its first character. */
  from: number;
  /** The keys the object already has, for a key. */
  keys: string[];
}

export interface Range {
  from: number;
  to: number;
}

type Frame =
  | {
      kind: "object";
      key: string | null;
      keyRange: Range | null;
      expecting: "key" | "colon" | "value" | "comma";
      keys: string[];
      start: number;
    }
  | {
      kind: "array";
      index: number;
      expecting: "value" | "comma";
      start: number;
    };

const WORD_END = /[\s,:\]}[{"]/;

/** The step a container frame adds to the path: its current key or index. */
const step = (f: Frame): Path =>
  f.kind === "object" ? (f.key === null ? [] : [f.key]) : [f.index];

interface Scan {
  /** Set when stopping at a position. */
  cursor: CursorContext | null;
  /** Each value's range by path, and its key's range when it's a property. */
  ranges: Map<string, { value: Range; key: Range | null }>;
}

export const pathKey = (path: Path) => JSON.stringify(path);

function scan(text: string, stopAt: number | null): Scan {
  const stack: Frame[] = [];
  const ranges: Scan["ranges"] = new Map();
  const pathOf = (): Path => stack.flatMap(step);
  const top = () => stack[stack.length - 1];
  // A value is about to start at `from`: the path it has.
  const valuePath = () => pathOf();
  const keyRangeOf = () => {
    const t = top();
    return t?.kind === "object" ? t.keyRange : null;
  };
  const valueDone = (path: Path, value: Range, key: Range | null) => {
    ranges.set(pathKey(path), { value, key });
    const t = top();
    if (t) t.expecting = "comma";
  };
  const expectsValue = () => {
    const t = top();
    return !t || t.expecting === "value";
  };
  const cursorAt = (
    kind: CursorContext["kind"],
    from: number,
  ): CursorContext => {
    const t = top();
    if (kind === "key" && t?.kind === "object") {
      const objectPath = stack.slice(0, -1).flatMap(step);
      return { kind, path: objectPath, from, keys: [...t.keys] };
    }
    return { kind, path: kind === "value" ? pathOf() : [], from, keys: [] };
  };
  const stop = (at: number) => stopAt !== null && at >= stopAt;

  // Containers open here, by path, so their range ends at the closing bracket.
  const opened: { path: Path; start: number; key: Range | null }[] = [];

  let i = 0;
  while (i < text.length) {
    if (stop(i)) {
      const t = top();
      const kind =
        t?.kind === "object" && t.expecting === "key"
          ? "key"
          : expectsValue()
            ? "value"
            : "none";
      return { cursor: cursorAt(kind, i), ranges };
    }
    const c = text[i];
    if (/\s/.test(c)) {
      i++;
      continue;
    }
    const t = top();
    if (c === '"') {
      let j = i + 1;
      while (j < text.length && text[j] !== '"') j += text[j] === "\\" ? 2 : 1;
      const closed = j < text.length;
      const end = closed ? j + 1 : text.length;
      if (stopAt !== null && stopAt > i && stopAt <= (closed ? j : end)) {
        // The cursor is inside this string.
        const kind =
          t?.kind === "object" && t.expecting === "key"
            ? "key"
            : expectsValue()
              ? "value"
              : "none";
        return { cursor: cursorAt(kind, i), ranges };
      }
      let value: string;
      try {
        value = JSON.parse(text.slice(i, end)) as string;
      } catch {
        value = text.slice(i + 1, closed ? j : end);
      }
      if (t?.kind === "object" && t.expecting === "key") {
        t.key = value;
        t.keyRange = { from: i, to: end };
        t.keys.push(value);
        t.expecting = "colon";
      } else {
        valueDone(valuePath(), { from: i, to: end }, keyRangeOf());
      }
      i = end;
      continue;
    }
    if (c === ":") {
      if (t?.kind === "object") t.expecting = "value";
      i++;
      continue;
    }
    if (c === ",") {
      if (t?.kind === "object") {
        t.expecting = "key";
        t.key = null;
        t.keyRange = null;
      } else if (t?.kind === "array") {
        t.index++;
        t.expecting = "value";
      }
      i++;
      continue;
    }
    if (c === "{" || c === "[") {
      opened.push({ path: valuePath(), start: i, key: keyRangeOf() });
      stack.push(
        c === "{"
          ? {
              kind: "object",
              key: null,
              keyRange: null,
              expecting: "key",
              keys: [],
              start: i,
            }
          : { kind: "array", index: 0, expecting: "value", start: i },
      );
      i++;
      continue;
    }
    if (c === "}" || c === "]") {
      stack.pop();
      const open = opened.pop();
      if (open) valueDone(open.path, { from: open.start, to: i + 1 }, open.key);
      i++;
      continue;
    }
    // A number, true, false or null.
    let j = i;
    while (j < text.length && !WORD_END.test(text[j])) j++;
    if (stopAt !== null && stopAt > i && stopAt <= j) {
      return { cursor: cursorAt("value", i), ranges };
    }
    if (expectsValue()) {
      valueDone(valuePath(), { from: i, to: j }, keyRangeOf());
    }
    i = j;
  }
  if (stopAt !== null) {
    const t = top();
    const kind =
      t?.kind === "object" && t.expecting === "key"
        ? "key"
        : expectsValue()
          ? "value"
          : "none";
    return { cursor: cursorAt(kind, text.length), ranges };
  }
  return { cursor: null, ranges };
}

/** What the cursor at `pos` is completing. */
export function cursorContext(text: string, pos: number): CursorContext {
  return scan(text, pos).cursor!;
}

/**
 * Where to underline a problem at `path`: a property's key for an object or array (its whole
 * body would be too much), otherwise the value. Falls back to the nearest enclosing value.
 */
export function rangeOf(text: string, path: Path): Range {
  const { ranges } = scan(text, null);
  for (let n = path.length; n >= 0; n--) {
    const found = ranges.get(pathKey(path.slice(0, n)));
    if (!found) continue;
    const container = /^[{[]/.test(text.slice(found.value.from));
    if (container) {
      return found.key ?? { from: found.value.from, to: found.value.from + 1 };
    }
    return found.value;
  }
  return { from: 0, to: Math.min(1, text.length) };
}
