/** One difference between two JSON values, at a path like `sensors[2].temp`. */
export interface JsonChange {
  path: string;
  kind: "added" | "removed" | "changed";
  before?: unknown;
  after?: unknown;
}

/** Field-by-field differences between two JSON values; empty when they are equal. */
export function jsonDiff(before: unknown, after: unknown): JsonChange[] {
  const changes: JsonChange[] = [];
  const walk = (a: unknown, b: unknown, path: string) => {
    if (
      isContainer(a) &&
      isContainer(b) &&
      Array.isArray(a) === Array.isArray(b)
    ) {
      const keys = Array.isArray(a)
        ? Array.from(
            { length: Math.max(a.length, (b as unknown[]).length) },
            (_, i) => i,
          )
        : [...new Set([...Object.keys(a), ...Object.keys(b)])];
      for (const key of keys) {
        const childPath =
          typeof key === "number" ? `${path}[${key}]` : joinKey(path, key);
        const inA = key in a;
        const inB = key in b;
        const va = (a as Record<string | number, unknown>)[key];
        const vb = (b as Record<string | number, unknown>)[key];
        if (inA && !inB)
          changes.push({ path: childPath, kind: "removed", before: va });
        else if (!inA && inB)
          changes.push({ path: childPath, kind: "added", after: vb });
        else walk(va, vb, childPath);
      }
    } else if (!sameValue(a, b)) {
      changes.push({
        path: path || "(root)",
        kind: "changed",
        before: a,
        after: b,
      });
    }
  };
  walk(before, after, "");
  return changes;
}

function isContainer(value: unknown): value is object {
  return typeof value === "object" && value !== null;
}

function sameValue(a: unknown, b: unknown): boolean {
  return a === b || (Number.isNaN(a) && Number.isNaN(b));
}

function joinKey(path: string, key: string): string {
  const safe = /^[A-Za-z_$][\w$]*$/.test(key) ? key : JSON.stringify(key);
  if (safe !== key) return `${path}[${safe}]`;
  return path ? `${path}.${key}` : key;
}

export interface DiffLine {
  kind: "same" | "added" | "removed";
  text: string;
}

/**
 * Line-by-line differences via the longest common subsequence. Quadratic, so large texts fall
 * back to a plain before/after listing.
 */
export function lineDiff(
  before: string,
  after: string,
  maxCells = 4_000_000,
): DiffLine[] {
  const a = before.split("\n");
  const b = after.split("\n");
  if (a.length * b.length > maxCells) {
    return [
      ...a.map((text) => ({ kind: "removed" as const, text })),
      ...b.map((text) => ({ kind: "added" as const, text })),
    ];
  }
  // lengths[i][j] = LCS length of a[i..] and b[j..]
  const lengths = Array.from(
    { length: a.length + 1 },
    () => new Uint32Array(b.length + 1),
  );
  for (let i = a.length - 1; i >= 0; i--) {
    for (let j = b.length - 1; j >= 0; j--) {
      lengths[i][j] =
        a[i] === b[j]
          ? lengths[i + 1][j + 1] + 1
          : Math.max(lengths[i + 1][j], lengths[i][j + 1]);
    }
  }
  const lines: DiffLine[] = [];
  let i = 0;
  let j = 0;
  while (i < a.length && j < b.length) {
    if (a[i] === b[j]) {
      lines.push({ kind: "same", text: a[i] });
      i++;
      j++;
    } else if (lengths[i + 1][j] >= lengths[i][j + 1]) {
      lines.push({ kind: "removed", text: a[i++] });
    } else {
      lines.push({ kind: "added", text: b[j++] });
    }
  }
  while (i < a.length) lines.push({ kind: "removed", text: a[i++] });
  while (j < b.length) lines.push({ kind: "added", text: b[j++] });
  return lines;
}
