import { describe, expect, it } from "vitest";
import { jsonDiff, lineDiff } from "./diff";

describe("jsonDiff", () => {
  it("finds changed, added and removed fields at their paths", () => {
    expect(
      jsonDiff(
        { temp: 21.5, battery: 80, meta: { fw: "1.0" }, old: true },
        { temp: 21.7, battery: 80, meta: { fw: "1.1" }, fresh: 1 },
      ),
    ).toEqual([
      { path: "temp", kind: "changed", before: 21.5, after: 21.7 },
      { path: "meta.fw", kind: "changed", before: "1.0", after: "1.1" },
      { path: "old", kind: "removed", before: true },
      { path: "fresh", kind: "added", after: 1 },
    ]);
  });

  it("compares arrays by index", () => {
    expect(jsonDiff([1, 2, 3], [1, 5])).toEqual([
      { path: "[1]", kind: "changed", before: 2, after: 5 },
      { path: "[2]", kind: "removed", before: 3 },
    ]);
  });

  it("quotes keys that aren't identifiers", () => {
    expect(jsonDiff({ "a b": 1 }, { "a b": 2 })[0].path).toBe('["a b"]');
  });

  it("treats a type change as one change", () => {
    expect(jsonDiff({ a: [1] }, { a: { 0: 1 } })).toEqual([
      { path: "a", kind: "changed", before: [1], after: { 0: 1 } },
    ]);
    expect(jsonDiff(1, "1")).toEqual([
      { path: "(root)", kind: "changed", before: 1, after: "1" },
    ]);
  });

  it("is empty for equal values, including null", () => {
    expect(jsonDiff({ a: [1, { b: null }] }, { a: [1, { b: null }] })).toEqual(
      [],
    );
  });
});

describe("lineDiff", () => {
  it("marks added and removed lines around common ones", () => {
    expect(lineDiff("a\nb\nc", "a\nc\nd")).toEqual([
      { kind: "same", text: "a" },
      { kind: "removed", text: "b" },
      { kind: "same", text: "c" },
      { kind: "added", text: "d" },
    ]);
  });

  it("falls back to before and after for very large texts", () => {
    expect(lineDiff("a\nb", "a\nc", 1)).toEqual([
      { kind: "removed", text: "a" },
      { kind: "removed", text: "b" },
      { kind: "added", text: "a" },
      { kind: "added", text: "c" },
    ]);
  });
});
