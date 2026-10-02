import { describe, expect, it } from "vitest";
import { cursorContext, rangeOf } from "./jsonCursor";

/** The context at the `|` in `text`. */
const at = (text: string) => {
  const pos = text.indexOf("|");
  return cursorContext(text.replace("|", ""), pos);
};

describe("the cursor in a payload", () => {
  it("completes keys, knowing the ones already there", () => {
    expect(at("{|")).toEqual({ kind: "key", path: [], from: 1, keys: [] });
    expect(at('{"a": 1, "te|')).toEqual({
      kind: "key",
      path: [],
      from: 9,
      keys: ["a"],
    });
    expect(at('{"s": [{"id": "x", |')).toMatchObject({
      kind: "key",
      path: ["s", 0],
      keys: ["id"],
    });
  });

  it("completes values at their path", () => {
    expect(at('{"unit": |')).toMatchObject({ kind: "value", path: ["unit"] });
    expect(at('{"unit": "C|"}')).toMatchObject({
      kind: "value",
      path: ["unit"],
      from: 9,
    });
    expect(at('{"ok": tr|')).toMatchObject({
      kind: "value",
      path: ["ok"],
      from: 7,
    });
    expect(at('{"s": [1, |')).toMatchObject({ kind: "value", path: ["s", 1] });
    expect(at("|")).toMatchObject({ kind: "value", path: [] });
  });

  it("completes nothing between a key and its colon", () => {
    expect(at('{"a" |').kind).toBe("none");
  });

  it("finds where to underline a problem", () => {
    const text = '{"t": "hot", "s": [{"id": 5}], "o": {"x": 1}}';
    const show = (path: (string | number)[]) => {
      const r = rangeOf(text, path);
      return text.slice(r.from, r.to);
    };
    expect(show(["t"])).toBe('"hot"');
    expect(show(["s", 0, "id"])).toBe("5");
    // A container: its key, not its whole body.
    expect(show(["o"])).toBe('"o"');
    expect(show([])).toBe("{");
    // Gone from the text: the nearest enclosing value.
    expect(show(["o", "y"])).toBe('"o"');
  });
});
