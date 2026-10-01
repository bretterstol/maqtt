import { describe, expect, it } from "vitest";
import { SINKS, assistAt, fieldsAt, wildcardNames } from "./assist";
import reference from "./reference.json";

const kind = (before: string) => assistAt(before).kind;

describe("what fits at the cursor", () => {
  it("starts a Pipeline with from, then offers Brokers", () => {
    expect(assistAt("# warm rooms\nfr")).toEqual({ kind: "from", from: 13 });
    expect(assistAt('from "Dev B')).toEqual({
      kind: "sources",
      from: 5,
      first: true,
    });
    expect(assistAt('from "A", "O')).toMatchObject({
      kind: "sources",
      from: 10,
      first: false,
    });
  });

  it("offers Pipelines after from pipeline", () => {
    expect(assistAt('from pipeline "cl')).toEqual({
      kind: "pipelines",
      from: 14,
    });
    expect(assistAt('from pipeline "a", ')).toEqual({
      kind: "pipelines",
      from: 19,
    });
  });

  it("offers Brokers after to broker, in or before the quotes", () => {
    expect(assistAt('from all |> to broker "Off')).toEqual({
      kind: "broker",
      from: 22,
    });
    expect(assistAt("from all\n|> to broker ")).toEqual({
      kind: "broker",
      from: 22,
    });
    expect(kind('from all |> to broker "Office" "x/')).toBe("none");
    expect(kind("from all |> to b")).toBe("sink");
  });

  it("offers by after delta and pairwise", () => {
    expect(kind("from all |> delta b")).toBe("by");
    expect(kind("from all |> pairwise ")).toBe("by");
    expect(kind("from all |> join ")).toBe("by");
  });

  it("reads brackets and fills as the parser does", () => {
    // A `|>` inside brackets isn't a new Stage: this is still map's jq.
    expect(kind("from all |> map (.a |> le")).toBe("function");
    // A fill inside a string is code, where a string holding a quote is still the fill's.
    expect(kind('from all |> to app "x/{.a + "y"} $')).not.toBe("variable");
    expect(kind('from all |> to app "x/{$')).toBe("variable");
  });

  it("offers Stages after |>, adding the space a bare |> needs", () => {
    expect(assistAt("from all\n|> thr")).toEqual({
      kind: "stage",
      from: 12,
      prefix: "",
      alsoFunctions: false,
    });
    expect(assistAt("from all |>")).toMatchObject({
      kind: "stage",
      prefix: " ",
    });
  });

  it("offers the next Stage, with its |>, on a new line", () => {
    expect(assistAt("from all\n|> map .a\nthr")).toMatchObject({
      kind: "stage",
      prefix: "|> ",
      alsoFunctions: true,
    });
  });

  it("knows the words that follow filter, throttle, changes and to", () => {
    expect(assistAt("from all |> filter to")).toMatchObject({
      kind: "filter",
      negated: false,
    });
    expect(assistAt("from all |> filter not ")).toMatchObject({
      kind: "filter",
      negated: true,
    });
    expect(kind("from all |> throttle 5s b")).toBe("by");
    expect(kind("from all |> throttle 5")).toBe("none");
    expect(kind("from all |> changes ")).toBe("by");
    expect(kind("from all |> to a")).toBe("sink");
  });

  it("offers variables, fields and functions inside expressions", () => {
    expect(assistAt("from all |> map $lev")).toEqual({
      kind: "variable",
      from: 16,
    });
    expect(assistAt("from all |> filter .temp > 1 |> map .a.b")).toEqual({
      kind: "field",
      from: 39,
      base: "value",
      path: ["a"],
      stage: 2,
    });
    expect(assistAt("from all |> map $payload.")).toMatchObject({
      kind: "field",
      base: "payload",
      path: [],
      stage: 1,
    });
    expect(assistAt("from all |> map .a | ascii_d")).toEqual({
      kind: "function",
      from: 21,
      word: "ascii_d",
    });
    expect(assistAt("from all |> map { t: len")).toMatchObject({
      kind: "function",
      word: "len",
    });
  });

  it("offers fields inside a string's {…}", () => {
    expect(assistAt('from all |> to app "hot/{.ro')).toMatchObject({
      kind: "field",
      path: [],
      stage: 1,
    });
  });

  it("stays quiet in strings, comments and numbers", () => {
    expect(kind('from all |> filter topic "home/')).toBe("none");
    expect(kind("from all # comm")).toBe("none");
    expect(kind("from all |> filter .t > 21.")).not.toBe("field");
  });

  it("isn't fooled by |> inside strings and objects", () => {
    expect(assistAt('from all |> map "a |> b" | .x')).toMatchObject({
      kind: "field",
      stage: 1,
    });
  });
});

describe("fields in a sample value", () => {
  const sample = '{"temperature": 21, "room": {"name": "kitchen"}}';

  it("lists an object's keys, at any depth", () => {
    expect(fieldsAt(sample, [])).toEqual(["temperature", "room"]);
    expect(fieldsAt(sample, ["room"])).toEqual(["name"]);
  });

  it("has nothing for other values", () => {
    expect(fieldsAt(sample, ["temperature"])).toEqual([]);
    expect(fieldsAt('"text"', [])).toEqual([]);
    expect(fieldsAt(null, [])).toEqual([]);
    expect(fieldsAt("not json", [])).toEqual([]);
  });
});

describe("named wildcards", () => {
  it("are found in filter topic lists, each once", () => {
    const text = `from all
|> filter topic "home/+room/#rest", "office/+room"
|> filter not topic "+/status"
|> map "+nope"`;
    expect(wildcardNames(text).map((w) => [w.name, w.written])).toEqual([
      ["room", "+room"],
      ["rest", "#rest"],
    ]);
  });
});

describe("Sink suggestions", () => {
  it("offer every Sink the Reference documents", () => {
    const to = reference.stages.find((s) => s.name === "to")!;
    const documented = [
      ...new Set(to.forms.map((f) => f.syntax.split(/\s+/)[1])),
    ].sort();
    expect(SINKS.map((s) => s.name).sort()).toEqual(documented);
  });
});
