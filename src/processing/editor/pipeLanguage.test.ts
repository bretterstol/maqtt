import { StringStream } from "@codemirror/language";
import { describe, expect, it } from "vitest";
import { pipeParser } from "./pipeLanguage";

/** Each non-space token of one line, with its style. */
function tokens(line: string): [string, string | null][] {
  const state = pipeParser.startState!(2);
  const stream = new StringStream(line, 2, 2);
  const out: [string, string | null][] = [];
  while (!stream.eol()) {
    const style = pipeParser.token(stream, state);
    const text = stream.current();
    if (text.trim() !== "") out.push([text, style]);
    stream.start = stream.pos;
  }
  return out;
}

describe("the pipeline tokenizer", () => {
  it("tells stages, modifiers, strings and durations apart", () => {
    expect(tokens("|> throttle 5s by .room # per room")).toEqual([
      ["|>", "operator"],
      ["throttle", "keyword"],
      ["5s", "number"],
      ["by", "modifier"],
      [".room", "propertyName"],
      ["# per room", "comment"],
    ]);
  });

  it("keeps a # inside a string out of comments", () => {
    expect(tokens('filter topic "home/#"')).toEqual([
      ["filter", "keyword"],
      ["topic", "modifier"],
      ['"home/#"', "string"],
    ]);
  });

  it("marks variables", () => {
    expect(tokens("map $payload.temp")).toEqual([
      ["map", "keyword"],
      ["$payload", "variableName"],
      [".temp", "propertyName"],
    ]);
  });

  it("colours a fill's code, and a quote inside it doesn't end the string", () => {
    expect(tokens('to app "x/{.a + "y"}/z"')).toEqual([
      ["to", "keyword"],
      ["app", "modifier"],
      ['"x/', "string"],
      ["{", "operator"],
      [".a", "propertyName"],
      ["+", null],
      ['"y"', "string"],
      ["}", "operator"],
      ['/z"', "string"],
    ]);
  });

  it("knows jq's own interpolation and literal braces", () => {
    expect(tokens('map "\\(.a)" |> map "{{x}}"')).toEqual([
      ["map", "keyword"],
      ['"', "string"],
      ["\\(", "operator"],
      [".a", "propertyName"],
      [")", "operator"],
      ['"', "string"],
      ["|>", "operator"],
      ["map", "keyword"],
      ['"{{x}}"', "string"],
    ]);
  });
});
