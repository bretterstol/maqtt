import { describe, expect, it } from "vitest";
import cases from "./scanCases.json";
import { scan } from "./scanner";

/** The pieces as `kind{strings}{depth}:text`, as scanCases.json writes them. */
const shown = (text: string) =>
  scan(text).pieces.map(
    (p) => `${p.kind}${p.strings}${p.depth}:${text.slice(p.start, p.end)}`,
  );

const open = (text: string) => scan(text).open.map((o) => `${o.kind}@${o.at}`);

describe("the scanner", () => {
  it.each(cases)("follows the shared rules for $text", (c) => {
    expect(shown(c.text)).toEqual(c.pieces);
    expect(open(c.text)).toEqual(c.open ?? []);
  });

  it("covers any text, every prefix of it, without gaps", () => {
    const text = `from "a" |> map { s: "x{.a + "y{.b}"}z", t: "\\(.c + "d")" } # é`;
    for (let end = 0; end <= text.length; end++) {
      const prefix = text.slice(0, end);
      let at = 0;
      for (const p of scan(prefix).pieces) {
        expect(p.start).toBe(at);
        at = p.end;
      }
      expect(at).toBe(prefix.length);
    }
  });
});
