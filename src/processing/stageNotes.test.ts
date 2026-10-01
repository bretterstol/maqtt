import { describe, expect, it } from "vitest";
import type { Preview } from "./api";
import { noSample, sampleCount, stageNotes } from "./stageNotes";

const base: Preview = {
  problems: [],
  messages: 2,
  payload: null,
  ends: [],
  stages: [],
  results: [],
  sampled: "messages",
  note: null,
  outputs: {
    emitted: 0,
    errors: 0,
    lastError: null,
    inspected: [],
    markers: [],
    chart: { lines: [], hidden: 0 },
  },
};

describe("stage notes", () => {
  it("puts the sample after from and each Stage's result after it", () => {
    const text = "from all\n|> map .t\n|> filter . > 1";
    const notes = stageNotes(text, {
      ...base,
      ends: [8, 18, 34],
      stages: [
        { passed: 2, last: "3", errors: 0, firstError: null },
        { passed: 0, last: null, errors: 0, firstError: null },
      ],
    });
    expect(
      notes.map((n) => [text.slice(0, n.at).split("\n").pop(), n.text]),
    ).toEqual([
      ["from all", "2 recent messages"],
      ["|> map .t", "2  3"],
      ["|> filter . > 1", "0"],
    ]);
  });

  it("maps byte offsets past non-ASCII text", () => {
    const text = 'from "Kjøkken"\n|> inspect';
    const [first] = stageNotes(text, {
      ...base,
      ends: [15],
      stages: [],
    });
    expect(first.at).toBe(14);
  });

  it("shows errors and shortens long values", () => {
    const long = "x".repeat(200);
    const notes = stageNotes("from all\n|> map .a", {
      ...base,
      ends: [8, 18],
      stages: [{ passed: 1, last: long, errors: 0, firstError: null }],
    });
    expect(notes[1].text.length).toBeLessThanOrEqual(60);
    expect(notes[1].title).toContain(long);

    const failing = stageNotes("from all\n|> map .a", {
      ...base,
      ends: [8, 18],
      stages: [
        { passed: 0, last: null, errors: 3, firstError: "map on t: bad" },
      ],
    });
    expect(failing[1]).toMatchObject({
      text: "3 errors: bad",
      error: true,
    });
    expect(failing[1].title).toContain("map on t: bad");
  });

  it("shows nothing for a draft with problems", () => {
    expect(
      stageNotes("from", {
        ...base,
        problems: [{ message: "x", line: 1, column: 1, start: 0, end: 1 }],
      }),
    ).toEqual([]);
  });
});

describe("sample wording", () => {
  it("counts what the Source reads in its own words", () => {
    expect(sampleCount({ ...base, messages: 1 })).toBe("1 recent message");
    expect(sampleCount({ ...base, sampled: "records", messages: 3 })).toBe(
      "3 records",
    );
    expect(sampleCount({ ...base, sampled: "derived", messages: 90 })).toBe(
      "90 recent Derived Topics",
    );
    expect(noSample({ ...base, sampled: "records", messages: 0 })).toBe(
      "no records in the files",
    );
  });
});
