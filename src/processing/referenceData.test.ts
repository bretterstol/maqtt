import { describe, expect, it } from "vitest";
import { BY_STAGES, MODIFIERS, STAGE_NAMES } from "./referenceData";

describe("the vocabulary read off the Reference", () => {
  it("names the Stages, from first", () => {
    expect(STAGE_NAMES[0]).toBe("from");
    expect(STAGE_NAMES).toContain("debounce");
    expect(STAGE_NAMES.at(-1)).toBe("to");
  });

  it("finds the other words, without placeholders, quotes or Stages", () => {
    for (const word of ["all", "topic", "not", "by", "as", "needs", "within"])
      expect(MODIFIERS).toContain(word);
    for (const word of [
      "rotate",
      "daily",
      "hourly",
      "follow",
      "rate",
      "replay",
    ])
      expect(MODIFIERS).toContain(word);
    for (const word of [
      "app",
      "broker",
      "file",
      "geo",
      "chart",
      "pipeline",
      "pipelines",
    ])
      expect(MODIFIERS).toContain(word);
    for (const word of MODIFIERS) {
      expect(word).toMatch(/^[a-z]+$/);
      expect(STAGE_NAMES).not.toContain(word);
    }
    for (const word of ["key", "duration", "name", "at", "s", "m"])
      expect(MODIFIERS).not.toContain(word);
  });

  it("knows which Stages take by, and after how many arguments", () => {
    expect(Object.fromEntries(BY_STAGES)).toEqual({
      changes: 0,
      delta: 0,
      pairwise: 0,
      join: 0,
      throttle: 1,
      debounce: 1,
      scan: 2,
      window: 1,
      take: 1,
    });
  });
});
