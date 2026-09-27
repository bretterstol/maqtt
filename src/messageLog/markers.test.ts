import { describe, expect, it } from "vitest";
import { describeMarker, formatDuration } from "./markers";

describe("describeMarker", () => {
  it("says how long a reconnect took", () => {
    expect(describeMarker({ kind: "reconnected", offlineMs: 12_400 })).toEqual({
      text: "reconnected after 12 s offline",
      tone: "ok",
    });
  });

  it("gives the reason a connection was lost", () => {
    expect(
      describeMarker({ kind: "lost", reason: "Connection refused" }).text,
    ).toBe("lost its connection: Connection refused");
  });
});

describe("formatDuration", () => {
  it.each([
    [400, "0 s"],
    [59_400, "59 s"],
    [61_000, "1 min 1 s"],
    [3_723_000, "1 h 2 min"],
  ])("%d ms reads %s", (ms, text) => {
    expect(formatDuration(ms)).toBe(text);
  });
});
