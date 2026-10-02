import { describe, expect, it } from "vitest";
import type { SpecChannel } from "../bridge";
import { knownValues, matchChannel, paramAt, topicSnippet } from "./channels";

const channel = (address: string, values: Record<string, string[]> = {}) =>
  ({
    address,
    description: null,
    parameters: Object.entries(values).map(([name, v]) => ({
      name,
      description: null,
      values: v,
    })),
    messages: [],
  }) as SpecChannel;

const climate = channel("home/{room}/climate", { room: ["kitchen", "office"] });
const status = channel("home/status");
const any = channel("home/{what}");

describe("channels", () => {
  it("matches a topic to its channel, the most literal one first", () => {
    expect(matchChannel([climate], "home/hall/climate")).toEqual({
      channel: climate,
      params: { room: "hall" },
    });
    expect(matchChannel([any, status], "home/status")?.channel).toBe(status);
    expect(matchChannel([climate], "home/hall")).toBeNull();
    expect(matchChannel([climate], "home//climate")).toBeNull();
    expect(matchChannel([climate], "home/+/climate")).toBeNull();
  });

  it("knows a parameter's values from the spec, then from topics seen", () => {
    const values = knownValues(climate, [
      "home/hall/climate",
      "home/kitchen/climate",
      "home/hall/light",
    ]);
    expect(values.get("room")).toEqual(["kitchen", "office", "hall"]);
  });

  it("makes a snippet with a field per parameter", () => {
    const values = new Map([["room", ["kitchen"]]]);
    expect(topicSnippet(climate, values)).toBe("home/${1:kitchen}/climate");
    expect(topicSnippet(channel("a/{x}/{y}"), new Map())).toBe(
      "a/${1:x}/${2:y}",
    );
  });

  it("finds the parameter the cursor's level stands for", () => {
    expect(paramAt([status, climate], "home/ki")).toEqual({
      channel: climate,
      name: "room",
      from: 5,
    });
    expect(paramAt([climate], "home/kitchen/cl")).toBeNull();
    expect(paramAt([climate], "away/ki")).toBeNull();
  });
});
