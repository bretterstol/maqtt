import { describe, expect, it } from "vitest";
import { isTopicFilter, topicFilterError, topicMatches } from "./match";

describe("topicMatches", () => {
  it.each([
    ["a/b", "a/b"],
    ["#", "a"],
    ["#", "a/b/c"],
    ["a/#", "a"],
    ["a/#", "a/b/c"],
    ["a/+", "a/b"],
    ["+/b", "a/b"],
    ["+/+", "/a"],
    ["a/+/c", "a/b/c"],
    ["+", ""],
    ["a//b", "a//b"],
    ["a/+/b", "a//b"],
    ["$SYS/#", "$SYS/broker/uptime"],
    ["$SYS/+", "$SYS/uptime"],
  ])("%s matches %s", (filter, topic) => {
    expect(topicMatches(filter, topic)).toBe(true);
  });

  it.each([
    ["a/b", "a/b/c"],
    ["a/b", "a"],
    ["a/+", "a"],
    ["a/+", "a/b/c"],
    ["+", "/a"],
    ["+/b", "a/c"],
    ["A/b", "a/b"],
    ["#", "$SYS/uptime"],
    ["+/uptime", "$SYS/uptime"],
  ])("%s does not match %s", (filter, topic) => {
    expect(topicMatches(filter, topic)).toBe(false);
  });
});

describe("topicFilterError", () => {
  it.each(["#", "a/b", "a/+/c", "+", "a/#", "+/+/#", "/a"])(
    "accepts %s",
    (filter) => {
      expect(topicFilterError(filter)).toBeNull();
    },
  );

  it.each(["", "a/#/b", "a#", "#/a", "a+/b", "a/b+"])(
    "rejects %s",
    (filter) => {
      expect(topicFilterError(filter)).not.toBeNull();
    },
  );
});

describe("isTopicFilter", () => {
  it("treats queries with wildcards as filters", () => {
    expect(isTopicFilter("home/+/temp")).toBe(true);
    expect(isTopicFilter("home/#")).toBe(true);
    expect(isTopicFilter("kitchen")).toBe(false);
  });
});
