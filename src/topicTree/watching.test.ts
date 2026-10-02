import { describe, expect, it } from "vitest";
import { TopicTreeStore } from "./store";
import { cameBack } from "./watching";

const update = (topic: string, seq: number) => ({
  connectionId: 1,
  topic,
  count: 1,
  retained: false,
  latest: {
    seq,
    connectionId: 1,
    topic,
    preview: "",
    binary: false,
    payloadSize: 0,
    qos: 0,
    retain: false,
    receivedAtMs: 0,
  },
});

describe("following a cleared topic", () => {
  it("waits for the clear, then for the topic to come back", () => {
    const tree = new TopicTreeStore();
    tree.apply([update("a/b", 1)]);
    const was = tree.find(1, "a/b")!;
    const watched = { connectionId: 1, path: "a/b", was };
    const find = (c: number, p: string) => tree.find(c, p);

    // Before the clear comes through: still the old node.
    tree.apply([update("a/b", 2)]);
    expect(cameBack(watched, find)).toBeNull();
    tree.clear(1, "a/b");
    expect(cameBack(watched, find)).toBeNull();
    tree.apply([update("a/b", 3)]);
    expect(cameBack(watched, find)?.topic?.latest.seq).toBe(3);
  });

  it("follows a cleared branch back when a topic below it returns", () => {
    const tree = new TopicTreeStore();
    tree.apply([update("a/b/c", 1)]);
    const watched = { connectionId: 1, path: "a/b", was: tree.find(1, "a/b")! };
    tree.clear(1, "a/b");
    tree.apply([update("a/b/d", 2)]);
    expect(cameBack(watched, (c, p) => tree.find(c, p))?.children.size).toBe(1);
  });
});
