import { describe, expect, it } from "vitest";
import type { TopicUpdate } from "../bridge";
import { TopicTreeStore, type TreeNode } from "./store";

const update = (
  topic: string,
  seq: number,
  count = 1,
  { retained = false, connectionId = 1 } = {},
): TopicUpdate => ({
  connectionId,
  topic,
  count,
  retained,
  latest: {
    seq,
    connectionId,
    topic,
    preview: `v${seq}`,
    binary: false,
    payloadSize: 2,
    qos: 0,
    retain: retained,
    receivedAtMs: 0,
  },
});

const labels = (rows: TreeNode[]) =>
  rows.map(
    (n) =>
      `${"  ".repeat(n.depth)}${n.depth === 0 ? `#${n.connectionId}` : n.name}`,
  );

const find = (store: TopicTreeStore, path: string) =>
  store.rows().find((n) => n.path === path && n.depth > 0)!;

describe("TopicTreeStore", () => {
  it("builds levels under a root per Connection, with only roots expanded", () => {
    const store = new TopicTreeStore();
    store.apply([
      update("home/kitchen/temp", 1),
      update("x", 2, 1, { connectionId: 2 }),
    ]);
    expect(labels(store.rows())).toEqual(["#1", "  home", "#2", "  x"]);
  });

  it("expands and collapses, sorting children naturally", () => {
    const store = new TopicTreeStore();
    store.apply([update("dev/10", 1), update("dev/9", 2), update("dev/a", 3)]);
    store.toggle(find(store, "dev"));
    expect(labels(store.rows())).toEqual([
      "#1",
      "  dev",
      "    9",
      "    10",
      "    a",
    ]);
    store.toggle(find(store, "dev"));
    expect(labels(store.rows())).toEqual(["#1", "  dev"]);
  });

  it("adds topic and message counts up through the levels", () => {
    const store = new TopicTreeStore();
    store.apply([update("a/b", 1, 3), update("a/c", 2, 2)]);
    store.apply([update("a/b", 3, 5)]);
    const a = find(store, "a");
    expect(a.topicCount).toBe(2);
    expect(a.messageCount).toBe(7);
    expect(a.parent!.messageCount).toBe(7);
    expect(store.topicCount).toBe(2);
  });

  it("keeps a node that is both a topic and a parent", () => {
    const store = new TopicTreeStore();
    store.apply([update("a", 1, 1), update("a/b", 2, 1)]);
    const a = find(store, "a");
    expect(a.topic?.count).toBe(1);
    expect(a.topicCount).toBe(2);
  });

  it("ignores an update older than what it holds", () => {
    const store = new TopicTreeStore();
    store.apply([update("a", 5, 5)]);
    store.apply([update("a", 3, 3)]);
    expect(find(store, "a").topic?.latest.seq).toBe(5);
    expect(find(store, "a").messageCount).toBe(5);
  });

  it("takes the retained state from the latest update, including resets with the same seq", () => {
    const store = new TopicTreeStore();
    store.apply([update("a", 1, 1, { retained: true })]);
    expect(find(store, "a").topic?.retained).toBe(true);
    store.apply([update("a", 1, 1, { retained: false })]);
    expect(find(store, "a").topic?.retained).toBe(false);
  });

  it("tells an empty first level apart from the root", () => {
    const store = new TopicTreeStore();
    store.apply([update("/a", 1)]);
    store.toggle(store.rows()[1]);
    expect(labels(store.rows())).toEqual(["#1", "  ", "    a"]);
  });

  describe("search", () => {
    const tree = () => {
      const store = new TopicTreeStore();
      store.apply([
        update("home/kitchen/temp", 1),
        update("home/office/temp", 2),
        update("home/office/humidity", 3),
        update("garage/door", 4),
      ]);
      return store;
    };

    it("shows substring matches with their ancestors, ignoring case", () => {
      const store = tree();
      store.setQuery("OFFICE");
      expect(labels(store.rows())).toEqual([
        "#1",
        "  home",
        "    office",
        "      humidity",
        "      temp",
      ]);
      expect(store.matchCount).toBe(2);
    });

    it("reads wildcards as an MQTT topic filter", () => {
      const store = tree();
      store.setQuery("home/+/temp");
      expect(
        store
          .rows()
          .filter((n) => n.topic)
          .map((n) => n.path),
      ).toEqual(["home/kitchen/temp", "home/office/temp"]);
    });

    it("reports an invalid filter and shows nothing", () => {
      const store = tree();
      store.setQuery("home/#/temp");
      expect(store.queryError).not.toBeNull();
      expect(store.rows()).toEqual([]);
    });

    it("picks up new matching topics as they arrive", () => {
      const store = tree();
      store.setQuery("door");
      store.apply([update("home/door", 5)]);
      expect(store.rows().filter((n) => n.topic).length).toBe(2);
      expect(store.matchCount).toBe(2);
    });

    it("restores the expanded view when the query is cleared", () => {
      const store = tree();
      store.setQuery("door");
      store.setQuery("");
      expect(labels(store.rows())).toEqual(["#1", "  garage", "  home"]);
    });
  });
});
