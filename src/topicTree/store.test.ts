import { describe, expect, it } from "vitest";
import type { TopicUpdate } from "../bridge";
import {
  ACTIVITY_BUCKET_MS,
  activityOf,
  GROUP_EXPAND_LIMIT,
  TopicTreeStore,
  type TreeNode,
} from "./store";

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

describe("activity", () => {
  const T0 = 1_000 * ACTIVITY_BUCKET_MS;
  const bucket = (n: number) => T0 + n * ACTIVITY_BUCKET_MS;
  const newest = (node: TreeNode, now: number) =>
    [...activityOf(node, now)].slice(-3);

  it("counts new messages in the current bucket, up through the ancestors", () => {
    const store = new TopicTreeStore();
    store.apply([update("a/b", 1, 1)], { now: bucket(0) });
    store.apply([update("a/b", 2, 4)], { now: bucket(0) + 10 });
    store.apply([update("a/c", 3, 2)], { now: bucket(1) });
    const a = find(store, "a");
    expect(newest(a, bucket(1))).toEqual([0, 4, 2]);
    expect(newest(a.parent!, bucket(1))).toEqual([0, 4, 2]);
    expect(a.lastActiveAt).toBe(bucket(1));
  });

  it("slides the window forward and forgets what's older than 30 s", () => {
    const store = new TopicTreeStore();
    store.apply([update("a", 1, 5)], { now: bucket(0) });
    const a = find(store, "a");
    expect(newest(a, bucket(2))).toEqual([5, 0, 0]);
    expect([...activityOf(a, bucket(12))].every((v) => v === 0)).toBe(true);
  });

  it("doesn't count a startup snapshot as activity", () => {
    const store = new TopicTreeStore();
    store.apply([update("a", 1, 500)], { now: bucket(0), live: false });
    const a = find(store, "a");
    expect(a.messageCount).toBe(500);
    expect([...activityOf(a, bucket(0))].every((v) => v === 0)).toBe(true);
    expect(a.lastActiveAt).toBe(0);
  });
});

describe("expand modes", () => {
  const tree = () => {
    const store = new TopicTreeStore();
    store.apply([
      update("home/kitchen/temp", 1),
      update("home/kitchen/lamp", 2),
      update("home/office/temp", 3),
      update("status", 4),
    ]);
    return store;
  };

  it("opens every level except the one holding only topics in groups mode", () => {
    const store = tree();
    store.setExpandMode("groups");
    expect(labels(store.rows())).toEqual([
      "#1",
      "  home",
      "    kitchen",
      "    office",
      "  status",
    ]);
    expect(store.isExpanded(find(store, "home/kitchen"))).toBe(false);
  });

  it("keeps a big group closed in groups mode", () => {
    const store = new TopicTreeStore();
    store.apply(
      Array.from({ length: GROUP_EXPAND_LIMIT + 1 }, (_, i) =>
        update(`fleet/${i}/telemetry`, i + 1),
      ),
    );
    store.setExpandMode("groups");
    expect(labels(store.rows())).toEqual(["#1", "  fleet"]);
  });

  it("opens everything in all mode", () => {
    const store = tree();
    store.setExpandMode("all");
    expect(labels(store.rows())).toEqual([
      "#1",
      "  home",
      "    kitchen",
      "      lamp",
      "      temp",
      "    office",
      "      temp",
      "  status",
    ]);
  });

  it("lets a hand-picked choice win until the mode changes", () => {
    const store = tree();
    store.setExpandMode("groups");
    store.toggle(find(store, "home/kitchen"));
    store.toggle(find(store, "home"));
    expect(labels(store.rows())).toEqual(["#1", "  home", "  status"]);
    store.setExpandMode("all");
    expect(store.rows().length).toBe(8);
  });

  it("opens new groups as they appear", () => {
    const store = tree();
    store.setExpandMode("groups");
    store.apply([update("garage/door/state", 5)]);
    expect(labels(store.rows())).toContain("    door");
  });

  it("clears a prefix, pruning levels left empty and keeping counts right", () => {
    const store = new TopicTreeStore();
    store.apply([
      update("warm/hot/kitchen", 1, 3, { connectionId: 0 }),
      update("warm/hot/garage", 2, 2, { connectionId: 0 }),
      update("warmer/x", 3, 1, { connectionId: 0 }),
    ]);
    store.setExpandMode("all");
    const kitchen = find(store, "warm/hot/kitchen");
    store.clear(0, "warm");

    expect(labels(store.rows())).toEqual(["#0", "  warmer", "    x"]);
    expect(store.topicCount).toBe(1);
    expect(store.contains(kitchen)).toBe(false);
    const root = store.rows()[0];
    expect([root.topicCount, root.messageCount]).toEqual([1, 1]);

    store.clear(0, "warmer");
    expect(store.rows()).toEqual([]);
    expect(store.contains(root)).toBe(false);
  });

  it("clears only whole levels, and ignores unknown prefixes", () => {
    const store = new TopicTreeStore();
    store.apply([update("a/b", 1), update("a/bc", 2)]);
    store.setExpandMode("all");
    store.clear(1, "a/b");
    store.clear(1, "nothing/here");
    store.clear(7, "a");
    expect(store.topicCount).toBe(1);
    expect(store.contains(find(store, "a/bc"))).toBe(true);
  });
});
