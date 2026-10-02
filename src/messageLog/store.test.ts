import { describe, expect, it, vi } from "vitest";
import type { Batch, LogPage, LogRow } from "../bridge";
import { MessageLogStore } from "./store";

const entry = (seq: number): LogRow => ({
  kind: "message",
  seq,
  connectionId: 1,
  topic: `t/${seq}`,
  preview: "",
  binary: false,
  payloadSize: 0,
  qos: 0,
  retain: false,
  receivedAtMs: 0,
});

const batch = (seqs: number[], extra: Partial<Batch> = {}): Batch => ({
  messages: seqs.map(entry),
  filterGeneration: 0,
  connectionStates: [],
  cleared: [],
  topics: [],
  received: seqs.length,
  dropped: 0,
  checks: [],
  ...extra,
});

const page = (seqs: number[], hasMore = false): LogPage<LogRow> => ({
  entries: seqs.map(entry),
  hasMore,
});

const seqs = (store: MessageLogStore) =>
  Array.from({ length: store.length }, (_, i) => store.at(i)?.seq);

const newStore = (liveCapacity = 5, maxScrollback = 100) =>
  new MessageLogStore(liveCapacity, maxScrollback);

describe("MessageLogStore while following", () => {
  it("appends live entries in order", () => {
    const store = newStore();
    store.applyBatch(batch([1, 2]));
    store.applyBatch(batch([3]));
    expect(seqs(store)).toEqual([1, 2, 3]);
  });

  it("trims the oldest beyond the live capacity and marks older as available", () => {
    const store = newStore(3);
    store.applyBatch(batch([1, 2, 3, 4, 5]));
    expect(seqs(store)).toEqual([3, 4, 5]);
    expect(store.hasOlder).toBe(true);
  });

  it("takes new limits from Settings, trimming a smaller live capacity at once", () => {
    const store = newStore(5, 100);
    store.applyBatch(batch([1, 2, 3, 4, 5]));
    store.setLimits(2, 100);
    expect(seqs(store)).toEqual([4, 5]);
    expect(store.canLoadOlder).toBe(true);
    store.setLimits(2, 2);
    expect(store.canLoadOlder).toBe(false);
  });

  it("skips entries it already holds", () => {
    const store = newStore();
    store.showLatest(page([1, 2, 3]));
    store.applyBatch(batch([3, 4]));
    expect(seqs(store)).toEqual([1, 2, 3, 4]);
  });

  it("ignores entries from another filter generation but still counts traffic", () => {
    const store = newStore();
    store.applyBatch(batch([1, 2], { filterGeneration: 7, dropped: 3 }));
    expect(store.length).toBe(0);
    expect(store.totalReceived).toBe(2);
    expect(store.dropped).toBe(3);
  });

  it("notifies subscribers once per batch", () => {
    const store = newStore();
    const listener = vi.fn();
    store.subscribe(listener);
    store.applyBatch(batch([1, 2]));
    expect(listener).toHaveBeenCalledTimes(1);
  });
});

describe("MessageLogStore while paused", () => {
  it("freezes the entries and counts what arrives", () => {
    const store = newStore(3);
    store.applyBatch(batch([1, 2, 3]));
    store.pause();
    store.applyBatch(batch([4, 5]));
    expect(seqs(store)).toEqual([1, 2, 3]);
    expect(store.newWhilePaused).toBe(2);
  });

  it("prepends older pages beyond the live capacity and reports the scroll adjustment", () => {
    const store = newStore(3);
    store.showLatest(page([4, 5, 6], true));
    store.pause();
    expect(store.prependOlder(page([1, 2, 3], false))).toBe(3);
    expect(seqs(store)).toEqual([1, 2, 3, 4, 5, 6]);
    expect(store.hasOlder).toBe(false);
    expect(store.takeScrollAdjustment()).toBe(3);
    expect(store.takeScrollAdjustment()).toBe(0);
  });

  it("does not prepend entries that overlap what it holds", () => {
    const store = newStore();
    store.showLatest(page([3, 4]));
    expect(store.prependOlder(page([2, 3]))).toBe(1);
    expect(seqs(store)).toEqual([2, 3, 4]);
  });

  it("stops offering older pages at the scrollback limit", () => {
    const store = newStore(2, 4);
    store.showLatest(page([5, 6], true));
    expect(store.canLoadOlder).toBe(true);
    store.prependOlder(page([3, 4], true));
    expect(store.canLoadOlder).toBe(false);
  });
});

describe("MessageLogStore.showLatest", () => {
  it("replaces a paused view and resumes following", () => {
    const store = newStore();
    store.applyBatch(batch([1, 2]));
    store.pause();
    store.applyBatch(batch([3]));
    store.showLatest(page([8, 9], true));
    expect(seqs(store)).toEqual([8, 9]);
    expect(store.following).toBe(true);
    expect(store.newWhilePaused).toBe(0);
    expect(store.hasOlder).toBe(true);
  });

  it("keeps live entries newer than the page that arrived while it was fetched", () => {
    const store = newStore();
    store.applyBatch(batch([10, 11]));
    store.showLatest(page([8, 9, 10]));
    expect(seqs(store)).toEqual([8, 9, 10, 11]);
  });
});

describe("MessageLogStore.resetForFilter", () => {
  it("clears the view for a newer generation", () => {
    const store = newStore();
    store.applyBatch(batch([1]));
    store.pause();
    expect(store.resetForFilter(1)).toBe(true);
    expect(store.length).toBe(0);
    expect(store.following).toBe(true);
    store.applyBatch(batch([2], { filterGeneration: 1 }));
    expect(seqs(store)).toEqual([2]);
  });

  it("refuses a generation that is not newer", () => {
    const store = newStore();
    store.resetForFilter(2);
    expect(store.resetForFilter(1)).toBe(false);
    expect(store.generation).toBe(2);
  });
});
