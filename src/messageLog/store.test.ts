import { describe, expect, it, vi } from "vitest";
import type { LogEntry } from "../bridge";
import { MessageLogStore } from "./store";

const entry = (topic: string): LogEntry => ({
  connectionId: 1,
  topic,
  preview: "",
  binary: false,
  payloadSize: 0,
  qos: 0,
  retain: false,
  receivedAtMs: 0,
});

const topics = (store: MessageLogStore) =>
  Array.from({ length: store.length }, (_, i) => store.at(i)?.topic);

describe("MessageLogStore", () => {
  it("keeps entries oldest first", () => {
    const store = new MessageLogStore(5);
    store.append([entry("a"), entry("b")]);
    store.append([entry("c")]);
    expect(topics(store)).toEqual(["a", "b", "c"]);
  });

  it("evicts the oldest entries once full", () => {
    const store = new MessageLogStore(3);
    store.append(["a", "b", "c", "d", "e"].map(entry));
    expect(topics(store)).toEqual(["c", "d", "e"]);
    store.append([entry("f")]);
    expect(topics(store)).toEqual(["d", "e", "f"]);
    expect(store.totalReceived).toBe(6);
  });

  it("returns undefined outside the held range", () => {
    const store = new MessageLogStore(3);
    store.append([entry("a")]);
    expect(store.at(-1)).toBeUndefined();
    expect(store.at(1)).toBeUndefined();
  });

  it("notifies subscribers once per append and bumps the version", () => {
    const store = new MessageLogStore(3);
    const listener = vi.fn();
    const unsubscribe = store.subscribe(listener);
    store.append([entry("a"), entry("b")]);
    expect(listener).toHaveBeenCalledTimes(1);
    expect(store.getVersion()).toBe(1);
    unsubscribe();
    store.append([entry("c")]);
    expect(listener).toHaveBeenCalledTimes(1);
  });

  it("counts dropped messages, even in an otherwise empty batch", () => {
    const store = new MessageLogStore(3);
    store.append([], 7);
    expect(store.dropped).toBe(7);
    expect(store.getVersion()).toBe(1);
  });

  it("ignores empty batches with nothing dropped", () => {
    const store = new MessageLogStore(3);
    store.append([]);
    expect(store.getVersion()).toBe(0);
  });
});
