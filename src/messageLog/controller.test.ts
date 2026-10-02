import { beforeEach, describe, expect, it, vi } from "vitest";
import type { LogPage, LogQuery, LogRow } from "../bridge";
import { queryLog, setLogFilter } from "../bridge";
import { MessageLogController } from "./controller";
import { MessageLogStore } from "./store";

vi.mock("../bridge", () => ({
  queryLog: vi.fn(),
  setLogFilter: vi.fn(),
}));

const entry = (seq: number): LogRow => ({
  kind: "message",
  seq,
  connectionId: 1,
  topic: "t",
  preview: "",
  binary: false,
  payloadSize: 0,
  qos: 0,
  retain: false,
  receivedAtMs: 0,
});

const page = (seqs: number[], hasMore = false): LogPage<LogRow> => ({
  entries: seqs.map(entry),
  hasMore,
});

function deferred<T>() {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>((r) => (resolve = r));
  return { promise, resolve };
}

const seqs = (store: MessageLogStore) =>
  Array.from({ length: store.length }, (_, i) => store.at(i)?.seq);

describe("MessageLogController", () => {
  let store: MessageLogStore;
  let controller: MessageLogController;

  beforeEach(() => {
    vi.mocked(queryLog).mockReset();
    vi.mocked(setLogFilter).mockReset();
    store = new MessageLogStore(100, 1_000);
    controller = new MessageLogController(store);
  });

  it("ignores a latest page that was superseded by a newer request", async () => {
    const first = deferred<LogPage<LogRow>>();
    const second = deferred<LogPage<LogRow>>();
    vi.mocked(queryLog)
      .mockReturnValueOnce(first.promise)
      .mockReturnValueOnce(second.promise);

    const a = controller.showLatest();
    const b = controller.showLatest();
    second.resolve(page([5, 6]));
    first.resolve(page([1, 2]));
    await Promise.all([a, b]);

    expect(seqs(store)).toEqual([5, 6]);
    expect(controller.isLoadingLatest).toBe(false);
  });

  it("ignores a latest page when the filter changed while it was fetched", async () => {
    const pending = deferred<LogPage<LogRow>>();
    vi.mocked(queryLog).mockReturnValueOnce(pending.promise);
    const loading = controller.showLatest();
    store.resetForFilter(1);
    pending.resolve(page([1]));
    await loading;
    expect(store.length).toBe(0);
  });

  it("doesn't resume following if the user paused while the latest page loaded", async () => {
    const pending = deferred<LogPage<LogRow>>();
    vi.mocked(queryLog).mockReturnValueOnce(pending.promise);
    const loading = controller.showLatest();
    store.pause();
    pending.resolve(page([1, 2]));
    await loading;
    expect(store.following).toBe(false);
    expect(store.length).toBe(0);
    expect(controller.isLoadingLatest).toBe(false);
  });

  it("loads the page before the oldest entry with the current filter", async () => {
    vi.mocked(setLogFilter).mockResolvedValue(1);
    vi.mocked(queryLog)
      .mockResolvedValueOnce(page([10, 11], true))
      .mockResolvedValueOnce(page([8, 9]));

    await controller.setFilter({ text: "kitchen" });
    store.pause();
    expect(await controller.loadOlder()).toBe(2);

    const query = vi.mocked(queryLog).mock.calls[1][0] as LogQuery;
    expect(query.beforeSeq).toBe(10);
    expect(query.filter).toEqual({ connectionId: null, text: "kitchen" });
    expect(seqs(store)).toEqual([8, 9, 10, 11]);
  });

  it("does not ask for older pages when there are none", async () => {
    vi.mocked(queryLog).mockResolvedValueOnce(page([1], false));
    await controller.showLatest();
    expect(await controller.loadOlder()).toBe(0);
    expect(queryLog).toHaveBeenCalledTimes(1);
  });

  it("treats an equivalent filter as unchanged", async () => {
    await controller.setFilter({ connectionId: undefined, text: "" });
    expect(setLogFilter).not.toHaveBeenCalled();
  });
});
