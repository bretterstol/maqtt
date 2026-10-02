import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { startAsking } from "./asking";

/** A load whose answers the test gives out, in any order. */
function answers<T>() {
  const pending: { resolve: (v: T) => void; reject: (e: unknown) => void }[] =
    [];
  const load = vi.fn(
    () =>
      new Promise<T>((resolve, reject) => pending.push({ resolve, reject })),
  );
  return { load, pending };
}

const settle = () => vi.advanceTimersByTimeAsync(0);

describe("asking for the latest answer", () => {
  beforeEach(() => vi.useFakeTimers());
  afterEach(() => vi.useRealTimers());

  it("asks once without a repeat", async () => {
    const { load, pending } = answers<number>();
    const onValue = vi.fn();
    startAsking(load, { onValue });
    pending[0].resolve(1);
    await settle();
    await vi.advanceTimersByTimeAsync(10_000);
    expect(load).toHaveBeenCalledTimes(1);
    expect(onValue).toHaveBeenCalledWith(1);
  });

  it("waits for the delay, and repeats only once each answer is in", async () => {
    const { load, pending } = answers<number>();
    const onValue = vi.fn();
    startAsking(load, { onValue }, { delay: 300, every: 1000 });
    await vi.advanceTimersByTimeAsync(299);
    expect(load).toHaveBeenCalledTimes(0);
    await vi.advanceTimersByTimeAsync(1);
    expect(load).toHaveBeenCalledTimes(1);
    // A slow answer: no second request while it's out.
    await vi.advanceTimersByTimeAsync(5000);
    expect(load).toHaveBeenCalledTimes(1);
    pending[0].resolve(1);
    await settle();
    await vi.advanceTimersByTimeAsync(999);
    expect(load).toHaveBeenCalledTimes(1);
    await vi.advanceTimersByTimeAsync(1);
    expect(load).toHaveBeenCalledTimes(2);
    expect(onValue).toHaveBeenCalledWith(1);
  });

  it("works the delay out when asking starts", async () => {
    const { load } = answers<number>();
    const delay = vi.fn(() => 500);
    startAsking(load, { onValue: () => {} }, { delay });
    expect(delay).toHaveBeenCalledTimes(1);
    await vi.advanceTimersByTimeAsync(499);
    expect(load).not.toHaveBeenCalled();
    await vi.advanceTimersByTimeAsync(1);
    expect(load).toHaveBeenCalledTimes(1);
  });

  it("drops an answer that arrives after a newer request", async () => {
    const { load, pending } = answers<string>();
    const onValue = vi.fn();
    const asking = startAsking(load, { onValue }, { every: 1000 });
    const now = asking.now();
    pending[1].resolve("new");
    await now;
    pending[0].resolve("old");
    await settle();
    expect(onValue.mock.calls).toEqual([["new"]]);
    // Only the newer request goes on repeating.
    await vi.advanceTimersByTimeAsync(1000);
    expect(load).toHaveBeenCalledTimes(3);
  });

  it("asks now instead of waiting out the repeat", async () => {
    const { load, pending } = answers<number>();
    const asking = startAsking(load, { onValue: () => {} }, { every: 1000 });
    pending[0].resolve(1);
    await settle();
    void asking.now();
    expect(load).toHaveBeenCalledTimes(2);
    pending[1].resolve(2);
    await settle();
    await vi.advanceTimersByTimeAsync(999);
    expect(load).toHaveBeenCalledTimes(2);
    await vi.advanceTimersByTimeAsync(1);
    expect(load).toHaveBeenCalledTimes(3);
  });

  it("drops what arrives after stopping, and asks no more", async () => {
    const { load, pending } = answers<number>();
    const onValue = vi.fn();
    const onError = vi.fn();
    const asking = startAsking(load, { onValue, onError }, { every: 1000 });
    asking.stop();
    pending[0].resolve(1);
    await settle();
    await vi.advanceTimersByTimeAsync(5000);
    expect(onValue).not.toHaveBeenCalled();
    expect(load).toHaveBeenCalledTimes(1);
    await asking.now();
    expect(load).toHaveBeenCalledTimes(1);
  });

  it("reports failures, and goes on asking unless told not to", async () => {
    const { load, pending } = answers<number>();
    const onError = vi.fn();
    startAsking(load, { onValue: () => {}, onError }, { every: 1000 });
    pending[0].reject("down");
    await settle();
    expect(onError).toHaveBeenCalledWith("down");
    await vi.advanceTimersByTimeAsync(1000);
    expect(load).toHaveBeenCalledTimes(2);

    const once = answers<number>();
    const asking = startAsking(
      once.load,
      { onValue: () => {} },
      { every: 1000, untilError: true },
    );
    once.pending[0].reject("bad");
    await settle();
    await vi.advanceTimersByTimeAsync(5000);
    expect(once.load).toHaveBeenCalledTimes(1);
    // Asked again, it carries on repeating.
    void asking.now();
    once.pending[1].resolve(1);
    await settle();
    await vi.advanceTimersByTimeAsync(1000);
    expect(once.load).toHaveBeenCalledTimes(3);
  });
});
