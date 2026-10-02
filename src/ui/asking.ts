import { useCallback, useEffect, useRef, type DependencyList } from "react";

/**
 * Asking the backend for something, once or over and over, where only the latest answer
 * counts. An answer is dropped when a newer request has been made since, or when the asking
 * has stopped (the view went away, or what it asks about changed). Repeats wait for the
 * previous answer before the next request, so answers can't arrive out of order.
 */

export interface Timing {
  /** Wait this long before the first request; a function is asked when asking starts. */
  delay?: number | (() => number);
  /** Ask again this long after each answer; without it, ask once. */
  every?: number;
  /** After a failure, don't ask again until told to (`now`). */
  untilError?: boolean;
}

export interface Handlers<T> {
  onValue: (value: T) => void;
  onError?: (error: unknown) => void;
}

export interface Asking {
  /** Asks now, dropping any answer still on its way; resolves once this one is handled. */
  now: () => Promise<void>;
  /** Stops asking, and drops any answer still on its way. */
  stop: () => void;
}

/** Starts asking with `load`. */
export function startAsking<T>(
  load: () => Promise<T>,
  handlers: Handlers<T>,
  timing: Timing = {},
): Asking {
  let asked = 0;
  let stopped = false;
  let timer: ReturnType<typeof setTimeout> | undefined;

  const schedule = (ms: number) => {
    clearTimeout(timer);
    timer = setTimeout(() => void ask(), ms);
  };

  const ask = async (): Promise<void> => {
    clearTimeout(timer);
    if (stopped) return;
    const ask = ++asked;
    let failed = false;
    try {
      const value = await load();
      if (stopped || ask !== asked) return;
      handlers.onValue(value);
    } catch (error) {
      if (stopped || ask !== asked) return;
      failed = true;
      handlers.onError?.(error);
    }
    if (timing.every !== undefined && !(failed && timing.untilError)) {
      schedule(timing.every);
    }
  };

  const delay =
    typeof timing.delay === "function" ? timing.delay() : timing.delay;
  if (delay) schedule(delay);
  else void ask();

  return {
    now: ask,
    stop: () => {
      stopped = true;
      clearTimeout(timer);
    },
  };
}

/**
 * Asks with `load` while the component is shown, starting over whenever `deps` change; `load`
 * and the handlers are the ones from the render where `deps` last changed. Returns `now`, to
 * ask straight away (after a save, say). With `enabled` false, nothing is asked.
 */
export function useAsking<T>(
  load: () => Promise<T>,
  handlers: Handlers<T>,
  timing: Timing & { enabled?: boolean },
  deps: DependencyList,
): () => Promise<void> {
  const asking = useRef<Asking | null>(null);
  const enabled = timing.enabled ?? true;
  useEffect(() => {
    if (!enabled) return;
    const started = startAsking(load, handlers, timing);
    asking.current = started;
    return () => {
      started.stop();
      if (asking.current === started) asking.current = null;
    };
  }, [enabled, ...deps]); // eslint-disable-line react-hooks/exhaustive-deps
  return useCallback(() => asking.current?.now() ?? Promise.resolve(), []);
}
