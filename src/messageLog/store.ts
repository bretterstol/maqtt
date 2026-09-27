import type { LogEntry } from "../bridge";

/**
 * The frontend's window onto the Message Log: a fixed-size ring buffer of the most recent
 * entries. Held outside React state so appending hundreds of entries per batch costs one
 * re-render, read through `useSyncExternalStore`.
 */
export class MessageLogStore {
  private readonly entries: LogEntry[] = [];
  private start = 0;
  private version = 0;
  private readonly listeners = new Set<() => void>();

  /** Every entry ever appended, including those since evicted. */
  totalReceived = 0;
  /** Messages the backend reported as never delivered to the frontend. */
  dropped = 0;

  constructor(private readonly capacity: number) {}

  get length(): number {
    return this.entries.length;
  }

  /** The entry at `index`, where 0 is the oldest one still held. */
  at(index: number): LogEntry | undefined {
    if (index < 0 || index >= this.entries.length) return undefined;
    return this.entries[(this.start + index) % this.entries.length];
  }

  append(entries: LogEntry[], dropped = 0): void {
    if (entries.length === 0 && dropped === 0) return;
    for (const entry of entries) {
      if (this.entries.length < this.capacity) {
        this.entries.push(entry);
      } else {
        this.entries[this.start] = entry;
        this.start = (this.start + 1) % this.capacity;
      }
    }
    this.totalReceived += entries.length;
    this.dropped += dropped;
    this.version++;
    this.listeners.forEach((listener) => listener());
  }

  subscribe = (listener: () => void): (() => void) => {
    this.listeners.add(listener);
    return () => this.listeners.delete(listener);
  };

  getVersion = (): number => this.version;
}
