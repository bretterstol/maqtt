import type { Batch, LogPage, LogRow } from "../bridge";

/**
 * What the Message Log view shows: a contiguous, seq-ordered slice of the backend's History
 * Store, held outside React state and read through `useSyncExternalStore`.
 *
 * While **following**, live entries are appended and the oldest trimmed beyond the live
 * capacity. While **paused** (the user scrolled up), the slice is frozen so nothing moves
 * under the reader: live entries are only counted, and older pages can be prepended.
 */
export class MessageLogStore {
  private entries: LogRow[] = [];
  private version = 0;
  private readonly listeners = new Set<() => void>();
  private scrollAdjustment = 0;

  following = true;
  /** Counts pauses, so a load started before the latest one can tell it is stale. */
  pauses = 0;
  /** Matching entries that arrived while paused. */
  newWhilePaused = 0;
  /** Whether the History Store may hold older matching entries than the first one here. */
  hasOlder = false;
  /** The filter generation whose live entries this store accepts. */
  generation = 0;
  /** Every message received, matching the filter or not. */
  totalReceived = 0;
  /** Matching messages the backend reported as never delivered. */
  dropped = 0;

  constructor(
    private liveCapacity: number,
    private maxScrollback: number,
  ) {}

  /** Changes how many messages it keeps live and how far back it scrolls; a smaller live
   * capacity trims at once. */
  setLimits(liveCapacity: number, maxScrollback: number): void {
    this.liveCapacity = liveCapacity;
    this.maxScrollback = maxScrollback;
    if (this.following && this.entries.length > liveCapacity) {
      this.trimToLiveCapacity();
      this.notify();
    }
  }

  get length(): number {
    return this.entries.length;
  }

  /** The entry at `index`, where 0 is the oldest one held. */
  at(index: number): LogRow | undefined {
    return this.entries[index];
  }

  get oldestSeq(): number | undefined {
    return this.entries[0]?.seq;
  }

  get canLoadOlder(): boolean {
    return this.hasOlder && this.entries.length < this.maxScrollback;
  }

  applyBatch(batch: Batch): void {
    this.totalReceived += batch.received;
    this.dropped += batch.dropped;
    if (batch.filterGeneration === this.generation) {
      if (this.following) {
        this.appendLive(batch.messages);
      } else {
        this.newWhilePaused += batch.messages.length;
      }
    }
    this.notify();
  }

  /** Shows the newest page and resumes following. */
  showLatest(page: LogPage): void {
    const newestInPage = page.entries.at(-1)?.seq ?? 0;
    // Live entries that arrived while the page was being fetched.
    const newer = this.following
      ? this.entries.filter((entry) => entry.seq > newestInPage)
      : [];
    this.entries = [...page.entries, ...newer];
    this.hasOlder = page.hasMore;
    this.following = true;
    this.newWhilePaused = 0;
    this.trimToLiveCapacity();
    this.notify();
  }

  /** Adds an older page in front; returns how many entries were added. */
  prependOlder(page: LogPage): number {
    const oldest = this.oldestSeq ?? Infinity;
    const older = page.entries.filter((entry) => entry.seq < oldest);
    this.entries = [...older, ...this.entries];
    this.hasOlder = page.hasMore;
    this.scrollAdjustment += older.length;
    this.notify();
    return older.length;
  }

  pause(): void {
    this.pauses++;
    if (!this.following) return;
    this.following = false;
    this.notify();
  }

  /** Clears the view for a new filter; returns false if `generation` is already superseded. */
  resetForFilter(generation: number): boolean {
    if (generation <= this.generation) return false;
    this.generation = generation;
    this.entries = [];
    this.following = true;
    this.newWhilePaused = 0;
    this.hasOlder = false;
    this.notify();
    return true;
  }

  /** Rows prepended since the last call, for keeping the scroll position anchored. */
  takeScrollAdjustment(): number {
    const rows = this.scrollAdjustment;
    this.scrollAdjustment = 0;
    return rows;
  }

  subscribe = (listener: () => void): (() => void) => {
    this.listeners.add(listener);
    return () => this.listeners.delete(listener);
  };

  getVersion = (): number => this.version;

  private appendLive(entries: LogRow[]): void {
    let newest = this.entries.at(-1)?.seq ?? 0;
    for (const entry of entries) {
      if (entry.seq > newest) {
        this.entries.push(entry);
        newest = entry.seq;
      }
    }
    this.trimToLiveCapacity();
  }

  private trimToLiveCapacity(): void {
    const excess = this.entries.length - this.liveCapacity;
    if (excess > 0) {
      this.entries.splice(0, excess);
      this.hasOlder = true;
    }
  }

  private notify(): void {
    this.version++;
    this.listeners.forEach((listener) => listener());
  }
}
