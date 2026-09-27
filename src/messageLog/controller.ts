import { queryLog, setLogFilter, type LogFilter } from "../bridge";
import type { MessageLogStore } from "./store";

const PAGE_SIZE = 1_000;

/** Fetches pages from the History Store into a `MessageLogStore`, ignoring stale responses. */
export class MessageLogController {
  private filter: LogFilter = { connectionId: null, text: null };
  private latestRequest = 0;
  private loadingLatest = false;
  private loadingOlder = false;

  constructor(readonly store: MessageLogStore) {}

  /** The filter the Message Log shows now. */
  get currentFilter(): LogFilter {
    return this.filter;
  }

  get isLoadingLatest(): boolean {
    return this.loadingLatest;
  }

  /** Loads the newest page and resumes following. */
  async showLatest(): Promise<void> {
    const request = ++this.latestRequest;
    const generation = this.store.generation;
    this.loadingLatest = true;
    try {
      const page = await queryLog({ filter: this.filter, limit: PAGE_SIZE });
      if (
        request === this.latestRequest &&
        generation === this.store.generation
      ) {
        this.store.showLatest(page);
      }
    } finally {
      if (request === this.latestRequest) this.loadingLatest = false;
    }
  }

  /** Loads the page before the oldest entry held; returns how many entries were added. */
  async loadOlder(): Promise<number> {
    if (this.loadingOlder || !this.store.canLoadOlder) return 0;
    this.loadingOlder = true;
    try {
      const generation = this.store.generation;
      const page = await queryLog({
        filter: this.filter,
        beforeSeq: this.store.oldestSeq,
        limit: PAGE_SIZE,
      });
      if (generation !== this.store.generation) return 0;
      return this.store.prependOlder(page);
    } finally {
      this.loadingOlder = false;
    }
  }

  /** Starts over after the backend opened a stream with the filter reset to `generation`. */
  async resetForStream(generation: number): Promise<void> {
    this.filter = { connectionId: null, text: null };
    this.store.resetForFilter(generation);
    await this.showLatest();
  }

  async setFilter(filter: LogFilter): Promise<void> {
    const normalized = {
      connectionId: filter.connectionId ?? null,
      text: filter.text || null,
    };
    if (
      normalized.connectionId === this.filter.connectionId &&
      normalized.text === this.filter.text
    ) {
      return;
    }
    this.filter = normalized;
    const generation = await setLogFilter(normalized);
    if (this.store.resetForFilter(generation)) {
      await this.showLatest();
    }
  }
}
