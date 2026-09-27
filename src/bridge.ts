// Types and entry point for the backend's UI Bridge stream (src-tauri/src/ui_bridge.rs).

import { Channel, invoke } from "@tauri-apps/api/core";

export type ConnectionId = number;

export type ConnectionState =
  | { kind: "connecting" }
  | { kind: "connected" }
  | { kind: "disconnected"; reason: string };

export interface ConnectionStatus {
  connectionId: ConnectionId;
  name: string;
  state: ConnectionState;
}

export interface LogEntry {
  seq: number;
  connectionId: ConnectionId;
  topic: string;
  preview: string;
  binary: boolean;
  payloadSize: number;
  qos: number;
  retain: boolean;
  receivedAtMs: number;
}

export interface Batch {
  /** New entries matching the filter of `filterGeneration`, oldest first. */
  messages: LogEntry[];
  filterGeneration: number;
  connectionStates: ConnectionStatus[];
  /** Every message received since the previous batch, matching the filter or not. */
  received: number;
  /** Matching messages that never reached the frontend. */
  dropped: number;
}

export interface LogFilter {
  connectionId?: ConnectionId | null;
  /** Matches the topic or the payload, ignoring case. */
  text?: string | null;
}

export interface LogQuery {
  filter: LogFilter;
  /** Only entries older than this; omit to start from the newest. */
  beforeSeq?: number;
  limit: number;
}

export interface LogPage {
  /** Oldest first. */
  entries: LogEntry[];
  hasMore: boolean;
}

/**
 * Starts the stream with the Message Log filter reset; resolves to that filter's generation.
 * A later call replaces the earlier stream on the backend.
 */
export function streamMessages(
  onBatch: (batch: Batch) => void,
): Promise<number> {
  const channel = new Channel<Batch>();
  channel.onmessage = onBatch;
  return invoke("stream_messages", { onBatch: channel });
}

/** Sets the live stream's filter; returns the generation its batches will carry. */
export function setLogFilter(filter: LogFilter): Promise<number> {
  return invoke("set_log_filter", { filter });
}

export function queryLog(query: LogQuery): Promise<LogPage> {
  return invoke("query_log", { query });
}

export function topicHistory(
  connectionId: ConnectionId,
  topic: string,
): Promise<LogEntry[]> {
  return invoke("topic_history", { connectionId, topic });
}
