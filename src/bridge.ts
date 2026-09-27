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
  messages: LogEntry[];
  connectionStates: ConnectionStatus[];
  dropped: number;
}

/** Starts the stream; a later call replaces the earlier one on the backend. */
export function streamMessages(onBatch: (batch: Batch) => void): Promise<void> {
  const channel = new Channel<Batch>();
  channel.onmessage = onBatch;
  return invoke("stream_messages", { onBatch: channel });
}
