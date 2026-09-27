// Types and entry point for the backend's UI Bridge stream (src-tauri/src/ui_bridge.rs).

import { Channel, invoke } from "@tauri-apps/api/core";

export type ConnectionId = number;

export type ProfileId = string;

export type ConnectionState =
  | { kind: "connecting" }
  | { kind: "connected" }
  | { kind: "disconnected"; reason: string; retryAtMs: number }
  | { kind: "closed" };

export type SubscriptionState =
  | { kind: "pending" }
  | { kind: "active"; grantedQos: number }
  | { kind: "rejected"; reason: string };

export interface SubscriptionStatus {
  filter: string;
  qos: number;
  state: SubscriptionState;
}

export interface ConnectionStatus {
  connectionId: ConnectionId;
  profileId: ProfileId;
  name: string;
  state: ConnectionState;
  subscriptions: SubscriptionStatus[];
}

export interface Subscription {
  filter: string;
  qos: number;
}

export interface TlsSettings {
  caFile?: string | null;
  clientCertFile?: string | null;
  clientKeyFile?: string | null;
  insecureSkipVerify: boolean;
}

export interface Profile {
  id: ProfileId;
  name: string;
  host: string;
  port: number;
  protocol: "mqtt5" | "mqtt311";
  clientId: string;
  username?: string | null;
  tls?: TlsSettings | null;
  cleanStart: boolean;
  keepAliveSecs: number;
  subscriptions: Subscription[];
  historyPerTopic: number;
}

export interface ProfileView extends Profile {
  hasPassword: boolean;
}

export type PasswordChange =
  { kind: "keep" } | { kind: "set"; value: string } | { kind: "clear" };

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

/** A change in a Connection's link, shown as a row in the Message Log. */
export type MarkerEvent =
  | { kind: "connected" }
  | { kind: "reconnected"; offlineMs: number }
  | { kind: "lost"; reason: string }
  | { kind: "failed"; reason: string }
  | { kind: "closed" };

export interface MarkerEntry {
  kind: "marker";
  seq: number;
  connectionId: ConnectionId;
  atMs: number;
  event: MarkerEvent;
}

/** One Message Log row: a message or a marker. */
export type LogRow = ({ kind: "message" } & LogEntry) | MarkerEntry;

/** Text when the bytes are UTF-8, base64 otherwise. */
export type PayloadData =
  { kind: "text"; text: string } | { kind: "binary"; base64: string };

export interface MessageProperties {
  payloadFormatIndicator?: number | null;
  messageExpiryInterval?: number | null;
  contentType?: string | null;
  responseTopic?: string | null;
  correlationData?: PayloadData | null;
  userProperties: [string, string][];
}

/** One message in full, for the topic detail panel. */
export interface MessageDetail extends LogEntry {
  payload: PayloadData;
  /** The payload was cut for display. */
  truncated: boolean;
  /** Present only for messages received over MQTT 5. */
  properties?: MessageProperties | null;
}

/** A topic's full current state; the update with the newest `latest.seq` wins. */
export interface TopicUpdate {
  connectionId: ConnectionId;
  topic: string;
  count: number;
  retained: boolean;
  latest: LogEntry;
}

export interface Batch {
  /** New rows matching the filter of `filterGeneration`, oldest first. */
  messages: LogRow[];
  filterGeneration: number;
  connectionStates: ConnectionStatus[];
  /** Topics that changed since the previous batch, each with its full current stats. */
  topics: TopicUpdate[];
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
  entries: LogRow[];
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

export function listProfiles(): Promise<ProfileView[]> {
  return invoke("list_profiles");
}

/** A new, unsaved Profile with defaults filled in by the backend. */
export function newProfile(): Promise<Profile> {
  return invoke("new_profile");
}

export function saveProfile(
  profile: Profile,
  password: PasswordChange,
): Promise<ProfileView[]> {
  return invoke("save_profile", { profile, password });
}

export function deleteProfile(profileId: ProfileId): Promise<ProfileView[]> {
  return invoke("delete_profile", { profileId });
}

export function connect(profileId: ProfileId): Promise<void> {
  return invoke("connect", { profileId });
}

/** Ends the backoff wait of a disconnected Profile and tries again now. */
export function retryNow(profileId: ProfileId): Promise<void> {
  return invoke("retry_now", { profileId });
}

export function disconnect(profileId: ProfileId): Promise<void> {
  return invoke("disconnect", { profileId });
}

/** Adds a Subscription to the live Connection, for this session only. */
export function subscribe(
  profileId: ProfileId,
  subscription: Subscription,
): Promise<void> {
  return invoke("subscribe", { profileId, subscription });
}

/** Removes a Subscription from the live Connection, for this session only. */
export function unsubscribe(
  profileId: ProfileId,
  filter: string,
): Promise<void> {
  return invoke("unsubscribe", { profileId, filter });
}

/** Every topic in the Topic Tree, for starting from scratch. */
export function topicSnapshot(): Promise<TopicUpdate[]> {
  return invoke("topic_snapshot");
}

/** One message in full, or null once it has left the History Store. */
export function messageDetail(
  connectionId: ConnectionId,
  seq: number,
): Promise<MessageDetail | null> {
  return invoke("message_detail", { connectionId, seq });
}
