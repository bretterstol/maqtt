// Types and entry point for the backend's UI Bridge stream (src-tauri/src/ui_bridge.rs).

import { Channel, invoke } from "@tauri-apps/api/core";

export type ConnectionId = number;

export type ProfileId = string;

export type ConnectionState =
  | { kind: "connecting" }
  | { kind: "connected" }
  | { kind: "disconnected"; reason: string }
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

/** A topic's full current state; the update with the newest `latest.seq` wins. */
export interface TopicUpdate {
  connectionId: ConnectionId;
  topic: string;
  count: number;
  retained: boolean;
  latest: LogEntry;
}

export interface Batch {
  /** New entries matching the filter of `filterGeneration`, oldest first. */
  messages: LogEntry[];
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
