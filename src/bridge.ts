// Types and entry point for the backend's UI Bridge stream (src-tauri/src/ui_bridge.rs).

import { Channel, invoke } from "@tauri-apps/api/core";
import type { Batch } from "./generated/Batch";
import type { SpecChannel } from "./generated/SpecChannel";
import type { SpecParameter } from "./generated/SpecParameter";
import type { PayloadProblem } from "./generated/PayloadProblem";
import type { Spec } from "./generated/Spec";
import type { SpecMessage } from "./generated/SpecMessage";
import type { SpecView } from "./generated/SpecView";
import type { ClearedTopics } from "./generated/ClearedTopics";
import type { ConnectionId } from "./generated/ConnectionId";
import type { ConnectionState } from "./generated/ConnectionState";
import type { ConnectionStatus } from "./generated/ConnectionStatus";
import type { ImportReport } from "./generated/ImportReport";
import type { Imported } from "./generated/Imported";
import type { LogEntry } from "./generated/LogEntry";
import type { LogFilter } from "./generated/LogFilter";
import type { LogPage } from "./generated/LogPage";
import type { LogQuery } from "./generated/LogQuery";
import type { LogRow } from "./generated/LogRow";
import type { MarkerEntry } from "./generated/MarkerEntry";
import type { MarkerEvent } from "./generated/MarkerEvent";
import type { MessageDetail } from "./generated/MessageDetail";
import type { OutgoingMessage } from "./generated/OutgoingMessage";
import type { OutgoingProperties } from "./generated/OutgoingProperties";
import type { PasswordChange } from "./generated/PasswordChange";
import type { PayloadData } from "./generated/PayloadData";
import type { Profile } from "./generated/Profile";
import type { ProfileId } from "./generated/ProfileId";
import type { ProfileView } from "./generated/ProfileView";
import type { PropertiesView } from "./generated/PropertiesView";
import type { PublishRecord } from "./generated/PublishRecord";
import type { Subscription } from "./generated/Subscription";
import type { SubscriptionState } from "./generated/SubscriptionState";
import type { SubscriptionStatus } from "./generated/SubscriptionStatus";
import type { TlsSettings } from "./generated/TlsSettings";
import type { TopicUpdate } from "./generated/TopicUpdate";

// Generated from the backend's Rust types by ts-rs, when its tests run.
export type {
  Batch,
  SpecChannel,
  SpecParameter,
  PayloadProblem,
  Spec,
  SpecMessage,
  SpecView,
  ClearedTopics,
  ConnectionId,
  ConnectionState,
  ConnectionStatus,
  ImportReport,
  Imported,
  LogEntry,
  LogFilter,
  LogPage,
  LogQuery,
  LogRow,
  MarkerEntry,
  MarkerEvent,
  MessageDetail,
  OutgoingMessage,
  OutgoingProperties,
  PasswordChange,
  PayloadData,
  Profile,
  ProfileId,
  ProfileView,
  PropertiesView,
  PublishRecord,
  Subscription,
  SubscriptionState,
  SubscriptionStatus,
  TlsSettings,
  TopicUpdate,
};

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

export function queryLog(query: LogQuery): Promise<LogPage<LogRow>> {
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

/** Publishes on the Profile's Connection; resolves to the updated Publish History. */
export function publish(
  profileId: ProfileId,
  message: OutgoingMessage,
): Promise<PublishRecord[]> {
  return invoke("publish", { profileId, message });
}

/**
 * Forgets a topic's messages in måqtt, and those of the topics below it; the Broker isn't
 * touched. The tree hears of it in the next batch's `cleared`.
 */
export function clearTopicMessages(
  connectionId: ConnectionId,
  topic: string,
): Promise<void> {
  return invoke("clear_topic_messages", { connectionId, topic });
}

/** Clears the Broker's Retained Message for a topic. */
export function clearRetained(
  profileId: ProfileId,
  topic: string,
): Promise<void> {
  return invoke("clear_retained", { profileId, topic });
}

export function publishHistory(): Promise<PublishRecord[]> {
  return invoke("publish_history");
}

export function clearPublishHistory(): Promise<void> {
  return invoke("clear_publish_history");
}

/** Writes every Message Log row the filter matches to `path` as JSON Lines; resolves to the count. */
export function exportLog(filter: LogFilter, path: string): Promise<number> {
  return invoke("export_log", { filter, path });
}

/** Writes all Broker Profiles, without passwords, to `path`; resolves to the count. */
export function exportProfiles(path: string): Promise<number> {
  return invoke("export_profiles", { path });
}

export function importProfiles(path: string): Promise<Imported> {
  return invoke("import_profiles", { path });
}

/** API Specs by path, read again when their files have changed. */
export function readSpecs(paths: string[]): Promise<SpecView[]> {
  return invoke("read_specs", { paths });
}

/** Checks a payload against a channel's message schemas; none when it fits one. */
export function checkPayload(
  schemas: unknown[],
  payload: string,
): Promise<PayloadProblem[]> {
  return invoke("check_payload", { schemas, payload });
}
