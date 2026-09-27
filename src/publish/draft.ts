import type { OutgoingMessage, OutgoingProperties, ProfileId } from "../bridge";

/** The publish form as the user edits it: everything as text, properties behind "Advanced". */
export interface PublishDraft {
  profileId: ProfileId | null;
  topic: string;
  payload: string;
  qos: number;
  retain: boolean;
  advanced: boolean;
  contentType: string;
  responseTopic: string;
  correlationData: string;
  expirySecs: string;
  payloadIsUtf8: boolean;
  userProperties: [string, string][];
}

export function emptyDraft(
  profileId: ProfileId | null = null,
  topic = "",
): PublishDraft {
  return {
    profileId,
    topic,
    payload: "",
    qos: 0,
    retain: false,
    advanced: false,
    contentType: "",
    responseTopic: "",
    correlationData: "",
    expirySecs: "",
    payloadIsUtf8: false,
    userProperties: [],
  };
}

/** Why the draft can't be sent yet, or null when it can. */
export function draftError(draft: PublishDraft, mqtt5: boolean): string | null {
  if (!draft.profileId) return "Pick a Connection to publish on";
  if (!draft.topic) return "Enter a topic";
  if (/[+#]/.test(draft.topic))
    return "A topic to publish to can't contain + or #";
  if (draft.advanced && !mqtt5 && !propertiesEmpty(toProperties(draft))) {
    return "This Connection uses MQTT 3.1.1, which can't send properties";
  }
  if (draft.expirySecs && !/^\d+$/.test(draft.expirySecs.trim())) {
    return "Message expiry must be a whole number of seconds";
  }
  return null;
}

export function toMessage(draft: PublishDraft): OutgoingMessage {
  const properties = draft.advanced ? toProperties(draft) : null;
  return {
    topic: draft.topic,
    payload: draft.payload,
    qos: draft.qos,
    retain: draft.retain,
    properties: properties && !propertiesEmpty(properties) ? properties : null,
  };
}

function toProperties(draft: PublishDraft): OutgoingProperties {
  const text = (value: string) => value.trim() || null;
  return {
    contentType: text(draft.contentType),
    responseTopic: text(draft.responseTopic),
    correlationData: text(draft.correlationData),
    messageExpiryInterval: draft.expirySecs.trim()
      ? Number(draft.expirySecs.trim())
      : null,
    payloadIsUtf8: draft.payloadIsUtf8,
    userProperties: draft.userProperties.filter(([key]) => key.trim()),
  };
}

function propertiesEmpty(p: OutgoingProperties): boolean {
  return (
    !p.contentType &&
    !p.responseTopic &&
    !p.correlationData &&
    p.messageExpiryInterval == null &&
    !p.payloadIsUtf8 &&
    p.userProperties.length === 0
  );
}

/** A Publish History entry back in the form, ready to edit or send again. */
export function fromMessage(
  profileId: ProfileId,
  message: OutgoingMessage,
): PublishDraft {
  const p = message.properties;
  return {
    ...emptyDraft(profileId, message.topic),
    payload: message.payload,
    qos: message.qos,
    retain: message.retain,
    advanced: !!p,
    contentType: p?.contentType ?? "",
    responseTopic: p?.responseTopic ?? "",
    correlationData: p?.correlationData ?? "",
    expirySecs: p?.messageExpiryInterval?.toString() ?? "",
    payloadIsUtf8: p?.payloadIsUtf8 ?? false,
    userProperties: p?.userProperties ?? [],
  };
}

/** The payload pretty-printed if it is JSON, or null if it isn't. */
export function formatJson(payload: string): string | null {
  try {
    return JSON.stringify(JSON.parse(payload), null, 2);
  } catch {
    return null;
  }
}
