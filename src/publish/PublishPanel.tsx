import { useEffect, useState, type KeyboardEvent } from "react";
import {
  clearPublishHistory,
  publish,
  publishHistory,
  type ConnectionStatus,
  type ProfileView,
  type PublishRecord,
} from "../bridge";
import { formatTime } from "../format";
import {
  draftError,
  formatJson,
  fromMessage,
  toMessage,
  type PublishDraft,
} from "./draft";
import "./PublishPanel.css";

interface Props {
  draft: PublishDraft;
  onDraftChange: (draft: PublishDraft) => void;
  profiles: ProfileView[];
  /** Live Connections by Profile. */
  statuses: Map<string, ConnectionStatus>;
  onClose: () => void;
}

export function PublishPanel({
  draft,
  onDraftChange,
  profiles,
  statuses,
  onClose,
}: Props) {
  const [history, setHistory] = useState<PublishRecord[]>([]);
  const [result, setResult] = useState<{ ok: boolean; text: string } | null>(
    null,
  );
  const [sending, setSending] = useState(false);

  useEffect(() => {
    publishHistory()
      .then(setHistory)
      .catch((err) => console.error("publish_history failed", err));
  }, []);

  const connected = profiles.filter(
    (p) => statuses.get(p.id)?.state.kind === "connected",
  );
  const profile = profiles.find((p) => p.id === draft.profileId);
  const mqtt5 = profile?.protocol !== "mqtt311";
  const error = draftError(draft, mqtt5);
  const set = (change: Partial<PublishDraft>) => {
    setResult(null);
    onDraftChange({ ...draft, ...change });
  };

  const send = async (profileId: string, next: PublishDraft) => {
    setSending(true);
    try {
      setHistory(await publish(profileId, toMessage(next)));
      setResult({ ok: true, text: `Sent to ${next.topic}` });
    } catch (err) {
      setResult({ ok: false, text: String(err) });
    } finally {
      setSending(false);
    }
  };

  const submit = () => {
    if (!error && draft.profileId && !sending)
      void send(draft.profileId, draft);
  };
  const onKeyDown = (e: KeyboardEvent) => {
    if (e.key === "Enter" && (e.ctrlKey || e.metaKey)) {
      e.preventDefault();
      submit();
    }
  };
  const json = formatJson(draft.payload);

  return (
    <section className="publish" onKeyDown={onKeyDown}>
      <form
        className="publish__form"
        onSubmit={(e) => {
          e.preventDefault();
          submit();
        }}
      >
        <div className="publish__row">
          <select
            id="publish-connection"
            value={draft.profileId ?? ""}
            onChange={(e) => set({ profileId: e.target.value || null })}
            aria-label="Connection"
          >
            <option value="">Connection…</option>
            {connected.map((p) => (
              <option key={p.id} value={p.id}>
                {p.name}
              </option>
            ))}
          </select>
          <input
            id="publish-topic"
            className="publish__grow"
            placeholder="Topic, e.g. home/lamp/set"
            value={draft.topic}
            onChange={(e) => set({ topic: e.target.value })}
          />
          <select
            id="publish-qos"
            value={draft.qos}
            onChange={(e) => set({ qos: Number(e.target.value) })}
            aria-label="QoS"
          >
            <option value={0}>QoS 0</option>
            <option value={1}>QoS 1</option>
            <option value={2}>QoS 2</option>
          </select>
          <label className="publish__check">
            <input
              id="publish-retain"
              type="checkbox"
              checked={draft.retain}
              onChange={(e) => set({ retain: e.target.checked })}
            />
            Retain
          </label>
          <button type="button" className="icon-button" onClick={onClose}>
            ×
          </button>
        </div>
        <textarea
          id="publish-payload"
          className="publish__payload"
          placeholder="Payload"
          value={draft.payload}
          onChange={(e) => set({ payload: e.target.value })}
          spellCheck={false}
        />
        <label className="publish__check">
          <input
            id="publish-advanced"
            type="checkbox"
            checked={draft.advanced}
            onChange={(e) => set({ advanced: e.target.checked })}
          />
          MQTT 5 properties
        </label>
        {draft.advanced && <Advanced draft={draft} set={set} mqtt5={mqtt5} />}
        <div className="publish__actions">
          {json !== null && json !== draft.payload && (
            <button type="button" onClick={() => set({ payload: json })}>
              Format JSON
            </button>
          )}
          <span
            className={`publish__result${result && !result.ok ? " publish__result--error" : ""}`}
          >
            {result?.text ?? (draft.topic || draft.profileId ? error : null)}
          </span>
          <button
            type="submit"
            className="primary"
            disabled={!!error || sending}
            title="Ctrl+Enter"
          >
            Publish
          </button>
        </div>
      </form>
      <History
        history={history}
        profiles={profiles}
        onLoad={(record) =>
          onDraftChange(fromMessage(record.profileId, record.message))
        }
        onResend={(record) =>
          void send(
            record.profileId,
            fromMessage(record.profileId, record.message),
          )
        }
        onClear={() => {
          void clearPublishHistory().then(() => setHistory([]));
        }}
      />
    </section>
  );
}

function Advanced({
  draft,
  set,
  mqtt5,
}: {
  draft: PublishDraft;
  set: (change: Partial<PublishDraft>) => void;
  mqtt5: boolean;
}) {
  const setUserProperty = (i: number, key: string, value: string) =>
    set({
      userProperties: draft.userProperties.map((p, j) =>
        j === i ? [key, value] : p,
      ),
    });
  return (
    <fieldset className="publish__advanced" disabled={!mqtt5}>
      {!mqtt5 && (
        <p className="publish__hint">
          This Connection uses MQTT 3.1.1, which has no message properties.
        </p>
      )}
      <div className="publish__grid">
        <input
          id="publish-content-type"
          placeholder="Content type"
          value={draft.contentType}
          onChange={(e) => set({ contentType: e.target.value })}
        />
        <input
          id="publish-response-topic"
          placeholder="Response topic"
          value={draft.responseTopic}
          onChange={(e) => set({ responseTopic: e.target.value })}
        />
        <input
          id="publish-correlation-data"
          placeholder="Correlation data"
          value={draft.correlationData}
          onChange={(e) => set({ correlationData: e.target.value })}
        />
        <input
          id="publish-expiry"
          placeholder="Expires after (s)"
          inputMode="numeric"
          value={draft.expirySecs}
          onChange={(e) => set({ expirySecs: e.target.value })}
        />
      </div>
      <label className="publish__check">
        <input
          id="publish-utf8"
          type="checkbox"
          checked={draft.payloadIsUtf8}
          onChange={(e) => set({ payloadIsUtf8: e.target.checked })}
        />
        Payload is UTF-8 text
      </label>
      {draft.userProperties.map(([key, value], i) => (
        <div className="publish__row" key={i}>
          <input
            placeholder="Name"
            value={key}
            onChange={(e) => setUserProperty(i, e.target.value, value)}
          />
          <input
            className="publish__grow"
            placeholder="Value"
            value={value}
            onChange={(e) => setUserProperty(i, key, e.target.value)}
          />
          <button
            type="button"
            onClick={() =>
              set({
                userProperties: draft.userProperties.filter((_, j) => j !== i),
              })
            }
          >
            Remove
          </button>
        </div>
      ))}
      <button
        type="button"
        onClick={() =>
          set({ userProperties: [...draft.userProperties, ["", ""]] })
        }
      >
        Add user property
      </button>
    </fieldset>
  );
}

function History({
  history,
  profiles,
  onLoad,
  onResend,
  onClear,
}: {
  history: PublishRecord[];
  profiles: ProfileView[];
  onLoad: (record: PublishRecord) => void;
  onResend: (record: PublishRecord) => void;
  onClear: () => void;
}) {
  const names = new Map(profiles.map((p) => [p.id, p.name]));
  return (
    <section className="publish__history">
      <div className="publish__history-title">
        <span>Publish History</span>
        {history.length > 0 && (
          <button type="button" className="icon-button" onClick={onClear}>
            Clear
          </button>
        )}
      </div>
      {history.length === 0 ? (
        <p className="publish__hint">Messages you publish appear here.</p>
      ) : (
        <ol>
          {history.map((record) => (
            <li
              key={`${record.atMs}-${record.message.topic}`}
              onClick={() => onLoad(record)}
              title="Load into the form"
            >
              <div className="publish__history-line">
                <span className="publish__time">{formatTime(record.atMs)}</span>
                <code className="publish__topic">{record.message.topic}</code>
                {record.message.retain && <span className="badge">R</span>}
                {record.message.qos > 0 && (
                  <span className="badge">Q{record.message.qos}</span>
                )}
                <button
                  type="button"
                  onClick={(e) => {
                    e.stopPropagation();
                    onResend(record);
                  }}
                >
                  Send again
                </button>
              </div>
              <div className="publish__history-line publish__muted">
                <span>{names.get(record.profileId) ?? "Deleted Profile"}</span>
                <span className="publish__preview">
                  {record.message.payload || "(empty)"}
                </span>
              </div>
            </li>
          ))}
        </ol>
      )}
    </section>
  );
}
