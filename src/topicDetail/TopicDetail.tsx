import {
  useEffect,
  useMemo,
  useState,
  useSyncExternalStore,
  type ReactNode,
} from "react";
import {
  messageDetail,
  topicHistory,
  type LogEntry,
  type MessageDetail,
  type MessageProperties,
  type PayloadData,
} from "../bridge";
import type { TopicTreeStore, TreeNode } from "../topicTree/store";
import { jsonDiff, lineDiff, type JsonChange } from "./diff";
import {
  detectFormat,
  formatPayload,
  parseJson,
  type PayloadFormat,
} from "./format";
import { formatDateTime, formatSize, formatTime } from "../format";
import { copy } from "../ui/clipboard";
import "./TopicDetail.css";

/** Busy topics refresh at most this often. */
const REFRESH_MS = 500;
const MAX_DIFF_LINES = 2_000;

interface Props {
  node: TreeNode;
  tree: TopicTreeStore;
  connectionName: string | undefined;
  /** Whether the node's Connection is online, so it can publish. */
  online: boolean;
  onPublish: () => void;
  confirmingClear: boolean;
  onClearRetained: () => void;
  onConfirmClear: () => void;
  onCancelClear: () => void;
}

export function TopicDetail({
  node,
  tree,
  connectionName,
  online,
  onPublish,
  confirmingClear,
  onClearRetained,
  onConfirmClear,
  onCancelClear,
}: Props) {
  // Re-render as the node's stats change; the node object itself is updated in place.
  useSyncExternalStore(tree.subscribe, tree.getVersion);
  const latestSeq = node.topic?.latest.seq;
  const rate = useRate(node);

  return (
    <aside className="topic-detail">
      <header className="topic-detail__header">
        <code className="topic-detail__topic">
          {node.path || connectionName}
        </code>
        <div className="topic-detail__stats">
          <span>{connectionName}</span>
          {node.topic?.retained && <span className="badge">Retained</span>}
          <span>{node.messageCount.toLocaleString()} msgs</span>
          {node.topic && <span>{formatRate(rate)} msg/s</span>}
          {node.children.size > 0 && (
            <span>{node.topicCount.toLocaleString()} topics below</span>
          )}
        </div>
        {node.depth > 0 && (
          <div className="topic-detail__actions">
            <button onClick={() => void copy(node.path, "topic")}>
              Copy topic
            </button>
            <button onClick={onPublish} disabled={!online}>
              Publish…
            </button>
            {node.topic?.retained && !confirmingClear && (
              <button
                className="topic-detail__danger"
                onClick={onClearRetained}
                disabled={!online}
              >
                Clear retained
              </button>
            )}
          </div>
        )}
        {confirmingClear && node.topic?.retained && (
          <div className="topic-detail__confirm" role="alertdialog">
            <span>
              Clear the retained message? The Broker stops sending it to new
              subscribers.
            </span>
            <button
              className="topic-detail__danger"
              onClick={onConfirmClear}
              disabled={!online}
            >
              Clear
            </button>
            <button onClick={onCancelClear}>Cancel</button>
          </div>
        )}
      </header>
      {node.topic && latestSeq !== undefined ? (
        <Messages node={node} latestSeq={latestSeq} />
      ) : (
        <p className="topic-detail__hint">
          No messages on this exact topic. Pick a topic below it to see its
          messages.
        </p>
      )}
    </aside>
  );
}

/** How far back the message rate looks, so slow topics don't flicker between 0 and 1. */
const RATE_WINDOW_S = 10;

/** Messages per second on this topic, averaged over the last few seconds of its live count. */
function useRate(node: TreeNode): number {
  const [rate, setRate] = useState(0);
  useEffect(() => {
    const samples = [{ at: Date.now(), count: node.topic?.count ?? 0 }];
    const timer = setInterval(() => {
      samples.push({ at: Date.now(), count: node.topic?.count ?? 0 });
      if (samples.length > RATE_WINDOW_S + 1) samples.shift();
      const first = samples[0];
      const last = samples[samples.length - 1];
      setRate(((last.count - first.count) * 1000) / (last.at - first.at));
    }, 1000);
    return () => clearInterval(timer);
  }, [node]);
  return rate;
}

function formatRate(rate: number): string {
  return rate < 10 ? rate.toFixed(1) : Math.round(rate).toLocaleString();
}

function Messages({ node, latestSeq }: { node: TreeNode; latestSeq: number }) {
  const history = useTopicHistory(node, latestSeq);
  const [pinnedSeq, setPinnedSeq] = useState<number | null>(null);
  const selectedSeq = pinnedSeq ?? latestSeq;
  // History is oldest first; the previous message is the one just before the selected one.
  const index = history.findIndex((e) => e.seq === selectedSeq);
  const previousSeq = index > 0 ? history[index - 1].seq : null;
  const [selected, previous] = useDetails(
    node.connectionId,
    selectedSeq,
    previousSeq,
  );

  return (
    <>
      <History
        history={history}
        selectedSeq={selectedSeq}
        pinned={pinnedSeq !== null}
        onPick={(seq) => setPinnedSeq(seq === latestSeq ? null : seq)}
        onFollow={() => setPinnedSeq(null)}
      />
      {selected ? (
        <Message detail={selected} previous={previous} />
      ) : (
        <p className="topic-detail__hint">This message has left the history.</p>
      )}
    </>
  );
}

/** The topic's history from the History Store, refreshed as new messages arrive. */
function useTopicHistory(node: TreeNode, latestSeq: number): LogEntry[] {
  const [history, setHistory] = useState<LogEntry[]>([]);
  const [fetchedAt, setFetchedAt] = useState(0);

  useEffect(() => {
    let cancelled = false;
    const wait = Math.max(0, fetchedAt + REFRESH_MS - Date.now());
    const timer = setTimeout(() => {
      topicHistory(node.connectionId, node.path)
        .then((entries) => {
          if (cancelled) return;
          setHistory(entries);
          setFetchedAt(Date.now());
        })
        .catch((err) => console.error("topic_history failed", err));
    }, wait);
    return () => {
      cancelled = true;
      clearTimeout(timer);
    };
    // `fetchedAt` only paces the next fetch; a new message is what triggers one.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [node.connectionId, node.path, latestSeq]);

  return history;
}

function useDetails(
  connectionId: number,
  seq: number,
  previousSeq: number | null,
): [MessageDetail | null, MessageDetail | null] {
  const [details, setDetails] = useState<
    [MessageDetail | null, MessageDetail | null]
  >([null, null]);

  useEffect(() => {
    let cancelled = false;
    Promise.all([
      messageDetail(connectionId, seq),
      previousSeq === null
        ? Promise.resolve(null)
        : messageDetail(connectionId, previousSeq),
    ])
      .then((result) => {
        if (!cancelled) setDetails(result);
      })
      .catch((err) => console.error("message_detail failed", err));
    return () => {
      cancelled = true;
    };
  }, [connectionId, seq, previousSeq]);

  return details;
}

function History({
  history,
  selectedSeq,
  pinned,
  onPick,
  onFollow,
}: {
  history: LogEntry[];
  selectedSeq: number;
  pinned: boolean;
  onPick: (seq: number) => void;
  onFollow: () => void;
}) {
  return (
    <section className="topic-detail__history">
      <div className="topic-detail__section-title">
        <span>History ({history.length})</span>
        {pinned && <button onClick={onFollow}>Follow latest</button>}
      </div>
      <ol>
        {[...history].reverse().map((entry) => (
          <li
            key={entry.seq}
            className={
              entry.seq === selectedSeq
                ? "topic-detail__history-item--selected"
                : undefined
            }
            onClick={() => onPick(entry.seq)}
          >
            <span className="topic-detail__time">
              {formatTime(entry.receivedAtMs)}
            </span>
            <span className="topic-detail__preview">{entry.preview}</span>
            {entry.retain && <span className="badge">R</span>}
          </li>
        ))}
      </ol>
    </section>
  );
}

type FormatChoice = "auto" | PayloadFormat;

function Message({
  detail,
  previous,
}: {
  detail: MessageDetail;
  previous: MessageDetail | null;
}) {
  const [choice, setChoice] = useState<FormatChoice>("auto");
  const [showDiff, setShowDiff] = useState(true);
  const format = choice === "auto" ? detectFormat(detail.payload) : choice;

  return (
    <section className="topic-detail__message">
      <div className="topic-detail__meta">
        <span>{formatDateTime(detail.receivedAtMs)}</span>
        <span>QoS {detail.qos}</span>
        {detail.retain && <span>Retained</span>}
        <span>{formatSize(detail.payloadSize)}</span>
        {detail.truncated && (
          <span className="topic-detail__warning">Shown cut to 4 MiB</span>
        )}
      </div>
      {detail.properties && <Properties properties={detail.properties} />}
      <div className="topic-detail__toolbar">
        {(["auto", "json", "text", "hex"] as const).map((option) => (
          <button
            key={option}
            className={choice === option ? "is-active" : undefined}
            onClick={() => setChoice(option)}
          >
            {option === "auto" ? `Auto (${format})` : option.toUpperCase()}
          </button>
        ))}
        <button
          onClick={() =>
            void copy(formatPayload(detail.payload, format), "payload")
          }
        >
          Copy
        </button>
        <label className="topic-detail__diff-toggle">
          <input
            type="checkbox"
            checked={showDiff}
            disabled={!previous}
            onChange={(e) => setShowDiff(e.target.checked)}
          />
          Diff with previous
        </label>
      </div>
      {showDiff && previous ? (
        <Diff before={previous} after={detail} format={format} />
      ) : (
        <pre className="topic-detail__payload">
          {formatPayload(detail.payload, format)}
        </pre>
      )}
    </section>
  );
}

function Diff({
  before,
  after,
  format,
}: {
  before: MessageDetail;
  after: MessageDetail;
  format: PayloadFormat;
}) {
  const content = useMemo(() => {
    const a = parseJson(before.payload);
    const b = parseJson(after.payload);
    if (format === "json" && a !== undefined && b !== undefined) {
      return { kind: "json" as const, changes: jsonDiff(a, b) };
    }
    const lines = lineDiff(
      formatPayload(before.payload, format),
      formatPayload(after.payload, format),
    );
    return { kind: "lines" as const, lines };
  }, [before, after, format]);

  if (content.kind === "json") {
    return (
      <>
        <JsonChanges changes={content.changes} />
        <pre className="topic-detail__payload">
          {formatPayload(after.payload, format)}
        </pre>
      </>
    );
  }
  const unchanged = content.lines.every((l) => l.kind === "same");
  return (
    <pre className="topic-detail__payload topic-detail__line-diff">
      {unchanged && (
        <div className="topic-detail__hint">Same as the previous message</div>
      )}
      {content.lines.slice(0, MAX_DIFF_LINES).map((line, i) => (
        <div key={i} className={`diff-line diff-line--${line.kind}`}>
          {line.kind === "added" ? "+ " : line.kind === "removed" ? "- " : "  "}
          {line.text}
        </div>
      ))}
    </pre>
  );
}

function JsonChanges({ changes }: { changes: JsonChange[] }) {
  if (changes.length === 0) {
    return <p className="topic-detail__hint">Same as the previous message</p>;
  }
  return (
    <table className="topic-detail__changes">
      <thead>
        <tr>
          <th>Field</th>
          <th>Before</th>
          <th>After</th>
        </tr>
      </thead>
      <tbody>
        {changes.map((change) => (
          <tr key={change.path} className={`change--${change.kind}`}>
            <td>
              <code>{change.path}</code>
            </td>
            <td>
              {change.kind === "added" ? "—" : JSON.stringify(change.before)}
            </td>
            <td>
              {change.kind === "removed" ? "—" : JSON.stringify(change.after)}
            </td>
          </tr>
        ))}
      </tbody>
    </table>
  );
}

function Properties({ properties }: { properties: MessageProperties }) {
  const rows: [string, ReactNode][] = [];
  if (properties.contentType)
    rows.push(["Content type", properties.contentType]);
  if (properties.payloadFormatIndicator != null)
    rows.push([
      "Payload format",
      properties.payloadFormatIndicator === 1 ? "UTF-8" : "Bytes",
    ]);
  if (properties.responseTopic)
    rows.push(["Response topic", <code>{properties.responseTopic}</code>]);
  if (properties.correlationData)
    rows.push(["Correlation data", showPayload(properties.correlationData)]);
  if (properties.messageExpiryInterval != null)
    rows.push(["Expires in", `${properties.messageExpiryInterval} s`]);
  for (const [key, value] of properties.userProperties) rows.push([key, value]);
  if (rows.length === 0) return null;
  return (
    <table className="topic-detail__properties">
      <tbody>
        {rows.map(([name, value], i) => (
          <tr key={i}>
            <th>{name}</th>
            <td>{value}</td>
          </tr>
        ))}
      </tbody>
    </table>
  );
}

function showPayload(payload: PayloadData): string {
  return payload.kind === "text" ? payload.text : formatPayload(payload, "hex");
}
