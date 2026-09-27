import { useVirtualizer } from "@tanstack/react-virtual";
import {
  useEffect,
  useLayoutEffect,
  useRef,
  useState,
  useSyncExternalStore,
  type KeyboardEvent,
  type WheelEvent,
} from "react";
import {
  exportLog,
  messageDetail,
  type ConnectionId,
  type LogEntry,
  type MarkerEntry,
} from "../bridge";
import { payloadAsText } from "../topicDetail/format";
import { copy } from "../ui/clipboard";
import { ContextMenu } from "../ui/ContextMenu";
import { chooseSavePath } from "../ui/dialogs";
import { toast } from "../ui/toast";
import { formatSize, formatTime } from "../format";
import type { MessageLogController } from "./controller";
import { describeMarker } from "./markers";
import "./MessageLog.css";

const ROW_HEIGHT = 24;
/** How close to the bottom (in px) still counts as "at the bottom" for following. */
const FOLLOW_SLACK = ROW_HEIGHT * 2;
/** How close to the top (in px) starts loading the previous page. */
const LOAD_OLDER_SLACK = ROW_HEIGHT * 20;
const FILTER_DEBOUNCE_MS = 250;

interface Props {
  controller: MessageLogController;
  connectionNames: Map<ConnectionId, string>;
}

export function MessageLog({ controller, connectionNames }: Props) {
  const { store } = controller;
  const version = useSyncExternalStore(store.subscribe, store.getVersion);
  const scrollRef = useRef<HTMLDivElement>(null);
  const [menu, setMenu] = useState<{
    entry: LogEntry;
    x: number;
    y: number;
  } | null>(null);

  // Not using the React Compiler, so its memoization caveat for TanStack Virtual does not apply.
  // eslint-disable-next-line react-hooks/incompatible-library
  const virtualizer = useVirtualizer({
    count: store.length,
    getScrollElement: () => scrollRef.current,
    estimateSize: () => ROW_HEIGHT,
    overscan: 20,
  });

  useLayoutEffect(() => {
    const el = scrollRef.current;
    if (!el) return;
    const prepended = store.takeScrollAdjustment();
    if (prepended > 0) {
      // Keep the rows the reader was looking at in place.
      el.scrollTop += prepended * ROW_HEIGHT;
    } else if (store.following && store.length > 0) {
      // Set directly rather than through `virtualizer.scrollToIndex`, which re-applies its
      // scroll over the next frames and could pull a reader who just scrolled up back down.
      // Rows have a fixed height, so the full height is already right.
      el.scrollTop = el.scrollHeight;
    }
  }, [version, store]);

  // While following, the view is re-pinned to the bottom on every batch, which can undo a
  // scroll before its `scroll` event is handled. So pause on what the user does, not only on
  // where the scroll position ends up.
  const onWheel = (e: WheelEvent) => {
    if (e.deltaY < 0) store.pause();
  };
  const onKeyDown = (e: KeyboardEvent) => {
    if (["ArrowUp", "PageUp", "Home"].includes(e.key)) store.pause();
  };
  // Also lets the reader select text without it moving away.
  const onMouseDown = () => store.pause();

  const onScroll = () => {
    const el = scrollRef.current;
    if (!el) return;
    const atBottom =
      el.scrollHeight - el.scrollTop - el.clientHeight < FOLLOW_SLACK;
    if (store.following) {
      if (!atBottom) store.pause();
    } else if (atBottom) {
      if (!controller.isLoadingLatest) void controller.showLatest();
    } else if (el.scrollTop < LOAD_OLDER_SLACK) {
      void controller.loadOlder();
    }
  };

  return (
    <section className="message-log">
      <FilterBar controller={controller} connectionNames={connectionNames} />
      <div className="message-log__header message-log__row">
        <span>Time</span>
        <span>Connection</span>
        <span>Topic</span>
        <span>Payload</span>
        <span className="message-log__num">Size</span>
        <span>Flags</span>
      </div>
      <div
        className="message-log__scroll"
        ref={scrollRef}
        tabIndex={0}
        onScroll={onScroll}
        onWheel={onWheel}
        onKeyDown={onKeyDown}
        onMouseDown={onMouseDown}
      >
        <div
          style={{ height: virtualizer.getTotalSize(), position: "relative" }}
        >
          {virtualizer.getVirtualItems().map((item) => {
            const row = store.at(item.index);
            if (!row) return null;
            const connectionName =
              connectionNames.get(row.connectionId) ?? `#${row.connectionId}`;
            return row.kind === "marker" ? (
              <MarkerRow
                key={row.seq}
                marker={row}
                connectionName={connectionName}
                top={item.start}
              />
            ) : (
              <Row
                key={row.seq}
                entry={row}
                connectionName={connectionName}
                top={item.start}
                onContextMenu={(x, y) => {
                  // Keep the row where it is while its menu is open.
                  store.pause();
                  setMenu({ entry: row, x, y });
                }}
              />
            );
          })}
        </div>
      </div>
      {menu && (
        <ContextMenu
          x={menu.x}
          y={menu.y}
          onClose={() => setMenu(null)}
          items={[
            {
              label: "Copy topic",
              onSelect: () => void copy(menu.entry.topic, "topic"),
            },
            {
              label: "Copy payload",
              onSelect: () => void copyPayload(menu.entry),
            },
          ]}
        />
      )}
      {!store.following && (
        <button
          className="message-log__follow"
          onClick={() => void controller.showLatest()}
        >
          Jump to latest
          {store.newWhilePaused > 0 &&
            ` (${store.newWhilePaused.toLocaleString()} new)`}
        </button>
      )}
    </section>
  );
}

function MarkerRow({
  marker,
  connectionName,
  top,
}: {
  marker: MarkerEntry;
  connectionName: string;
  top: number;
}) {
  const { text, tone } = describeMarker(marker.event);
  return (
    <div
      className={`message-log__marker message-log__marker--${tone}`}
      style={{ transform: `translateY(${top}px)` }}
    >
      <span className="message-log__time">{formatTime(marker.atMs)}</span>
      <span className="message-log__marker-text">
        <strong>{connectionName}</strong> {text}
      </span>
    </div>
  );
}

/** `maqtt-log-2026-09-27-1253.jsonl` in local time. */
function defaultExportName(): string {
  const d = new Date();
  const pad = (n: number) => String(n).padStart(2, "0");
  return `maqtt-log-${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())}-${pad(d.getHours())}${pad(d.getMinutes())}.jsonl`;
}

async function exportFiltered(controller: MessageLogController) {
  const path = await chooseSavePath({
    title: "Export the Message Log",
    defaultPath: defaultExportName(),
    filters: [{ name: "JSON Lines", extensions: ["jsonl"] }],
  });
  if (!path) return;
  try {
    const count = await exportLog(controller.currentFilter, path);
    toast(`Exported ${count.toLocaleString()} rows to ${path}`);
  } catch (err) {
    toast(`Couldn't export: ${err}`, true);
  }
}

function FilterBar({ controller, connectionNames }: Props) {
  const [connectionId, setConnectionId] = useState<ConnectionId | null>(null);
  const [text, setText] = useState("");

  useEffect(() => {
    const timer = setTimeout(() => {
      void controller.setFilter({ connectionId, text: text || null });
    }, FILTER_DEBOUNCE_MS);
    return () => clearTimeout(timer);
  }, [controller, connectionId, text]);

  return (
    <div className="message-log__filters">
      <select
        value={connectionId ?? ""}
        onChange={(e) =>
          setConnectionId(e.target.value ? Number(e.target.value) : null)
        }
      >
        <option value="">All Connections</option>
        {[...connectionNames].map(([id, name]) => (
          <option key={id} value={id}>
            {name}
          </option>
        ))}
      </select>
      <input
        type="search"
        placeholder="Filter topic or payload…"
        value={text}
        onChange={(e) => setText(e.target.value)}
      />
      <button
        className="message-log__export"
        onClick={() => void exportFiltered(controller)}
        title="Everything the filter matches in the history, as JSON Lines"
      >
        Export…
      </button>
    </div>
  );
}

async function copyPayload(entry: LogEntry) {
  const detail = await messageDetail(entry.connectionId, entry.seq);
  if (detail) await copy(payloadAsText(detail.payload), "payload");
  else toast("That message has left the history", true);
}

function Row({
  entry,
  connectionName,
  top,
  onContextMenu,
}: {
  entry: LogEntry;
  connectionName: string;
  top: number;
  onContextMenu: (x: number, y: number) => void;
}) {
  return (
    <div
      className="message-log__row"
      style={{ transform: `translateY(${top}px)` }}
      onContextMenu={(e) => {
        e.preventDefault();
        onContextMenu(e.clientX, e.clientY);
      }}
    >
      <span className="message-log__time">
        {formatTime(entry.receivedAtMs)}
      </span>
      <span title={connectionName}>{connectionName}</span>
      <span title={entry.topic}>{entry.topic}</span>
      <span className={entry.binary ? "message-log__binary" : undefined}>
        {entry.preview}
      </span>
      <span className="message-log__num">{formatSize(entry.payloadSize)}</span>
      <span className="message-log__flags">
        {entry.retain && <span className="badge">R</span>}
        {entry.qos > 0 && <span className="badge">Q{entry.qos}</span>}
      </span>
    </div>
  );
}
