import { useVirtualizer } from "@tanstack/react-virtual";
import { useLayoutEffect, useRef, useState, useSyncExternalStore } from "react";
import type { ConnectionId, LogEntry } from "../bridge";
import type { MessageLogStore } from "./store";
import "./MessageLog.css";

const ROW_HEIGHT = 24;
/** How close to the bottom (in px) still counts as "at the bottom" for following. */
const FOLLOW_SLACK = ROW_HEIGHT * 2;

interface Props {
  store: MessageLogStore;
  connectionNames: Map<ConnectionId, string>;
}

export function MessageLog({ store, connectionNames }: Props) {
  const version = useSyncExternalStore(store.subscribe, store.getVersion);
  const scrollRef = useRef<HTMLDivElement>(null);
  const [following, setFollowing] = useState(true);

  // Not using the React Compiler, so its memoization caveat for TanStack Virtual does not apply.
  // eslint-disable-next-line react-hooks/incompatible-library
  const virtualizer = useVirtualizer({
    count: store.length,
    getScrollElement: () => scrollRef.current,
    estimateSize: () => ROW_HEIGHT,
    overscan: 20,
  });

  useLayoutEffect(() => {
    if (following && store.length > 0) {
      virtualizer.scrollToIndex(store.length - 1, { align: "end" });
    }
  }, [version, following, store, virtualizer]);

  const onScroll = () => {
    const el = scrollRef.current;
    if (!el) return;
    const atBottom =
      el.scrollHeight - el.scrollTop - el.clientHeight < FOLLOW_SLACK;
    if (atBottom !== following) setFollowing(atBottom);
  };

  return (
    <section className="message-log">
      <div className="message-log__header message-log__row">
        <span>Time</span>
        <span>Connection</span>
        <span>Topic</span>
        <span>Payload</span>
        <span className="message-log__num">Size</span>
        <span>Flags</span>
      </div>
      <div className="message-log__scroll" ref={scrollRef} onScroll={onScroll}>
        <div
          style={{ height: virtualizer.getTotalSize(), position: "relative" }}
        >
          {virtualizer.getVirtualItems().map((item) => {
            const entry = store.at(item.index);
            if (!entry) return null;
            return (
              <Row
                key={item.key}
                entry={entry}
                connectionName={
                  connectionNames.get(entry.connectionId) ??
                  `#${entry.connectionId}`
                }
                top={item.start}
              />
            );
          })}
        </div>
      </div>
      {!following && (
        <button
          className="message-log__follow"
          onClick={() => setFollowing(true)}
        >
          Jump to latest
        </button>
      )}
    </section>
  );
}

function Row({
  entry,
  connectionName,
  top,
}: {
  entry: LogEntry;
  connectionName: string;
  top: number;
}) {
  return (
    <div
      className="message-log__row"
      style={{ transform: `translateY(${top}px)` }}
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

function formatTime(ms: number): string {
  const d = new Date(ms);
  const pad = (n: number, width = 2) => String(n).padStart(width, "0");
  return `${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}.${pad(d.getMilliseconds(), 3)}`;
}

function formatSize(bytes: number): string {
  if (bytes < 1024) return `${bytes} B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KiB`;
  return `${(bytes / 1024 / 1024).toFixed(1)} MiB`;
}
