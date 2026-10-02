import { useVirtualizer } from "@tanstack/react-virtual";
import {
  type MouseEvent,
  useEffect,
  useMemo,
  useRef,
  useState,
  useSyncExternalStore,
} from "react";
import type { ConnectionId, ConnectionState, TopicCheck } from "../bridge";
import { plural } from "../format";
import { useNow } from "../ui/useNow";
import { ActivityMeter } from "./ActivityMeter";
import {
  GROUP_EXPAND_LIMIT,
  type ExpandMode,
  type TopicTreeStore,
  type TreeNode,
} from "./store";
import "./TopicTree.css";

const ROW_HEIGHT = 24;
const INDENT_PX = 16;
const SEARCH_DEBOUNCE_MS = 150;

interface Props {
  store: TopicTreeStore;
  connectionNames: Map<ConnectionId, string>;
  connectionStates: Map<ConnectionId, ConnectionState>;
  selectedKey: string | null;
  onSelect: (node: TreeNode) => void;
  onContextMenu: (node: TreeNode, x: number, y: number) => void;
  /**
   * A label for a branch whose topics have gone quiet for a reason the tree can't see, such as
   * a stopped Pipeline. The branch shows it and is dimmed.
   */
  branchLabel?: (node: TreeNode) => string | null;
  /** A Connection whose root isn't shown, such as the Pipelines root when they're off. */
  hidden?: ConnectionId;
  /** The user picked how the tree opens itself, to be remembered. */
  onExpandMode?: (mode: ExpandMode) => void;
}

/** The label of the node or its nearest labelled ancestor, and whether it's the node's own. */
function labelFor(
  node: TreeNode,
  branchLabel: Props["branchLabel"],
): { label: string; own: boolean } | null {
  if (!branchLabel) return null;
  for (let n: TreeNode | null = node; n && n.depth > 0; n = n.parent) {
    const label = branchLabel(n);
    if (label) return { label, own: n === node };
  }
  return null;
}

export function TopicTree({
  store,
  connectionNames,
  connectionStates,
  selectedKey,
  onSelect,
  onContextMenu,
  branchLabel,
  hidden,
  onExpandMode,
}: Props) {
  const version = useSyncExternalStore(store.subscribe, store.getVersion);
  const rows = useMemo(
    () =>
      hidden === undefined
        ? store.rows()
        : store.rows().filter((row) => row.connectionId !== hidden),
    // `version` stands for the store's contents, which `rows()` reads.
    [store, version, hidden], // eslint-disable-line react-hooks/exhaustive-deps
  );
  const scrollRef = useRef<HTMLDivElement>(null);
  const now = useNow();

  // Not using the React Compiler, so its memoization caveat for TanStack Virtual does not apply.
  // eslint-disable-next-line react-hooks/incompatible-library
  const virtualizer = useVirtualizer({
    count: rows.length,
    getScrollElement: () => scrollRef.current,
    estimateSize: () => ROW_HEIGHT,
    overscan: 20,
  });

  return (
    <section className="topic-tree">
      <SearchBar store={store} onExpandMode={onExpandMode} />
      <div className="topic-tree__scroll" ref={scrollRef}>
        {rows.length === 0 && (
          <p className="topic-tree__empty">
            {store.topicCount === 0
              ? "No topics yet. Connect to a Broker to see its topics here."
              : "No topics match."}
          </p>
        )}
        <div
          style={{ height: virtualizer.getTotalSize(), position: "relative" }}
        >
          {virtualizer.getVirtualItems().map((item) => {
            const node = rows[item.index];
            return (
              <Row
                key={node.key}
                node={node}
                top={item.start}
                expanded={store.isExpanded(node)}
                selected={node.key === selectedKey}
                now={now}
                rootName={connectionNames.get(node.connectionId)}
                rootState={connectionStates.get(node.connectionId)}
                branch={labelFor(node, branchLabel)}
                check={store.checkOf(node)}
                onToggle={() => store.toggle(node)}
                onSelect={() => onSelect(node)}
                onContextMenu={(x, y) => onContextMenu(node, x, y)}
              />
            );
          })}
        </div>
      </div>
    </section>
  );
}

function SearchBar({
  store,
  onExpandMode,
}: {
  store: TopicTreeStore;
  onExpandMode?: (mode: ExpandMode) => void;
}) {
  const [query, setQuery] = useState("");
  const setMode = (mode: ExpandMode) => {
    store.setExpandMode(mode);
    onExpandMode?.(mode);
  };

  useEffect(() => {
    const timer = setTimeout(() => store.setQuery(query), SEARCH_DEBOUNCE_MS);
    return () => clearTimeout(timer);
  }, [store, query]);

  return (
    <div className="topic-tree__search">
      <input
        type="search"
        placeholder="Search topics, or filter with + and #…"
        value={query}
        onChange={(e) => setQuery(e.target.value)}
      />
      <select
        id="topic-tree-expand"
        value={store.expandMode}
        onChange={(e) => setMode(e.target.value as ExpandMode)}
        aria-label="How the tree opens"
        title={`Groups: open every level except the one holding the topics. Groups with more than ${GROUP_EXPAND_LIMIT} entries stay closed.`}
        disabled={query.trim() !== ""}
      >
        <option value="manual">Open by hand</option>
        <option value="groups">Open groups</option>
        <option value="all">Open everything</option>
      </select>
      <span className="topic-tree__count">
        {store.queryError ??
          (query.trim()
            ? `${store.matchCount.toLocaleString()} of ${store.topicCount.toLocaleString()} topics`
            : plural(store.topicCount, "topic"))}
      </span>
    </div>
  );
}

interface RowProps {
  node: TreeNode;
  top: number;
  expanded: boolean;
  selected: boolean;
  now: number;
  rootName: string | undefined;
  rootState: ConnectionState | undefined;
  branch: { label: string; own: boolean } | null;
  onToggle: () => void;
  onSelect: () => void;
  onContextMenu: (x: number, y: number) => void;
  /** How its messages compare with the Broker's API Specs, when they're checked. */
  check: TopicCheck | null;
}

function Row({
  node,
  top,
  expanded,
  selected,
  now,
  rootName,
  rootState,
  branch,
  onToggle,
  onSelect,
  onContextMenu,
  check,
}: RowProps) {
  const isRoot = node.depth === 0;
  const hasChildren = node.children.size > 0;
  const toggle = (e: MouseEvent) => {
    e.stopPropagation();
    onToggle();
  };
  const className = [
    "topic-tree__row",
    selected && "topic-tree__row--selected",
    isRoot && "topic-tree__row--root",
    branch && "topic-tree__row--quiet",
  ]
    .filter(Boolean)
    .join(" ");

  return (
    <div
      className={className}
      style={{
        transform: `translateY(${top}px)`,
        paddingLeft: node.depth * INDENT_PX + 8,
      }}
      onClick={onSelect}
      onDoubleClick={onToggle}
      onContextMenu={(e) => {
        e.preventDefault();
        onSelect();
        onContextMenu(e.clientX, e.clientY);
      }}
    >
      <button
        className="topic-tree__expander"
        onClick={toggle}
        tabIndex={-1}
        style={{ visibility: hasChildren ? "visible" : "hidden" }}
      >
        {expanded ? "▾" : "▸"}
      </button>
      {isRoot && (
        <span
          className={`status-dot status-dot--${rootState?.kind ?? "closed"}`}
        />
      )}
      <span className="topic-tree__name" title={node.path || rootName}>
        {isRoot
          ? (rootName ?? `Connection ${node.connectionId}`)
          : node.name || <em>(empty)</em>}
      </span>
      {node.topic && !isRoot && (
        <span
          className={`topic-tree__value${node.topic.latest.binary ? " topic-tree__value--binary" : ""}`}
        >
          {node.topic.latest.preview}
        </span>
      )}
      <span className="topic-tree__meta">
        {branch?.own && <span className="badge">{branch.label}</span>}
        <ActivityMeter node={node} now={now} />
        {check && check.mismatched > 0 && (
          <span
            className="badge badge--bad"
            title={`${check.mismatched.toLocaleString()} of ${check.messages.toLocaleString()} messages differ from the spec: ${check.problem ?? ""}`}
          >
            ≠ spec
          </span>
        )}
        {node.topic?.retained && (
          <span className="badge" title="The Broker holds a Retained Message">
            R
          </span>
        )}
        {hasChildren ? (
          <span title={`${node.messageCount.toLocaleString()} messages`}>
            {plural(node.topicCount, "topic")}
          </span>
        ) : (
          <span>{plural(node.messageCount, "msg")}</span>
        )}
      </span>
    </div>
  );
}
