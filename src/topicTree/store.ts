import type { ConnectionId, LogEntry, TopicUpdate } from "../bridge";
import { isTopicFilter, topicFilterError, topicMatches } from "./match";

/** A topic's current state, present on nodes where messages were received. */
export interface TopicInfo {
  count: number;
  retained: boolean;
  latest: LogEntry;
  /** When this node last changed, for highlighting fresh values. */
  changedAt: number;
}

export interface TreeNode {
  /** Unique across Connections. */
  key: string;
  connectionId: ConnectionId;
  /** This level's name; the root's is empty. */
  name: string;
  /** The full topic up to this level; the root's is empty. */
  path: string;
  depth: number;
  parent: TreeNode | null;
  children: Map<string, TreeNode>;
  topic: TopicInfo | null;
  /** Topics at or below this node. */
  topicCount: number;
  /** Messages received at or below this node. */
  messageCount: number;
}

const collator = new Intl.Collator(undefined, { numeric: true });

/**
 * The Topic Tree: one root per Connection, topics grouped by `/` levels. Updates carry absolute
 * stats, so applying them in any order converges; the newest `latest.seq` wins.
 */
export class TopicTreeStore {
  private readonly roots = new Map<ConnectionId, TreeNode>();
  private readonly topicNodes: TreeNode[] = [];
  private readonly expanded = new Set<string>();
  private readonly sortedChildren = new WeakMap<TreeNode, TreeNode[]>();
  private query = "";
  private cachedRows: TreeNode[] | null = null;
  private version = 0;
  private readonly listeners = new Set<() => void>();

  /** Why the search query is not a valid topic filter, if it is meant as one. */
  queryError: string | null = null;
  /** Topics matching the search query; all topics when there is none. */
  matchCount = 0;

  get topicCount(): number {
    return this.topicNodes.length;
  }

  apply(updates: TopicUpdate[], now = Date.now()): void {
    if (updates.length === 0) return;
    for (const update of updates) this.applyOne(update, now);
    this.notify();
  }

  /** The rows to show: expanded nodes, or while searching, matches and their ancestors. */
  rows(): TreeNode[] {
    this.cachedRows ??= this.query ? this.searchRows() : this.expandedRows();
    return this.cachedRows;
  }

  isExpanded(node: TreeNode): boolean {
    return this.query !== "" || this.expanded.has(node.key);
  }

  toggle(node: TreeNode): void {
    if (node.children.size === 0 || this.query) return;
    if (!this.expanded.delete(node.key)) this.expanded.add(node.key);
    this.structureChanged();
    this.notify();
  }

  setQuery(query: string): void {
    const trimmed = query.trim();
    if (trimmed === this.query) return;
    this.query = trimmed;
    this.queryError =
      trimmed && isTopicFilter(trimmed) ? topicFilterError(trimmed) : null;
    this.structureChanged();
    this.notify();
  }

  subscribe = (listener: () => void): (() => void) => {
    this.listeners.add(listener);
    return () => this.listeners.delete(listener);
  };

  getVersion = (): number => this.version;

  private applyOne(update: TopicUpdate, now: number): void {
    const node = this.nodeFor(update.connectionId, update.topic);
    const previous = node.topic;
    if (previous && update.latest.seq < previous.latest.seq) return;

    const addedMessages = update.count - (previous?.count ?? 0);
    node.topic = {
      count: update.count,
      retained: update.retained,
      latest: update.latest,
      changedAt: now,
    };
    if (!previous) this.topicNodes.push(node);
    for (let n: TreeNode | null = node; n; n = n.parent) {
      n.messageCount += addedMessages;
      if (!previous) n.topicCount += 1;
    }
    // A new topic may add a row, or change which rows a search shows.
    if (!previous) this.structureChanged();
  }

  private nodeFor(connectionId: ConnectionId, topic: string): TreeNode {
    let root = this.roots.get(connectionId);
    if (!root) {
      root = newNode(connectionId, "", "", 0, null);
      this.roots.set(connectionId, root);
      this.expanded.add(root.key);
      this.structureChanged();
    }
    let node: TreeNode = root;
    const levels = topic.split("/");
    for (const [i, level] of levels.entries()) {
      let child: TreeNode | undefined = node.children.get(level);
      if (!child) {
        const path = levels.slice(0, i + 1).join("/");
        child = newNode(connectionId, level, path, i + 1, node);
        node.children.set(level, child);
        this.sortedChildren.delete(node);
      }
      node = child;
    }
    return node;
  }

  private childrenOf(node: TreeNode): TreeNode[] {
    let sorted = this.sortedChildren.get(node);
    if (!sorted) {
      sorted = [...node.children.values()].sort((a, b) =>
        collator.compare(a.name, b.name),
      );
      this.sortedChildren.set(node, sorted);
    }
    return sorted;
  }

  private sortedRoots(): TreeNode[] {
    return [...this.roots.values()].sort(
      (a, b) => a.connectionId - b.connectionId,
    );
  }

  private expandedRows(): TreeNode[] {
    const rows: TreeNode[] = [];
    const visit = (node: TreeNode) => {
      rows.push(node);
      if (this.expanded.has(node.key)) this.childrenOf(node).forEach(visit);
    };
    this.sortedRoots().forEach(visit);
    this.matchCount = this.topicNodes.length;
    return rows;
  }

  private searchRows(): TreeNode[] {
    if (this.queryError) {
      this.matchCount = 0;
      return [];
    }
    const matches = this.matchingTopics();
    this.matchCount = matches.length;
    const shown = new Set<TreeNode>();
    for (const node of matches) {
      for (let n: TreeNode | null = node; n && !shown.has(n); n = n.parent) {
        shown.add(n);
      }
    }
    const rows: TreeNode[] = [];
    const visit = (node: TreeNode) => {
      if (!shown.has(node)) return;
      rows.push(node);
      this.childrenOf(node).forEach(visit);
    };
    this.sortedRoots().forEach(visit);
    return rows;
  }

  private matchingTopics(): TreeNode[] {
    const query = this.query;
    if (isTopicFilter(query)) {
      return this.topicNodes.filter((n) => topicMatches(query, n.path));
    }
    const needle = query.toLowerCase();
    return this.topicNodes.filter((n) => n.path.toLowerCase().includes(needle));
  }

  private structureChanged(): void {
    this.cachedRows = null;
  }

  private notify(): void {
    this.version++;
    this.listeners.forEach((listener) => listener());
  }
}

function newNode(
  connectionId: ConnectionId,
  name: string,
  path: string,
  depth: number,
  parent: TreeNode | null,
): TreeNode {
  return {
    key: `${connectionId}\u0000${depth}\u0000${path}`,
    connectionId,
    name,
    path,
    depth,
    parent,
    children: new Map(),
    topic: null,
    topicCount: 0,
    messageCount: 0,
  };
}
