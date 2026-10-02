import type { TreeOpens } from "../generated/TreeOpens";
import type { ConnectionId, LogEntry, TopicUpdate } from "../bridge";
import { isTopicFilter, topicFilterError, topicMatches } from "./match";

/** A topic's current state, present on nodes where messages were received. */
export interface TopicInfo {
  count: number;
  retained: boolean;
  latest: LogEntry;
}

/**
 * How the tree opens itself: only by hand, every level except the one holding the topics
 * ("groups"), or everything.
 */
/** How the Topic Tree opens itself (`TreeOpens` in settings.rs). */
export type ExpandMode = TreeOpens;

/** In "groups" mode, a group with more entries than this stays closed, so it can't flood the tree. */
export const GROUP_EXPAND_LIMIT = 50;

/** The activity meter covers the last 30 s in 12 buckets. */
export const ACTIVITY_BUCKETS = 12;
export const ACTIVITY_BUCKET_MS = 2_500;

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
  /** Messages at or below this node per activity bucket, oldest first, ending at `activityEpoch`. */
  activity: Uint32Array;
  activityEpoch: number;
  /** When a message last arrived at or below this node; 0 if none since the app started. */
  lastActiveAt: number;
}

const collator = new Intl.Collator(undefined, { numeric: true });

/**
 * The Topic Tree: one root per Connection, topics grouped by `/` levels. Updates carry absolute
 * stats, so applying them in any order converges; the newest `latest.seq` wins.
 */
export class TopicTreeStore {
  private readonly roots = new Map<ConnectionId, TreeNode>();
  private readonly topicNodes: TreeNode[] = [];
  private mode: ExpandMode = "manual";
  /** Nodes opened or closed by hand, which win over the mode until the mode changes. */
  private readonly overrides = new Map<string, boolean>();
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

  /**
   * Applies topic updates. `live` updates count as activity for the meters; a startup snapshot
   * doesn't, or every topic would show its whole history as one burst.
   */
  apply(updates: TopicUpdate[], { now = Date.now(), live = true } = {}): void {
    if (updates.length === 0) return;
    for (const update of updates) this.applyOne(update, now, live);
    this.notify();
  }

  /**
   * Forgets the topics at and under `prefix` on a Connection, removing levels left empty. The
   * root goes too when nothing is left under it.
   */
  clear(connectionId: ConnectionId, prefix: string): void {
    let node = this.roots.get(connectionId);
    for (const level of prefix.split("/")) node = node?.children.get(level);
    if (!node) return;

    const gone = new Set<TreeNode>();
    const collect = (n: TreeNode) => {
      if (n.topic) gone.add(n);
      n.children.forEach(collect);
    };
    collect(node);
    const removed = this.topicNodes.filter((n) => gone.has(n));
    this.topicNodes.splice(
      0,
      this.topicNodes.length,
      ...this.topicNodes.filter((n) => !gone.has(n)),
    );
    for (let n = node.parent; n; n = n.parent) {
      n.topicCount -= removed.length;
      n.messageCount -= node.messageCount;
    }

    // Detach it, then any ancestor it leaves empty.
    let child: TreeNode = node;
    for (let parent = child.parent; parent; parent = parent.parent) {
      parent.children.delete(child.name);
      this.sortedChildren.delete(parent);
      if (parent.children.size > 0 || parent.topic) break;
      child = parent;
      if (!parent.parent) this.roots.delete(connectionId);
    }
    this.structureChanged();
    this.notify();
  }

  /** The node at `path` on a Connection (its root for ""), if the tree has it. */
  find(connectionId: ConnectionId, path: string): TreeNode | null {
    let node = this.roots.get(connectionId);
    if (path !== "") {
      for (const level of path.split("/")) node = node?.children.get(level);
    }
    return node ?? null;
  }

  /** Whether a node is still in the tree, rather than cleared away. */
  contains(node: TreeNode): boolean {
    let n = node;
    while (n.parent) {
      if (n.parent.children.get(n.name) !== n) return false;
      n = n.parent;
    }
    return this.roots.get(n.connectionId) === n;
  }

  /** The rows to show: expanded nodes, or while searching, matches and their ancestors. */
  rows(): TreeNode[] {
    this.cachedRows ??= this.query ? this.searchRows() : this.expandedRows();
    return this.cachedRows;
  }

  get expandMode(): ExpandMode {
    return this.mode;
  }

  /** Switches how the tree opens itself; choices made by hand are forgotten. */
  setExpandMode(mode: ExpandMode): void {
    if (mode === this.mode) return;
    this.mode = mode;
    this.overrides.clear();
    this.structureChanged();
    this.notify();
  }

  isExpanded(node: TreeNode): boolean {
    return this.query !== "" || this.isOpen(node);
  }

  private isOpen(node: TreeNode): boolean {
    return this.overrides.get(node.key) ?? this.opensItself(node);
  }

  private opensItself(node: TreeNode): boolean {
    if (node.depth === 0) return true;
    if (node.children.size === 0) return false;
    switch (this.mode) {
      case "manual":
        return false;
      case "all":
        return true;
      case "groups":
        // Open unless this is the last level, the one holding only topics.
        return (
          node.children.size <= GROUP_EXPAND_LIMIT &&
          [...node.children.values()].some((child) => child.children.size > 0)
        );
    }
  }

  toggle(node: TreeNode): void {
    if (node.children.size === 0 || this.query) return;
    this.overrides.set(node.key, !this.isOpen(node));
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

  private applyOne(update: TopicUpdate, now: number, live: boolean): void {
    const node = this.nodeFor(update.connectionId, update.topic);
    const previous = node.topic;
    if (previous && update.latest.seq < previous.latest.seq) return;

    const addedMessages = update.count - (previous?.count ?? 0);
    node.topic = {
      count: update.count,
      retained: update.retained,
      latest: update.latest,
    };
    if (!previous) this.topicNodes.push(node);
    for (let n: TreeNode | null = node; n; n = n.parent) {
      n.messageCount += addedMessages;
      if (!previous) n.topicCount += 1;
      if (live && addedMessages > 0) recordActivity(n, addedMessages, now);
    }
    // A new topic may add a row, or change which rows a search shows.
    if (!previous) this.structureChanged();
  }

  private nodeFor(connectionId: ConnectionId, topic: string): TreeNode {
    let root = this.roots.get(connectionId);
    if (!root) {
      root = newNode(connectionId, "", "", 0, null);
      this.roots.set(connectionId, root);
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
      if (this.isOpen(node)) this.childrenOf(node).forEach(visit);
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

function bucketOf(now: number): number {
  return Math.floor(now / ACTIVITY_BUCKET_MS);
}

/** Moves a node's buckets forward to `epoch`, dropping what fell out of the window. */
function advance(activity: Uint32Array, from: number, to: number): void {
  const gap = to - from;
  if (gap <= 0) return;
  if (gap >= activity.length) {
    activity.fill(0);
  } else {
    activity.copyWithin(0, gap);
    activity.fill(0, activity.length - gap);
  }
}

function recordActivity(node: TreeNode, count: number, now: number): void {
  const epoch = bucketOf(now);
  advance(node.activity, node.activityEpoch, epoch);
  node.activityEpoch = epoch;
  node.activity[node.activity.length - 1] += count;
  node.lastActiveAt = now;
}

/** Messages per bucket over the last 30 s as of `now`, oldest first. */
export function activityOf(node: TreeNode, now = Date.now()): Uint32Array {
  const activity = node.activity.slice();
  advance(activity, node.activityEpoch, bucketOf(now));
  return activity;
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
    activity: new Uint32Array(ACTIVITY_BUCKETS),
    activityEpoch: 0,
    lastActiveAt: 0,
  };
}
