import type { ConnectionId } from "../bridge";
import type { TreeNode } from "./store";

/**
 * A topic the user cleared while looking at it. The detail panel stays on it and takes it up
 * again once it's back: when the tree holds a new node at its path. Until the clear comes
 * through, the old node is still there; taking that up would be undone by the clear.
 */
export interface Watching {
  connectionId: ConnectionId;
  path: string;
  /** The node that was cleared. */
  was: TreeNode;
}

/** The topic's new node, once the clear has come through and a message brought it back. */
export function cameBack(
  watched: Watching,
  find: (connectionId: ConnectionId, path: string) => TreeNode | null,
): TreeNode | null {
  const node = find(watched.connectionId, watched.path);
  return node && node !== watched.was ? node : null;
}
