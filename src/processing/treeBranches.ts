// The Topic Tree's one entry point for Pipelines (ADR 0004): it marks a stopped Pipeline's
// branch under the Pipelines root.
import { useCallback, useEffect, useState } from "react";
import type { TreeNode } from "../topicTree/store";
import { listPipelines, PIPELINES_CONNECTION } from "./api";

const POLL_MS = 2_000;

/**
 * Labels the branch of each Pipeline that isn't running. It polls only while `active`, which
 * App sets once the Pipelines root exists: nothing runs for someone who never uses Pipelines.
 */
export function usePipelineBranchLabel(
  active: boolean,
): (node: TreeNode) => string | null {
  const [running, setRunning] = useState<Set<string> | null>(null);

  useEffect(() => {
    if (!active) return;
    let stale = false;
    const poll = () =>
      void listPipelines().then(
        (list) => {
          if (!stale)
            setRunning(
              new Set(list.filter((p) => p.status.running).map((p) => p.name)),
            );
        },
        () => {},
      );
    poll();
    const timer = setInterval(poll, POLL_MS);
    return () => {
      stale = true;
      clearInterval(timer);
    };
  }, [active]);

  return useCallback(
    (node: TreeNode) =>
      running !== null &&
      node.connectionId === PIPELINES_CONNECTION &&
      node.depth === 1 &&
      !running.has(node.name)
        ? "stopped"
        : null,
    [running],
  );
}
