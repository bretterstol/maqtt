import { ACTIVITY_BUCKETS, activityOf, type TreeNode } from "./store";
import "./ActivityMeter.css";

const BAR = 3;
const GAP = 1.5;
const HEIGHT = 12;
/** The lamp stays lit this long after a message. */
const LAMP_MS = 1_000;

/**
 * The last 30 s of traffic at or below a node as 12 amber bars, newest on the right, with a lamp
 * that lights when a message has just arrived. Heights are relative to the node's own busiest
 * bucket, on a log scale so a trickle still shows next to a burst.
 */
export function ActivityMeter({ node, now }: { node: TreeNode; now: number }) {
  const activity = activityOf(node, now);
  const peak = Math.max(...activity);
  const lit = node.lastActiveAt > 0 && now - node.lastActiveAt < LAMP_MS;
  const width = ACTIVITY_BUCKETS * (BAR + GAP) - GAP;
  const total = activity.reduce((sum, n) => sum + n, 0);
  return (
    <span
      className="activity"
      title={`${total.toLocaleString()} messages in the last 30 s`}
    >
      <svg
        width={width}
        height={HEIGHT}
        viewBox={`0 0 ${width} ${HEIGHT}`}
        aria-hidden="true"
      >
        {Array.from(activity, (count, i) => {
          const h =
            count === 0
              ? 1
              : 2 + ((HEIGHT - 2) * Math.log1p(count)) / Math.log1p(peak);
          return (
            <rect
              key={i}
              x={i * (BAR + GAP)}
              y={HEIGHT - h}
              width={BAR}
              height={h}
              className={count === 0 ? "activity__idle" : "activity__bar"}
            />
          );
        })}
      </svg>
      <span className={`activity__lamp${lit ? " activity__lamp--lit" : ""}`} />
    </span>
  );
}
