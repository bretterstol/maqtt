import { useEffect, useMemo, useRef, useState } from "react";
import type { ChartLines } from "./api";

// The plot's margins: room for the value ticks on the left and the times below.
const M = { top: 10, right: 16, bottom: 22, left: 52 };

/** Clean tick values across [min, max]: steps of 1, 2 or 5 times a power of ten. */
function ticks(min: number, max: number, count: number): number[] {
  if (!(max > min)) return [min];
  const raw = (max - min) / count;
  const power = 10 ** Math.floor(Math.log10(raw));
  const step =
    [1, 2, 5, 10].map((f) => f * power).find((s) => s >= raw) ?? 10 * power;
  const out: number[] = [];
  for (let v = Math.ceil(min / step) * step; v <= max + step / 1e6; v += step) {
    out.push(Math.abs(v) < step / 1e6 ? 0 : v);
  }
  return out;
}

// Time ticks fall on whole seconds, minutes or hours.
const TIME_STEPS = [
  1, 2, 5, 10, 15, 30, 60, 120, 300, 600, 900, 1800, 3600, 7200, 21600, 43200,
  86400,
].map((s) => s * 1000);

function timeTicks(t0: number, t1: number, count: number): number[] {
  const step =
    TIME_STEPS.find((s) => s >= (t1 - t0) / count) ??
    TIME_STEPS[TIME_STEPS.length - 1];
  // Local time, so minute and hour ticks land on the clock the labels show.
  const offset = new Date(t0).getTimezoneOffset() * 60_000;
  const out: number[] = [];
  for (
    let t = Math.ceil((t0 - offset) / step) * step + offset;
    t <= t1;
    t += step
  ) {
    out.push(t);
  }
  return out;
}

const number = (v: number) =>
  v.toLocaleString(undefined, { maximumFractionDigits: 3 });

function clock(ms: number, seconds: boolean): string {
  const d = new Date(ms);
  const pad = (n: number) => String(n).padStart(2, "0");
  return seconds
    ? `${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}`
    : `${pad(d.getHours())}:${pad(d.getMinutes())}`;
}

/** The point of a line nearest to a time. */
function nearest(points: [number, number][], at: number): [number, number] {
  let lo = 0;
  let hi = points.length - 1;
  while (hi - lo > 1) {
    const mid = (lo + hi) >> 1;
    if (points[mid][0] < at) lo = mid;
    else hi = mid;
  }
  return Math.abs(points[lo][0] - at) <= Math.abs(points[hi][0] - at)
    ? points[lo]
    : points[hi];
}

/**
 * `to chart`'s lines over time on one value axis. Each line's colour follows the order it
 * first appeared in (the validated categorical palette, see PipelinesTab.css), never its rank.
 * A legend with each line's latest value comes with two or more; hovering shows every line's
 * value at the nearest time.
 */
export default function LineChart({ chart }: { chart: ChartLines }) {
  const box = useRef<HTMLDivElement>(null);
  const [size, setSize] = useState({ width: 0, height: 0 });
  const [hover, setHover] = useState<number | null>(null);

  useEffect(() => {
    const element = box.current;
    if (!element) return;
    const measure = () =>
      setSize({ width: element.clientWidth, height: element.clientHeight });
    measure();
    const observer = new ResizeObserver(measure);
    observer.observe(element);
    return () => observer.disconnect();
    // The plot element is replaced when the chart goes from empty to drawn.
  }, [chart.lines.length === 0]); // eslint-disable-line react-hooks/exhaustive-deps

  const lines = useMemo(
    () => chart.lines.filter((l) => l.points.length > 0),
    [chart],
  );
  // About one value tick per 40px of height.
  const yCount = Math.max(2, Math.min(8, Math.floor(size.height / 40)));
  const scale = useMemo(() => {
    const all = lines.flatMap((l) => l.points);
    if (all.length === 0) return null;
    let [t0, t1, v0, v1] = [Infinity, -Infinity, Infinity, -Infinity];
    for (const [t, v] of all) {
      t0 = Math.min(t0, t);
      t1 = Math.max(t1, t);
      v0 = Math.min(v0, v);
      v1 = Math.max(v1, v);
    }
    if (t1 === t0) t1 = t0 + 1000;
    // A flat line sits mid-height; otherwise leave a little air above and below.
    const pad = v1 === v0 ? Math.abs(v0) * 0.1 || 1 : (v1 - v0) * 0.08;
    const yTicks = ticks(v0 - pad, v1 + pad, yCount);
    const lo = Math.min(v0 - pad, yTicks[0]);
    const hi = Math.max(v1 + pad, yTicks[yTicks.length - 1]);
    return { t0, t1, lo, hi, yTicks };
  }, [lines, yCount]);

  const plotW = Math.max(0, size.width - M.left - M.right);
  const plotH = Math.max(0, size.height - M.top - M.bottom);
  const x = (t: number) =>
    scale ? M.left + ((t - scale.t0) / (scale.t1 - scale.t0)) * plotW : 0;
  const y = (v: number) =>
    scale ? M.top + (1 - (v - scale.lo) / (scale.hi - scale.lo)) * plotH : 0;

  if (!scale) {
    return (
      <div className="line-chart">
        <div className="line-chart__plot" ref={box}>
          <p className="line-chart__empty">
            No values yet. <code>to chart</code> draws a line for each number,
            or each <code>{"{ value, series }"}</code>.
          </p>
        </div>
      </div>
    );
  }

  const span = scale.t1 - scale.t0;
  const xTicks = timeTicks(
    scale.t0,
    scale.t1,
    Math.max(2, Math.floor(plotW / 90)),
  );
  const at =
    hover === null ? null : scale.t0 + ((hover - M.left) / plotW) * span;
  const readout =
    at === null
      ? []
      : lines.map((line, i) => ({ i, line, point: nearest(line.points, at) }));
  const crossT =
    at === null || readout.length === 0
      ? null
      : readout.reduce((best, r) =>
          Math.abs(r.point[0] - at) < Math.abs(best.point[0] - at) ? r : best,
        ).point[0];

  return (
    <div className="line-chart">
      {lines.length >= 2 ? (
        <ul className="line-chart__legend">
          {lines.map((line, i) => (
            <li key={line.name}>
              <span className={`line-chart__key line-chart__s${i + 1}`} />
              <span className="line-chart__name">{line.name}</span>
              <span className="line-chart__latest">
                {number(line.points[line.points.length - 1][1])}
              </span>
            </li>
          ))}
        </ul>
      ) : (
        <p className="line-chart__title">
          <span className="line-chart__name">{lines[0].name}</span>{" "}
          <span className="line-chart__latest">
            {number(lines[0].points[lines[0].points.length - 1][1])}
          </span>
        </p>
      )}
      {chart.hidden > 0 && (
        <p className="line-chart__note">
          {chart.hidden.toLocaleString()} more series not drawn: a chart shows 8
          at most. Filter, or give fewer <code>series</code>.
        </p>
      )}
      <div className="line-chart__plot" ref={box}>
        <svg
          width={size.width}
          height={size.height}
          role="img"
          aria-label={`Chart of ${lines.map((l) => l.name).join(", ")}`}
          onPointerMove={(e) => {
            const left = e.currentTarget.getBoundingClientRect().left;
            const px = e.clientX - left;
            setHover(px >= M.left && px <= M.left + plotW ? px : null);
          }}
          onPointerLeave={() => setHover(null)}
        >
          {scale.yTicks.map((v) => (
            <g key={v}>
              <line
                className="line-chart__grid"
                x1={M.left}
                x2={M.left + plotW}
                y1={y(v)}
                y2={y(v)}
              />
              <text
                className="line-chart__tick"
                x={M.left - 6}
                y={y(v)}
                textAnchor="end"
                dominantBaseline="middle"
              >
                {number(v)}
              </text>
            </g>
          ))}
          <line
            className="line-chart__axis"
            x1={M.left}
            x2={M.left + plotW}
            y1={M.top + plotH}
            y2={M.top + plotH}
          />
          {xTicks.map((t) => (
            <text
              key={t}
              className="line-chart__tick"
              x={x(t)}
              y={M.top + plotH + 15}
              textAnchor="middle"
            >
              {clock(t, span < 30 * 60_000)}
            </text>
          ))}
          {lines.map((line, i) => (
            <polyline
              key={line.name}
              className={`line-chart__line line-chart__s${i + 1}`}
              points={line.points.map(([t, v]) => `${x(t)},${y(v)}`).join(" ")}
            />
          ))}
          {lines.map((line, i) => {
            const [t, v] = line.points[line.points.length - 1];
            return (
              <circle
                key={line.name}
                className={`line-chart__dot line-chart__s${i + 1}`}
                cx={x(t)}
                cy={y(v)}
                r={4}
              />
            );
          })}
          {crossT !== null && (
            <line
              className="line-chart__cross"
              x1={x(crossT)}
              x2={x(crossT)}
              y1={M.top}
              y2={M.top + plotH}
            />
          )}
        </svg>
        {crossT !== null && (
          <div
            className="line-chart__tip"
            style={
              x(crossT) > size.width / 2
                ? { right: size.width - x(crossT) + 10 }
                : { left: x(crossT) + 10 }
            }
          >
            <div className="line-chart__tip-time">{clock(crossT, true)}</div>
            {readout.map(({ i, line, point }) => (
              <div key={line.name} className="line-chart__tip-row">
                <span className={`line-chart__key line-chart__s${i + 1}`} />
                <strong>{number(point[1])}</strong>
                <span className="line-chart__name">{line.name}</span>
              </div>
            ))}
          </div>
        )}
      </div>
    </div>
  );
}
