import type { PipelineStatus } from "./api";

function clock(atMs: number): string {
  const d = new Date(atMs);
  const pad = (n: number, width = 2) => String(n).padStart(width, "0");
  return `${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}.${pad(d.getMilliseconds(), 3)}`;
}

/** What a Pipeline has done: its counts, its last error and the values `inspect` saw. */
export function OutputPanel({ status }: { status: PipelineStatus }) {
  const inspected = [...status.inspected].reverse();
  return (
    <section className="pipeline-output" aria-label="Pipeline output">
      <div className="pipeline-output__counts">
        <span>{status.running ? "Running" : "Stopped"}</span>
        <span>{status.received.toLocaleString()} in</span>
        <span>{status.emitted.toLocaleString()} out</span>
        {status.missed > 0 && (
          <span title="Messages that arrived faster than the Pipeline could handle them">
            {status.missed.toLocaleString()} missed
          </span>
        )}
        {status.errors > 0 && (
          <span className="pipeline-output__bad">
            {status.errors.toLocaleString()} errors
          </span>
        )}
      </div>
      {status.lastError && (
        <p className="pipeline-output__error">{status.lastError}</p>
      )}
      <ol className="pipeline-output__inspected">
        {inspected.map((item, i) => (
          <li key={`${item.atMs}-${i}`}>
            <span className="pipeline-output__time">{clock(item.atMs)}</span>
            <span className="pipeline-output__where">
              {item.label ?? item.topic}
            </span>
            <code>{item.value}</code>
          </li>
        ))}
      </ol>
      {inspected.length === 0 && (
        <p className="pipeline-output__hint">
          {status.running
            ? "Waiting for messages. Add |> inspect after any Stage to see its values here."
            : "Start the Pipeline to see what |> inspect shows."}
        </p>
      )}
    </section>
  );
}
