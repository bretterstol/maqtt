import type { PipelineStatus, Preview, PreviewValue } from "./api";

export type OutputView = "preview" | "live";

interface Props {
  status: PipelineStatus;
  /** Null while the draft has problems or hasn't been previewed yet. */
  preview: Preview | null;
  /** Why the latest Preview couldn't run, if it couldn't. */
  previewError: string | null;
  view: OutputView;
  onView: (view: OutputView) => void;
}

function clock(atMs: number): string {
  const d = new Date(atMs);
  const pad = (n: number, width = 2) => String(n).padStart(width, "0");
  return `${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}.${pad(d.getMilliseconds(), 3)}`;
}

/** What a Pipeline does: a Preview of the draft on recent messages, or the running Pipeline. */
export function OutputPanel({
  status,
  preview,
  previewError,
  view,
  onView,
}: Props) {
  return (
    <section className="pipeline-output" aria-label="Pipeline output">
      <div className="pipeline-output__bar">
        <div className="pipeline-output__switch" role="tablist">
          <button
            role="tab"
            aria-selected={view === "preview"}
            onClick={() => onView("preview")}
          >
            Preview
          </button>
          <button
            role="tab"
            aria-selected={view === "live"}
            onClick={() => onView("live")}
          >
            {status.running
              ? "Running"
              : status.finished
                ? "Finished"
                : "Stopped"}
          </button>
        </div>
        {view === "preview" ? (
          <PreviewCounts preview={preview} />
        ) : (
          <LiveCounts status={status} />
        )}
      </div>
      {view === "preview" ? (
        <PreviewOutput preview={preview} error={previewError} />
      ) : (
        <LiveOutput status={status} />
      )}
    </section>
  );
}

function PreviewCounts({ preview }: { preview: Preview | null }) {
  if (!preview) return null;
  return (
    <div className="pipeline-output__counts">
      <span>
        {preview.messages.toLocaleString()} recent message
        {preview.messages === 1 ? "" : "s"} in
      </span>
      <span>
        {(preview.stages.at(-1)?.passed ?? preview.messages).toLocaleString()}{" "}
        out
      </span>
    </div>
  );
}

function PreviewOutput({
  preview,
  error,
}: {
  preview: Preview | null;
  error: string | null;
}) {
  if (error) return <p className="pipeline-output__error">{error}</p>;
  if (!preview) {
    return (
      <p className="pipeline-output__hint">
        Preview runs your draft over recent messages as you type, once it has no
        problems.
      </p>
    );
  }
  if (preview.messages === 0) {
    return (
      <p className="pipeline-output__hint">
        No recent messages match <code>from</code> and the first{" "}
        <code>filter topic</code>. Connect, or wait for traffic, and Preview
        will pick them up.
      </p>
    );
  }
  const rows: [string, PreviewValue][] = [
    ...preview.inspected.map((v): [string, PreviewValue] => ["inspect", v]),
    ...preview.results.map((v): [string, PreviewValue] => [
      v.sink ?? "to app",
      v,
    ]),
  ];
  if (rows.length === 0) {
    return (
      <p className="pipeline-output__hint">
        Nothing got through. The note after each Stage in the editor shows where
        the messages stopped.
      </p>
    );
  }
  return (
    <ol className="pipeline-output__inspected">
      {rows.map(([kind, item], i) => (
        <li key={i}>
          <span className="pipeline-output__time">{kind}</span>
          <span className="pipeline-output__where" title={item.topic}>
            {item.label ?? item.topic}
          </span>
          <code>{item.value}</code>
        </li>
      ))}
    </ol>
  );
}

function LiveCounts({ status }: { status: PipelineStatus }) {
  return (
    <div className="pipeline-output__counts">
      <span>{status.received.toLocaleString()} in</span>
      <span>{status.emitted.toLocaleString()} out</span>
      {status.missed > 0 && (
        <span title="Messages that arrived faster than the Pipeline could handle them">
          {status.missed.toLocaleString()} missed
        </span>
      )}
      {status.returned > 0 && (
        <span title="Messages this Pipeline republished that came back from a Broker. They were left out, so the Pipeline doesn't feed itself.">
          {status.returned.toLocaleString()} came back
        </span>
      )}
      {status.errors > 0 && (
        <span className="pipeline-output__bad">
          {status.errors.toLocaleString()} errors
        </span>
      )}
    </div>
  );
}

function LiveOutput({ status }: { status: PipelineStatus }) {
  const inspected = [...status.inspected].reverse();
  return (
    <>
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
            : status.finished
              ? "It finished: its take passed all it would. Start it again for more."
              : "Start the Pipeline to see what |> inspect shows."}
        </p>
      )}
    </>
  );
}
