import { useCallback, useEffect, useRef, useState } from "react";
import { toast } from "../ui/toast";
import {
  checkPipeline,
  deletePipeline,
  listPipelines,
  pipelinesFolder,
  renamePipeline,
  savePipeline,
  startPipeline,
  stopPipeline,
  type Diagnostic,
  type PipelineView,
} from "./api";
import { PipelineEditor } from "./editor/PipelineEditor";
import { OutputPanel } from "./OutputPanel";
import "./PipelinesTab.css";

const POLL_MS = 1_000;
const CHECK_DELAY_MS = 300;

const TEMPLATE = `# Describe what this Pipeline does
from all
|> filter topic "#"
|> inspect
`;

function freeName(taken: string[]): string {
  for (let n = 1; ; n++) {
    const name = n === 1 ? "pipeline" : `pipeline ${n}`;
    if (!taken.includes(name)) return name;
  }
}

function problemText(problems: Diagnostic[]): string {
  const first = problems[0];
  const more = problems.length > 1 ? ` (and ${problems.length - 1} more)` : "";
  return `Line ${first.line}, column ${first.column}: ${first.message}${more}`;
}

/** The Pipelines tab: the saved Pipelines, an editor for one, and what it's doing. */
export default function PipelinesTab() {
  const [pipelines, setPipelines] = useState<PipelineView[]>([]);
  const [selected, setSelected] = useState<string | null>(null);
  // Unsaved edits per Pipeline, so switching between them keeps the text.
  const [drafts, setDrafts] = useState<Map<string, string>>(new Map());
  // The problems found in a Pipeline's text, kept with its name so another Pipeline's don't show.
  const [checked, setChecked] = useState<{
    name: string;
    problems: Diagnostic[];
  } | null>(null);
  const [renaming, setRenaming] = useState<string | null>(null);
  const [confirmingDelete, setConfirmingDelete] = useState(false);
  const [folder, setFolder] = useState("");

  const show = useCallback((list: PipelineView[]) => {
    setPipelines(list);
    setSelected((current) =>
      current !== null && list.some((p) => p.name === current)
        ? current
        : (list[0]?.name ?? null),
    );
  }, []);

  // A poll sent before a save can answer after it; only the newest answer counts.
  const asked = useRef(0);
  const refresh = useCallback(() => {
    const ask = ++asked.current;
    return listPipelines().then(
      (list) => {
        if (ask === asked.current) show(list);
      },
      (error) => toast(`Couldn't read the Pipelines: ${String(error)}`, true),
    );
  }, [show]);

  useEffect(() => {
    void pipelinesFolder().then(setFolder);
    const poll = () => void refresh();
    poll();
    const timer = setInterval(poll, POLL_MS);
    return () => clearInterval(timer);
  }, [refresh]);

  const current = pipelines.find((p) => p.name === selected) ?? null;
  const text = current ? (drafts.get(current.name) ?? current.source) : "";
  const dirty = current !== null && text !== current.source;
  const problems =
    current && checked?.name === current.name ? checked.problems : [];

  // Check the text shortly after typing stops, for the underlines in the editor.
  useEffect(() => {
    if (!current) return;
    const name = current.name;
    let stale = false;
    const timer = setTimeout(() => {
      void checkPipeline(text).then((found) => {
        if (!stale) setChecked({ name, problems: found });
      });
    }, CHECK_DELAY_MS);
    return () => {
      stale = true;
      clearTimeout(timer);
    };
  }, [current?.name, text]); // eslint-disable-line react-hooks/exhaustive-deps

  const edit = (value: string) => {
    if (!current) return;
    const name = current.name;
    setDrafts((old) => new Map(old).set(name, value));
  };

  const forgetDraft = (name: string) =>
    setDrafts((old) => {
      const next = new Map(old);
      next.delete(name);
      return next;
    });

  /** Saves the open Pipeline; false when its problems kept it from being saved. */
  const save = async (): Promise<boolean> => {
    if (!current) return false;
    const name = current.name;
    try {
      const found = await savePipeline(name, text);
      if (found.length > 0) {
        setChecked({ name, problems: found });
        toast(`Not saved. ${problemText(found)}`, true);
        return false;
      }
      forgetDraft(name);
      await refresh();
      toast(
        current.status.running
          ? `Saved and restarted ${name}`
          : `Saved ${name}`,
      );
      return true;
    } catch (error) {
      toast(`Couldn't save ${name}: ${String(error)}`, true);
      return false;
    }
  };

  const start = async () => {
    if (!current) return;
    if (dirty && !(await save())) return;
    try {
      await startPipeline(current.name);
      await refresh();
    } catch (error) {
      toast(`Couldn't start ${current.name}. ${String(error)}`, true);
    }
  };

  const stop = async () => {
    if (!current) return;
    await stopPipeline(current.name);
    await refresh();
  };

  const create = async () => {
    const name = freeName(pipelines.map((p) => p.name));
    try {
      await savePipeline(name, TEMPLATE);
      setSelected(name);
      await refresh();
      setRenaming(name);
    } catch (error) {
      toast(`Couldn't create a Pipeline: ${String(error)}`, true);
    }
  };

  const rename = async (from: string, to: string) => {
    setRenaming(null);
    to = to.trim();
    if (to === "" || to === from) return;
    try {
      await renamePipeline(from, to);
      setDrafts((old) => {
        const next = new Map(old);
        const draft = next.get(from);
        next.delete(from);
        if (draft !== undefined) next.set(to, draft);
        return next;
      });
      setSelected(to);
      await refresh();
    } catch (error) {
      toast(`Couldn't rename ${from}: ${String(error)}`, true);
    }
  };

  const remove = async () => {
    if (!current) return;
    const name = current.name;
    setConfirmingDelete(false);
    try {
      await deletePipeline(name);
      forgetDraft(name);
      setSelected(null);
      await refresh();
      toast(`Deleted ${name}`);
    } catch (error) {
      toast(`Couldn't delete ${name}: ${String(error)}`, true);
    }
  };

  return (
    <div className="pipelines">
      <aside className="pipelines__list">
        <button className="pipelines__new" onClick={() => void create()}>
          New Pipeline
        </button>
        <ul>
          {pipelines.map((p) => (
            <li key={p.name}>
              <button
                className={
                  p.name === selected
                    ? "pipelines__item pipelines__item--selected"
                    : "pipelines__item"
                }
                onClick={() => {
                  setSelected(p.name);
                  setConfirmingDelete(false);
                }}
              >
                <span
                  className={
                    p.status.running
                      ? "pipelines__lamp pipelines__lamp--on"
                      : "pipelines__lamp"
                  }
                  title={p.status.running ? "Running" : "Stopped"}
                />
                <span className="pipelines__name">
                  {p.name}
                  {drafts.has(p.name) && drafts.get(p.name) !== p.source && (
                    <span
                      className="pipelines__unsaved"
                      title="Unsaved changes"
                    >
                      {" "}
                      •
                    </span>
                  )}
                </span>
                {p.status.running && (
                  <span className="pipelines__count">
                    {p.status.emitted.toLocaleString()}
                  </span>
                )}
              </button>
            </li>
          ))}
        </ul>
        {pipelines.length === 0 && (
          <p className="pipelines__empty">
            A Pipeline reads live messages and turns them into new topics.
            Nothing runs until you start it.
          </p>
        )}
        {folder && (
          <p className="pipelines__folder" title={folder}>
            Saved as .pipe files in {folder}
          </p>
        )}
      </aside>

      {current ? (
        <section className="pipelines__work">
          <header className="pipelines__header">
            {renaming === current.name ? (
              <input
                className="pipelines__rename"
                autoFocus
                defaultValue={current.name}
                aria-label="Pipeline name"
                onBlur={(e) => void rename(current.name, e.target.value)}
                onKeyDown={(e) => {
                  if (e.key === "Enter") e.currentTarget.blur();
                  if (e.key === "Escape") setRenaming(null);
                }}
              />
            ) : (
              <h2
                className="pipelines__title"
                title="Double-click to rename"
                onDoubleClick={() => setRenaming(current.name)}
              >
                {current.name}
              </h2>
            )}
            {renaming !== current.name && (
              <button
                className="icon-button"
                onClick={() => setRenaming(current.name)}
              >
                Rename
              </button>
            )}
            <span className="pipelines__spacer" />
            {confirmingDelete ? (
              <>
                <span className="pipelines__confirm">
                  Delete {current.name}?
                </span>
                <button onClick={() => void remove()}>Delete</button>
                <button onClick={() => setConfirmingDelete(false)}>
                  Cancel
                </button>
              </>
            ) : (
              <>
                <button onClick={() => setConfirmingDelete(true)}>
                  Delete
                </button>
                <button
                  disabled={!dirty}
                  onClick={() => void save()}
                  title="Ctrl+S"
                >
                  Save
                </button>
                {current.status.running ? (
                  <button onClick={() => void stop()}>Stop</button>
                ) : (
                  <button className="primary" onClick={() => void start()}>
                    Start
                  </button>
                )}
              </>
            )}
          </header>
          <PipelineEditor
            docKey={current.name}
            value={text}
            onChange={edit}
            onSave={() => void save()}
            diagnostics={problems}
          />
          <p className="pipelines__problems" aria-live="polite">
            {problems.length > 0
              ? problemText(problems)
              : dirty
                ? "Unsaved changes"
                : ""}
          </p>
          <OutputPanel status={current.status} />
        </section>
      ) : (
        <section className="pipelines__work pipelines__work--empty">
          <p>
            Pick a Pipeline, or make a new one to start from a small example.
          </p>
        </section>
      )}
    </div>
  );
}
