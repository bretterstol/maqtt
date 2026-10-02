import { useEffect, useState } from "react";
import { formatDateTime, formatSize } from "../format";
import { copy } from "../ui/clipboard";
import { listPipelineFiles, tailPipelineFile, type FileEntry } from "./api";

const LIST_EVERY_MS = 2_000;
const TAIL_EVERY_MS = 1_000;

/** Keeps the files folder's list fresh while the tab is open. */
export function usePipelineFiles(): FileEntry[] {
  const [files, setFiles] = useState<FileEntry[]>([]);
  useEffect(() => {
    let live = true;
    const load = () =>
      void listPipelineFiles().then(
        (list) => live && setFiles(list),
        () => {},
      );
    load();
    const timer = setInterval(load, LIST_EVERY_MS);
    return () => {
      live = false;
      clearInterval(timer);
    };
  }, []);
  return files;
}

/** The files `to file` wrote (and `from file` can read), under the Pipeline list. */
export function FileList({
  files,
  viewing,
  onView,
}: {
  files: FileEntry[];
  viewing: string | null;
  onView: (name: string) => void;
}) {
  return (
    <section className="pipeline-files">
      <h3 className="pipeline-files__heading">Files</h3>
      {files.length === 0 ? (
        <p className="pipelines__empty">
          None yet. <code>to file</code> writes them here.
        </p>
      ) : (
        <ul>
          {files.map((file) => (
            <li key={file.name}>
              <button
                className={
                  file.name === viewing
                    ? "pipelines__item pipelines__item--selected"
                    : "pipelines__item"
                }
                title={`${file.name}, ${formatSize(file.size)}, changed ${formatDateTime(file.modifiedMs)}`}
                onClick={() => onView(file.name)}
              >
                <span className="pipelines__name">{file.name}</span>
                <span className="pipelines__count">
                  {formatSize(file.size)}
                </span>
              </button>
            </li>
          ))}
        </ul>
      )}
    </section>
  );
}

/** One file: what it is, how to read it back, and its last lines, kept up to date. */
export function FileView({
  name,
  file,
  folder,
}: {
  name: string;
  /** The file as listed; null once it's no longer there. */
  file: FileEntry | null;
  folder: string;
}) {
  const [tail, setTail] = useState<{ name: string; text: string } | null>(null);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    let live = true;
    const load = () =>
      void tailPipelineFile(name).then(
        (text) => {
          if (!live) return;
          setTail({ name, text });
          setError(null);
        },
        (err) => live && setError(String(err)),
      );
    load();
    const timer = setInterval(load, TAIL_EVERY_MS);
    return () => {
      live = false;
      clearInterval(timer);
    };
  }, [name]);

  if (!file) {
    return (
      <section className="pipelines__work pipelines__work--empty">
        {name} is no longer in the files folder.
      </section>
    );
  }
  const readBack = `from file "${file.name}"`;
  const text = tail?.name === file.name ? tail.text : "";
  return (
    <section className="pipelines__work">
      <header className="pipelines__header">
        <h2 className="pipelines__title">{file.name}</h2>
        <span className="pipeline-files__meta">
          {formatSize(file.size)} · changed {formatDateTime(file.modifiedMs)}
        </span>
        <span className="pipelines__spacer" />
        <button
          title="Copy the Source line that reads this file"
          onClick={() => void copy(readBack, readBack)}
        >
          Copy {readBack}
        </button>
      </header>
      <p className="pipeline-files__where" title={`${folder}/${file.name}`}>
        {folder}/{file.name}
      </p>
      {error ? (
        <p className="pipeline-output__error">{error}</p>
      ) : (
        <>
          <p className="pipeline-files__hint">
            The last 50 lines, as they change.
          </p>
          <pre className="pipeline-files__tail">{text || "(empty)"}</pre>
        </>
      )}
    </section>
  );
}
