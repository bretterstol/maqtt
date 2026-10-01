// The Pipelines commands (src-tauri/src/processing/mod.rs). Kept here, not in bridge.ts, so the
// whole feature lives in this folder (ADR 0004).

import { invoke } from "@tauri-apps/api/core";

/** The pseudo-Connection Derived Topics arrive on (`PIPELINES` in runner.rs). */
export const PIPELINES_CONNECTION = 0;

/** A problem in a Pipeline's source; `start` and `end` are UTF-8 byte offsets. */
export interface Diagnostic {
  message: string;
  line: number;
  column: number;
  start: number;
  end: number;
}

export interface Inspected {
  label: string | null;
  topic: string;
  value: string;
  atMs: number;
}

/** Where one key's `to geo` values put it, and a short trail of where it was. */
export interface Marker {
  key: string;
  label: string | null;
  lat: number;
  lon: number;
  /** Earlier positions, oldest first. */
  trail: [number, number][];
  atMs: number;
}

/** `to chart`'s lines: a series each, in the order they first appeared. */
export interface ChartLines {
  lines: { name: string; points: [number, number][] }[];
  /** Series past the chart's limit of 8, not drawn. */
  hidden: number;
}

/** What a Pipeline's Sinks show, the same for a running Pipeline and for Preview. */
export interface Outputs {
  /** Values its Sinks took. */
  emitted: number;
  errors: number;
  lastError: string | null;
  /** The latest `inspect` values, newest last. */
  inspected: Inspected[];
  markers: Marker[];
  chart: ChartLines;
}

export interface PipelineStatus {
  name: string;
  running: boolean;
  /** It stopped by itself: a `take` passed all it would. */
  finished: boolean;
  received: number;
  missed: number;
  /** Its own republished messages that came back from a Broker and were left out. */
  returned: number;
  outputs: Outputs;
}

export interface PipelineView {
  name: string;
  source: string;
  status: PipelineStatus;
  /** Starts whenever måqtt starts. */
  startsWithApp: boolean;
}

export const listPipelines = (): Promise<PipelineView[]> =>
  invoke("list_pipelines");

/** Saves if there are no problems (and restarts it if running); resolves to the problems. */
export const savePipeline = (
  name: string,
  source: string,
): Promise<Diagnostic[]> => invoke("save_pipeline", { name, source });

export const deletePipeline = (name: string): Promise<void> =>
  invoke("delete_pipeline", { name });

/** Marks a Pipeline to start with måqtt, or unmarks it; doesn't start or stop it now. */
export const setPipelineStartsWithApp = (
  name: string,
  on: boolean,
): Promise<void> => invoke("set_pipeline_starts_with_app", { name, on });

export const renamePipeline = (from: string, to: string): Promise<void> =>
  invoke("rename_pipeline", { from, to });

export const startPipeline = (name: string): Promise<void> =>
  invoke("start_pipeline", { name });

export const stopPipeline = (name: string): Promise<void> =>
  invoke("stop_pipeline", { name });

export const pipelinesFolder = (): Promise<string> =>
  invoke("pipelines_folder");

/** A file in the Pipeline files folder. */
export interface PipelineFile {
  /** Relative to the folder: what `from file` takes. */
  name: string;
  size: number;
  modifiedMs: number;
}

export const listPipelineFiles = (): Promise<PipelineFile[]> =>
  invoke("list_pipeline_files");

/** The file's last lines. */
export const tailPipelineFile = (name: string): Promise<string> =>
  invoke("tail_pipeline_file", { name });

/** Where `to file` writes names that aren't absolute. */
export const pipelineFilesFolder = (): Promise<string> =>
  invoke("pipeline_files_folder");

/** What one Stage did in a Preview. */
export interface StageTrace {
  passed: number;
  last: string | null;
  errors: number;
  /** The first error in full: `map on home/a: …`. */
  firstError: string | null;
  /** Just what went wrong in it; the note sits on its Stage already. */
  firstReason: string | null;
}

export interface PreviewValue {
  /** For results: the Sink, as written (`to app`, `to broker "Office"`). */
  sink: string | null;
  label: string | null;
  topic: string;
  value: string;
}

export interface Preview {
  /** When not empty, nothing ran. */
  problems: Diagnostic[];
  messages: number;
  /** The newest sample's payload as JSON. */
  payload: string | null;
  /** Where `from` and then each Stage end in the source (UTF-8 byte offsets). */
  ends: number[];
  stages: StageTrace[];
  /** What its Sinks would send, oldest first; nothing is sent. */
  results: PreviewValue[];
  /** What it ran over: recent messages, records from files, or Derived Topics. */
  sampled: "messages" | "records" | "derived";
  /** A file Source's file that couldn't be read. */
  note: string | null;
  outputs: Outputs;
}

/** Runs a draft over what it reads (recent messages, files' records, Derived Topics) without starting it. */
export const previewPipeline = (source: string): Promise<Preview> =>
  invoke("preview_pipeline", { source });

/** The Brokers' names, for suggestions after `from`. */
export const brokerNames = (): Promise<string[]> =>
  invoke<{ name: string }[]>("list_profiles").then((profiles) =>
    profiles.map((p) => p.name),
  );
