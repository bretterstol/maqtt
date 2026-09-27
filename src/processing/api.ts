// The Pipelines commands (src-tauri/src/processing/mod.rs). Kept here, not in bridge.ts, so the
// whole feature lives in this folder (ADR 0004).

import { invoke } from "@tauri-apps/api/core";

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

export interface PipelineStatus {
  name: string;
  running: boolean;
  received: number;
  emitted: number;
  missed: number;
  errors: number;
  lastError: string | null;
  inspected: Inspected[];
}

export interface PipelineView {
  name: string;
  source: string;
  status: PipelineStatus;
}

export const listPipelines = (): Promise<PipelineView[]> =>
  invoke("list_pipelines");

export const checkPipeline = (source: string): Promise<Diagnostic[]> =>
  invoke("check_pipeline", { source });

/** Saves if there are no problems (and restarts it if running); resolves to the problems. */
export const savePipeline = (
  name: string,
  source: string,
): Promise<Diagnostic[]> => invoke("save_pipeline", { name, source });

export const deletePipeline = (name: string): Promise<void> =>
  invoke("delete_pipeline", { name });

export const renamePipeline = (from: string, to: string): Promise<void> =>
  invoke("rename_pipeline", { from, to });

export const startPipeline = (name: string): Promise<void> =>
  invoke("start_pipeline", { name });

export const stopPipeline = (name: string): Promise<void> =>
  invoke("stop_pipeline", { name });

export const pipelinesFolder = (): Promise<string> =>
  invoke("pipelines_folder");
