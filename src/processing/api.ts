// The Pipelines commands (src-tauri/src/processing/mod.rs). Kept here, not in bridge.ts, so the
// whole feature lives in this folder (ADR 0004).

import { invoke } from "@tauri-apps/api/core";
import type { Diagnostic } from "./generated/Diagnostic";
import type { FileEntry } from "./generated/FileEntry";
import type { Inspected } from "./generated/Inspected";
import type { Line } from "./generated/Line";
import type { Lines } from "./generated/Lines";
import type { Marker } from "./generated/Marker";
import type { Outputs } from "./generated/Outputs";
import type { PipelineStatus } from "./generated/PipelineStatus";
import type { PipelineView } from "./generated/PipelineView";
import type { Preview } from "./generated/Preview";
import type { PreviewValue } from "./generated/PreviewValue";
import type { Sampled } from "./generated/Sampled";
import type { StageTrace } from "./generated/StageTrace";

// Generated from the backend's Rust types by ts-rs, when its tests run.
export type {
  Diagnostic,
  FileEntry,
  Inspected,
  Line,
  Lines,
  Marker,
  Outputs,
  PipelineStatus,
  PipelineView,
  Preview,
  PreviewValue,
  Sampled,
  StageTrace,
};

/** The pseudo-Connection Derived Topics arrive on (`PIPELINES` in runner.rs). */
export const PIPELINES_CONNECTION = 0;

export const listPipelines = (): Promise<PipelineView[]> =>
  invoke("list_pipelines");

/** Saves if there are no problems (and restarts it if running); resolves to the problems. */
export const savePipeline = (
  name: string,
  source: string,
): Promise<Diagnostic[]> => invoke("save_pipeline", { name, source });

export const deletePipeline = (name: string): Promise<void> =>
  invoke("delete_pipeline", { name });

/** Marks a Pipeline to start with mÅqtt, or unmarks it; doesn't start or stop it now. */
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

export const listPipelineFiles = (): Promise<FileEntry[]> =>
  invoke("list_pipeline_files");

/** The file's last lines. */
export const tailPipelineFile = (name: string): Promise<string> =>
  invoke("tail_pipeline_file", { name });

/** Where `to file` writes names that aren't absolute. */
export const pipelineFilesFolder = (): Promise<string> =>
  invoke("pipeline_files_folder");

/** Runs a draft over what it reads (recent messages, files' records, Derived Topics) without starting it. */
export const previewPipeline = (source: string): Promise<Preview> =>
  invoke("preview_pipeline", { source });

/** The Brokers' names, for suggestions after `from`. */
export const brokerNames = (): Promise<string[]> =>
  invoke<{ name: string }[]>("list_profiles").then((profiles) =>
    profiles.map((p) => p.name),
  );
