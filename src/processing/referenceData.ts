// The language reference, shared by the Reference panel, autocomplete and hover help. The data
// lives in reference.json, which a Rust test checks: every example parses, and every function
// example gives its output.
import data from "./reference.json";

export interface StageForm {
  syntax: string;
  does: string;
}

export interface StageDoc {
  name: string;
  summary: string;
  forms: StageForm[];
  details?: string;
  /** A CodeMirror snippet, with ${…} placeholders. */
  snippet: string;
  examples: string[];
  /** A whole Pipeline showing the Stage at work, from `from` on. */
  example?: string;
}

export interface NameDoc {
  name: string;
  does: string;
}

export interface FunctionDoc extends NameDoc {
  example: string;
  input: string;
  output: string;
}

export const STAGES: StageDoc[] = data.stages;
export const VARIABLES: NameDoc[] = data.variables;
export const SYNTAX: NameDoc[] = data.syntax;
export const FUNCTIONS: FunctionDoc[] = data.functions;

export const stageDoc = (name: string) => STAGES.find((s) => s.name === name);
export const variableDoc = (name: string) =>
  VARIABLES.find((v) => v.name === name);
export const functionDoc = (name: string) =>
  FUNCTIONS.find((f) => f.name === name);
