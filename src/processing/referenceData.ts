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

// The language's words, read off the Reference so the editor can't fall out of step with it
// (a Rust test keeps the parser in step with the same file).

/** The Stage keywords, `from` first. */
export const STAGE_NAMES: string[] = STAGES.map((s) => s.name);

/** The words a form's syntax spells out, leaving out `<placeholders>` and quoted text. */
function formWords(syntax: string): string[] {
  return syntax
    .replace(/<[^>]*>/g, " <> ")
    .replace(/"[^"]*"/g, ' "" ')
    .split(/[\s[\]|,]+/)
    .filter((word) => word !== "");
}

/** The other words the forms use (`topic`, `not`, `by`, `rotate`, …), each once. */
export const MODIFIERS: string[] = [
  ...new Set(
    STAGES.flatMap((s) => s.forms.flatMap((f) => formWords(f.syntax))).filter(
      (word) => /^[a-z]+$/.test(word) && !STAGE_NAMES.includes(word),
    ),
  ),
];

/**
 * The Stages that take `by <key>`, with how many arguments come before it
 * (`throttle <duration> by …` has one, `scan <start> <update> by …` two).
 */
export const BY_STAGES: Map<string, number> = new Map(
  STAGES.flatMap((s) =>
    s.forms.flatMap((f) => {
      const words = formWords(f.syntax);
      const by = words.indexOf("by");
      return words[0] === s.name && by > 0 ? [[s.name, by - 1] as const] : [];
    }),
  ),
);
