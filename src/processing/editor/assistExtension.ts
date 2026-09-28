import {
  autocompletion,
  snippetCompletion,
  type Completion,
  type CompletionContext,
  type CompletionResult,
} from "@codemirror/autocomplete";
import { hoverTooltip, type EditorView } from "@codemirror/view";
import {
  assistAt,
  fieldsAt,
  wildcardNames,
  type WildcardName,
} from "../assist";
import {
  FUNCTIONS,
  STAGES,
  VARIABLES,
  functionDoc,
  stageDoc,
  variableDoc,
  type FunctionDoc,
  type NameDoc,
  type StageDoc,
} from "../reference";

/** What the editor knows about real data, for suggestions. */
export interface Hints {
  /** The newest sample payload, as JSON. */
  payload: string | null;
  /** The latest value out of each Stage in the last Preview, as JSON. */
  stageValues: (string | null)[];
  brokers: string[];
}

function el(tag: string, className: string, text?: string): HTMLElement {
  const node = document.createElement(tag);
  node.className = className;
  if (text !== undefined) node.textContent = text;
  return node;
}

export function stageDocNode(doc: StageDoc): HTMLElement {
  const root = el("div", "pipe-doc");
  root.append(el("p", "pipe-doc__summary", doc.summary));
  for (const form of doc.forms) {
    const row = el("div", "pipe-doc__form");
    row.append(el("code", "", form.syntax), el("span", "", form.does));
    root.append(row);
  }
  if (doc.details) root.append(el("p", "pipe-doc__details", doc.details));
  return root;
}

function nameDocNode(doc: NameDoc): HTMLElement {
  const root = el("div", "pipe-doc");
  root.append(el("p", "pipe-doc__summary", doc.does));
  return root;
}

function functionDocNode(doc: FunctionDoc): HTMLElement {
  const root = nameDocNode(doc);
  const example = el("div", "pipe-doc__form");
  example.append(
    el("code", "", `${doc.input} | ${doc.example}`),
    el("span", "", `→ ${doc.output}`),
  );
  root.append(example);
  return root;
}

const stageOptions = (prefix: string): Completion[] =>
  STAGES.filter((s) => s.name !== "from").map((doc) =>
    snippetCompletion(prefix + doc.snippet, {
      label: doc.name,
      detail: doc.summary,
      type: "keyword",
      info: () => stageDocNode(doc),
      boost: 2,
    }),
  );

const functionOptions: Completion[] = FUNCTIONS.map((doc) => ({
  label: doc.name,
  detail: doc.does,
  type: "function",
  info: () => functionDocNode(doc),
}));

const variableOptions: Completion[] = VARIABLES.map((doc) => ({
  label: doc.name,
  type: "variable",
  info: () => nameDocNode(doc),
}));

function wildcardDoc(wildcard: WildcardName): NameDoc {
  const what = wildcard.written.startsWith("#")
    ? 'the levels its # matched, like "a/b", or null for none'
    : "the level its + matched";
  return {
    name: `$${wildcard.name}`,
    does: `Named wildcard ${wildcard.written} in "${wildcard.filter}": ${what}. Null until that filter topic.`,
  };
}

const wildcardOptions = (before: string): Completion[] =>
  wildcardNames(before).map((wildcard) => ({
    label: `$${wildcard.name}`,
    detail: wildcard.written,
    type: "variable",
    info: () => nameDocNode(wildcardDoc(wildcard)),
    boost: 1,
  }));

function keyword(
  label: string,
  snippet: string,
  detail: string,
  boost = 1,
): Completion {
  return snippetCompletion(snippet, { label, detail, type: "keyword", boost });
}

function options(
  context: CompletionContext,
  hints: Hints,
): CompletionResult | null {
  const before = context.state.sliceDoc(0, context.pos);
  const assist = assistAt(before);
  const IDENT = /^[\w$]*$/;
  switch (assist.kind) {
    case "none":
      return null;
    case "from":
      return {
        from: assist.from,
        options: [
          snippetCompletion(stageDoc("from")!.snippet, {
            label: "from",
            detail: stageDoc("from")!.summary,
            type: "keyword",
            info: () => stageDocNode(stageDoc("from")!),
          }),
        ],
        validFor: IDENT,
      };
    case "sources":
      return {
        from: assist.from,
        options: [
          ...(assist.first
            ? [
                {
                  label: "all",
                  detail: "every connected Broker",
                  type: "keyword",
                },
              ]
            : []),
          ...hints.brokers.map((name) => ({
            label: `"${name}"`,
            detail: "Broker",
            type: "constant",
          })),
        ],
      };
    case "broker":
      return {
        from: assist.from,
        options: hints.brokers.map((name) => ({
          label: `"${name}"`,
          detail: "Broker",
          type: "constant",
        })),
      };
    case "stage":
      return {
        from: assist.from,
        options: [
          ...stageOptions(assist.prefix),
          ...(assist.alsoFunctions ? functionOptions : []),
        ],
        validFor: IDENT,
      };
    case "variable":
      return {
        from: assist.from,
        options: [...wildcardOptions(before), ...variableOptions],
        validFor: /^\$\w*$/,
      };
    case "field": {
      const json =
        assist.base === "payload" || assist.stage <= 1
          ? hints.payload
          : (hints.stageValues[assist.stage - 2] ?? null);
      const fields = fieldsAt(json, assist.path);
      if (fields.length === 0) return null;
      return {
        from: assist.from,
        options: fields.map((name) => ({
          label: /^[A-Za-z_]\w*$/.test(name) ? name : JSON.stringify(name),
          detail: "in recent data",
          type: "property",
        })),
        validFor: /^\w*$/,
      };
    }
    case "filter":
      return {
        from: assist.from,
        options: [
          keyword(
            "topic",
            'topic "${home/#}"',
            "the original topic matches an MQTT filter",
            3,
          ),
          ...(assist.negated
            ? []
            : [
                keyword(
                  "not topic",
                  'not topic "${+/status}"',
                  "the original topic doesn't match",
                  3,
                ),
              ]),
          ...functionOptions,
        ],
        validFor: IDENT,
      };
    case "by":
      return {
        from: assist.from,
        options: [
          keyword(
            "by",
            "by ${.room}",
            "keep state per key instead of per topic",
          ),
        ],
        validFor: IDENT,
      };
    case "sink":
      return {
        from: assist.from,
        options: [
          keyword("app", 'app "${topic}"', "a Derived Topic in the Topic Tree"),
          keyword(
            "broker",
            'broker "${Broker}" "${topic}"',
            "republish on a connected Broker",
          ),
        ],
        validFor: IDENT,
      };
    case "function":
      if (assist.word === "" && !context.explicit) return null;
      return { from: assist.from, options: functionOptions, validFor: IDENT };
  }
}

/** Suggestions from the reference and from real data. */
export function pipeAutocomplete(hints: () => Hints) {
  return autocompletion({
    override: [(context) => options(context, hints())],
    icons: false,
  });
}

/** Help for the Stage, variable or function under the pointer. */
export const pipeHover = hoverTooltip((view: EditorView, pos: number) => {
  const line = view.state.doc.lineAt(pos);
  const text = line.text;
  let start = pos - line.from;
  let end = start;
  while (start > 0 && /[\w$]/.test(text[start - 1])) start--;
  while (end < text.length && /\w/.test(text[end])) end++;
  if (start === end) return null;
  const word = text.slice(start, end);
  const before = text.slice(0, start);

  let dom: HTMLElement | null = null;
  const stage = stageDoc(word);
  if (stage && /(^|\|>)\s*$/.test(before)) dom = stageDocNode(stage);
  else if (word.startsWith("$") && variableDoc(word))
    dom = nameDocNode(variableDoc(word)!);
  else if (word.startsWith("$")) {
    const wildcard = wildcardNames(view.state.doc.toString()).find(
      (w) => `$${w.name}` === word,
    );
    if (wildcard) dom = nameDocNode(wildcardDoc(wildcard));
  } else if (functionDoc(word) && !/\.\s*$/.test(before))
    dom = functionDocNode(functionDoc(word)!);
  if (!dom) return null;

  const node = dom;
  return {
    pos: line.from + start,
    end: line.from + end,
    create: () => ({ dom: node }),
  };
});
