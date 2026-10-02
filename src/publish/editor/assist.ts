// Suggestions from an API Spec while publishing: channels and their parameters' values in the
// topic, and keys and values from the message schemas in the payload, with its problems
// underlined.
import {
  snippetCompletion,
  type Completion,
  type CompletionContext,
  type CompletionResult,
} from "@codemirror/autocomplete";
import { jsonParseLinter } from "@codemirror/lang-json";
import type { Diagnostic } from "@codemirror/lint";
import type { EditorView } from "@codemirror/view";
import { checkPayload, type SpecChannel } from "../../bridge";
import {
  knownValues,
  paramAt,
  paramName,
  topicSnippet,
} from "../../spec/channels";
import { cursorContext, rangeOf } from "../../spec/jsonCursor";
import {
  describe,
  propertyOptions,
  schemasAt,
  skeleton,
  valueOptions,
  type Schema,
} from "../../spec/schema";

export interface TopicHints {
  channels: SpecChannel[];
  /** Topics already seen on the Connection, for parameters' values. */
  seen: () => string[];
}

/** Whether what's typed so far could become a topic on the channel. */
export function fitsSoFar(channel: SpecChannel, before: string): boolean {
  const levels = before.split("/");
  const pattern = channel.address.split("/");
  if (levels.length > pattern.length) return false;
  return levels.every((level, i) => {
    const literal = paramName(pattern[i]) === null;
    if (i === levels.length - 1)
      return !literal || pattern[i].startsWith(level);
    return literal ? level === pattern[i] : level !== "";
  });
}

const channelDetail = (c: SpecChannel) =>
  c.messages
    .map((m) => m.name)
    .filter(Boolean)
    .join(", ");

/** Channels whose address fits what's typed, as snippets with a field per parameter. */
export function topicChannels(hints: () => TopicHints) {
  return (ctx: CompletionContext): CompletionResult | null => {
    const { channels, seen } = hints();
    // In the middle of a topic (filling in a parameter), only that level's values fit.
    if (channels.length === 0 || ctx.pos < ctx.state.doc.length) return null;
    const before = ctx.state.sliceDoc(0, ctx.pos);
    const fitting = channels.filter((c) => fitsSoFar(c, before));
    if (fitting.length === 0) return null;
    return {
      from: 0,
      filter: false,
      options: fitting.map((c) =>
        snippetCompletion(topicSnippet(c, knownValues(c, seen())), {
          label: c.address,
          detail: channelDetail(c),
          info: c.description ?? undefined,
          type: "channel",
        }),
      ),
    };
  };
}

/** Values for the parameter the cursor's level stands for. */
export function topicParams(hints: () => TopicHints) {
  return (ctx: CompletionContext): CompletionResult | null => {
    const { channels, seen } = hints();
    const at = paramAt(channels, ctx.state.sliceDoc(0, ctx.pos));
    if (!at) return null;
    const values = knownValues(at.channel, seen()).get(at.name) ?? [];
    if (values.length === 0) return null;
    const description = at.channel.parameters.find((p) => p.name === at.name);
    return {
      from: at.from,
      validFor: /^[^/]*$/,
      options: values.map((value) => ({
        label: value,
        detail: `{${at.name}}`,
        info: description?.description ?? undefined,
        type: "constant",
      })),
    };
  };
}

/** A value as it's typed in a payload, with a field to fill where it's empty. */
function valueSnippet(schema: Schema): string {
  const value = skeleton(schema);
  if (value === "") return '"${1}"';
  if (typeof value === "object" && value !== null) {
    const text = JSON.stringify(value);
    if (text === "{}") return "{${1}}";
    if (text === "[]") return "[${1}]";
    return text.replace(/[\\$}]/g, "\\$&");
  }
  return `\${1:${JSON.stringify(value).replace(/[\\$}]/g, "\\$&")}}`;
}

/** Keys and values from the message schemas, where the cursor is in the payload. */
export function payloadCompletions(schemas: () => unknown[]) {
  return (ctx: CompletionContext): CompletionResult | null => {
    const list = schemas();
    if (list.length === 0) return null;
    const text = ctx.state.doc.toString();
    const at = cursorContext(text, ctx.pos);
    const typed = ctx.matchBefore(/"?[^\s"{}[\],:]*/);
    const from =
      text[at.from] === '"' || at.from < ctx.pos
        ? at.from
        : (typed?.from ?? ctx.pos);
    if (
      !ctx.explicit &&
      from === ctx.pos &&
      !/[{,[:]\s*$/.test(text.slice(0, ctx.pos))
    )
      return null;
    const here = schemasAt(list, at.path);
    let options: Completion[] = [];
    if (at.kind === "key") {
      options = propertyOptions(here)
        .filter((p) => !at.keys.includes(p.name))
        .map((p) =>
          snippetCompletion(
            `${JSON.stringify(p.name)}: ${valueSnippet(p.schema)}`,
            {
              label: JSON.stringify(p.name),
              detail: describe(p.schema) + (p.required ? " · required" : ""),
              info: p.schema.description,
              type: "property",
              boost: p.required ? 1 : 0,
            },
          ),
        );
    } else if (at.kind === "value") {
      options = valueOptions(here).map((v) => ({
        label: JSON.stringify(v),
        type: "constant",
      }));
      const shaped = here.find(
        (s) => s.properties || s.type === "object" || s.type === "array",
      );
      if (shaped) {
        options.push({
          label: JSON.stringify(skeleton(shaped)),
          detail: "to start from",
          type: "text",
        });
      }
    }
    if (options.length === 0) return null;
    return { from, options };
  };
}

const syntax = jsonParseLinter();

/**
 * JSON problems, and what the message schemas don't allow. Text that isn't meant as JSON (no
 * schema, and not starting like JSON) isn't checked.
 */
export function payloadLinter(schemas: () => unknown[]) {
  return async (view: EditorView): Promise<Diagnostic[]> => {
    const text = view.state.doc.toString();
    const list = schemas();
    if (!text.trim()) return [];
    try {
      JSON.parse(text);
    } catch {
      return list.length > 0 || /^\s*[[{"]/.test(text) ? syntax(view) : [];
    }
    if (list.length === 0) return [];
    const problems = await checkPayload(list, text);
    if (view.state.doc.toString() !== text) return [];
    return problems.map((p) => {
      const range = rangeOf(text, p.path);
      return { ...range, severity: "error", message: p.message };
    });
  };
}
