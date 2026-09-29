import {
  HighlightStyle,
  StreamLanguage,
  syntaxHighlighting,
  type StreamParser,
} from "@codemirror/language";
import { EditorView } from "@codemirror/view";
import { tags } from "@lezer/highlight";

export const STAGE_KEYWORDS = [
  "from",
  "filter",
  "map",
  "changes",
  "throttle",
  "inspect",
  "to",
];
const MODIFIERS = [
  "all",
  "topic",
  "not",
  "app",
  "broker",
  "file",
  "rotate",
  "daily",
  "hourly",
  "by",
];

interface State {
  inString: boolean;
}

/** Tokens for måqtt's pipeline language; the jq inside is highlighted by the same rules. */
export const pipeParser: StreamParser<State> = {
  name: "maqtt-pipe",
  startState: () => ({ inString: false }),
  token(stream, state) {
    if (state.inString) {
      while (!stream.eol()) {
        const ch = stream.next();
        if (ch === "\\") stream.next();
        else if (ch === '"') {
          state.inString = false;
          break;
        }
      }
      return "string";
    }
    if (stream.eatSpace()) return null;
    if (stream.match("#")) {
      stream.skipToEnd();
      return "comment";
    }
    if (stream.match("|>")) return "operator";
    if (stream.peek() === '"') {
      stream.next();
      state.inString = true;
      return pipeParser.token(stream, state);
    }
    if (stream.match(/^\$[A-Za-z_][\w]*/)) return "variableName";
    if (stream.match(/^\.[A-Za-z_][\w]*/)) return "propertyName";
    if (stream.match(/^\d+(\.\d+)?(ms|s|m|h)?\b/)) return "number";
    const word = stream.match(/^[A-Za-z_][\w]*/) as RegExpMatchArray | null;
    if (word) {
      if (STAGE_KEYWORDS.includes(word[0])) return "keyword";
      if (MODIFIERS.includes(word[0])) return "modifier";
      return null;
    }
    stream.next();
    return null;
  },
  languageData: { commentTokens: { line: "#" } },
};

export const pipeLanguage = StreamLanguage.define(pipeParser);

/** Code stays in the bridge-console palette: fog and kelp, told apart by weight and slant. */
const highlight = HighlightStyle.define([
  { tag: tags.keyword, color: "var(--text)", fontWeight: "700" },
  { tag: tags.modifier, color: "var(--text)", fontStyle: "italic" },
  { tag: tags.operator, color: "var(--text-muted)", fontWeight: "700" },
  { tag: tags.string, color: "var(--code-string)" },
  { tag: tags.variableName, color: "var(--code-variable)" },
  { tag: tags.propertyName, color: "var(--text)" },
  { tag: tags.number, color: "var(--code-string)" },
  { tag: tags.comment, color: "var(--text-muted)", fontStyle: "italic" },
]);

export const pipeTheme = [
  syntaxHighlighting(highlight),
  EditorView.theme({
    "&": {
      height: "100%",
      backgroundColor: "var(--bg-field)",
      color: "var(--text)",
      fontSize: "13px",
    },
    "&.cm-focused": { outline: "none" },
    ".cm-scroller": {
      fontFamily: "var(--font)",
      fontVariationSettings: "var(--mono)",
      lineHeight: "1.55",
    },
    ".cm-content": { caretColor: "var(--text)" },
    ".cm-cursor": { borderLeftColor: "var(--text)" },
    ".cm-gutters": {
      backgroundColor: "var(--bg-raised)",
      color: "var(--text-muted)",
      border: "none",
      borderRight: "1px solid var(--border)",
    },
    ".cm-activeLine": { backgroundColor: "var(--border-subtle)" },
    ".cm-activeLineGutter": { backgroundColor: "var(--border-subtle)" },
    "&.cm-focused .cm-selectionBackground, .cm-selectionBackground, ::selection":
      { backgroundColor: "var(--select)" },
    ".cm-matchingBracket": {
      backgroundColor: "var(--select)",
      outline: "none",
    },
    ".cm-lintRange-error": {
      backgroundImage: "none",
      textDecoration: "underline wavy var(--bad)",
      textUnderlineOffset: "3px",
    },
    ".cm-tooltip": {
      backgroundColor: "var(--bg-raised)",
      border: "1px solid var(--border)",
      color: "var(--text)",
    },
    ".cm-diagnostic-error": { borderLeftColor: "var(--bad)" },
    ".cm-stage-note": {
      marginLeft: "3ch",
      color: "var(--text-muted)",
      fontSize: "12px",
      opacity: "0.85",
    },
    ".cm-stage-note::before": { content: '"→ "' },
    ".cm-stage-note--error": { color: "var(--bad)", opacity: "1" },
    ".cm-stage-note--error::before": { content: '"✕ "' },
  }),
];
