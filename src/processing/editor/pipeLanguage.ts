import {
  HighlightStyle,
  StreamLanguage,
  syntaxHighlighting,
  type StreamParser,
} from "@codemirror/language";
import { EditorView } from "@codemirror/view";
import { tags } from "@lezer/highlight";
import { editorTheme } from "../../ui/editor/theme";
import { MODIFIERS, STAGE_NAMES } from "../referenceData";
import { copyState, next, startState, type ScanState } from "../scanner";

const inStringNow = (state: ScanState) =>
  state.stack[state.stack.length - 1].kind === "string";

/** Characters that change where the scanner is, so the scanner reads them itself. */
const STRUCTURE = '"#([{)]}';

/**
 * Tokens for mÅqtt's pipeline language, by the parser's own rules (scanner.ts): the state
 * carried from line to line is the scanner's, so strings, `{…}` fills and comments end where
 * the parser says. Code, inside fills too, is coloured by the same word rules.
 */
export const pipeParser: StreamParser<ScanState> = {
  name: "maqtt-pipe",
  startState,
  copyState,
  token(stream, state) {
    const inString = inStringNow(state);
    const c = stream.peek() ?? "";
    // Strings, and what opens, closes or comments: the scanner decides.
    if (inString || STRUCTURE.includes(c)) {
      const line = stream.string;
      const piece = next(state, line, stream.pos);
      stream.pos = Math.max(piece.end, stream.pos + 1);
      if (["quote", "text", "escape", "brace"].includes(piece.kind)) {
        // One token for the string, up to its end or the next fill, whose code is coloured.
        while (stream.pos < line.length && inStringNow(state)) {
          const at = stream.pos;
          const opensFill =
            (line[at] === "{" && line[at + 1] !== "{") ||
            (line[at] === "\\" && line[at + 1] === "(");
          if (opensFill) break;
          stream.pos = next(state, line, at).end;
        }
        return "string";
      }
      switch (piece.kind) {
        case "comment":
          return "comment";
        case "fill":
        case "end":
          return "operator";
        case "code":
          return null;
        default:
          return "string";
      }
    }
    if (stream.eatSpace()) return null;
    if (stream.match("|>")) return "operator";
    if (stream.match(/^\$[A-Za-z_][\w]*/)) return "variableName";
    if (stream.match(/^\.[A-Za-z_][\w]*/)) return "propertyName";
    if (stream.match(/^\d+(\.\d+)?(ms|s|m|h)?\b/)) return "number";
    const word = stream.match(/^[A-Za-z_][\w]*/) as RegExpMatchArray | null;
    if (word) {
      if (STAGE_NAMES.includes(word[0])) return "keyword";
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
  editorTheme,
  EditorView.theme({
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
