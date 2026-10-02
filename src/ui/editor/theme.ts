import { EditorView } from "@codemirror/view";
import "./editor.css";

/**
 * The look every code editor in måqtt shares (Pipelines, Publish): the app's colours and
 * monospace, underlined problems, and tooltips. Languages add their own highlighting.
 */
export const editorTheme = EditorView.theme({
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
});
