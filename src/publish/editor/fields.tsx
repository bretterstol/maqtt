import {
  autocompletion,
  closeBrackets,
  closeBracketsKeymap,
  completionKeymap,
  startCompletion,
} from "@codemirror/autocomplete";
import { defaultKeymap, history, historyKeymap } from "@codemirror/commands";
import { json } from "@codemirror/lang-json";
import {
  bracketMatching,
  HighlightStyle,
  indentOnInput,
  syntaxHighlighting,
} from "@codemirror/language";
import { linter } from "@codemirror/lint";
import {
  Annotation,
  EditorState,
  Prec,
  type Extension,
} from "@codemirror/state";
import {
  drawSelection,
  EditorView,
  keymap,
  placeholder,
} from "@codemirror/view";
import { tags } from "@lezer/highlight";
import { useEffect, useRef } from "react";
import { editorTheme } from "../../ui/editor/theme";
import {
  payloadCompletions,
  payloadLinter,
  topicChannels,
  topicParams,
  type TopicHints,
} from "./assist";
import "./fields.css";

/** Marks text put in from outside, which isn't an edit to report back. */
const fromOutside = Annotation.define<boolean>();

/** Ctrl+Enter publishes, as in the rest of the form (CodeMirror would add a line). */
const submitKey = (submit: () => void) =>
  Prec.highest(
    keymap.of([
      {
        key: "Mod-Enter",
        run: () => {
          submit();
          return true;
        },
      },
    ]),
  );

/** A CodeMirror editor whose text the parent owns. */
function useEditor(
  host: React.RefObject<HTMLDivElement | null>,
  value: string,
  onChange: (value: string) => void,
  extensions: () => Extension[],
) {
  const view = useRef<EditorView | null>(null);
  const change = useRef(onChange);
  useEffect(() => {
    change.current = onChange;
  });
  useEffect(() => {
    const editor = new EditorView({
      parent: host.current!,
      state: EditorState.create({
        doc: value,
        extensions: [
          ...extensions(),
          EditorView.updateListener.of((update) => {
            const outside = update.transactions.some((t) =>
              t.annotation(fromOutside),
            );
            if (update.docChanged && !outside) {
              change.current(update.state.doc.toString());
            }
          }),
        ],
      }),
    });
    view.current = editor;
    return () => editor.destroy();
    // The editor is made once; its text follows `value` below.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);
  useEffect(() => {
    const editor = view.current;
    if (editor && editor.state.doc.toString() !== value) {
      editor.dispatch({
        changes: { from: 0, to: editor.state.doc.length, insert: value },
        annotations: fromOutside.of(true),
      });
    }
  }, [value]);
  return view;
}

/** The topic: one line, suggesting the spec's channels and their parameters' values. */
export function TopicField({
  value,
  onChange,
  onSubmit,
  hints,
}: {
  value: string;
  onChange: (value: string) => void;
  onSubmit: () => void;
  hints: TopicHints;
}) {
  const host = useRef<HTMLDivElement>(null);
  const latest = useRef({ hints, onSubmit });
  useEffect(() => {
    latest.current = { hints, onSubmit };
  });
  useEditor(host, value, onChange, () => [
    submitKey(() => latest.current.onSubmit()),
    history(),
    drawSelection(),
    keymap.of([...completionKeymap, ...historyKeymap, ...defaultKeymap]),
    // One line: a pasted line break is left out.
    EditorState.transactionFilter.of((tr) =>
      tr.docChanged && tr.newDoc.lines > 1
        ? [
            tr,
            {
              changes: {
                from: 0,
                to: tr.newDoc.length,
                insert: tr.newDoc.toString().replace(/\r?\n/g, ""),
              },
              sequential: true,
            },
          ]
        : tr,
    ),
    placeholder("Topic, e.g. home/lamp/set"),
    autocompletion({
      override: [
        topicChannels(() => latest.current.hints),
        topicParams(() => latest.current.hints),
      ],
      icons: false,
    }),
    // An empty topic offers the spec's channels as soon as it's focused.
    EditorView.domEventHandlers({
      focus: (_, view) => {
        if (
          view.state.doc.length === 0 &&
          latest.current.hints.channels.length > 0
        )
          setTimeout(() => startCompletion(view));
        return false;
      },
    }),
    editorTheme,
  ]);
  return <div className="publish-field publish-field--topic" ref={host} />;
}

const jsonHighlight = HighlightStyle.define([
  { tag: tags.propertyName, color: "var(--text)" },
  { tag: tags.string, color: "var(--code-string)" },
  { tag: tags.number, color: "var(--code-string)" },
  { tag: [tags.bool, tags.null], color: "var(--code-variable)" },
  { tag: tags.punctuation, color: "var(--text-muted)" },
]);

/**
 * The payload: JSON is highlighted and checked, and with a channel's schemas, its keys and
 * values are suggested and what they don't allow is underlined. Any other text is sent as is.
 */
export function PayloadField({
  value,
  onChange,
  onSubmit,
  schemas,
}: {
  value: string;
  onChange: (value: string) => void;
  onSubmit: () => void;
  /** The matched channel's message schemas; empty without a spec. */
  schemas: unknown[];
}) {
  const host = useRef<HTMLDivElement>(null);
  const latest = useRef({ schemas, onSubmit });
  useEffect(() => {
    latest.current = { schemas, onSubmit };
  });
  useEditor(host, value, onChange, () => [
    submitKey(() => latest.current.onSubmit()),
    history(),
    drawSelection(),
    indentOnInput(),
    bracketMatching(),
    closeBrackets(),
    keymap.of([
      ...closeBracketsKeymap,
      ...completionKeymap,
      ...historyKeymap,
      ...defaultKeymap,
    ]),
    placeholder("Payload"),
    json(),
    syntaxHighlighting(jsonHighlight),
    autocompletion({
      override: [payloadCompletions(() => latest.current.schemas)],
      icons: false,
    }),
    linter(
      payloadLinter(() => latest.current.schemas),
      { delay: 300 },
    ),
    EditorView.lineWrapping,
    editorTheme,
  ]);
  return <div className="publish-field publish-field--payload" ref={host} />;
}
