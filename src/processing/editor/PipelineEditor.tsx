import {
  setDiagnostics,
  type Diagnostic as CmDiagnostic,
} from "@codemirror/lint";
import { Annotation, EditorState } from "@codemirror/state";
import { EditorView, keymap } from "@codemirror/view";
import { basicSetup } from "codemirror";
import { useEffect, useRef } from "react";
import type { Diagnostic } from "../api";
import { byteToIndex } from "../offsets";
import type { StageNote } from "../stageNotes";
import { pipeAutocomplete, pipeHover, type Hints } from "./assistExtension";
import { pipeLanguage, pipeTheme } from "./pipeLanguage";
import { setStageNotes, stageNotesField } from "./stageNotesField";

/** Marks text put in from outside, which isn't an edit to report back. */
const fromOutside = Annotation.define<boolean>();

interface Props {
  /** A new `docKey` loads `value` into a fresh document (switching Pipelines). */
  docKey: string;
  value: string;
  onChange: (value: string) => void;
  onSave: () => void;
  diagnostics: Diagnostic[];
  /** Preview's result after each Stage. */
  notes: StageNote[];
  /** Real data and names for autocomplete. */
  hints: Hints;
}

/** CodeMirror with mÅqtt's pipeline language; the parent owns the text. */
export function PipelineEditor({
  docKey,
  value,
  onChange,
  onSave,
  diagnostics,
  notes,
  hints,
}: Props) {
  const host = useRef<HTMLDivElement>(null);
  const view = useRef<EditorView | null>(null);
  // Callbacks change every render; the editor reads the latest through refs.
  const handlers = useRef({ onChange, onSave, hints });
  useEffect(() => {
    handlers.current = { onChange, onSave, hints };
  });

  useEffect(() => {
    const editor = new EditorView({
      parent: host.current!,
      state: EditorState.create({
        doc: value,
        extensions: [
          keymap.of([
            {
              key: "Mod-s",
              preventDefault: true,
              run: () => {
                handlers.current.onSave();
                return true;
              },
            },
          ]),
          basicSetup,
          pipeLanguage,
          pipeTheme,
          stageNotesField,
          pipeAutocomplete(() => handlers.current.hints),
          pipeHover,
          EditorView.lineWrapping,
          EditorView.updateListener.of((update) => {
            const outside = update.transactions.some((t) =>
              t.annotation(fromOutside),
            );
            if (update.docChanged && !outside) {
              handlers.current.onChange(update.state.doc.toString());
            }
          }),
        ],
      }),
    });
    view.current = editor;
    return () => {
      editor.destroy();
      view.current = null;
    };
    // Only a new document (another Pipeline) recreates the editor; typing doesn't.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [docKey]);

  // Text changed from outside (the file was edited elsewhere): take it in.
  useEffect(() => {
    const editor = view.current;
    if (editor && editor.state.doc.toString() !== value) {
      editor.dispatch({
        changes: { from: 0, to: editor.state.doc.length, insert: value },
        annotations: fromOutside.of(true),
      });
    }
  }, [value]);

  useEffect(() => {
    const editor = view.current;
    if (!editor) return;
    const text = editor.state.doc.toString();
    const marks: CmDiagnostic[] = diagnostics.map((d) => {
      const from = Math.min(byteToIndex(text, d.start), text.length);
      const to = Math.min(
        Math.max(byteToIndex(text, d.end), from),
        text.length,
      );
      return { from, to, severity: "error", message: d.message };
    });
    editor.dispatch(setDiagnostics(editor.state, marks));
  }, [diagnostics]);

  useEffect(() => {
    view.current?.dispatch({ effects: setStageNotes.of(notes) });
  }, [notes, docKey]);

  return <div className="pipeline-editor" ref={host} />;
}
