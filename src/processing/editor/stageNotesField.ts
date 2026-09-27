import { StateEffect, StateField, type Range } from "@codemirror/state";
import {
  Decoration,
  EditorView,
  WidgetType,
  type DecorationSet,
} from "@codemirror/view";
import type { StageNote } from "../stageNotes";

class NoteWidget extends WidgetType {
  constructor(readonly note: StageNote) {
    super();
  }

  eq(other: NoteWidget) {
    return (
      other.note.text === this.note.text && other.note.error === this.note.error
    );
  }

  toDOM() {
    const span = document.createElement("span");
    span.className = this.note.error
      ? "cm-stage-note cm-stage-note--error"
      : "cm-stage-note";
    span.textContent = this.note.text;
    span.title = this.note.title;
    return span;
  }

  ignoreEvent() {
    return false;
  }
}

/** Replaces the notes shown after each Stage. */
export const setStageNotes = StateEffect.define<StageNote[]>();

/** Preview's result for each Stage, shown at the end of its line and kept in place while typing. */
export const stageNotesField = StateField.define<DecorationSet>({
  create: () => Decoration.none,
  update(notes, tr) {
    notes = notes.map(tr.changes);
    for (const effect of tr.effects) {
      if (effect.is(setStageNotes)) {
        const length = tr.state.doc.length;
        const widgets: Range<Decoration>[] = effect.value
          .filter((note) => note.text !== "" && note.at <= length)
          .map((note) =>
            Decoration.widget({ widget: new NoteWidget(note), side: 1 }).range(
              note.at,
            ),
          );
        notes = Decoration.set(widgets, true);
      }
    }
    return notes;
  },
  provide: (field) => EditorView.decorations.from(field),
});
